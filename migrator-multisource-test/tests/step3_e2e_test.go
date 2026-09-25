//go:build step3

// Step 3 end-to-end test: sources A and B replicated into one destination by two migrators, each
// through its own ConsumerGroupPrefix proxy. All assertions run directly against the brokers,
// bypassing the proxies. Driven by `make step3` / `make step3-negative`:
//
//	TestStep3Seed            before the migrators start
//	TestStep3Replicated      migrators via proxies: topics, records, groups, offsets, schemas
//	TestStep3LiveSync        migrators via proxies: later changes propagate to the right side only
//	TestStep3NegativeControl migrators straight to redpanda-dest (no proxies)
package migratortest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type source struct {
	name, prefix    string // "A", "a_"
	addr, schemaReg string
	counts          map[string]int              // records per topic
	groupOffsets    map[string]map[int32]int64 // app-group commits on the source
	// trimBefore deletes the first records of a partition during seeding (DeleteRecords), so the
	// migrator starts copying from a later source offset and destination offsets end up lower
	// than source offsets by exactly that amount. Without it, both sides have identical offsets
	// and an untranslated offset would pass the checks just as well.
	trimBefore map[string]map[int32]int64
}

var (
	destAddr      = env("DEST_ADDR", "localhost:39092")
	destSchemaReg = env("DEST_SR", "http://localhost:38081")

	sources = []source{
		{
			name: "A", prefix: "a_",
			addr: env("SRC_A_ADDR", "localhost:19092"), schemaReg: env("SRC_A_SR", "http://localhost:18081"),
			counts:       map[string]int{"orders": 1000, "payments": 200},
			groupOffsets: map[string]map[int32]int64{"orders": {0: 500}},
			trimBefore:   map[string]map[int32]int64{"orders": {0: 200}},
		},
		{
			name: "B", prefix: "b_",
			addr: env("SRC_B_ADDR", "localhost:29092"), schemaReg: env("SRC_B_SR", "http://localhost:28081"),
			counts:       map[string]int{"orders": 700, "payments": 300},
			groupOffsets: map[string]map[int32]int64{"orders": {0: 250}, "payments": {0: 100}},
			trimBefore:   map[string]map[int32]int64{"orders": {0: 100}, "payments": {0: 50}},
		},
	}

	partitions = map[string]int32{"orders": 3, "payments": 1}
)

const (
	appGroup     = "app-group"
	syncInterval = 10 * time.Second // consumer_groups.interval in migrator/*.yaml
	provenance   = "x-source-cluster"
)

// partitionFor spreads orders 60/20/20 so partition 0 holds enough records for the spec's
// group positions (A orders/0=500 needs >= 500 records there).
func partitionFor(topic string, n int) int32 {
	if topic != "orders" {
		return 0
	}
	switch n % 5 {
	case 0, 1, 2:
		return 0
	case 3:
		return 1
	default:
		return 2
	}
}

func TestStep3Seed(t *testing.T) {
	ctx := testCtx(t)
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			cl, adm := newClient(t, src.addr)
			for topic, n := range partitions {
				ensureTopic(t, ctx, adm, topic, n)
			}
			if end := totalEnd(t, ctx, adm, "orders"); end > 0 {
				t.Skipf("source %s already seeded (%d orders)", src.name, end)
			}

			for topic, count := range src.counts {
				rs := make([]*kgo.Record, count)
				for n := range rs {
					rs[n] = &kgo.Record{Topic: topic, Partition: partitionFor(topic, n), Value: fmt.Appendf(nil, "%s-%s-%d", src.name, topic, n)}
				}
				if err := cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
					t.Fatalf("produce %s: %v", topic, err)
				}
			}

			var trim kadm.Offsets
			for topic, ps := range src.trimBefore {
				for p, before := range ps {
					trim.Add(kadm.Offset{Topic: topic, Partition: p, At: before})
				}
			}
			resp, err := adm.DeleteRecords(ctx, trim)
			if err == nil {
				err = resp.Error()
			}
			if err != nil {
				t.Fatalf("delete records %v: %v", trim, err)
			}
			for topic, ps := range src.trimBefore {
				for p, before := range ps {
					if got := startOffset(t, ctx, adm, topic, p); got != before {
						t.Fatalf("%s/%d start offset %d after DeleteRecords, want %d", topic, p, got, before)
					}
				}
			}

			// A plain admin commit leaves app-group Empty, as a stopped consumer would.
			for topic, ps := range src.groupOffsets {
				for p, at := range ps {
					commit(t, ctx, adm, appGroup, topic, p, at)
				}
			}

			// Different schema per source so the destination assigns different IDs (translate_ids).
			schema := fmt.Sprintf(`{"type":"record","name":"Order%s","fields":[{"name":"id","type":"string"}]}`, src.name)
			registerSchema(t, src.schemaReg, "orders-value", schema)
		})
	}
}

func TestStep3Replicated(t *testing.T) {
	ctx := testCtx(t)
	_, dest := newClient(t, destAddr)

	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			srcCl, srcAdm := newClient(t, src.addr)
			meta, err := srcAdm.Metadata(ctx)
			if err != nil {
				t.Fatal(err)
			}
			srcClusterID := meta.Cluster

			for topic := range partitions {
				dt := src.prefix + topic
				waitFor(t, 2*time.Minute, func() error {
					if got, want := recordCount(t, ctx, dest, dt), recordCount(t, ctx, srcAdm, topic); got != want {
						return fmt.Errorf("%s has %d records, want %d", dt, got, want)
					}
					return nil
				})

				dm, err := dest.Metadata(ctx, dt)
				if err != nil {
					t.Fatal(err)
				}
				if got := int32(len(dm.Topics[dt].Partitions)); got != partitions[topic] {
					t.Fatalf("%s has %d partitions, want %d", dt, got, partitions[topic])
				}

				// Same partition, same order, same value, and the provenance header of this source. The
				// destination offset is the source offset minus the records trimmed before seeding.
				srcRecs := readAll(t, ctx, srcCl, topic)
				destCl, _ := newClient(t, destAddr)
				destRecs := readAll(t, ctx, destCl, dt)
				for at, r := range srcRecs {
					want := tp{at.partition, at.offset - src.shift(topic, at.partition)}
					d, ok := destRecs[want]
					if !ok {
						t.Fatalf("%s: source record %v expected at destination %v, missing", dt, at, want)
					}
					if !bytes.Equal(d.Value, r.Value) || !strings.HasPrefix(string(d.Value), src.name+"-") {
						t.Fatalf("%s %v: value %q, want %q (source %v)", dt, want, d.Value, r.Value, at)
					}
					if h := header(d, provenance); h != srcClusterID {
						t.Fatalf("%s %v: %s=%q, want source cluster %q", dt, want, provenance, h, srcClusterID)
					}
				}
				if len(destRecs) != len(srcRecs) {
					t.Fatalf("%s: %d records, source %s has %d", dt, len(destRecs), topic, len(srcRecs))
				}
			}

			// Committed offsets translated: the destination position points at the same record.
			dg := src.prefix + appGroup
			for topic, ps := range src.groupOffsets {
				for p, at := range ps {
					dt := src.prefix + topic
					var got int64
					waitFor(t, 2*time.Minute, func() error {
						if got = committedOffset(t, ctx, dest, dg, dt, p); got < 0 {
							return fmt.Errorf("%s has no offset for %s/%d yet", dg, dt, p)
						}
						return nil
					})
					assertSamePosition(t, ctx, srcCl, destAddr, topic, dt, p, at, got)
					// With a trimmed source partition, an untranslated offset would be wrong.
					if shift := src.shift(topic, p); got != at-shift || (shift > 0 && got == at) {
						t.Fatalf("%s %s/%d: destination offset %d, want %d (source %d minus %d trimmed)", dg, dt, p, got, at-shift, at, shift)
					}
				}
			}
		})
	}

	t.Run("no unprefixed topics or groups", func(t *testing.T) {
		topics, err := dest.ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"orders", "payments"} {
			if topics.Has(bad) {
				t.Errorf("destination has unprefixed topic %q", bad)
			}
		}
		groups := listGroupNames(t, ctx, dest)
		for _, want := range []string{"a_app-group", "b_app-group"} {
			if !groups[want] {
				t.Errorf("destination is missing group %q (have %v)", want, keys(groups))
			}
		}
		for _, bad := range []string{"app-group", "migrator", "a_migrator", "b_migrator"} {
			if groups[bad] {
				t.Errorf("destination has group %q", bad)
			}
		}
	})

	t.Run("schema subjects prefixed", func(t *testing.T) {
		subjects := listSubjects(t, destSchemaReg)
		for _, want := range []string{"a_orders-value", "b_orders-value"} {
			if !subjects[want] {
				t.Errorf("destination schema registry is missing %q (have %v)", want, keys(subjects))
			}
		}
		if subjects["orders-value"] {
			t.Error("destination schema registry has unprefixed orders-value")
		}
	})
}

func TestStep3LiveSync(t *testing.T) {
	ctx := testCtx(t)
	_, dest := newClient(t, destAddr)
	a, b := sources[0], sources[1]
	aCl, aAdm := newClient(t, a.addr)
	bCl, _ := newClient(t, b.addr)

	bBefore := groupOffsets(t, ctx, dest, "b_app-group")
	aBefore := committedOffset(t, ctx, dest, "a_app-group", "a_orders", 0)

	// New records on both sources, then move A's app-group forward onto them.
	const extra = 20
	produceLive(t, ctx, aCl, a.name, "orders", 0, extra)
	produceLive(t, ctx, bCl, b.name, "payments", 0, extra)
	aEnd := endOffset(t, ctx, aAdm, "orders", 0)
	aShift := a.shift("orders", 0)
	newAt := aEnd - extra/2
	commit(t, ctx, aAdm, appGroup, "orders", 0, newAt)
	start := time.Now()

	waitFor(t, 2*time.Minute, func() error {
		if got, want := endOffset(t, ctx, dest, "a_orders", 0), aEnd-aShift; got != want {
			return fmt.Errorf("a_orders/0 end %d, want %d", got, want)
		}
		if got := committedOffset(t, ctx, dest, "a_app-group", "a_orders", 0); got == aBefore {
			return fmt.Errorf("a_app-group a_orders/0 still at %d", got)
		}
		return nil
	})
	got := committedOffset(t, ctx, dest, "a_app-group", "a_orders", 0)
	t.Logf("a_app-group a_orders/0: %d -> %d after %s (source moved to %d)", aBefore, got, time.Since(start).Round(time.Second), newAt)
	assertSamePosition(t, ctx, aCl, destAddr, "orders", "a_orders", 0, newAt, got)
	if got != newAt-aShift {
		t.Fatalf("a_app-group a_orders/0 = %d, want %d (source %d minus %d trimmed)", got, newAt-aShift, newAt, aShift)
	}

	// New B records arrive under b_, and B's group is untouched by A's move.
	waitFor(t, time.Minute, func() error {
		destCl, _ := newClient(t, destAddr)
		recs := readAll(t, ctx, destCl, "b_payments")
		for i := 0; i < extra; i++ {
			if !hasValue(recs, fmt.Sprintf("B-payments-live-%d", i)) {
				return fmt.Errorf("b_payments is missing B-payments-live-%d", i)
			}
		}
		return nil
	})
	if wait := 2*syncInterval + 5*time.Second - time.Since(start); wait > 0 {
		time.Sleep(wait) // give migrator-b at least two sync cycles to (wrongly) react
	}
	if bAfter := groupOffsets(t, ctx, dest, "b_app-group"); fmt.Sprint(bAfter) != fmt.Sprint(bBefore) {
		t.Fatalf("b_app-group changed: %v -> %v", bBefore, bAfter)
	}
}

// TestStep3NegativeControl runs with both migrators writing straight to redpanda-dest. It records
// what happens to the shared group name rather than asserting a particular failure; see
// docs/findings.md for the prediction from Step 1.
func TestStep3NegativeControl(t *testing.T) {
	ctx := testCtx(t)
	_, dest := newClient(t, destAddr)

	want := map[string]int32{"a_orders": 0, "b_orders": 0, "b_payments": 0}
	waitFor(t, 2*time.Minute, func() error {
		for topic, p := range want {
			if committedOffset(t, ctx, dest, appGroup, topic, p) < 0 {
				return fmt.Errorf("app-group has no offset for %s/%d yet", topic, p)
			}
		}
		return nil
	})
	offsets := groupOffsets(t, ctx, dest, appGroup)
	t.Logf("OBSERVED destination app-group (shared by both migrators): %v", offsets)
	groups := listGroupNames(t, ctx, dest)
	t.Logf("OBSERVED destination groups: %v", keys(groups))
	if groups["a_app-group"] || groups["b_app-group"] {
		t.Fatal("prefixed groups exist without the proxy; is the negative control really bypassing it?")
	}
	for _, src := range sources {
		srcCl, _ := newClient(t, src.addr)
		for topic, ps := range src.groupOffsets {
			for p, at := range ps {
				assertSamePosition(t, ctx, srcCl, destAddr, topic, src.prefix+topic, p, at, offsets[fmt.Sprintf("%s/%d", src.prefix+topic, p)])
			}
		}
	}

	// Variant: an application consumes a_orders on the destination under app-group, as it would
	// after cutover, so the group is Stable. Then source A's app-group moves.
	t.Run("active consumer on destination app-group", func(t *testing.T) {
		consumer, _ := newClient(t, destAddr, kgo.ConsumerGroup(appGroup), kgo.ConsumeTopics("a_orders"), kgo.DisableAutoCommit())
		pollCtx, stop := context.WithCancel(ctx)
		defer stop()
		go func() {
			for pollCtx.Err() == nil {
				consumer.PollFetches(pollCtx)
			}
		}()
		waitFor(t, time.Minute, func() error {
			dg, err := dest.DescribeGroups(ctx, appGroup)
			if err != nil {
				return err
			}
			if st := dg[appGroup].State; st != "Stable" {
				return fmt.Errorf("app-group state %q", st)
			}
			return nil
		})

		a := sources[0]
		aCl, aAdm := newClient(t, a.addr)
		produceLive(t, ctx, aCl, a.name, "orders", 0, 10)
		before := committedOffset(t, ctx, dest, appGroup, "a_orders", 0)
		newAt := endOffset(t, ctx, aAdm, "orders", 0) - 5
		commit(t, ctx, aAdm, appGroup, "orders", 0, newAt)
		time.Sleep(3 * syncInterval)
		after := committedOffset(t, ctx, dest, appGroup, "a_orders", 0)
		t.Logf("OBSERVED with Stable destination group: source A moved app-group orders/0 to %d; destination app-group a_orders/0 %d -> %d", newAt, before, after)
	})
}

// ---- helpers ----

// assertSamePosition checks that the destination committed offset dstAt for dstTopic/p points at
// the same record as source offset srcAt for srcTopic/p: the records just before (last consumed)
// and at (next to consume) the position must match.
func assertSamePosition(t *testing.T, ctx context.Context, srcCl *kgo.Client, destAddr, srcTopic, dstTopic string, p int32, srcAt, dstAt int64) {
	t.Helper()
	destCl, _ := newClient(t, destAddr)
	src := readAll(t, ctx, srcCl, srcTopic)
	dst := readAll(t, ctx, destCl, dstTopic)
	t.Logf("%s/%d source offset %d -> %s/%d destination offset %d", srcTopic, p, srcAt, dstTopic, p, dstAt)
	for _, d := range []int64{-1, 0} {
		s, sok := src[tp{p, srcAt + d}]
		x, xok := dst[tp{p, dstAt + d}]
		if sok != xok || (sok && !bytes.Equal(s.Value, x.Value)) {
			t.Fatalf("%s/%d position mismatch at delta %d: source@%d=%q destination@%d=%q", dstTopic, p, d, srcAt+d, value(s), dstAt+d, value(x))
		}
	}
}

type tp struct {
	partition int32
	offset    int64
}

// readAll reads every record of topic, keyed by partition/offset.
func readAll(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) map[tp]*kgo.Record {
	t.Helper()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) { want[o.Partition] = o.Offset })

	offsets := map[string]map[int32]kgo.Offset{topic: {}}
	for p := range want {
		offsets[topic][p] = kgo.NewOffset().AtStart()
	}
	cl.AddConsumePartitions(offsets)
	defer cl.RemoveConsumePartitions(map[string][]int32{topic: keysInt32(want)})

	out := map[tp]*kgo.Record{}
	done := func() bool {
		for p, end := range want {
			if end > 0 {
				if _, ok := out[tp{p, end - 1}]; !ok {
					return false
				}
			}
		}
		return true
	}
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for !done() {
		fs := cl.PollFetches(readCtx)
		if readCtx.Err() != nil {
			t.Fatalf("reading %s: timed out with %d records", topic, len(out))
		}
		fs.EachRecord(func(r *kgo.Record) { out[tp{r.Partition, r.Offset}] = r })
	}
	return out
}

func groupOffsets(t *testing.T, ctx context.Context, adm *kadm.Client, group string) map[string]int64 {
	t.Helper()
	os, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	os.Each(func(o kadm.OffsetResponse) { out[fmt.Sprintf("%s/%d", o.Topic, o.Partition)] = o.At })
	return out
}

// shift is how far destination offsets trail source offsets for topic/p.
func (s source) shift(topic string, p int32) int64 {
	return s.trimBefore[topic][p]
}

// recordCount is the number of records currently in topic (end minus start, summed over partitions).
func recordCount(t *testing.T, ctx context.Context, adm *kadm.Client, topic string) int64 {
	t.Helper()
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	n := totalEnd(t, ctx, adm, topic)
	starts.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			n -= o.Offset
		}
	})
	return n
}

func startOffset(t *testing.T, ctx context.Context, adm *kadm.Client, topic string, p int32) int64 {
	t.Helper()
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := starts.Lookup(topic, p)
	if !ok || o.Err != nil {
		return -1
	}
	return o.Offset
}

func totalEnd(t *testing.T, ctx context.Context, adm *kadm.Client, topic string) int64 {
	t.Helper()
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			n += o.Offset
		}
	})
	return n
}

func endOffset(t *testing.T, ctx context.Context, adm *kadm.Client, topic string, p int32) int64 {
	t.Helper()
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := ends.Lookup(topic, p)
	if !ok || o.Err != nil {
		return -1
	}
	return o.Offset
}

func produceLive(t *testing.T, ctx context.Context, cl *kgo.Client, name, topic string, p int32, n int) {
	t.Helper()
	rs := make([]*kgo.Record, n)
	for i := range rs {
		rs[i] = &kgo.Record{Topic: topic, Partition: p, Value: fmt.Appendf(nil, "%s-%s-live-%d", name, topic, i)}
	}
	if err := cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatalf("produce live %s: %v", topic, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %s: %v", timeout, err)
		}
		time.Sleep(2 * time.Second)
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func hasValue(recs map[tp]*kgo.Record, v string) bool {
	for _, r := range recs {
		if string(r.Value) == v {
			return true
		}
	}
	return false
}

func value(r *kgo.Record) string {
	if r == nil {
		return "<none>"
	}
	return string(r.Value)
}

func keysInt32[V any](m map[int32]V) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func registerSchema(t *testing.T, url, subject, schema string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"schema": schema})
	resp, err := http.Post(url+"/subjects/"+subject+"/versions", "application/vnd.schemaregistry.v1+json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register %s: %v", subject, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("register %s: %s %s", subject, resp.Status, b)
	}
}

func listSubjects(t *testing.T, url string) map[string]bool {
	t.Helper()
	resp, err := http.Get(url + "/subjects")
	if err != nil {
		t.Fatalf("list subjects: %v", err)
	}
	defer resp.Body.Close()
	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		t.Fatalf("list subjects: %v", err)
	}
	out := map[string]bool{}
	for _, s := range subjects {
		out[s] = true
	}
	return out
}

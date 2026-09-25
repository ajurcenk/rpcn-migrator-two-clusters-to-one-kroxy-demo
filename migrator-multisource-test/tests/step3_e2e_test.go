//go:build step3

// Step 3 end-to-end test: sources A and B replicated into one destination by two migrators, each
// through its own ConsumerGroupPrefix proxy. All assertions run directly against the brokers,
// bypassing the proxies. Driven by `make step3` / `make step3-negative`:
//
//	TestStep3Seed            before the migrators start
//	TestStep3Replicated      migrators via proxies: topics, records, groups, offsets, schemas
//	TestStep3LiveSync        migrators via proxies: later changes propagate to the right side only
//	TestStep3ActiveConsumer  migrators via proxies: groups with live members (timestamp translation)
//	TestStep3KnownIssue...   expected to fail with Connect 4.100.0; `make step3-known-issues` only
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
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
			counts:       map[string]int{"orders": 1000, "payments": 200, "events": 300},
			groupOffsets: map[string]map[int32]int64{"orders": {0: 500}},
			trimBefore:   map[string]map[int32]int64{"orders": {0: 200}, "events": {0: 50}},
		},
		{
			name: "B", prefix: "b_",
			addr: env("SRC_B_ADDR", "localhost:29092"), schemaReg: env("SRC_B_SR", "http://localhost:28081"),
			counts:       map[string]int{"orders": 700, "payments": 300, "events": 300},
			groupOffsets: map[string]map[int32]int64{"orders": {0: 250}, "payments": {0: 100}},
			trimBefore:   map[string]map[int32]int64{"orders": {0: 100}, "payments": {0: 50}, "events": {0: 50}},
		},
	}

	// orders and payments are produced in bulk, so many records share a millisecond timestamp.
	// events records are 1 ms apart, as with a steady real-world producer.
	partitions = map[string]int32{"orders": 3, "payments": 1, "events": 1}
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
				base := time.Now().Add(-time.Duration(count) * time.Millisecond)
				for n := range rs {
					rs[n] = &kgo.Record{Topic: topic, Partition: partitionFor(topic, n), Value: fmt.Appendf(nil, "%s-%s-%d", src.name, topic, n)}
					if topic == "events" {
						rs[n].Timestamp = base.Add(time.Duration(n) * time.Millisecond)
					}
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

// TestStep3ActiveConsumer checks translation for source groups that have live members. The
// migrator only uses exact (offset_header) translation for Empty groups (migrator_groups.go:444);
// a Stable group gets timestamp-only translation (translateOffset). Both sources run members of
// groups with the same names, which also checks the prefixes keep them apart while members exist.
func TestStep3ActiveConsumer(t *testing.T) {
	// Several sync cycles per source and case; longer than testCtx's default.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	_, dest := newClient(t, destAddr)

	// Case 1: records 1 ms apart. Timestamp translation should be exact.
	t.Run("distinct timestamps", func(t *testing.T) {
		positions := map[string][]int64{"A": {200, 250}, "B": {120, 180}}
		for _, src := range sources {
			t.Run(src.name, func(t *testing.T) {
				srcCl, srcAdm := newClient(t, src.addr)
				member, _ := startMember(t, ctx, src.addr, srcAdm, "live-group", "events")
				dg, dt, shift := src.prefix+"live-group", src.prefix+"events", src.shift("events", 0)
				prev := int64(-1)
				for _, at := range positions[src.name] {
					commitAsMember(t, ctx, member, "events", 0, at)
					got := waitForChange(t, ctx, dest, dg, dt, 0, prev)
					prev = got
					requireStable(t, ctx, srcAdm, "live-group")
					t.Logf("Stable source live-group events/0=%d -> %s %s/0=%d (exact %d)", at, dg, dt, got, at-shift)
					if got != at-shift {
						explainTimestampTranslation(t, ctx, srcCl, "events", dt, 0, at, got)
						t.Fatalf("%s %s/0 = %d, want %d (source %d minus %d trimmed)", dg, dt, got, at-shift, at, shift)
					}
					assertSamePosition(t, ctx, srcCl, destAddr, "events", dt, 0, at, got)
				}
			})
		}
	})

	// Case 2: bulk-produced records sharing a timestamp. Translation can't be exact while the
	// group is Stable; it must never be ahead (that would skip records) and must land in the run
	// of records sharing the timestamp. What happens after the consumer stops is covered by
	// TestStep3KnownIssueCorrectionAfterStop.
	t.Run("shared timestamps", func(t *testing.T) {
		for _, src := range sources {
			t.Run(src.name, func(t *testing.T) {
				_, _, _ = sharedTimestampStable(t, ctx, dest, src, "batch-group")
			})
		}
	})

	if groups := listGroupNames(t, ctx, dest); groups["live-group"] || groups["batch-group"] {
		t.Fatalf("destination has unprefixed live-group or batch-group: %v", keys(groups))
	}
}

// TestStep3KnownIssueCorrectionAfterStop is EXPECTED TO FAIL with Redpanda Connect 4.100.0 and
// runs only via `make step3-known-issues`, not `make step3`. It states the requirement that once
// a source group's consumers stop (the group goes Empty), the migrator corrects the imprecise
// timestamp-based destination offset to the exact one. It doesn't: the migrator skips source
// offsets it has already translated (migrator_groups.go:378), so without another commit on the
// source the offset is never re-translated. See docs/findings.md.
func TestStep3KnownIssueCorrectionAfterStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	_, dest := newClient(t, destAddr)
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			srcCl, srcAdm := newClient(t, src.addr)
			stop, at, exact := sharedTimestampStable(t, ctx, dest, src, "stop-group")
			dg, dt := src.prefix+"stop-group", src.prefix+"orders"

			stop()
			waitFor(t, time.Minute, func() error {
				if st := groupState(t, ctx, srcAdm, "stop-group"); st != "Empty" {
					return fmt.Errorf("source stop-group is %q", st)
				}
				return nil
			})
			var final int64
			waitFor(t, 4*syncInterval, func() error {
				if final = committedOffset(t, ctx, dest, dg, dt, 0); final != exact {
					return fmt.Errorf("%s %s/0 = %d after the consumer stopped, want exact %d", dg, dt, final, exact)
				}
				return nil
			})
			assertSamePosition(t, ctx, srcCl, destAddr, "orders", dt, 0, at, final)
		})
	}
}

// sharedTimestampStable runs a Stable member of group on the source's bulk-produced orders
// topic, commits orders/0 through it, and checks the translated destination offset is never
// ahead of the exact one and lies within the run of records sharing the looked-up timestamp.
// It returns the member's stop function, the source offset and the exact destination offset.
func sharedTimestampStable(t *testing.T, ctx context.Context, dest *kadm.Client, src source, group string) (stop func(), at, exact int64) {
	t.Helper()
	positions := map[string]int64{"A": 450, "B": 300}
	srcCl, srcAdm := newClient(t, src.addr)
	member, stop := startMember(t, ctx, src.addr, srcAdm, group, "orders")
	dg, dt, at := src.prefix+group, src.prefix+"orders", positions[src.name]
	exact = at - src.shift("orders", 0)

	commitAsMember(t, ctx, member, "orders", 0, at)
	got := waitForChange(t, ctx, dest, dg, dt, 0, -1)
	requireStable(t, ctx, srcAdm, group)
	first, last := sharedTimestampRun(t, ctx, srcCl, "orders", dt, 0, at)
	t.Logf("Stable source %s orders/0=%d -> %s %s/0=%d (exact %d; records sharing the timestamp: %d-%d)", group, at, dg, dt, got, exact, first, last)
	if got > exact {
		t.Fatalf("%s %s/0 = %d is ahead of the exact position %d: a consumer would skip records", dg, dt, got, exact)
	}
	if got < first || got > last+1 {
		t.Fatalf("%s %s/0 = %d is outside the shared-timestamp run %d-%d", dg, dt, got, first, last)
	}
	return stop, at, exact
}

func waitForChange(t *testing.T, ctx context.Context, dest *kadm.Client, group, topic string, p int32, prev int64) int64 {
	t.Helper()
	var got int64
	waitFor(t, 2*time.Minute, func() error {
		if got = committedOffset(t, ctx, dest, group, topic, p); got == prev {
			return fmt.Errorf("%s %s/%d still at %d", group, topic, p, got)
		}
		return nil
	})
	return got
}

func requireStable(t *testing.T, ctx context.Context, adm *kadm.Client, group string) {
	t.Helper()
	if st := groupState(t, ctx, adm, group); st != "Stable" {
		t.Fatalf("source %s is %q, not Stable: the migrator may have seen an Empty group", group, st)
	}
}

// startMember joins group on the source with a real consumer that keeps polling (without
// committing on its own), and waits for the group to become Stable. stop closes the consumer,
// which leaves the group; it also runs at test cleanup.
func startMember(t *testing.T, ctx context.Context, addr string, adm *kadm.Client, group, topic string) (*kgo.Client, func()) {
	t.Helper()
	member, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	pollCtx, cancel := context.WithCancel(ctx)
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for pollCtx.Err() == nil {
			member.PollFetches(pollCtx)
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-polled
			member.Close()
		})
	}
	t.Cleanup(stop)
	waitFor(t, time.Minute, func() error {
		if st := groupState(t, ctx, adm, group); st != "Stable" {
			return fmt.Errorf("%s is %q", group, st)
		}
		return nil
	})
	return member, stop
}

// commitAsMember commits through the member's own group session (its generation), as a real
// application does, so the group stays Stable.
func commitAsMember(t *testing.T, ctx context.Context, member *kgo.Client, topic string, p int32, at int64) {
	t.Helper()
	var commitErr error
	member.CommitOffsetsSync(ctx, map[string]map[int32]kgo.EpochOffset{topic: {p: {Epoch: -1, Offset: at}}},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, rt := range resp.Topics {
				for _, rp := range rt.Partitions {
					if e := kerr.ErrorForCode(rp.ErrorCode); e != nil {
						commitErr = e
					}
				}
			}
		})
	if commitErr != nil {
		t.Fatalf("member commit %s/%d=%d: %v", topic, p, at, commitErr)
	}
}

func groupState(t *testing.T, ctx context.Context, adm *kadm.Client, group string) string {
	t.Helper()
	dg, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	return dg[group].State
}

// sharedTimestampRun returns the destination offsets whose records share the timestamp of the
// source record just before srcAt (the timestamp translateOffset looks up).
func sharedTimestampRun(t *testing.T, ctx context.Context, srcCl *kgo.Client, srcTopic, dstTopic string, p int32, srcAt int64) (first, last int64) {
	t.Helper()
	destCl, _ := newClient(t, destAddr)
	src := readAll(t, ctx, srcCl, srcTopic)
	dst := readAll(t, ctx, destCl, dstTopic)
	prev, ok := src[tp{p, srcAt - 1}]
	if !ok {
		t.Fatalf("source %s/%d has no record at %d", srcTopic, p, srcAt-1)
	}
	first, last = -1, -1
	for k, r := range dst {
		if k.partition == p && r.Timestamp.Equal(prev.Timestamp) {
			if first < 0 || k.offset < first {
				first = k.offset
			}
			if k.offset > last {
				last = k.offset
			}
		}
	}
	return first, last
}

// explainTimestampTranslation logs what timestamp translation had to work with.
func explainTimestampTranslation(t *testing.T, ctx context.Context, srcCl *kgo.Client, srcTopic, dstTopic string, p int32, srcAt, dstAt int64) {
	t.Helper()
	first, last := sharedTimestampRun(t, ctx, srcCl, srcTopic, dstTopic, p, srcAt)
	t.Logf("timestamp translation: destination records sharing the timestamp of source %s/%d@%d: offsets %d-%d; migrator chose %d",
		srcTopic, p, srcAt-1, first, last, dstAt)
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

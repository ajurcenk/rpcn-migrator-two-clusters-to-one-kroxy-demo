//go:build step2

// Step 2 integration test: the ConsumerGroupPrefix filter (prefix "a_") in front of a single
// Redpanda. Run with `make step2`, or against an already running stack with
//
//	go test -tags step2 -run TestStep2 -v ./...
package migratortest

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

var (
	proxyAddr  = env("PROXY_ADDR", "localhost:9192")
	directAddr = env("DIRECT_ADDR", "localhost:19092")
	metricsURL = env("PROXY_METRICS_URL", "http://localhost:9190/metrics")
)

const (
	prefix  = "a_"
	topic   = "orders"
	records = 100
)

// TestStep2Proxy walks through the spec's Step 2 sequence. Subtests depend on each other.
func TestStep2Proxy(t *testing.T) {
	ctx := testCtx(t)
	proxyCl, proxy := newClient(t, proxyAddr)
	_, direct := newClient(t, directAddr)

	deleteGroups(t, ctx, direct, "app-group", "a_app-group", "other", "a_other", "a_x", "a_a_x", "b2", "a_b2")
	logNegotiatedVersions(t, ctx, proxy)

	var produceBase int64
	t.Run("1 create topic and produce via proxy", func(t *testing.T) {
		ensureTopic(t, ctx, proxy, topic, 3)
		end, err := direct.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		o, _ := end.Lookup(topic, 0)
		produceBase = o.Offset

		rs := make([]*kgo.Record, records)
		for i := range rs {
			rs[i] = &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "A-orders-%d", i)}
		}
		if err := proxyCl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
			t.Fatalf("produce via proxy: %v", err)
		}
		end, err = direct.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if o, _ := end.Lookup(topic, 0); o.Offset != produceBase+records {
			t.Fatalf("end offset = %d, want %d", o.Offset, produceBase+records)
		}
	})

	// Created directly on the broker, so it has no prefix and must stay invisible via the proxy.
	commit(t, ctx, direct, "other", topic, 0, 7)

	commitAt := produceBase + 42
	t.Run("2 commit app-group via proxy", func(t *testing.T) {
		commit(t, ctx, proxy, "app-group", topic, 0, commitAt)
	})

	t.Run("3 backend has a_app-group and no app-group", func(t *testing.T) {
		names := listGroupNames(t, ctx, direct)
		if !names["a_app-group"] || names["app-group"] {
			t.Fatalf("direct groups = %v, want a_app-group present and app-group absent", keys(names))
		}
		if got := committedOffset(t, ctx, direct, "a_app-group", topic, 0); got != commitAt {
			t.Fatalf("direct a_app-group offset = %d, want %d", got, commitAt)
		}
		if got := committedOffset(t, ctx, direct, "app-group", topic, 0); got != -1 {
			t.Fatalf("direct app-group has offset %d, want none", got)
		}
	})

	t.Run("4 FetchOffsets via proxy", func(t *testing.T) {
		if got := committedOffset(t, ctx, proxy, "app-group", topic, 0); got != commitAt {
			t.Fatalf("proxy app-group offset = %d, want %d", got, commitAt)
		}
	})

	// The migrator's exact call: one entry per group partition, so duplicates (migrator_groups.go:516).
	// More than one entry forces OffsetFetch v8+ batching; a missing strip would silently miss here.
	t.Run("4b FetchManyOffsets migrator shape via proxy", func(t *testing.T) {
		commit(t, ctx, proxy, "b2", topic, 1, 0)
		assertFetchMany(t, proxy, map[string]int64{"app-group": commitAt, "b2": 0}, "app-group", "app-group", "b2")
	})

	t.Run("5 DescribeGroups via proxy", func(t *testing.T) {
		dg, err := proxy.DescribeGroups(ctx, "app-group")
		if err != nil {
			t.Fatal(err)
		}
		g, ok := dg["app-group"]
		if !ok || len(dg) != 1 {
			t.Fatalf("describe returned %v, want exactly app-group", keysOf(dg))
		}
		if g.Err != nil || g.State != "Empty" {
			t.Fatalf("app-group: err=%v state=%q, want no error and Empty", g.Err, g.State)
		}
	})

	t.Run("6 ListGroups via proxy hides unprefixed groups", func(t *testing.T) {
		names := listGroupNames(t, ctx, proxy)
		if !names["app-group"] || names["other"] || names["a_app-group"] {
			t.Fatalf("proxy groups = %v, want app-group, not other or a_app-group", keys(names))
		}
	})

	t.Run("6b group already carrying the prefix", func(t *testing.T) {
		commit(t, ctx, proxy, "a_x", topic, 0, commitAt)
		if !listGroupNames(t, ctx, direct)["a_a_x"] {
			t.Fatal("direct: a_a_x missing")
		}
		if !listGroupNames(t, ctx, proxy)["a_x"] {
			t.Fatal("proxy: a_x missing")
		}
		if got := committedOffset(t, ctx, proxy, "a_x", topic, 0); got != commitAt {
			t.Fatalf("proxy a_x offset = %d, want %d", got, commitAt)
		}
	})

	// Redpanda drops an Empty group as soon as its last committed offset is deleted (same without
	// the proxy), so keep a second partition committed until DeleteGroups.
	t.Run("7 DeleteOffsets and DeleteGroups via proxy only affect a_app-group", func(t *testing.T) {
		commit(t, ctx, proxy, "app-group", topic, 1, 1)
		resp, err := proxy.DeleteOffsets(ctx, "app-group", kadm.TopicsSet{topic: {0: {}}})
		if err == nil {
			err = resp.Error()
		}
		if err != nil {
			t.Fatalf("delete offsets: %v", err)
		}
		if got := committedOffset(t, ctx, direct, "a_app-group", topic, 0); got != -1 {
			t.Fatalf("direct a_app-group %s/0 still has offset %d", topic, got)
		}
		if got := committedOffset(t, ctx, direct, "a_app-group", topic, 1); got != 1 {
			t.Fatalf("direct a_app-group %s/1 offset = %d, want 1 (untouched)", topic, got)
		}

		dr, err := proxy.DeleteGroups(ctx, "app-group")
		if err != nil {
			t.Fatal(err)
		}
		if r, ok := dr["app-group"]; !ok || r.Err != nil {
			t.Fatalf("delete app-group: result %+v (present %v)", r, ok)
		}
		names := listGroupNames(t, ctx, direct)
		if names["a_app-group"] || !names["other"] {
			t.Fatalf("direct groups = %v, want a_app-group gone and other kept", keys(names))
		}
		if got := committedOffset(t, ctx, direct, "other", topic, 0); got != 7 {
			t.Fatalf("direct other offset = %d, want 7", got)
		}
	})

	t.Run("8 non-group APIs behave identically", func(t *testing.T) {
		pm, err := proxy.Metadata(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		dm, err := direct.Metadata(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if p, d := len(pm.Topics[topic].Partitions), len(dm.Topics[topic].Partitions); p != d || p != 3 {
			t.Fatalf("partitions via proxy=%d direct=%d, want 3", p, d)
		}
		if pm.Topics[topic].ID != dm.Topics[topic].ID {
			t.Fatalf("topic ID via proxy %v != direct %v", pm.Topics[topic].ID, dm.Topics[topic].ID)
		}
		pe, err := proxy.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		de, err := direct.ListEndOffsets(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		for p := int32(0); p < 3; p++ {
			po, _ := pe.Lookup(topic, p)
			do, _ := de.Lookup(topic, p)
			if po.Offset != do.Offset {
				t.Fatalf("end offset p%d via proxy=%d direct=%d", p, po.Offset, do.Offset)
			}
		}

		// Read back what was produced through the proxy, through the proxy.
		consumer, _ := newClient(t, proxyAddr, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(produceBase)},
		}))
		var got int
		for got < records {
			fs := consumer.PollFetches(ctx)
			if err := fs.Err0(); err != nil {
				t.Fatalf("consume via proxy: %v", err)
			}
			fs.EachRecord(func(r *kgo.Record) {
				if want := fmt.Sprintf("A-orders-%d", r.Offset-produceBase); string(r.Value) != want {
					t.Fatalf("offset %d value %q, want %q", r.Offset, r.Value, want)
				}
				got++
			})
		}
	})

	t.Run("9 rewrite counters exported", func(t *testing.T) {
		counts := scrapeRewriteCounters(t)
		for _, want := range []string{
			"FIND_COORDINATOR/request", "FIND_COORDINATOR/response",
			"OFFSET_FETCH/request", "OFFSET_FETCH/response",
			"OFFSET_COMMIT/request",
			"DESCRIBE_GROUPS/request", "DESCRIBE_GROUPS/response",
			"LIST_GROUPS/response",
			"OFFSET_DELETE/request",
			"DELETE_GROUPS/request", "DELETE_GROUPS/response",
		} {
			if counts[want] <= 0 {
				t.Errorf("no rewrites counted for %s (have %v)", want, counts)
			}
		}
	})
}

// TestStep2OlderVersions pins franz-go to older group API versions to exercise the
// pre-batching FindCoordinator (v0-3) and OffsetFetch (v0-7) paths, which Redpanda would otherwise
// never see from this client.
func TestStep2OlderVersions(t *testing.T) {
	variants := []struct {
		name string
		max  map[int16]int16
	}{
		// Last versions before batching: FindCoordinator v3 (single Key), OffsetFetch v7 (single GroupId).
		{"pre-batching", map[int16]int16{10: 3, 9: 7}},
		// The oldest versions usable end to end. The 0.24.0 proxy decodes OffsetCommit >= v2 and
		// OffsetFetch >= v1, but Redpanda returns no offsets for OffsetFetch v1 (also without the proxy).
		{"oldest", map[int16]int16{10: 0, 9: 2, 8: 2, 15: 0, 16: 0, 42: 0}},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			ctx := testCtx(t)
			versions := kversion.Stable()
			for key, max := range v.max {
				versions.SetMaxKeyVersion(key, max)
			}
			_, proxy := newClient(t, proxyAddr, kgo.MaxVersions(versions))
			_, direct := newClient(t, directAddr)
			ensureTopic(t, ctx, direct, topic, 3)

			g1, g2 := "legacy-"+v.name, "legacy2-"+v.name
			deleteGroups(t, ctx, direct, prefix+g1, prefix+g2)

			commit(t, ctx, proxy, g1, topic, 0, 3)
			commit(t, ctx, proxy, g2, topic, 2, 5)

			if got := committedOffset(t, ctx, direct, prefix+g1, topic, 0); got != 3 {
				t.Fatalf("direct %s%s offset = %d, want 3", prefix, g1, got)
			}
			if got := committedOffset(t, ctx, proxy, g1, topic, 0); got != 3 {
				t.Fatalf("proxy %s offset = %d, want 3", g1, got)
			}
			// Several groups with batching unavailable: franz-go splits into one request per group.
			assertFetchMany(t, proxy, map[string]int64{g1: 3, g2: 5}, g1, g2, g1)

			dg, err := proxy.DescribeGroups(ctx, g1)
			if err != nil || dg[g1].Err != nil || dg[g1].Group != g1 {
				t.Fatalf("describe %s: err=%v result=%+v", g1, err, dg[g1])
			}
			names := listGroupNames(t, ctx, proxy)
			if !names[g1] || !names[g2] {
				t.Fatalf("proxy groups = %v, want %s and %s", keys(names), g1, g2)
			}
			dr, err := proxy.DeleteGroups(ctx, g1, g2)
			if err != nil || dr[g1].Err != nil || dr[g2].Err != nil {
				t.Fatalf("delete: err=%v results=%+v", err, dr)
			}
			if names := listGroupNames(t, ctx, direct); names[prefix+g1] || names[prefix+g2] {
				t.Fatalf("direct groups still contain %s%s or %s%s: %v", prefix, g1, prefix, g2, keys(names))
			}
		})
	}
}

// assertFetchMany checks FetchManyOffsets results by the unprefixed group names, the way the
// migrator reads them (dstOffsets[g], migrator_groups.go:532).
func assertFetchMany(t *testing.T, adm *kadm.Client, want map[string]int64, groups ...string) {
	t.Helper()
	resp := adm.FetchManyOffsets(testCtx(t), groups...)
	for g, at := range want {
		r, ok := resp[g]
		if !ok {
			t.Fatalf("FetchManyOffsets: no entry for %q (got %v)", g, keysOf(resp))
		}
		if r.Err != nil {
			t.Fatalf("FetchManyOffsets %q: %v", g, r.Err)
		}
		var found bool
		r.Fetched.Each(func(o kadm.OffsetResponse) {
			if o.At == at {
				found = true
			}
		})
		if !found {
			t.Fatalf("FetchManyOffsets %q: no offset %d in %+v", g, at, r.Fetched)
		}
	}
	for g := range resp {
		if _, asked := want[g]; !asked {
			t.Fatalf("FetchManyOffsets returned unrequested name %q (prefix not stripped?)", g)
		}
	}
}

// logNegotiatedVersions records, for each group API, the version range the proxy advertises
// (broker range intersected with what Kroxylicious 0.24.0 can decode) and what this client will use.
func logNegotiatedVersions(t *testing.T, ctx context.Context, adm *kadm.Client) {
	t.Helper()
	bvs, err := adm.ApiVersions(ctx)
	if err != nil {
		t.Fatalf("api versions via proxy: %v", err)
	}
	client := kversion.Stable()
	for _, bv := range bvs {
		if bv.Err != nil {
			t.Fatalf("api versions broker %d: %v", bv.NodeID, bv.Err)
		}
		for _, key := range []int16{10, 9, 8, 15, 16, 42, 47} {
			min, max, ok := bv.KeyVersions(key)
			cmax, _ := client.LookupMaxKeyVersion(key)
			name := kmsg.NameForKey(key)
			if !ok {
				t.Logf("%-16s not advertised by proxy", name)
				continue
			}
			t.Logf("%-16s proxy advertises v%d-v%d, franz-go max v%d -> uses v%d", name, min, max, cmax, minInt16(max, cmax))
		}
	}
}

func minInt16(a, b int16) int16 {
	if a < b {
		return a
	}
	return b
}

// scrapeRewriteCounters reads kroxylicious_consumer_group_prefix_rewrites_total from the proxy's
// Prometheus endpoint, keyed "API/direction".
func scrapeRewriteCounters(t *testing.T) map[string]float64 {
	t.Helper()
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("scrape %s: %v", metricsURL, err)
	}
	defer resp.Body.Close()
	counts := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "kroxylicious_consumer_group_prefix_rewrites_total{") {
			continue
		}
		labels, value, _ := strings.Cut(strings.TrimPrefix(line, "kroxylicious_consumer_group_prefix_rewrites_total{"), "} ")
		api, direction := label(labels, "api"), label(labels, "direction")
		v, _ := strconv.ParseFloat(value, 64)
		counts[api+"/"+direction] += v
	}
	return counts
}

func label(labels, name string) string {
	for _, kv := range strings.Split(labels, ",") {
		k, v, _ := strings.Cut(kv, "=")
		if k == name {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}


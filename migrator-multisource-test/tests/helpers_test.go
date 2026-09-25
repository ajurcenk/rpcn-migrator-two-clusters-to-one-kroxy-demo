package migratortest

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// newClient returns a kgo client and kadm admin for addr, closed on test cleanup.
func newClient(t *testing.T, addr string, opts ...kgo.Opt) (*kgo.Client, *kadm.Client) {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner())}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("new client for %s: %v", addr, err)
	}
	t.Cleanup(cl.Close)
	return cl, kadm.NewClient(cl)
}

// ensureTopic creates topic if it does not exist yet.
func ensureTopic(t *testing.T, ctx context.Context, adm *kadm.Client, topic string, partitions int32) {
	t.Helper()
	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

// deleteGroups removes groups (ignoring ones that don't exist), so reruns start clean.
func deleteGroups(t *testing.T, ctx context.Context, adm *kadm.Client, groups ...string) {
	t.Helper()
	resps, err := adm.DeleteGroups(ctx, groups...)
	if err != nil {
		t.Fatalf("delete groups %v: %v", groups, err)
	}
	for g, r := range resps {
		if r.Err != nil && !errors.Is(r.Err, kerr.GroupIDNotFound) {
			t.Fatalf("delete group %s: %v", g, r.Err)
		}
	}
}

func listGroupNames(t *testing.T, ctx context.Context, adm *kadm.Client) map[string]bool {
	t.Helper()
	lg, err := adm.ListGroups(ctx)
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	names := make(map[string]bool, len(lg))
	for _, g := range lg.Groups() {
		names[g] = true
	}
	return names
}

// committedOffset returns the committed offset for group/topic/partition, or -1 if none.
func committedOffset(t *testing.T, ctx context.Context, adm *kadm.Client, group, topic string, partition int32) int64 {
	t.Helper()
	os, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets %s: %v", group, err)
	}
	if o, ok := os.Lookup(topic, partition); ok {
		if o.Err != nil {
			t.Fatalf("fetch offsets %s %s/%d: %v", group, topic, partition, o.Err)
		}
		return o.At
	}
	return -1
}

func commit(t *testing.T, ctx context.Context, adm *kadm.Client, group, topic string, partition int32, at int64) {
	t.Helper()
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: topic, Partition: partition, At: at, LeaderEpoch: -1})
	resp, err := adm.CommitOffsets(ctx, group, os)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatalf("commit %s %s/%d=%d: %v", group, topic, partition, at, err)
	}
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

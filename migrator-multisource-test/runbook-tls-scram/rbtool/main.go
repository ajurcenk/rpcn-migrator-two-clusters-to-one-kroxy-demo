// rbtool is the helper behind the runbook scripts: producers and consumers that behave like real
// applications on the source clusters, and the checks that compare source and destination.
//
// This is the TLS/SCRAM runbook's copy of runbook/rbtool. Every client connects with TLS and
// SASL/SCRAM, configured from the environment (see connOpts): RB_TLS_CA, RB_SASL_USER,
// RB_SASL_PASS, RB_SASL_MECHANISM. It adds the check-auth subcommand, which proves the endpoints
// reject plaintext, missing SASL and wrong passwords.
//
// Positions are compared through the x-source-offset header, which the migrator writes into every
// destination record (offset_header in migrator/*.yaml): the destination record just before a
// committed offset tells exactly which source offset that position corresponds to.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type source struct {
	name, prefix, addr string
}

var (
	sources = []source{
		{"A", "a_", env("SRC_A_ADDR", "localhost:19093")},
		{"B", "b_", env("SRC_B_ADDR", "localhost:29093")},
	}
	destAddr = env("DEST_ADDR", "localhost:39093")
	topics   = map[string]int32{"orders": 3, "payments": 1}
)

const (
	offsetHeader     = "x-source-offset"
	provenanceHeader = "x-source-cluster"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "create-topics":
		err = createTopics(ctx, args)
	case "produce":
		err = produce(ctx, args)
	case "consume":
		err = consume(ctx, args)
	case "check-data":
		err = checkData(ctx, args)
	case "check-offsets":
		err = checkOffsets(ctx, args)
	case "verify-cutover":
		err = verifyCutover(ctx, args)
	case "check-duplicates":
		err = checkDuplicates(ctx, args)
	case "check-auth":
		err = checkAuth(ctx, args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: rbtool create-topics|produce|consume|check-data|check-offsets|verify-cutover|check-duplicates|check-auth [flags]")
	os.Exit(2)
}

// ---- create-topics ----

func createTopics(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("create-topics", flag.ExitOnError)
	src := fs.String("source", "", "source name (A or B)")
	fs.Parse(args)
	s, err := sourceByName(*src)
	if err != nil {
		return err
	}
	adm := admin(s.addr)
	for topic, parts := range topics {
		resp, err := adm.CreateTopic(ctx, parts, 1, nil, topic)
		if err == nil {
			err = resp.Err
		}
		switch {
		case errors.Is(err, kerr.TopicAlreadyExists):
			fmt.Printf("source %s: topic %s already exists\n", s.name, topic)
		case err != nil:
			return fmt.Errorf("create %s on %s: %w", topic, s.name, err)
		default:
			fmt.Printf("source %s: created topic %s (%d partitions)\n", s.name, topic, parts)
		}
	}
	return nil
}

// ---- produce ----

// produce writes "<source>-<topic>-<n>" records to every topic at a steady rate until stopped.
func produce(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	src := fs.String("source", "", "source name (A or B)")
	rate := fs.Int("rate", 20, "records per second per topic")
	fs.Parse(args)
	s, err := sourceByName(*src)
	if err != nil {
		return err
	}
	// Round-robin so every partition of orders gets records; the default sticky partitioner would
	// keep a slow producer on one partition.
	cl, err := kgo.NewClient(connOpts(s.addr, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))...)
	if err != nil {
		return err
	}
	defer cl.Close()

	seq := map[string]int{}
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	log("producer %s started: %d records/s per topic to %v", s.name, *rate, topicNames())
	for {
		select {
		case <-ctx.Done():
			if err := cl.Flush(context.Background()); err != nil {
				return err
			}
			log("producer %s stopped: produced %v", s.name, seq)
			return nil
		case <-report.C:
			log("producer %s: produced %v", s.name, seq)
		case <-tick.C:
			for _, topic := range topicNames() {
				r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "%s-%s-%d", s.name, topic, seq[topic])}
				seq[topic]++
				cl.Produce(ctx, r, func(_ *kgo.Record, err error) {
					if err != nil && ctx.Err() == nil {
						log("producer %s: produce error: %v", s.name, err)
					}
				})
			}
		}
	}
}

// ---- consume ----

type consumed struct {
	Group        string `json:"group"`
	Topic        string `json:"topic"`
	Partition    int32  `json:"partition"`
	Offset       int64  `json:"offset"`
	SourceOffset *int64 `json:"source_offset,omitempty"` // from x-source-offset, on destination records
	Value        string `json:"value"`
}

// consume is a normal consumer group member with auto-commit. It appends every record to a JSON
// lines log and, when stopped, commits its final position before leaving the group, as a
// well-behaved application does. With -until-idle it stops by itself once no records arrived for
// that long.
func consume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	brokers := fs.String("brokers", "", "bootstrap address")
	group := fs.String("group", "", "consumer group")
	topicList := fs.String("topics", "", "comma-separated topics")
	logPath := fs.String("log", "", "JSON lines file to append consumed records to")
	untilIdle := fs.Duration("until-idle", 0, "stop after this long without records (0 = run until signalled)")
	fs.Parse(args)
	if *brokers == "" || *group == "" || *topicList == "" || *logPath == "" {
		return errors.New("consume needs -brokers, -group, -topics and -log")
	}

	f, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	enc := json.NewEncoder(w)

	cl, err := kgo.NewClient(connOpts(*brokers,
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(strings.Split(*topicList, ",")...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitInterval(time.Second),
	)...)
	if err != nil {
		return err
	}
	log("consumer %s on %s started: topics %s", *group, *brokers, *topicList)

	counts := map[string]int{}
	lastRecord := time.Now()
	for ctx.Err() == nil {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				log("consumer %s: fetch error %s/%d: %v", *group, t, p, err)
			}
		})
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			n++
			counts[fmt.Sprintf("%s/%d", r.Topic, r.Partition)]++
			c := consumed{Group: *group, Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Value: string(r.Value)}
			if so, ok := sourceOffset(r); ok {
				c.SourceOffset = &so
			}
			enc.Encode(c)
		})
		if n > 0 {
			lastRecord = time.Now()
			w.Flush()
		} else if *untilIdle > 0 && time.Since(lastRecord) > *untilIdle {
			log("consumer %s: no records for %s, stopping", *group, *untilIdle)
			break
		}
	}

	// Final commit of everything consumed, then leave the group.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.CommitUncommittedOffsets(commitCtx); err != nil {
		log("consumer %s: final commit failed: %v", *group, err)
	}
	cl.Close()
	log("consumer %s stopped: consumed %v", *group, counts)
	return nil
}

// ---- check-data ----

type topicStats struct {
	src, dst int64 // records currently held (end - start, over all partitions)
}

// check-data compares record counts per topic and checks that the newest destination records
// carry the right source's values and provenance header.
func checkData(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check-data", flag.ExitOnError)
	watch := fs.Duration("watch", 0, "measure twice this far apart and require the destination to grow")
	caughtUp := fs.Bool("caught-up", false, "require the destination to hold every source record")
	fs.Parse(args)

	dst := admin(destAddr)
	first, err := collectStats(ctx, dst)
	if err != nil {
		return err
	}
	stats := first
	if *watch > 0 {
		log("measuring again in %s ...", *watch)
		select {
		case <-time.After(*watch):
		case <-ctx.Done():
			return ctx.Err()
		}
		if stats, err = collectStats(ctx, dst); err != nil {
			return err
		}
	}

	var problems []string
	fmt.Printf("%-7s %-10s %10s %12s %8s", "SOURCE", "TOPIC", "SOURCE", "DESTINATION", "LAG")
	if *watch > 0 {
		fmt.Printf(" %10s", "GROWTH")
	}
	fmt.Println()
	for _, s := range sources {
		for _, topic := range topicNames() {
			key := s.name + "/" + topic
			st := stats[key]
			fmt.Printf("%-7s %-10s %10d %12d %8d", s.name, s.prefix+topic, st.src, st.dst, st.src-st.dst)
			if *watch > 0 {
				growth := st.dst - first[key].dst
				fmt.Printf(" %+10d", growth)
				if growth <= 0 {
					problems = append(problems, fmt.Sprintf("%s%s did not grow in %s", s.prefix, topic, *watch))
				}
			}
			fmt.Println()
			if *caughtUp && st.dst != st.src {
				problems = append(problems, fmt.Sprintf("%s%s holds %d of %d source records", s.prefix, topic, st.dst, st.src))
			}
		}
	}

	// Newest records: value from the right source, provenance header = that source's cluster ID.
	for _, s := range sources {
		meta, err := admin(s.addr).Metadata(ctx)
		if err != nil {
			return err
		}
		for _, topic := range topicNames() {
			recs, err := readTail(ctx, destAddr, s.prefix+topic, 20)
			if err != nil {
				return err
			}
			for _, r := range recs {
				if !strings.HasPrefix(string(r.Value), s.name+"-") {
					problems = append(problems, fmt.Sprintf("%s%s/%d@%d has value %q from the wrong source", s.prefix, topic, r.Partition, r.Offset, r.Value))
				}
				if h := header(r, provenanceHeader); h != meta.Cluster {
					problems = append(problems, fmt.Sprintf("%s%s/%d@%d: %s=%q, want %q", s.prefix, topic, r.Partition, r.Offset, provenanceHeader, h, meta.Cluster))
				}
			}
			fmt.Printf("sample: %s%s newest %d records: values %s-*, provenance %s\n", s.prefix, topic, len(recs), s.name, meta.Cluster)
		}
	}
	for _, t := range []string{"orders", "payments"} {
		if exists(ctx, dst, t) {
			problems = append(problems, fmt.Sprintf("destination has unprefixed topic %q", t))
		}
	}
	return verdict(problems)
}

func collectStats(ctx context.Context, dst *kadm.Client) (map[string]topicStats, error) {
	out := map[string]topicStats{}
	for _, s := range sources {
		src := admin(s.addr)
		for _, topic := range topicNames() {
			sc, err := recordCount(ctx, src, topic)
			if err != nil {
				return nil, err
			}
			dc, _ := recordCount(ctx, dst, s.prefix+topic)
			out[s.name+"/"+topic] = topicStats{sc, dc}
		}
	}
	return out, nil
}

// ---- check-offsets ----

// check-offsets compares each source group's committed offsets with the translated ones in the
// prefixed destination group. The destination record just before the committed destination offset
// names (via x-source-offset) the source offset the position corresponds to.
func checkOffsets(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check-offsets", flag.ExitOnError)
	group := fs.String("group", "app-group", "source consumer group")
	exact := fs.Bool("exact", false, "require every translated position to be exact (source consumers stopped)")
	fs.Parse(args)

	var problems []string
	fmt.Printf("%-6s %-22s %-12s %8s %10s %13s  %s\n", "SOURCE", "DEST GROUP", "PARTITION", "SOURCE", "DEST", "= SOURCE POS", "RESULT")
	for _, s := range sources {
		srcAdm := admin(s.addr)
		srcOffsets, err := srcAdm.FetchOffsets(ctx, *group)
		if err != nil {
			return err
		}
		dstOffsets, err := admin(destAddr).FetchOffsets(ctx, s.prefix+*group)
		if err != nil {
			return err
		}
		for _, o := range sortedOffsets(srcOffsets) {
			if _, known := topics[o.Topic]; !known {
				continue
			}
			dt := s.prefix + o.Topic
			part := fmt.Sprintf("%s/%d", dt, o.Partition)
			d, ok := dstOffsets.Lookup(dt, o.Partition)
			if !ok || d.Err != nil {
				fmt.Printf("%-6s %-22s %-12s %8d %10s %13s  %s\n", s.name, s.prefix+*group, part, o.At, "-", "-", "not synced yet")
				problems = append(problems, fmt.Sprintf("%s%s %s not synced yet", s.prefix, *group, part))
				continue
			}
			equiv, err := sourcePosition(ctx, s.addr, o.Topic, dt, o.Partition, d.At)
			if err != nil {
				return fmt.Errorf("%s %s: %w", s.prefix+*group, part, err)
			}
			var result string
			switch {
			case equiv > o.At:
				result = fmt.Sprintf("AHEAD by %d: a consumer would skip records", equiv-o.At)
				problems = append(problems, fmt.Sprintf("%s%s %s is ahead of the source", s.prefix, *group, part))
			case equiv == o.At:
				result = "exact"
			default:
				result = fmt.Sprintf("behind by %d (re-reads; sync lag or timestamp translation)", o.At-equiv)
				if *exact {
					problems = append(problems, fmt.Sprintf("%s%s %s is behind the source by %d", s.prefix, *group, part, o.At-equiv))
				}
			}
			fmt.Printf("%-6s %-22s %-12s %8d %10d %13d  %s\n", s.name, s.prefix+*group, part, o.At, d.At, equiv, result)
		}
	}
	return verdict(problems)
}

// sourcePosition returns the source offset that destination position dstAt corresponds to: one
// past the source offset of the destination record at dstAt-1, whose value must match the source.
func sourcePosition(ctx context.Context, srcAddr, srcTopic, dstTopic string, p int32, dstAt int64) (int64, error) {
	dst := admin(destAddr)
	start, err := startOffset(ctx, dst, dstTopic, p)
	if err != nil {
		return 0, err
	}
	if dstAt <= start {
		// Nothing consumed on the destination side yet: the position is the source start.
		return startOffset(ctx, admin(srcAddr), srcTopic, p)
	}
	prev, err := readAt(ctx, destAddr, dstTopic, p, dstAt-1)
	if err != nil {
		return 0, err
	}
	so, ok := sourceOffset(prev)
	if !ok {
		return 0, fmt.Errorf("%s/%d@%d has no %s header", dstTopic, p, dstAt-1, offsetHeader)
	}
	srcRec, err := readAt(ctx, srcAddr, srcTopic, p, so)
	if err != nil {
		return 0, err
	}
	if string(srcRec.Value) != string(prev.Value) {
		return 0, fmt.Errorf("destination %s/%d@%d = %q but source %s/%d@%d = %q", dstTopic, p, dstAt-1, prev.Value, srcTopic, p, so, srcRec.Value)
	}
	return so + 1, nil
}

// ---- verify-cutover ----

// verify-cutover checks that consumers moved to the destination resumed exactly where the source
// consumers stopped: per partition, the first record the destination consumer read must be the
// source record at the source group's final committed offset.
func verifyCutover(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("verify-cutover", flag.ExitOnError)
	group := fs.String("group", "app-group", "source consumer group")
	logPaths := fs.String("log", "", "comma-separated JSON lines logs written by the destination consumers")
	fs.Parse(args)

	first, last := map[string]consumed{}, map[string]consumed{}
	for _, path := range strings.Split(*logPaths, ",") {
		if err := readConsumedLog(path, first, last); err != nil {
			return err
		}
	}

	var problems []string
	fmt.Printf("%-6s %-12s %12s %14s %14s  %s\n", "SOURCE", "PARTITION", "SOURCE FINAL", "DEST 1ST READ", "DEST LAST READ", "RESULT")
	for _, s := range sources {
		srcAdm := admin(s.addr)
		srcOffsets, err := srcAdm.FetchOffsets(ctx, *group)
		if err != nil {
			return err
		}
		for _, topic := range topicNames() {
			for p := int32(0); p < topics[topic]; p++ {
				dt := s.prefix + topic
				part := fmt.Sprintf("%s/%d", dt, p)
				final := int64(-1)
				if o, ok := srcOffsets.Lookup(topic, p); ok {
					final = o.At
				}
				end, err := endOffset(ctx, srcAdm, topic, p)
				if err != nil {
					return err
				}
				key := fmt.Sprintf("%s|%s/%d", s.prefix+*group, dt, p)
				fc, read := first[key]
				switch {
				case !read && final == end:
					fmt.Printf("%-6s %-12s %12d %14s %14s  %s\n", s.name, part, final, "-", "-", "OK: nothing left to consume")
				case !read:
					fmt.Printf("%-6s %-12s %12d %14s %14s  %s\n", s.name, part, final, "-", "-", "FAIL: nothing consumed")
					problems = append(problems, fmt.Sprintf("%s: nothing consumed, but source records %d..%d remain", part, final, end-1))
				case fc.SourceOffset == nil:
					problems = append(problems, fmt.Sprintf("%s: consumed records have no %s header", part, offsetHeader))
				default:
					f, l := *fc.SourceOffset, *last[key].SourceOffset
					result := "OK: resumed exactly"
					switch {
					case f > final:
						result = fmt.Sprintf("FAIL: skipped source offsets %d..%d", final, f-1)
						problems = append(problems, part+": "+result)
					case f < final:
						result = fmt.Sprintf("re-read %d records already consumed on the source", final-f)
					}
					if l != end-1 {
						result += fmt.Sprintf("; FAIL: stopped at source offset %d of %d", l, end-1)
						problems = append(problems, fmt.Sprintf("%s: did not consume to the end", part))
					}
					fmt.Printf("%-6s %-12s %12d %14d %14d  %s\n", s.name, part, final, f, l, result)
				}
			}
		}
	}
	return verdict(problems)
}

// ---- check-duplicates ----

// check-duplicates combines what the applications consumed on the source (before the cutover) and
// on the destination (after it) and checks that every source record, from each partition's start
// to its end, was consumed exactly once. Destination records are mapped back to their source
// offset through x-source-offset. Gaps always fail; duplicates fail unless -allow-duplicates.
func checkDuplicates(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check-duplicates", flag.ExitOnError)
	sourceLogs := fs.String("source-log", "", "source consumer logs as NAME=path,... (e.g. A=a.jsonl,B=b.jsonl)")
	destLogs := fs.String("dest-log", "", "comma-separated destination consumer logs")
	allowDup := fs.Bool("allow-duplicates", false, "report duplicates without failing (at-least-once is acceptable)")
	fs.Parse(args)

	type key struct {
		source, topic string
		partition     int32
	}
	type phaseCounts struct{ src, dst map[int64]int }
	seen := map[key]*phaseCounts{}
	get := func(k key) *phaseCounts {
		if seen[k] == nil {
			seen[k] = &phaseCounts{map[int64]int{}, map[int64]int{}}
		}
		return seen[k]
	}

	for _, entry := range strings.Split(*sourceLogs, ",") {
		name, path, ok := strings.Cut(entry, "=")
		if !ok {
			return fmt.Errorf("-source-log entry %q is not NAME=path", entry)
		}
		if _, err := sourceByName(name); err != nil {
			return err
		}
		if err := eachConsumed(path, func(c consumed) error {
			get(key{name, c.Topic, c.Partition}).src[c.Offset]++
			return nil
		}); err != nil {
			return err
		}
	}
	for _, path := range strings.Split(*destLogs, ",") {
		if err := eachConsumed(path, func(c consumed) error {
			s, topic, ok := sourceForDestTopic(c.Topic)
			if !ok {
				return fmt.Errorf("%s: topic %q has no known source prefix", path, c.Topic)
			}
			if c.SourceOffset == nil {
				return fmt.Errorf("%s: %s/%d@%d has no %s", path, c.Topic, c.Partition, c.Offset, offsetHeader)
			}
			get(key{s.name, topic, c.Partition}).dst[*c.SourceOffset]++
			return nil
		}); err != nil {
			return err
		}
	}

	var problems []string
	fmt.Printf("%-6s %-12s %9s %9s %9s %9s %11s %11s  %s\n", "SOURCE", "PARTITION", "RECORDS", "ON SOURCE", "ON DEST", "ONCE", "DUPLICATES", "MISSING", "DETAILS")
	for _, s := range sources {
		adm := admin(s.addr)
		for _, topic := range topicNames() {
			for p := int32(0); p < topics[topic]; p++ {
				start, err := startOffset(ctx, adm, topic, p)
				if err != nil {
					return err
				}
				end, err := endOffset(ctx, adm, topic, p)
				if err != nil {
					return err
				}
				c := get(key{s.name, topic, p})
				var onSrc, onDst, once, dups int64
				var dupSwitch, dupSource, dupDest, missing []int64
				for o := start; o < end; o++ {
					ns, nd := int64(c.src[o]), int64(c.dst[o])
					if ns > 0 {
						onSrc++
					}
					if nd > 0 {
						onDst++
					}
					switch n := ns + nd; {
					case n == 0:
						missing = append(missing, o)
					case n == 1:
						once++
					default:
						dups += n - 1
						switch {
						case ns > 0 && nd > 0:
							dupSwitch = append(dupSwitch, o) // read before and after the cutover
						case ns > 1:
							dupSource = append(dupSource, o)
						default:
							dupDest = append(dupDest, o)
						}
					}
				}
				part := fmt.Sprintf("%s%s/%d", s.prefix, topic, p)
				var details []string
				if len(dupSwitch) > 0 {
					details = append(details, "re-read after cutover "+ranges(dupSwitch))
				}
				if len(dupSource) > 0 {
					details = append(details, "repeated on source "+ranges(dupSource))
				}
				if len(dupDest) > 0 {
					details = append(details, "repeated on destination "+ranges(dupDest))
				}
				if len(missing) > 0 {
					details = append(details, "never consumed "+ranges(missing))
					problems = append(problems, fmt.Sprintf("%s: %d records never consumed (%s)", part, len(missing), ranges(missing)))
				}
				if dups > 0 && !*allowDup {
					problems = append(problems, fmt.Sprintf("%s: %d duplicate reads", part, dups))
				}
				if len(details) == 0 {
					details = append(details, "every record exactly once")
				}
				fmt.Printf("%-6s %-12s %9d %9d %9d %9d %11d %11d  %s\n", s.name, part, end-start, onSrc, onDst, once, dups, len(missing), strings.Join(details, "; "))
			}
		}
	}
	return verdict(problems)
}

func eachConsumed(path string, fn func(consumed) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var c consumed
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := fn(c); err != nil {
			return err
		}
	}
	return sc.Err()
}

func sourceForDestTopic(t string) (source, string, bool) {
	for _, s := range sources {
		if rest, ok := strings.CutPrefix(t, s.prefix); ok {
			return s, rest, true
		}
	}
	return source{}, "", false
}

// ranges formats sorted offsets as "3-7,12".
func ranges(offsets []int64) string {
	var parts []string
	for i := 0; i < len(offsets); {
		j := i
		for j+1 < len(offsets) && offsets[j+1] == offsets[j]+1 {
			j++
		}
		if i == j {
			parts = append(parts, fmt.Sprint(offsets[i]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", offsets[i], offsets[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ",")
}

// readConsumedLog records the first and last record read per group/partition, in log order.
func readConsumedLog(path string, first, last map[string]consumed) error {
	return eachConsumed(path, func(c consumed) error {
		key := fmt.Sprintf("%s|%s/%d", c.Group, c.Topic, c.Partition)
		if _, ok := first[key]; !ok {
			first[key] = c
		}
		last[key] = c
		return nil
	})
}

// ---- check-auth ----

// check-auth connects to each endpoint five ways and issues a Metadata request (ApiVersions is
// answered before authentication, so a ping would prove nothing). Only TLS + the right SCRAM
// credentials may succeed; plaintext, TLS without SASL, a wrong password and TLS without the demo
// CA must all be refused, the last two for the right reason. Endpoints can be brokers or the
// Kroxylicious proxies: through a proxy the SASL exchange reaches redpanda-dest unchanged.
func checkAuth(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("check-auth", flag.ExitOnError)
	endpoints := fs.String("endpoints", "", "comma-separated name=address[@user:password] (default credentials from RB_SASL_USER/RB_SASL_PASS)")
	fs.Parse(args)
	if *endpoints == "" {
		return errors.New("check-auth needs -endpoints")
	}
	base := envSecurity()
	if base.caFile == "" || base.user == "" {
		return errors.New("check-auth needs RB_TLS_CA and RB_SASL_USER")
	}

	type attempt struct {
		name    string
		sec     security
		sysCA   bool // TLS, but trust only the system CAs
		wantOK  bool
		wantErr func(error) bool // the reason a refusal must have (nil: any error)
	}
	var problems []string
	for _, ep := range strings.Split(*endpoints, ",") {
		name, addr, ok := strings.Cut(ep, "=")
		if !ok {
			return fmt.Errorf("bad endpoint %q, want name=address", ep)
		}
		good := base
		if a, creds, ok := strings.Cut(addr, "@"); ok {
			addr = a
			good.user, good.pass, _ = strings.Cut(creds, ":")
		}
		wrong := good
		wrong.pass += "-wrong"
		attempts := []attempt{
			{name: "TLS + SCRAM (" + good.user + ")", sec: good, wantOK: true},
			{name: "plaintext + SCRAM", sec: security{user: good.user, pass: good.pass, mechanism: good.mechanism}},
			{name: "TLS, no SASL", sec: security{caFile: good.caFile}},
			{name: "TLS + wrong password", sec: wrong, wantErr: func(err error) bool { return errors.Is(err, kerr.SaslAuthenticationFailed) }},
			{name: "TLS, untrusted CA", sec: good, sysCA: true, wantErr: func(err error) bool {
				var cv *tls.CertificateVerificationError
				return errors.As(err, &cv) || strings.Contains(err.Error(), "x509:")
			}},
		}
		fmt.Printf("%s (%s):\n", name, addr)
		for _, a := range attempts {
			err := authAttempt(ctx, addr, a.sec, a.sysCA)
			var status string
			switch {
			case a.wantOK && err == nil:
				status = "OK      accepted"
			case a.wantOK:
				status = "FAILED  refused: " + err.Error()
				problems = append(problems, fmt.Sprintf("%s: %s was refused: %v", name, a.name, err))
			case err == nil:
				status = "FAILED  accepted"
				problems = append(problems, fmt.Sprintf("%s: %s was accepted", name, a.name))
			case a.wantErr != nil && !a.wantErr(err):
				status = "FAILED  refused for the wrong reason: " + err.Error()
				problems = append(problems, fmt.Sprintf("%s: %s refused for the wrong reason: %v", name, a.name, err))
			default:
				status = "OK      refused: " + firstLine(err.Error())
			}
			fmt.Printf("  %-28s %s\n", a.name, status)
		}
	}
	return verdict(problems)
}

func authAttempt(ctx context.Context, addr string, sec security, sysCA bool) error {
	opts, err := sec.opts(addr)
	if err != nil {
		return err
	}
	if sysCA {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	opts = append(opts, kgo.DialTimeout(3*time.Second), kgo.RequestRetries(1), kgo.RetryTimeout(5*time.Second))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = kadm.NewClient(cl).BrokerMetadata(reqCtx)
	return err
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 110 {
		s = s[:110] + "..."
	}
	return s
}

// ---- helpers ----

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func log(format string, args ...any) {
	fmt.Printf("%s %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func verdict(problems []string) error {
	if len(problems) == 0 {
		fmt.Println("RESULT: OK")
		return nil
	}
	fmt.Println("RESULT: FAILED")
	for _, p := range problems {
		fmt.Println("  -", p)
	}
	return errors.New("check failed")
}

func sourceByName(name string) (source, error) {
	for _, s := range sources {
		if s.name == name {
			return s, nil
		}
	}
	return source{}, fmt.Errorf("unknown source %q (want A or B)", name)
}

func topicNames() []string {
	out := make([]string, 0, len(topics))
	for t := range topics {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// ---- connection security ----

// security is how a client connects: TLS with the demo CA, then SASL/SCRAM. The zero value of a
// field disables that layer, which check-auth uses for its negative cases.
type security struct {
	caFile, user, pass, mechanism string
}

// envSecurity is what every subcommand uses: the runbook's lib.sh exports RB_TLS_CA and admin
// credentials, and the producer / consumer steps override RB_SASL_USER / RB_SASL_PASS with the
// application user.
func envSecurity() security {
	return security{
		caFile:    os.Getenv("RB_TLS_CA"),
		user:      os.Getenv("RB_SASL_USER"),
		pass:      os.Getenv("RB_SASL_PASS"),
		mechanism: env("RB_SASL_MECHANISM", "SCRAM-SHA-256"),
	}
}

// connOpts returns the seed broker plus TLS and SASL options from the environment, followed by extra.
func connOpts(addr string, extra ...kgo.Opt) []kgo.Opt {
	opts, err := envSecurity().opts(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	return append(opts, extra...)
}

func (s security) opts(addr string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(addr)}
	if s.caFile != "" {
		pem, err := os.ReadFile(s.caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", s.caFile)
		}
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	}
	if s.user != "" {
		auth := scram.Auth{User: s.user, Pass: s.pass}
		var m sasl.Mechanism
		switch s.mechanism {
		case "SCRAM-SHA-256":
			m = auth.AsSha256Mechanism()
		case "SCRAM-SHA-512":
			m = auth.AsSha512Mechanism()
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism %q", s.mechanism)
		}
		opts = append(opts, kgo.SASL(m))
	}
	return opts, nil
}

func client(addr string) *kgo.Client {
	cl, err := kgo.NewClient(connOpts(addr)...)
	if err != nil {
		panic(err)
	}
	return cl
}

func admin(addr string) *kadm.Client { return kadm.NewClient(client(addr)) }

func exists(ctx context.Context, adm *kadm.Client, topic string) bool {
	td, err := adm.ListTopics(ctx, topic)
	return err == nil && td.Has(topic) && td[topic].Err == nil
}

func recordCount(ctx context.Context, adm *kadm.Client, topic string) (int64, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	var n int64
	var lookupErr error
	ends.Each(func(e kadm.ListedOffset) {
		if e.Err != nil {
			lookupErr = e.Err
			return
		}
		s, _ := starts.Lookup(e.Topic, e.Partition)
		n += e.Offset - s.Offset
	})
	return n, lookupErr
}

func startOffset(ctx context.Context, adm *kadm.Client, topic string, p int32) (int64, error) {
	los, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	o, ok := los.Lookup(topic, p)
	if !ok || o.Err != nil {
		return 0, fmt.Errorf("no start offset for %s/%d", topic, p)
	}
	return o.Offset, nil
}

func endOffset(ctx context.Context, adm *kadm.Client, topic string, p int32) (int64, error) {
	los, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	o, ok := los.Lookup(topic, p)
	if !ok || o.Err != nil {
		return 0, fmt.Errorf("no end offset for %s/%d", topic, p)
	}
	return o.Offset, nil
}

// readAt reads the single record at topic/p/offset.
func readAt(ctx context.Context, addr, topic string, p int32, offset int64) (*kgo.Record, error) {
	cl, err := kgo.NewClient(connOpts(addr, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {p: kgo.NewOffset().At(offset)}}))...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(readCtx)
		if readCtx.Err() != nil {
			return nil, fmt.Errorf("no record at %s/%d@%d", topic, p, offset)
		}
		for _, r := range fs.Records() {
			if r.Offset == offset {
				return r, nil
			}
		}
	}
}

// readTail reads up to n newest records of every partition of topic.
func readTail(ctx context.Context, addr, topic string, n int64) ([]*kgo.Record, error) {
	adm := admin(addr)
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	offsets := map[int32]kgo.Offset{}
	want := map[int32]int64{}
	ends.Each(func(e kadm.ListedOffset) {
		s, _ := starts.Lookup(e.Topic, e.Partition)
		if e.Offset > s.Offset {
			offsets[e.Partition] = kgo.NewOffset().At(max(s.Offset, e.Offset-n))
			want[e.Partition] = e.Offset - 1
		}
	})
	if len(offsets) == 0 {
		return nil, nil
	}
	cl, err := kgo.NewClient(connOpts(addr, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}))...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(want) > 0 {
		fs := cl.PollFetches(readCtx)
		if readCtx.Err() != nil {
			return nil, fmt.Errorf("reading the tail of %s: timed out", topic)
		}
		for _, r := range fs.Records() {
			out = append(out, r)
			if last, ok := want[r.Partition]; ok && r.Offset >= last {
				delete(want, r.Partition)
			}
		}
	}
	return out, nil
}

func sourceOffset(r *kgo.Record) (int64, bool) {
	for _, h := range r.Headers {
		if h.Key == offsetHeader && len(h.Value) == 8 {
			return int64(binary.BigEndian.Uint64(h.Value)), true
		}
	}
	return 0, false
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func sortedOffsets(os kadm.OffsetResponses) []kadm.OffsetResponse {
	var out []kadm.OffsetResponse
	os.Each(func(o kadm.OffsetResponse) { out = append(out, o) })
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

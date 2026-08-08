// Samples CPU and memory of the load generator and the server under test, for
// as long as a run lasts.
//
//	go run bench/k6/watch.go -match beam.smp -match '^k6 run' -duration 65s
//
// Anchor the k6 pattern. A pattern is a regexp over the whole command line, and
// an unanchored `k6 run` matches the shell that was invoked to start this
// sampler — its own arguments contain that text. The false match reports a
// process that is idle by construction, so the generator's real cost silently
// reads as zero, which is exactly the mistake this file exists to prevent.
//
// Nothing else here can do this. k6 runs its script in a sandbox with no view of
// the operating system, so the one process that must be proven *not* to be the
// bottleneck cannot report its own cost. Hence Go, stdlib only, no module file:
// `go run` on this one file is the whole setup.
//
// Why it matters more than it looks. k6 and the server share the same ten cores.
// Each request moves megabytes through loopback, and that work grows with VUs.
// A closed-loop sweep that flattens at eight writers is either a saturated
// server or a saturated machine, and the two have opposite conclusions. Without
// a number for the generator's own CPU, the sweep cannot tell them apart — and
// every earlier benchmark in this repo got this wrong by running the driver
// inside the BEAM it was measuring.
//
// CPU is measured as consumed CPU time between samples, divided by wall time. It
// is not `ps %cpu`, which on Linux is an average over the process's whole
// lifetime — a server that booted an hour ago would report a number that has
// nothing to do with the run.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type patterns []string

func (p *patterns) String() string { return strings.Join(*p, ",") }

func (p *patterns) Set(value string) error {
	*p = append(*p, value)

	return nil
}

// One process at one instant: CPU seconds consumed since it started, and how
// much resident memory it holds right now.
type sample struct {
	at  time.Time
	cpu float64
	rss float64
}

type tracked struct {
	Pid     int    `json:"pid"`
	Command string `json:"command"`
	Match   string `json:"match"`
	samples []sample
}

// What a run cost, per process. AvgCPU can exceed 100: it is percent of one
// core, so 412 on a ten-core machine means four cores' worth.
type result struct {
	Pid       int     `json:"pid"`
	Match     string  `json:"match"`
	Command   string  `json:"command"`
	Seconds   float64 `json:"window_seconds"`
	CPUTime   float64 `json:"cpu_seconds"`
	AvgCPU    float64 `json:"avg_cpu_percent"`
	PeakCPU   float64 `json:"peak_cpu_percent"`
	MeanRSS   float64 `json:"mean_rss_mib"`
	PeakRSS   float64 `json:"peak_rss_mib"`
	Samples   int     `json:"samples"`
	Vanished  bool    `json:"vanished,omitempty"`
	StartedAt string  `json:"started_at"`
}

func main() {
	var match patterns

	flag.Var(&match, "match", "regexp matched against the full command line; repeatable")
	interval := flag.Duration("interval", 500*time.Millisecond, "sampling interval")
	duration := flag.Duration("duration", 0, "how long to sample; 0 means until interrupted")
	out := flag.String("out", "", "write the full result as JSON here")
	label := flag.String("label", "", "free-text label carried into the JSON")
	flag.Parse()

	if len(match) == 0 {
		fmt.Fprintln(os.Stderr, "at least one -match is required, e.g. -match beam.smp -match k6")
		os.Exit(2)
	}

	compiled := make([]*regexp.Regexp, 0, len(match))

	for _, pattern := range match {
		expression, err := regexp.Compile(pattern)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad -match %q: %v\n", pattern, err)
			os.Exit(2)
		}

		compiled = append(compiled, expression)
	}

	results := run(compiled, match, *interval, *duration)

	fmt.Print(report(results))

	if *out != "" {
		write(*out, *label, match, *interval, results)
	}
}

func run(compiled []*regexp.Regexp, names patterns, interval, duration time.Duration) []*tracked {
	// Processes are re-matched every tick rather than resolved once, so the
	// watcher can be started before k6 is and still catch it. A process that
	// exits mid-run keeps the samples it produced and is reported as vanished.
	seen := map[int]*tracked{}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var deadline <-chan time.Time

	if duration > 0 {
		deadline = time.After(duration)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	collect(seen, compiled, names)

	for {
		select {
		case <-ticker.C:
			collect(seen, compiled, names)
		case <-stop:
			return ordered(seen)
		case <-deadline:
			return ordered(seen)
		}
	}
}

func collect(seen map[int]*tracked, compiled []*regexp.Regexp, names patterns) {
	// The watcher and the `go run` parent that spawned it are skipped: their own
	// command lines contain the -match arguments, so they match everything the
	// user asked for and would otherwise appear as processes under test.
	self := map[int]bool{os.Getpid(): true, os.Getppid(): true}
	now := time.Now()

	for _, line := range ps() {
		pid, rss, cpu, command, ok := parse(line)

		if !ok || self[pid] {
			continue
		}

		for index, expression := range compiled {
			if !expression.MatchString(command) {
				continue
			}

			entry, known := seen[pid]

			if !known {
				entry = &tracked{Pid: pid, Command: command, Match: names[index]}
				seen[pid] = entry
			}

			entry.samples = append(entry.samples, sample{at: now, cpu: cpu, rss: rss})

			break
		}
	}
}

func ps() []string {
	// One call per tick for every process, so matching happens here rather than
	// in N calls to `ps -p`. rss is KiB and time is accumulated CPU time on both
	// macOS and Linux.
	output, err := exec.Command("ps", "-axo", "pid=,rss=,time=,args=").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ps failed: %v\n", err)

		return nil
	}

	return strings.Split(strings.TrimRight(string(output), "\n"), "\n")
}

func parse(line string) (int, float64, float64, string, bool) {
	fields := strings.Fields(line)

	if len(fields) < 4 {
		return 0, 0, 0, "", false
	}

	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, 0, "", false
	}

	rss, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, 0, 0, "", false
	}

	cpu, ok := cpuSeconds(fields[2])
	if !ok {
		return 0, 0, 0, "", false
	}

	return pid, rss / 1024, cpu, strings.Join(fields[3:], " "), true
}

// ps prints accumulated CPU time as [[dd-]hh:]mm:ss[.cc]. macOS gives hundredths,
// Linux whole seconds; both are handled, and the hundredths are what make a
// sixty-second window measurable to better than a percent.
func cpuSeconds(text string) (float64, bool) {
	days := 0.0

	if dash := strings.Index(text, "-"); dash >= 0 {
		value, err := strconv.ParseFloat(text[:dash], 64)
		if err != nil {
			return 0, false
		}

		days, text = value, text[dash+1:]
	}

	total := 0.0

	for _, part := range strings.Split(text, ":") {
		value, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, false
		}

		total = total*60 + value
	}

	return days*86400 + total, true
}

func ordered(seen map[int]*tracked) []*tracked {
	entries := make([]*tracked, 0, len(seen))

	for _, entry := range seen {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(a, b int) bool {
		if entries[a].Match != entries[b].Match {
			return entries[a].Match < entries[b].Match
		}

		return entries[a].Pid < entries[b].Pid
	})

	return entries
}

func summarize(entry *tracked, last time.Time) result {
	summary := result{Pid: entry.Pid, Match: entry.Match, Command: entry.Command, Samples: len(entry.samples)}

	if len(entry.samples) == 0 {
		return summary
	}

	first, final := entry.samples[0], entry.samples[len(entry.samples)-1]

	summary.StartedAt = first.at.Format(time.RFC3339)
	summary.Seconds = final.at.Sub(first.at).Seconds()
	summary.CPUTime = final.cpu - first.cpu
	summary.Vanished = final.at.Before(last.Add(-2 * time.Second))

	if summary.Seconds > 0 {
		summary.AvgCPU = summary.CPUTime / summary.Seconds * 100
	}

	total := 0.0

	for index, point := range entry.samples {
		total += point.rss

		if point.rss > summary.PeakRSS {
			summary.PeakRSS = point.rss
		}

		// Peak is the busiest single interval, not the busiest instant: a burst
		// shorter than the sampling interval is averaged into it and invisible.
		if index > 0 {
			previous := entry.samples[index-1]
			elapsed := point.at.Sub(previous.at).Seconds()

			if elapsed > 0 {
				if percent := (point.cpu - previous.cpu) / elapsed * 100; percent > summary.PeakCPU {
					summary.PeakCPU = percent
				}
			}
		}
	}

	summary.MeanRSS = total / float64(len(entry.samples))

	return summary
}

func summaries(entries []*tracked) []result {
	last := time.Time{}

	for _, entry := range entries {
		if len(entry.samples) > 0 {
			if at := entry.samples[len(entry.samples)-1].at; at.After(last) {
				last = at
			}
		}
	}

	out := make([]result, 0, len(entries))

	for _, entry := range entries {
		out = append(out, summarize(entry, last))
	}

	return out
}

func report(entries []*tracked) string {
	lines := []string{
		"",
		fmt.Sprintf("  %-14s %7s %9s %9s %10s %10s", "match", "pid", "avg %CPU", "peak %CPU", "mean RSS", "peak RSS"),
	}

	for _, summary := range summaries(entries) {
		note := ""

		if summary.Vanished {
			note = "  (exited during the run)"
		}

		lines = append(lines, fmt.Sprintf("  %-14s %7d %9.1f %9.1f %7.0f MiB %7.0f MiB%s",
			summary.Match, summary.Pid, summary.AvgCPU, summary.PeakCPU, summary.MeanRSS, summary.PeakRSS, note))
	}

	if len(entries) == 0 {
		lines = append(lines, "  nothing matched — check the -match patterns against `ps -axo args=`")
	}

	// Percent of one core, so the ceiling is 100 × cores. Said here because the
	// number goes into a results document where nobody will remember to ask.
	return strings.Join(lines, "\n") + "\n\n  %CPU is per core; 1000 is all ten cores of an M1 Pro.\n\n"
}

func write(path, label string, names patterns, interval time.Duration, entries []*tracked) {
	document := map[string]any{
		"label":       label,
		"match":       []string(names),
		"interval_ms": interval.Milliseconds(),
		"processes":   summaries(entries),
	}

	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot encode result: %v\n", err)

		return
	}

	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", path, err)
	}
}

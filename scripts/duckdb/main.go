// Writes OTel-shaped rows straight into a DuckDB file and into Parquet, with
// no server, no HTTP and no Elixir in the way.
//
//	go run . -duration 60s
//
// Why this exists. smolquery stores through DuckDB, so every throughput number
// the k6 benchmark produces is bounded by what DuckDB alone can absorb on this
// machine. Until that bound is known, a slow arm cannot be blamed on the write
// path rather than on the storage engine underneath it. This measures the
// bound directly: one process, the library linked in, a file on disk.
//
// It reads the same bodies scripts/k6/generate.js writes and the same column
// list from scripts/k6/schema.json, so the rows are the rows the HTTP arms
// posted — not a fixture that happens to look similar.
//
// Three writes are measured, because they are three different questions:
//
//	append   the Appender API: pre-parsed values pushed a row at a time from Go.
//	         This is go-duckdb's ceiling, not DuckDB's — every value crosses cgo
//	         boxed as an interface, and at 62 columns that cost dominates.
//	json     INSERT ... SELECT * FROM read_json(...): DuckDB parses the same
//	         NDJSON file itself. The difference from `append` is what parsing
//	         costs when a C++ reader does it instead of a BEAM one.
//	parquet-in  INSERT ... SELECT * FROM read_parquet(...) over one pre-written
//	         3062-row file. Nothing crosses cgo per row and nothing parses text,
//	         so this is the closest thing here to the storage engine's own
//	         ceiling — the bound smolquery's write path cannot beat.
//	parquet-out COPY ... TO a Parquet file, zstd and uncompressed. What it costs
//	         to get the table back out in the format everything else reads.
//
// Each ingest runs against a freshly created table. Measured on this machine, a
// table left full by the previous write made the next one visibly slower, which
// would have made the results depend on the order they happen to run in.
//
// CPU and peak memory come from getrusage on this process, which is exact and
// covers the linked-in DuckDB as well — unlike sampling from the outside, there
// is nothing here that a watcher would have to be pointed at.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type table struct {
	ID     string   `json:"id"`
	Schema []column `json:"schema"`
}

// What one measured write cost. Rows and bytes are what landed; the rest is
// what the process spent to put them there.
type result struct {
	label   string
	rows    int64
	batches int64
	wall    time.Duration
	cpu     time.Duration
	peakRSS int64
	bytes   int64
	note    string
}

const stamp = "2006-01-02 15:04:05.000"

func main() {
	src := flag.String("src", "/tmp/smolquery-bodies/eachrow.3062.ndjson", "NDJSON bodies from generate.js")
	schemaPath := flag.String("schema", "../k6/schema.json", "column list, the same file the k6 arms post")
	out := flag.String("out", "/tmp/duckdb-bench", "directory for the database and the Parquet files")
	duration := flag.Duration("duration", 60*time.Second, "how long each measured write runs")
	threads := flag.Int("threads", runtime.NumCPU(), "DuckDB threads")
	checkpoint := flag.Bool("checkpoint", false, "CHECKPOINT after every batch — durability, not speed; see README")
	sweepTo := flag.Int("sweep-to", 32, "largest number of batches per transaction to try; 1 disables the sweep")
	writersTo := flag.Int("writers-to", 8, "largest number of concurrent writers to try; 1 disables it")
	concurrentPerTx := flag.Int("writers-per-tx", 1, "batches per transaction during the concurrency sweep")
	flag.Parse()

	columns, err := loadSchema(*schemaPath)
	if err != nil {
		fail("schema: %v", err)
	}

	rows, err := loadRows(*src, columns)
	if err != nil {
		fail("rows: %v", err)
	}

	if err := os.RemoveAll(*out); err != nil {
		fail("cannot clear %s: %v", *out, err)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail("cannot create %s: %v", *out, err)
	}

	dbPath := filepath.Join(*out, "bench.duckdb")

	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		fail("open %s: %v", dbPath, err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx := context.Background()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("SET threads TO %d", *threads)); err != nil {
		fail("set threads: %v", err)
	}

	if _, err := db.ExecContext(ctx, create("otel_logs", columns)); err != nil {
		fail("create table: %v", err)
	}

	// One batch, written once, exported once. read_parquet then replays exactly
	// the rows the other two writes insert, so the three ingest numbers differ
	// by how the rows arrive and by nothing else.
	batchFile := filepath.Join(*out, "batch.parquet")

	if err := seed(ctx, db, connector, columns, rows, batchFile); err != nil {
		fail("seed batch: %v", err)
	}

	fmt.Printf("\n  source       %s\n", *src)
	fmt.Printf("  batch        %d rows x %d columns\n", len(rows), len(columns))
	fmt.Printf("  database     %s\n", dbPath)
	fmt.Printf("  threads      %d\n", *threads)

	if *checkpoint {
		fmt.Printf("  checkpoint   after every batch\n")
	}

	results := []result{}

	if err := reset(ctx, db, columns); err != nil {
		fail("reset: %v", err)
	}

	appended, err := runAppend(ctx, db, connector, rows, *duration, *checkpoint)
	if err != nil {
		fail("append: %v", err)
	}
	results = append(results, appended)

	if err := reset(ctx, db, columns); err != nil {
		fail("reset: %v", err)
	}

	parsed, err := runJSON(ctx, db, *src, columns, int64(len(rows)), *duration, *checkpoint)
	if err != nil {
		fail("json: %v", err)
	}
	results = append(results, parsed)

	if err := reset(ctx, db, columns); err != nil {
		fail("reset: %v", err)
	}

	// One batch per transaction first, to sit next to the other two ingests,
	// then the same write with more batches under one commit.
	for _, perTx := range sweep(*sweepTo) {
		if err := reset(ctx, db, columns); err != nil {
			fail("reset: %v", err)
		}

		replayed, err := runParquetIn(ctx, db, batchFile, int64(len(rows)), *duration, *checkpoint, perTx, 1)
		if err != nil {
			fail("parquet-in x%d: %v", perTx, err)
		}

		results = append(results, verify(ctx, db, replayed))
	}

	// The same write again, split across writers. ClickHouse takes its
	// throughput from concurrent connections; this asks whether a single-writer
	// engine can do anything with them.
	for _, writers := range sweep(*writersTo)[1:] {
		if err := reset(ctx, db, columns); err != nil {
			fail("reset: %v", err)
		}

		shared, err := runParquetIn(ctx, db, batchFile, int64(len(rows)), *duration, *checkpoint, *concurrentPerTx, writers)
		if err != nil {
			fail("parquet-in %d writers: %v", writers, err)
		}

		results = append(results, verify(ctx, db, shared))
	}

	// The table is whatever the last ingest left in it. Both Parquet runs copy
	// that same table, so their sizes and times compare with each other.
	total, err := count(ctx, db)
	if err != nil {
		fail("count: %v", err)
	}

	for _, codec := range []string{"zstd", "uncompressed"} {
		written, err := runParquet(ctx, db, filepath.Join(*out, "otel_logs."+codec+".parquet"), codec, total)
		if err != nil {
			fail("parquet %s: %v", codec, err)
		}

		results = append(results, written)
	}

	if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		fail("final checkpoint: %v", err)
	}

	report(results, dbPath, total)
}

// The Appender is DuckDB's bulk path: values go in already typed, so nothing
// here parses, plans or binds. Whatever this reports is the ceiling every other
// way of writing the same rows is measured against.
func runAppend(ctx context.Context, db *sql.DB, connector *duckdb.Connector, rows [][]driver.Value, limit time.Duration, checkpoint bool) (result, error) {
	conn, err := connector.Connect(ctx)
	if err != nil {
		return result{}, err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "otel_logs")
	if err != nil {
		return result{}, err
	}

	before := usage()
	start := time.Now()

	var batches, written int64

	for time.Since(start) < limit {
		for _, row := range rows {
			if err := appender.AppendRow(row...); err != nil {
				return result{}, err
			}
		}

		// One Flush per batch, so a batch here means the same thing a request
		// means on the HTTP arms: the rows are in the table, not in a buffer
		// this loop is still holding.
		if err := appender.Flush(); err != nil {
			return result{}, err
		}

		if checkpoint {
			if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
				return result{}, err
			}
		}

		batches++
		written += int64(len(rows))
	}

	wall := time.Since(start)

	if err := appender.Close(); err != nil {
		return result{}, err
	}

	return finish("append (Appender API)", written, batches, wall, before), nil
}

// Writes the one reference batch and exports it, so read_parquet has a file of
// exactly the rows the other writes insert.
func seed(ctx context.Context, db *sql.DB, connector *duckdb.Connector, columns []column, rows [][]driver.Value, path string) error {
	if _, err := db.ExecContext(ctx, create("batch", columns)); err != nil {
		return err
	}

	conn, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", "batch")
	if err != nil {
		return err
	}

	for _, row := range rows {
		if err := appender.AppendRow(row...); err != nil {
			return err
		}
	}

	if err := appender.Close(); err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("COPY batch TO '%s' (FORMAT parquet)", path))

	return err
}

// A fresh, empty target. Every ingest starts from the same place.
func reset(ctx context.Context, db *sql.DB, columns []column) error {
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS otel_logs"); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, create("otel_logs", columns)); err != nil {
		return err
	}

	_, err := db.ExecContext(ctx, "CHECKPOINT")

	return err
}

// Rows arrive already typed, already columnar, already compressed, and never
// cross cgo one at a time. What is left is the storage engine writing.
//
// perTx is how many batches share one transaction. It is the knob that matters:
// at one batch per transaction all three ingest paths here land within a few
// percent of each other, which says the cost is the commit and not the rows.
func runParquetIn(ctx context.Context, db *sql.DB, path string, batch int64, limit time.Duration, checkpoint bool, perTx, writers int) (result, error) {
	statement := fmt.Sprintf("INSERT INTO otel_logs SELECT * FROM read_parquet('%s')", path)
	label := "parquet-in (read_parquet)"

	switch {
	case writers > 1:
		label = fmt.Sprintf("parquet-in x%d, %d writers", perTx, writers)
	case perTx > 1:
		label = fmt.Sprintf("parquet-in x%d per tx", perTx)
	}

	before := usage()
	start := time.Now()

	// Concurrent appenders do not conflict in DuckDB — they touch different
	// rows — but they still serialize at commit on one WAL, and a checkpoint
	// takes a global lock. Whether that leaves anything for a second writer to
	// win is the question this answers.
	var batches, written, failed int64
	var lock sync.Mutex
	var group sync.WaitGroup

	worker := func() {
		defer group.Done()

		var mine, rows int64

		for time.Since(start) < limit {
			transaction, err := db.BeginTx(ctx, nil)
			if err != nil {
				lock.Lock()
				failed++
				lock.Unlock()

				continue
			}

			ok := true

			for i := 0; i < perTx; i++ {
				if _, err := transaction.ExecContext(ctx, statement); err != nil {
					ok = false

					break
				}
			}

			if !ok {
				transaction.Rollback()

				lock.Lock()
				failed++
				lock.Unlock()

				continue
			}

			// A write-write conflict is a legitimate outcome, not a crash: it
			// is what a single-writer engine does when asked for two. Counted
			// and reported, because a retry loop that hid it would report the
			// retries as throughput.
			if err := transaction.Commit(); err != nil {
				lock.Lock()
				failed++
				lock.Unlock()

				continue
			}

			if checkpoint {
				db.ExecContext(ctx, "CHECKPOINT")
			}

			mine += int64(perTx)
			rows += batch * int64(perTx)
		}

		lock.Lock()
		batches += mine
		written += rows
		lock.Unlock()
	}

	group.Add(writers)

	for i := 0; i < writers; i++ {
		go worker()
	}

	group.Wait()

	outcome := finish(label, written, batches, time.Since(start), before)

	if failed > 0 {
		outcome.note = fmt.Sprintf("  (%d transactions conflicted)", failed)
	}

	return outcome, nil
}

// DuckDB reads the file itself. Same bytes k6 posted to ClickHouse, parsed by
// the C++ JSON reader instead of being handed over pre-typed.
func runJSON(ctx context.Context, db *sql.DB, src string, columns []column, batch int64, limit time.Duration, checkpoint bool) (result, error) {
	statement := fmt.Sprintf(
		"INSERT INTO otel_logs SELECT * FROM read_json('%s', format='newline_delimited', columns={%s})",
		src, columnSpec(columns))

	before := usage()
	start := time.Now()

	var batches, written int64

	for time.Since(start) < limit {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return result{}, err
		}

		if checkpoint {
			if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
				return result{}, err
			}
		}

		batches++
		written += batch
	}

	return finish("json (read_json)", written, batches, time.Since(start), before), nil
}

// One COPY of the whole table. This is a bulk export, not a loop: Parquet is
// written once per file, and running it repeatedly would measure rewriting the
// same rows rather than writing them.
func runParquet(ctx context.Context, db *sql.DB, path, codec string, rows int64) (result, error) {
	before := usage()
	start := time.Now()

	statement := fmt.Sprintf("COPY otel_logs TO '%s' (FORMAT parquet, COMPRESSION %s)", path, codec)

	if _, err := db.ExecContext(ctx, statement); err != nil {
		return result{}, err
	}

	written := finish("parquet-out ("+codec+")", rows, 1, time.Since(start), before)

	if info, err := os.Stat(path); err == nil {
		written.bytes = info.Size()
	}

	return written, nil
}

func finish(label string, rows, batches int64, wall time.Duration, before rusage) result {
	after := usage()

	return result{
		label:   label,
		rows:    rows,
		batches: batches,
		wall:    wall,
		cpu:     after.cpu - before.cpu,
		peakRSS: after.peakRSS,
	}
}

// 1, 2, 4 … up to the limit. Always starts at 1 so the sweep's first row is
// directly comparable with the append and json ingests above it.
func sweep(limit int) []int {
	sizes := []int{}

	for size := 1; size <= limit; size *= 2 {
		sizes = append(sizes, size)
	}

	if len(sizes) == 0 {
		sizes = append(sizes, 1)
	}

	return sizes
}

// Counts what is actually in the table and marks the row if it disagrees with
// what the loop thinks it wrote. A throughput number nobody checked against the
// data is a count of round trips, not of rows — and a surprising result is
// exactly the one that has to survive this before it is believed.
func verify(ctx context.Context, db *sql.DB, outcome result) result {
	landed, err := count(ctx, db)
	if err != nil {
		outcome.note = "  (count failed: " + err.Error() + ")"

		return outcome
	}

	if landed != outcome.rows {
		outcome.note = fmt.Sprintf("  (MISMATCH: %d rows in table, loop counted %d)", landed, outcome.rows)
	}

	return outcome
}

func count(ctx context.Context, db *sql.DB) (int64, error) {
	var rows int64

	err := db.QueryRowContext(ctx, "SELECT count(*) FROM otel_logs").Scan(&rows)

	return rows, err
}

func create(name string, columns []column) string {
	parts := make([]string, 0, len(columns))

	for _, c := range columns {
		parts = append(parts, fmt.Sprintf("%q %s", c.Name, sqlType(c.Type)))
	}

	return "CREATE TABLE " + name + " (" + strings.Join(parts, ", ") + ")"
}

func columnSpec(columns []column) string {
	parts := make([]string, 0, len(columns))

	for _, c := range columns {
		parts = append(parts, fmt.Sprintf("'%s': '%s'", c.Name, sqlType(c.Type)))
	}

	return strings.Join(parts, ", ")
}

// The schema's own vocabulary, mapped once. An unknown type becomes VARCHAR
// rather than a silent skip: a column that quietly vanished would make every
// row narrower than the one the HTTP arms sent.
func sqlType(name string) string {
	switch name {
	case "TIMESTAMP":
		return "TIMESTAMP"
	case "INT64":
		return "BIGINT"
	case "FLOAT64":
		return "DOUBLE"
	case "BOOL":
		return "BOOLEAN"
	default:
		return "VARCHAR"
	}
}

func loadSchema(path string) ([]column, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var parsed table

	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	if len(parsed.Schema) == 0 {
		return nil, fmt.Errorf("%s declares no columns", path)
	}

	return parsed.Schema, nil
}

// Parsed once, up front, into the types the Appender wants. Decoding inside the
// measured loop would put Go's JSON reader on the critical path and report it
// as DuckDB's cost.
func loadRows(path string, columns []column) ([][]driver.Value, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	rows := make([][]driver.Value, 0, len(lines))

	for number, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var decoded map[string]any

		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			return nil, fmt.Errorf("line %d: %w", number+1, err)
		}

		row := make([]driver.Value, len(columns))

		for index, c := range columns {
			value, err := convert(decoded[c.Name], c.Type)
			if err != nil {
				return nil, fmt.Errorf("line %d, column %s: %w", number+1, c.Name, err)
			}

			row[index] = value
		}

		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("%s holds no rows", path)
	}

	return rows, nil
}

func convert(value any, kind string) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}

	switch kind {
	case "TIMESTAMP":
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("want a timestamp string, got %T", value)
		}

		// generate.js writes ClickHouse's native form for this file. Same
		// instants as the smolquery bodies, different spelling.
		return time.Parse(stamp, text)
	case "INT64":
		number, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("want a number, got %T", value)
		}

		return int64(number), nil
	case "FLOAT64":
		number, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("want a number, got %T", value)
		}

		return number, nil
	case "BOOL":
		flag, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("want a bool, got %T", value)
		}

		return flag, nil
	default:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("want a string, got %T", value)
		}

		return text, nil
	}
}

type rusage struct {
	cpu     time.Duration
	peakRSS int64
}

// User plus system time for this process, and the high-water memory mark. This
// covers the linked-in DuckDB threads too, which is the point: there is no
// second process here for an outside sampler to find.
func usage() rusage {
	var used syscall.Rusage

	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &used); err != nil {
		return rusage{}
	}

	total := time.Duration(used.Utime.Sec+used.Stime.Sec)*time.Second +
		time.Duration(used.Utime.Usec+used.Stime.Usec)*time.Microsecond

	// ru_maxrss is bytes on darwin and kilobytes on linux. Getting this wrong
	// misreports memory by 1024x, in the direction nobody checks.
	peak := int64(used.Maxrss)

	if runtime.GOOS != "darwin" {
		peak *= 1024
	}

	return rusage{cpu: total, peakRSS: peak}
}

func report(results []result, dbPath string, rows int64) {
	fmt.Printf("\n  %-27s %10s %8s %9s %11s %12s\n",
		"write", "rows/s", "batches", "avg %CPU", "CPU s/Mrow", "RSS mark")

	for _, r := range results {
		seconds := r.wall.Seconds()
		if seconds <= 0 {
			continue
		}

		perMillion := 0.0
		if r.rows > 0 {
			perMillion = r.cpu.Seconds() / (float64(r.rows) / 1e6)
		}

		fmt.Printf("  %-27s %10.0f %8d %9.1f %11.1f %8.0f MiB%s\n",
			r.label,
			float64(r.rows)/seconds,
			r.batches,
			r.cpu.Seconds()/seconds*100,
			perMillion,
			float64(r.peakRSS)/1048576,
			r.note)
	}

	fmt.Printf("\n  %d rows in the table\n", rows)

	if info, err := os.Stat(dbPath); err == nil {
		fmt.Printf("  %.0f MiB  %s  (%.1f bytes/row)\n",
			float64(info.Size())/1048576, filepath.Base(dbPath), float64(info.Size())/float64(rows))
	}

	for _, r := range results {
		if r.bytes > 0 {
			fmt.Printf("  %.0f MiB  %s  (%.1f bytes/row)\n",
				float64(r.bytes)/1048576, r.label, float64(r.bytes)/float64(r.rows))
		}
	}

	// Percent of one core, so the ceiling is 100 x cores. Said here for the same
	// reason watch.go says it: the number ends up in a document without it.
	fmt.Printf("\n  %%CPU is per core; %d00 is all %d cores.\n", runtime.NumCPU(), runtime.NumCPU())

	// ru_maxrss is a high-water mark for the process and never falls, so each
	// row's figure is "the most this process had ever held by the end of that
	// write", not that write's own peak. Only the last row is a peak.
	fmt.Printf("  RSS mark is the process high-water mark so far, not per-write.\n\n")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

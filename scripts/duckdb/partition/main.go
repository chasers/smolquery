// What a partition key does to pruning, and what more segments cost the sealer.
//
//	cd scripts/duckdb && go run ./partition
//
// Two claims need testing, and neither is answerable from the write benchmark.
//
// First: sharding a table's buffer means each shard writes its own segment, and
// which rows land together then depends on the partition key. If shards are fed
// round-robin, every segment carries every tenant, its project_id min/max spans
// the whole space, and a tenant-filtered scan can skip nothing. If shards are
// fed by hash of project_id, each segment carries a slice of tenants and most
// segments prune away. That is a claim about row-group statistics, so it is
// measured against row-group statistics — `parquet_metadata` says exactly which
// groups a predicate can skip, with no profiler and no guessing.
//
// Second: more shards means more, smaller micro-segments per unit time, and the
// sealer has to merge them. The seal sweep holds the row count fixed and varies
// how many files carry it.
//
// Faithfulness. Segments here are written by DuckDB rather than by Polars, which
// is what `Smolquery.Segments.Writer` uses at flush. The pruning property being
// measured depends only on the sort and the row-group boundaries, and both are
// set to what the real seal uses: ORDER BY project_id, timestamp with NULLS
// LAST, ZSTD, ROW_GROUP_SIZE 16384 — `Smolquery.StorageService.Merge` and
// `seal_row_group_size` in config/config.exs. Absolute times are DuckDB's, not
// Polars'; the ratios between layouts are the result.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// The real seal's shape, so a measurement here is a measurement of that.
const (
	orderBy      = `"project_id" ASC NULLS LAST, "timestamp" ASC NULLS LAST`
	rowGroupSize = 16384
	codec        = "ZSTD"
)

type layout struct {
	name     string
	selector string
	note     string
}

func main() {
	src := flag.String("src", "/tmp/smolquery-bodies/eachrow.3062.ndjson", "NDJSON fixture from generate.js")
	out := flag.String("out", "/tmp/duckdb-partition", "where segments are written")
	rows := flag.Int("rows", 1_000_000, "total rows in the dataset")
	projects := flag.Int("projects", 1000, "distinct tenants, log-uniform")
	segments := flag.Int("segments", 100, "segments per layout")
	shards := flag.Int("shards", 4, "buffer shards being simulated")
	repeats := flag.Int("repeats", 5, "query timing repeats")
	threads := flag.Int("threads", 0, "DuckDB threads; 0 leaves the default")
	flag.Parse()

	if err := os.RemoveAll(*out); err != nil {
		fail("cannot clear %s: %v", *out, err)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail("cannot create %s: %v", *out, err)
	}

	db, err := sql.Open("duckdb", filepath.Join(*out, "work.duckdb"))
	if err != nil {
		fail("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if *threads > 0 {
		exec(ctx, db, fmt.Sprintf("SET threads TO %d", *threads))
	}

	build(ctx, db, *src, *rows, *projects, *shards, *segments)

	total := scalarInt(ctx, db, "SELECT count(*) FROM source")
	tenants := scalarInt(ctx, db, "SELECT count(DISTINCT project_id) FROM source")
	// Two probes, because one of them flatters hash partitioning by accident. The
	// largest tenant is also the lexicographically smallest — rank 0 of the skew —
	// so every shard that does not hold it has a min above it and prunes on the
	// boundary alone. A tenant in the middle of the key space is the honest test
	// of whether a shard's min/max is tight or merely wide.
	probe := scalarText(ctx, db,
		"SELECT project_id FROM source GROUP BY project_id ORDER BY count(*) DESC LIMIT 1")
	probeRows := scalarInt(ctx, db, "SELECT count(*) FROM source WHERE project_id = '"+probe+"'")

	mid := scalarText(ctx, db, fmt.Sprintf(`
		SELECT project_id FROM source GROUP BY project_id
		ORDER BY project_id LIMIT 1 OFFSET %d`, tenants/2))
	midRows := scalarInt(ctx, db, "SELECT count(*) FROM source WHERE project_id = '"+mid+"'")

	fmt.Printf("\n  dataset      %d rows, %d tenants, %d segments, %d shards\n",
		total, tenants, *segments, *shards)
	fmt.Printf("  segment      %d rows, sorted by (project_id, timestamp), %s, ROW_GROUP_SIZE %d\n",
		total/int64(*segments), codec, rowGroupSize)
	fmt.Printf("  probe A      %s — largest tenant, %d rows (%.1f%%), lexicographically first\n",
		probe, probeRows, float64(probeRows)/float64(total)*100)
	fmt.Printf("  probe B      %s — mid key space, %d rows (%.2f%%)\n",
		mid, midRows, float64(midRows)/float64(total)*100)

	// seg is assigned in build(): round-robin ignores the tenant, hash groups by
	// it. Both produce the same number of segments holding the same rows, so the
	// only difference between the two layouts is which rows share a file.
	layouts := []layout{
		{"round-robin", "seg_roundrobin", "no tenant grouping — today's shape"},
		{"hash(project_id)", "seg_hash", "a shard holds few tenants, scattered lexicographically"},
		{"range(project_id)", "seg_range", "a shard holds one contiguous slice of the key space"},
	}

	fmt.Printf("\n  %-18s %6s %8s %13s %12s %10s %9s\n",
		"layout", "probe", "groups", "groups read", "rows read", "query ms", "size")

	for _, l := range layouts {
		paths := write(ctx, db, *out, l, *segments)
		bytes := totalSize(paths)

		for _, p := range []struct {
			label string
			value string
		}{{"A", probe}, {"B", mid}} {
			files, groups, hit, hitRows := prune(ctx, db, paths, p.value)
			elapsed := timeQuery(ctx, db, paths, p.value, *repeats)

			name := ""
			size := ""

			if p.label == "A" {
				name = l.name
				size = fmt.Sprintf("%.0f MiB", float64(bytes)/1048576)
			}

			fmt.Printf("  %-18s %6s %8d %6d /%3d %12d %10.1f %9s\n",
				name, p.label, groups, hit, files, hitRows, elapsed, size)
		}
	}

	fmt.Println()

	for _, l := range layouts {
		fmt.Printf("  %-18s %s\n", l.name, l.note)
	}

	sealSweep(ctx, db, *out, total)

	fmt.Printf("\n  groups read is the row groups whose project_id min/max contain the probe,\n")
	fmt.Printf("  computed from parquet_metadata — the ones a scan cannot skip.\n\n")
}

// One source table of distinct rows. The fixture's 3062 rows are replayed, but
// project_id and timestamp are replaced, so no two rows are identical and the
// compression figures are not measuring duplicate detection — the mistake the
// write benchmark's bytes/row numbers fell into.
func build(ctx context.Context, db *sql.DB, src string, rows, projects, shards, segments int) {
	copies := rows / 3062
	if copies < 1 {
		copies = 1
	}

	// Log-uniform tenants, the same skew generate.js uses: trunc((N+1)^u) - 1
	// covers rank 0, the largest tenant, which trunc(N^u) never returns.
	project := fmt.Sprintf(
		"'p' || lpad(CAST(trunc(pow(%d, random())) - 1 AS VARCHAR), 5, '0')", projects+1)

	// REPLACE, not a trailing "AS project_id": the latter appends a second column
	// of that name and leaves the fixture's own in place, which silently turns the
	// whole experiment into 326 copies of one batch.
	exec(ctx, db, fmt.Sprintf(`
		CREATE TABLE source AS
		SELECT * EXCLUDE (rep) REPLACE (
		  %s AS project_id,
		  TIMESTAMP '2026-08-01 10:00:00'
		    + INTERVAL (CAST(random() * 86400000 AS BIGINT)) MILLISECOND AS timestamp
		)
		FROM read_json_auto('%s') AS f, range(0, %d) AS t(rep)
	`, project, src, copies))

	// Three assignments over the same rows, each modelling one way of feeding
	// shards. In every case a shard's segments accumulate over time, so a
	// segment's tenant range is its shard's tenant set — which is the thing
	// row-group statistics see.
	perShard := segments / shards
	if perShard < 1 {
		perShard = 1
	}

	exec(ctx, db, fmt.Sprintf(`
		CREATE TABLE laid_out AS
		SELECT
		  *,
		  CAST(row_number() OVER () AS BIGINT) AS rn,
		  CAST((row_number() OVER ()) %% %d AS INTEGER) AS seg_roundrobin,
		  CAST((hash(project_id) %% %d) * %d
		       + ((row_number() OVER ()) %% %d) AS INTEGER) AS seg_hash,
		  CAST((ntile(%d) OVER (ORDER BY project_id) - 1) * %d
		       + ((row_number() OVER ()) %% %d) AS INTEGER) AS seg_range
		FROM source
	`, segments, shards, perShard, perShard, shards, perShard, perShard))
}

// One COPY per segment, each sorted on the clustering key, exactly as one
// Writer.write produces one file.
func write(ctx context.Context, db *sql.DB, out string, l layout, segments int) []string {
	dir := filepath.Join(out, l.selector)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail("mkdir %s: %v", dir, err)
	}

	paths := make([]string, 0, segments)

	for i := 0; i < segments; i++ {
		path := filepath.Join(dir, fmt.Sprintf("seg_%04d.parquet", i))

		exec(ctx, db, fmt.Sprintf(`
			COPY (
			  SELECT * EXCLUDE (rn, seg_roundrobin, seg_hash, seg_range) FROM laid_out
			  WHERE %s = %d
			  ORDER BY %s
			) TO '%s' (FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE %d)
		`, l.selector, i, orderBy, path, codec, rowGroupSize))

		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			paths = append(paths, path)
		}
	}

	return paths
}

// What a scan cannot skip. parquet_metadata reports per-row-group min/max, which
// is the same statistic DuckDB's reader prunes on, so this is the pruning
// decision itself rather than a proxy for it.
func prune(ctx context.Context, db *sql.DB, paths []string, probe string) (int64, int64, int64, int64) {
	list := quoted(paths)

	groups := scalarInt(ctx, db, fmt.Sprintf(
		`SELECT count(*) FROM parquet_metadata([%s]) WHERE path_in_schema = 'project_id'`, list))

	hit := scalarInt(ctx, db, fmt.Sprintf(
		`SELECT count(*) FROM parquet_metadata([%s])
		 WHERE path_in_schema = 'project_id'
		   AND stats_min <= '%s' AND stats_max >= '%s'`, list, probe, probe))

	hitRows := scalarInt(ctx, db, fmt.Sprintf(
		`SELECT coalesce(sum(row_group_num_rows), 0) FROM parquet_metadata([%s])
		 WHERE path_in_schema = 'project_id'
		   AND stats_min <= '%s' AND stats_max >= '%s'`, list, probe, probe))

	files := scalarInt(ctx, db, fmt.Sprintf(
		`SELECT count(DISTINCT file_name) FROM parquet_metadata([%s])
		 WHERE path_in_schema = 'project_id'
		   AND stats_min <= '%s' AND stats_max >= '%s'`, list, probe, probe))

	return files, groups, hit, hitRows
}

func timeQuery(ctx context.Context, db *sql.DB, paths []string, probe string, repeats int) float64 {
	statement := fmt.Sprintf(
		`SELECT count(*), min(timestamp) FROM read_parquet([%s]) WHERE project_id = '%s'`,
		quoted(paths), probe)

	best := 0.0

	for i := 0; i < repeats; i++ {
		start := time.Now()

		var count int64
		var at time.Time

		if err := db.QueryRowContext(ctx, statement).Scan(&count, &at); err != nil {
			fail("probe query: %v", err)
		}

		elapsed := float64(time.Since(start).Microseconds()) / 1000

		if best == 0 || elapsed < best {
			best = elapsed
		}
	}

	return best
}

// The sealer's own work: merge N micro-segments into one sorted file. Rows are
// held constant and N varies, which is what more shards actually change.
func sealSweep(ctx context.Context, db *sql.DB, out string, total int64) {
	fmt.Printf("\n  %-12s %10s %10s %12s %9s\n", "segments", "seal ms", "rows/s", "sealed size", "inputs")

	for _, count := range []int{25, 50, 100, 200, 400} {
		dir := filepath.Join(out, fmt.Sprintf("seal_%d", count))

		if err := os.MkdirAll(dir, 0o755); err != nil {
			fail("mkdir %s: %v", dir, err)
		}

		paths := make([]string, 0, count)

		for i := 0; i < count; i++ {
			path := filepath.Join(dir, fmt.Sprintf("in_%04d.parquet", i))

			// rn is materialized in laid_out: a window function cannot appear in a
			// WHERE clause, and recomputing row_number() per segment would also
			// renumber the rows each time.
			exec(ctx, db, fmt.Sprintf(`
				COPY (
				  SELECT * EXCLUDE (rn, seg_roundrobin, seg_hash, seg_range) FROM laid_out
				  WHERE rn %% %d = %d
				  ORDER BY %s
				) TO '%s' (FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE %d)
			`, count, i, orderBy, path, codec, rowGroupSize))

			if info, err := os.Stat(path); err == nil && info.Size() > 0 {
				paths = append(paths, path)
			}
		}

		sealed := filepath.Join(dir, "sealed.parquet")
		inputs := totalSize(paths)

		start := time.Now()

		// union_by_name is what the real merge uses, so additive schema changes
		// merge rather than fail. It is not free, and it is included on purpose.
		exec(ctx, db, fmt.Sprintf(`
			COPY (
			  SELECT * FROM read_parquet([%s], union_by_name := true) ORDER BY %s
			) TO '%s' (FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE %d)
		`, quoted(paths), orderBy, sealed, codec, rowGroupSize))

		elapsed := time.Since(start)
		size := int64(0)

		if info, err := os.Stat(sealed); err == nil {
			size = info.Size()
		}

		fmt.Printf("  %-12d %10.0f %10.0f %9.0f MiB %6.0f MiB\n",
			len(paths),
			float64(elapsed.Milliseconds()),
			float64(total)/elapsed.Seconds(),
			float64(size)/1048576,
			float64(inputs)/1048576)

		os.RemoveAll(dir)
	}
}

func totalSize(paths []string) int64 {
	total := int64(0)

	for _, path := range paths {
		if info, err := os.Stat(path); err == nil {
			total += info.Size()
		}
	}

	return total
}

func quoted(paths []string) string {
	parts := make([]string, 0, len(paths))

	for _, path := range paths {
		parts = append(parts, "'"+path+"'")
	}

	return strings.Join(parts, ", ")
}

func exec(ctx context.Context, db *sql.DB, statement string) {
	if _, err := db.ExecContext(ctx, statement); err != nil {
		fail("%v\n  in: %s", err, strings.TrimSpace(statement))
	}
}

func scalarInt(ctx context.Context, db *sql.DB, statement string) int64 {
	var value int64

	if err := db.QueryRowContext(ctx, statement).Scan(&value); err != nil {
		fail("%v\n  in: %s", err, strings.TrimSpace(statement))
	}

	return value
}

func scalarText(ctx context.Context, db *sql.DB, statement string) string {
	var value string

	if err := db.QueryRowContext(ctx, statement).Scan(&value); err != nil {
		fail("%v\n  in: %s", err, strings.TrimSpace(statement))
	}

	return value
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

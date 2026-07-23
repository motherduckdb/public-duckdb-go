package duckdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// wideTestMap returns a 26-entry MAP(VARCHAR, VARCHAR) value shaped like a
// telemetry resource-attributes map, including unicode keys and values.
func wideTestMap() Map {
	m := Map{
		"service.name":         "checkout",
		"k8s.pod.name":         "checkout-6d5f9b7c8-x2x9p",
		"k8s.namespace.name":   "ns-prod-checkout",
		"deployment.unïcode":   "prod-üs-east-1",
		"telemetry.sdk.name":   "opentelemetry",
		"telemetry.sdk.lang":   "go",
		"host.name":            "ip-10-20-30-40.ec2.internal",
		"cloud.provider":       "aws",
		"cloud.region":         "us-east-1",
		"日本語キー":                "日本語の値",
		"container.image.name": "785715327372.dkr.ecr.us-east-1.amazonaws.com/checkout",
	}
	for i := len(m); i < 26; i++ {
		m[fmt.Sprintf("attr.filler.%02d", i)] = fmt.Sprintf("value-%02d-abcdefghijklmnop", i)
	}
	return m
}

// TestAppenderMapRoundTrip verifies MAP column content survives the appender
// byte-for-byte across the shapes that matter: empty vs NULL, single entry,
// wide string maps with unicode, and non-string key/value types.
// asMap normalizes a scanned MAP value for content comparison: this base's
// scanner returns OrderedMap where the pre-rebase base returned Map. These
// tests assert round-trip CONTENT, not the scan type.
func asMap(t require.TestingT, v any) Map {
	switch m := v.(type) {
	case Map:
		return m
	case OrderedMap:
		out := Map{}
		keys, vals := m.Keys(), m.Values()
		for i := range keys {
			out[keys[i]] = vals[i]
		}
		return out
	default:
		require.Fail(t, fmt.Sprintf("unexpected scanned map type %T", v))
		return nil
	}
}

func TestAppenderMapRoundTrip(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `
	CREATE TABLE test (
		id BIGINT,
		attrs MAP(VARCHAR, VARCHAR),
		nums MAP(INTEGER, DOUBLE)
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	wide := wideTestMap()
	rows := []struct {
		attrs Map
		nums  Map
	}{
		{attrs: wide, nums: Map{int32(1): float64(1.5), int32(2): float64(-2.25)}},
		{attrs: Map{}, nums: Map{}},
		{attrs: nil, nums: nil},
		{attrs: Map{"single": "entry"}, nums: Map{int32(-7): float64(0)}},
		{attrs: Map{"nil-value": nil}, nums: Map{int32(0): nil}},
	}
	for i, r := range rows {
		if r.attrs == nil {
			require.NoError(t, a.AppendRow(int64(i), nil, nil))
			continue
		}
		require.NoError(t, a.AppendRow(int64(i), r.attrs, r.nums))
	}
	require.NoError(t, a.Flush())

	res, err := db.QueryContext(context.Background(), `SELECT attrs, nums FROM test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		// Scan into any: Map's sql.Scanner rejects NULL, and NULL-vs-empty is
		// exactly what this test must distinguish.
		var attrs, nums any
		require.NoError(t, res.Scan(&attrs, &nums))
		if rows[i].attrs == nil {
			require.Nil(t, attrs, "attrs row %d", i)
			require.Nil(t, nums, "nums row %d", i)
			i++
			continue
		}
		require.Equal(t, rows[i].attrs, asMap(t, attrs), "attrs row %d", i)
		require.Equal(t, rows[i].nums, asMap(t, nums), "nums row %d", i)
		i++
	}
	require.NoError(t, res.Err())
	require.Equal(t, len(rows), i)

	// Empty map and NULL map must remain distinct in storage.
	var nullCount, emptyCount int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT count(*) FILTER (WHERE attrs IS NULL), count(*) FILTER (WHERE attrs IS NOT NULL AND cardinality(attrs) = 0) FROM test`).
		Scan(&nullCount, &emptyCount))
	require.Equal(t, 1, nullCount)
	require.Equal(t, 1, emptyCount)
}

// TestAppenderMapManyRows crosses the vector-size (2048) and chunk boundaries
// with alternating empty/full maps, verifying list-offset bookkeeping: every
// row must read back exactly what was appended.
func TestAppenderMapManyRows(t *testing.T) {
	c, db, conn, a := prepareAppender(t, appenderTypeDefault, `CREATE TABLE test (id BIGINT, attrs MAP(VARCHAR, VARCHAR))`)
	defer cleanupAppender(t, c, db, conn, a)

	const count = 5000
	wide := wideTestMap()
	for i := 0; i < count; i++ {
		switch i % 3 {
		case 0:
			require.NoError(t, a.AppendRow(int64(i), wide))
		case 1:
			require.NoError(t, a.AppendRow(int64(i), Map{}))
		case 2:
			require.NoError(t, a.AppendRow(int64(i), Map{"i": fmt.Sprintf("%d", i)}))
		}
	}
	require.NoError(t, a.Flush())

	res, err := db.QueryContext(context.Background(), `SELECT id, attrs FROM test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var id int64
		var attrs any
		require.NoError(t, res.Scan(&id, &attrs))
		require.Equal(t, int64(i), id)
		switch i % 3 {
		case 0:
			require.Equal(t, wide, asMap(t, attrs), "row %d", i)
		case 1:
			require.Equal(t, Map{}, asMap(t, attrs), "row %d", i)
		case 2:
			require.Equal(t, Map{"i": fmt.Sprintf("%d", i)}, asMap(t, attrs), "row %d", i)
		}
		i++
	}
	require.NoError(t, res.Err())
	require.Equal(t, count, i)
}

// BenchmarkAppenderWideMap replicates the telemetry-exporter row shape: two
// scalars plus a 26-entry MAP(VARCHAR, VARCHAR) column. rows/op = 1.
func BenchmarkAppenderWideMap(b *testing.B) {
	c, db, conn, a := prepareAppender(b, appenderTypeDefault, `CREATE TABLE test (id BIGINT, v DOUBLE, attrs MAP(VARCHAR, VARCHAR))`)
	defer cleanupAppender(b, c, db, conn, a)

	wide := wideTestMap()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.AppendRow(int64(i), float64(i), wide); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := a.Flush(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkAppenderProductionRow approximates the OTel exporter's gauge row:
// BIGINT id, two TIMESTAMPs, six VARCHAR scalars, a 26-key resource map and a
// 5-key datapoint map. Ground truth for where appender time goes after the
// setMap direct-write change.
func BenchmarkAppenderProductionRow(b *testing.B) {
	c, db, conn, a := prepareAppender(b, appenderTypeDefault, `CREATE TABLE test (
		id BIGINT, ts TIMESTAMP, start_ts TIMESTAMP,
		metric_name VARCHAR, metric_description VARCHAR, metric_unit VARCHAR,
		service_name VARCHAR, scope_name VARCHAR, scope_version VARCHAR,
		res_attrs MAP(VARCHAR, VARCHAR), dp_attrs MAP(VARCHAR, VARCHAR))`)
	defer cleanupAppender(b, c, db, conn, a)

	wide := wideTestMap()
	dp := Map{
		"state": "used", "pod": "checkout-6d5f9b7c8-x2x9p",
		"namespace": "ns-prod-checkout", "container": "server", "device": "/dev/nvme0n1",
	}
	ts := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a.AppendRow(int64(i), ts, ts,
			"kube_pod_container_resource_requests", "Resource requests by container", "unit",
			"checkout", "otelcol/prometheusreceiver", "0.154.0",
			wide, dp); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := a.Flush(); err != nil {
		b.Fatal(err)
	}
}

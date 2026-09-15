SELECT id, small, big, ratio, score, name, code, note, tag, ok,
       CAST(at AS DateTime('UTC')) AS at,
       CAST(at_ns AS DateTime64(9, 'UTC')) AS at_ns,
       day, old_day, amount, wide,
       CAST(attrs AS Map(LowCardinality(String), String)) AS attrs,
       payload
FROM values(
  'id Int64, small Int8, big UInt64, ratio Float32, score Float64, name String, code LowCardinality(String), note Nullable(String), tag FixedString(4), ok Bool, at DateTime, at_ns DateTime64(9), day Date, old_day Date32, amount Decimal(18, 2), wide Decimal(38, 6), attrs Map(String, String), payload String',
  (1, -5, 9223372036854775807, 0.5, nan, 'héllo "world"\nline2', 'lc', NULL, 'ab', true, '2026-09-14 10:00:00', '2026-09-14 10:00:00.123456789', '2026-09-14', '1900-01-01', -123.45, 1234567890.123456, map('host','a','zone','b'), '{"k": [1, 2]}'),
  (2, 127, 0, -1.25, -inf, '', 'lc', 'note', 'abcd', false, '1970-01-01 00:00:00', '1969-12-31 23:59:59.999999999', '1970-01-01', '2299-12-31', 0, -0.000001, map(), '3')
)

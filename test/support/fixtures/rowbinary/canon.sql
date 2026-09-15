SELECT * FROM values(
  'id Int64, name Nullable(String), score Nullable(Float64), ok Nullable(Bool), at Nullable(DateTime64(6)), day Nullable(Date32), amount Nullable(Decimal(18, 2)), attrs Map(String, String), payload Nullable(String)',
  (1, 'a', 1.5, true, '2026-09-14 10:00:00.000001', '2026-09-14', 12.5, map('k','v'), '{"x":1}'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, map(), NULL)
)

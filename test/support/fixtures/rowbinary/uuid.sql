SELECT * FROM values(
  'id UUID, maybe Nullable(UUID)',
  ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', NULL),
  ('00000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
  ('00000000-0000-0000-0001-000000000005', '0123abcd-4567-89ef-fedc-ba9876543210')
)

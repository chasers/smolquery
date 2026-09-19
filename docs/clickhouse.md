# ClickHouse HTTP insert

smolquery takes the insert a ClickHouse HTTP client sends, on its own
listener, so a producer that writes `RowBinary` to ClickHouse can point at
smolquery instead. The `:clickhouse` role starts the edge
(`SmolqueryClickHouse`, T-477). The insert itself is T-476.

```sh
curl -sS "http://127.0.0.1:8123/?query=INSERT%20INTO%20logs.events%20FORMAT%20RowBinaryWithNamesAndTypes" \
  -H "X-ClickHouse-User: default" \
  -H "X-ClickHouse-Key: $SMOLQUERY_API_KEY" \
  --data-binary @rows.bin
```

## The listener

- **Port.** `8123`, ClickHouse's HTTP port (`18123` in dev). `SMOLQUERY_CLICKHOUSE_IP` and `SMOLQUERY_CLICKHOUSE_PORT` move it. It binds `127.0.0.1` by default.
- **URL length.** The statement travels in the URL, so a request line may be up to 1 MiB, ClickHouse's `http_max_uri_size`. A longer one is a bare 414. Each URL parameter holds one value; a list or a map (`database[x]=1`) is a 400 `BAD_ARGUMENTS`.
- **Plain HTTP.** The edge does not terminate TLS. Bind it beyond the node only behind a TLS terminator.
- **Password.** The API key (`SMOLQUERY_API_KEY`), or `SMOLQUERY_CLICKHOUSE_PASSWORD` when set. A node with the `:clickhouse` role and neither refuses to boot.
- **How a client sends the password.** The ways ClickHouse takes it: the `X-ClickHouse-Key` header, HTTP basic auth, or the `password` parameter. A `Bearer` token works too. When a request carries more than one, the first in that order wins. The user name is accepted as given.
- **Health checks.** `GET /` and `GET /ping` answer `Ok.` without a password, as ClickHouse does.
- **Refused before the body.** A missing or wrong password is a 401 with code 516 `AUTHENTICATION_FAILED`, on every path. Next, the insert is counted against the in-flight limit before its body is read, as the API's insert is. Over the limit is a 429 with code 202 and `retry-after: 1`.
- **Inserts only.** A query sent with `GET` is a 501 `NOT_IMPLEMENTED`. Any other path is a 404.
- **Metrics.** Requests are counted in `smolquery_clickhouse_requests_total`, by status class.

## The insert

`POST /?query=INSERT INTO db.table (columns) FORMAT RowBinary`. The body holds the rows.

- **Statement.** `INSERT INTO [TABLE] [db.]table [(column, ...)] [SETTINGS name = value, ...] FORMAT name`. Keywords are case-insensitive, and names may be backquoted or double-quoted. Nothing but whitespace and one `;` may follow the format name.
- **Table.** The dataset is the statement's qualifier, else the `database` parameter, else the `X-ClickHouse-Database` header, else `default`.
- **Formats.** `RowBinary`, `RowBinaryWithNames` and `RowBinaryWithNamesAndTypes`. A plain `RowBinary` body carries no types, so each column is read as the type its smolquery column implies: `Int64`, `Float64`, `String`, `Bool`, `DateTime64(6)` or `DateTime64(9)`, `Date32`, `Decimal(P, S)`, `Map(String, String)`, wrapped in `Nullable` when the column is nullable. A producer with other ClickHouse types, such as `UUID` or `UInt8`, must send `RowBinaryWithNamesAndTypes`.
- **Timestamps.** `DateTime` and `DateTime64(P)` go into a `TIMESTAMP` column at microseconds, dropping any digit past the sixth, or into a `TIMESTAMP_NS` column with all nine. `TIMESTAMP_NS` holds 1677-09-22 to 2262-04-11 23:47:16.854775806, and a value outside that refuses its row. The zone argument is ignored: values are UTC.
- **All or nothing.** When any row is refused, none is written, as in ClickHouse. A nonzero `input_format_allow_errors_num` or `input_format_allow_errors_ratio` writes the other rows instead, without enforcing the number.
- **Settings.** `insert_deduplication_token` is the idempotency key, as `insertId` is on the API's NDJSON insert. Every other setting is accepted and ignored. A statement's `SETTINGS` clause wins over the URL.
- **Limits.** The body and the NDJSON its rows decode to are each held to `SMOLQUERY_INSERT_MAX_NDJSON_BYTES`, the API's limit. Over it is a 413; send smaller blocks. The edge keeps its own in-flight counter, sized by `SMOLQUERY_INSERT_MAX_IN_FLIGHT_BYTES` as the API's is. A node running both the `api` and `clickhouse` roles holds two counters, each with that limit.
- **Answers.** A 200 has an empty body and an `X-ClickHouse-Summary` header. A failure answers ClickHouse's text form, `Code: N. DB::Exception: message. (NAME)`, with `X-ClickHouse-Exception-Code`: 62 `SYNTAX_ERROR`, 73 `UNKNOWN_FORMAT` on a 404, as ClickHouse answers it, 60 `UNKNOWN_TABLE`, 81 `UNKNOWN_DATABASE`, 33 `CANNOT_READ_ALL_DATA` for a body that ends mid-row, 117 `INCORRECT_DATA` for rows that cannot be written, 202 `TOO_MANY_SIMULTANEOUS_QUERIES` on a 429, 516 `AUTHENTICATION_FAILED` on a 401, and 1002 `UNKNOWN_EXCEPTION` on a 503. A retryable answer carries `retry-after`. A statement other than an insert is a 501 `NOT_IMPLEMENTED`.

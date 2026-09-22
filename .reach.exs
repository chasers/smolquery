[
  checks: [source_paths: ["lib"]],
  layers: [
    api: "Smolquery.Api.*",
    ingest_service: "Smolquery.IngestService.*",
    buffer_service: "Smolquery.BufferService.*",
    storage_service: "Smolquery.StorageService.*",
    query_service: "Smolquery.QueryService.*",
    pg: "SmolqueryPg.*",
    clickhouse: "SmolqueryClickHouse.*",
    victoriametrics: "SmolqueryVictoriaMetrics.*"
  ],
  deps: [
    forbidden: [
      {:buffer_service, :ingest_service},
      {:buffer_service, :storage_service},
      {:buffer_service, :query_service},
      {:storage_service, :ingest_service},
      {:storage_service, :query_service},
      {:query_service, :ingest_service},
      {:ingest_service, :storage_service},
      {:ingest_service, :query_service},
      {:buffer_service, :api},
      {:storage_service, :api},
      {:query_service, :api},
      {:ingest_service, :api},
      {:buffer_service, :pg},
      {:storage_service, :pg},
      {:query_service, :pg},
      {:ingest_service, :pg},
      {:buffer_service, :clickhouse},
      {:storage_service, :clickhouse},
      {:query_service, :clickhouse},
      {:ingest_service, :clickhouse},
      {:buffer_service, :victoriametrics},
      {:storage_service, :victoriametrics},
      {:query_service, :victoriametrics},
      {:ingest_service, :victoriametrics}
    ]
  ],
  calls: [
    forbidden: [
      {"Smolquery.*", ["String.to_atom"]}
    ]
  ]
]

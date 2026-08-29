[
  checks: [source_paths: ["lib"]],
  layers: [
    api: "Smolquery.Api.*",
    ingest_service: "Smolquery.IngestService.*",
    buffer_service: "Smolquery.BufferService.*",
    storage_service: "Smolquery.StorageService.*",
    query_service: "Smolquery.QueryService.*",
    pg: "SmolqueryPg.*"
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
      {:ingest_service, :pg}
    ]
  ],
  calls: [
    forbidden: [
      {"Smolquery.*", ["String.to_atom"]}
    ]
  ]
]

[
  checks: [source_paths: ["lib"]],
  layers: [
    ingest_service: "Smolquery.IngestService.*",
    buffer_service: "Smolquery.BufferService.*",
    storage_service: "Smolquery.StorageService.*",
    query_service: "Smolquery.QueryService.*"
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
      {:ingest_service, :query_service}
    ]
  ],
  calls: [
    forbidden: [
      {"Smolquery.*", ["String.to_atom"]}
    ]
  ]
]

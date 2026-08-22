FROM elixir:1.20-slim@sha256:e500da1777164f9be05f7ffc0fe06cdb692f453bf7d651755e72310ec8a92eed AS build

ENV MIX_ENV=prod
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mix local.hex --force && mix local.rebar --force

COPY mix.exs mix.lock ./
COPY config/config.exs config/prod.exs config/
RUN mix deps.get --only prod
RUN mix deps.compile

COPY lib lib
COPY config/runtime.exs config/
RUN mix compile
RUN mix run --no-start -e 'name = Smolquery.DockerSmoke; {:ok, _pid} = Smolquery.Engine.start_link(name: name, extensions: []); version = Smolquery.Engine.version(name); expected = "v" <> Smolquery.DuckDB.version(); if version != expected, do: raise("expected DuckDB #{expected}, got #{version}"); IO.puts("DuckDB #{version}")'
ENV SMOLQUERY_EXTENSION_DIRECTORY=/app/duckdb-extensions
RUN mix run --no-start -e '{:ok, _pid} = Smolquery.Engine.start_link(name: Smolquery.DockerExtensions, extensions: [:httpfs, :json, :ducklake, :aws, :postgres]); IO.puts("installed: " <> Enum.join(File.ls!(hd(Path.wildcard("/app/duckdb-extensions/*/*"))), " "))'
COPY assets assets
COPY priv/static priv/static
RUN mix assets.deploy
COPY rel rel
RUN mix release

FROM elixir:1.20-slim@sha256:e500da1777164f9be05f7ffc0fe06cdb692f453bf7d651755e72310ec8a92eed

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/_build/prod/rel/smolquery ./
COPY --from=build /app/duckdb-extensions ./duckdb-extensions

ENV HOME=/data \
    SMOLQUERY_DATA_DIR=/data \
    SMOLQUERY_EXTENSION_DIRECTORY=/app/duckdb-extensions

VOLUME /data
EXPOSE 4000 4002

ENTRYPOINT ["/app/bin/smolquery"]
CMD ["start"]

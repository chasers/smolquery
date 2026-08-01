FROM elixir:1.20-slim AS build

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
RUN mix run --no-start -e 'Adbc.download_driver!(:duckdb)'
RUN mix release

FROM elixir:1.20-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/_build/prod/rel/smolquery ./

ENV HOME=/data \
    SMOLQUERY_DATA_DIR=/data

VOLUME /data
EXPOSE 4000

ENTRYPOINT ["/app/bin/smolquery"]
CMD ["start"]

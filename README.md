# gtfs-rt-mcp

MCP server that lets an agent read GTFS static schedules and GTFS-Realtime feeds. It is the agent-facing companion to a visual GTFS explorer: register a feed, then ask where a train is, how late it is, which track it was assigned, what the next departures are, and which alerts are active.

It works with any agency. A feed can be a static zip plus realtime protobuf URLs, or a feed-set document like the one published by [Amtrak-GTFS-RT](https://github.com/sohampatwardhan/Amtrak-GTFS-RT) at `/v1/feed-set.json`.

## Why Rust

The server is Rust because that is the same stack as the Amtrak GTFS-RT publisher, and it fits a long-running agent tool better than the other options that were open:

- The official Rust MCP SDK (`rmcp`) speaks stdio and streamable HTTP, and `prost` decodes the same GTFS-Realtime protobuf the publisher encodes.
- Static GTFS lands in an in-memory SQLite database (bundled, so the Pi does not need a system SQLite). Realtime messages stay decoded in memory and refresh on a TTL.
- One binary, about 9.4 MB stripped, links only glibc, libm, and libgcc. No JVM and no Python runtime. On a Raspberry Pi that means a small RSS and a predictable pause when a tool runs, without a garbage collector scanning a multi-million-row stop-time table.
- Python is a reasonable language for a one-shot script, and the GTFS libraries are convenient, but a resident interpreter plus protobuf and HTTP stacks is a larger and less predictable process for a Pi that is also publishing or proxying feeds.
- Java can host GTFS libraries, and it pays for that with a JVM that is large next to this binary.
- Swift is a good fit on macOS and less so for a small Linux MCP server with stdio and streamable HTTP.

The same crate builds on linux/arm64 and macOS. Release builds use thin LTO, `opt-level = "z"`, and stripped symbols.

## Architecture

```mermaid
flowchart LR
  Agent["MCP client"] -->|"stdio, or HTTP POST /mcp"| Server["gtfs-rt-mcp"]
  Server --> Catalog["Catalog"]
  Catalog --> Static[("in-memory SQLite<br/>stops, routes, trips, stop_times")]
  Catalog --> Cache["decoded TripUpdates,<br/>VehiclePositions, Alerts"]
  Catalog -->|"re-read on the realtime TTL"| Manifest["feed-set.json"]
  Manifest -->|"only when generation_id changes"| Generation["immutable generation<br/>static.zip and protobufs"]
  Catalog -->|"direct mode, refetch on TTL"| Agency["agency static zip<br/>and realtime URLs"]
  Generation --> Agency
```

Static GTFS is parsed once into SQLite. Feed-set generations are immutable: the server re-fetches `feed-set.json` when the realtime TTL expires and downloads `static.zip` and the protobufs only when `generation_id` or `static_version` changes. Direct agency URLs are mutable, so those bytes are fetched again on the TTL. A failed refresh keeps the last good copy and records `last_error` on `feed_health`.

## Build

Rust 1.88 or newer. The VM used for development was rustc 1.99.

Debian, Ubuntu, or Raspberry Pi OS:

```bash
sudo apt-get update
sudo apt-get install -y build-essential protobuf-compiler pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo build --release --locked
./target/release/gtfs-rt-mcp --version
```

macOS:

```bash
brew install protobuf
cargo build --release --locked
```

`protobuf-compiler` (`protoc`) is required at build time. The GTFS-Realtime schema is vendored in `proto/gtfs-realtime.proto` (Apache-2.0, from the MobilityData GTFS-Realtime spec). Nothing is downloaded during `cargo test`.

## Run

Logs go to stderr. stdio mode must not print on stdout.

```bash
# Speak MCP on stdin/stdout. Preload a feed-set. Header values are never logged.
gtfs-rt-mcp \
  --feed-set https://127.0.0.1:8080/v1/feed-set.json \
  --http-headers '{"Authorization":"Bearer ${TOKEN}"}' \
  stdio

# Or preload a public static zip and realtime protobufs.
gtfs-rt-mcp \
  --static https://cdn.mbta.com/MBTA_GTFS.zip \
  --trip-updates https://cdn.mbta.com/realtime/TripUpdates.pb \
  --vehicle-positions https://cdn.mbta.com/realtime/VehiclePositions.pb \
  --alerts https://cdn.mbta.com/realtime/Alerts.pb \
  stdio

# Streamable HTTP on 127.0.0.1:8091, MCP at /mcp, liveness at /healthz.
gtfs-rt-mcp http
```

Environment variables mirror the flags: `GTFS_RT_MCP_FEED_SET`, `GTFS_RT_MCP_FEED_ID`, `GTFS_RT_MCP_STATIC`, `GTFS_RT_MCP_TRIP_UPDATES`, `GTFS_RT_MCP_VEHICLE_POSITIONS`, `GTFS_RT_MCP_ALERTS`, `GTFS_RT_MCP_RT_TTL`, `GTFS_RT_MCP_HTTP_HEADERS` (a JSON object), `GTFS_RT_MCP_BIND`, `GTFS_RT_MCP_ALLOWED_HOST`, and `GTFS_RT_MCP_LOG` or `RUST_LOG`.

There are no built-in credentials. If an agency requires a header, pass it at runtime. `feed_health` returns header names only.

HTTP accepts `Host: localhost`, `127.0.0.1`, and `::1` by default. Add `--allowed-host your-pi.local` when a non-loopback client connects.

## Docker

The image is multi-arch. Buildx builds each platform natively (or under QEMU) from the same Dockerfile, so a Raspberry Pi and an amd64 host produce their own binaries.

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t gtfs-rt-mcp .
docker run --rm -p 8091:8091 gtfs-rt-mcp
```

The runtime image is `debian:bookworm-slim` plus CA certificates, running as uid 10001. The process listens on `0.0.0.0:8091` inside the container. From the host, `http://127.0.0.1:8091/mcp` matches the default allowed hosts.

The release binary built on this x86_64 machine is **9,843,096 bytes (9.4 MB)**, stripped, dynamically linked to glibc. The amd64 image built from this Dockerfile (`gtfs-rt-mcp:0.1.0`) uses **140 MB** of disk once `debian:bookworm-slim` is unpacked, and its content size is **35.2 MB**. `docker run --rm --entrypoint gtfs-rt-mcp gtfs-rt-mcp:0.1.0 --version` prints `gtfs-rt-mcp 0.1.0`.

## MCP clients

Cursor `.cursor/mcp.json`, stdio:

```json
{
  "mcpServers": {
    "gtfs-rt": {
      "command": "gtfs-rt-mcp",
      "args": ["--feed-set", "http://127.0.0.1:8080/v1/feed-set.json", "stdio"]
    }
  }
}
```

Cursor, streamable HTTP (start `gtfs-rt-mcp http` first):

```json
{
  "mcpServers": {
    "gtfs-rt": {
      "url": "http://127.0.0.1:8091/mcp"
    }
  }
}
```

Claude Desktop `claude_desktop_config.json` uses the same `mcpServers` shape. Put a token in the environment of the client config (`env`), not in the binary.

The server name is `gtfs-rt-mcp`. Instructions on `initialize` tell the model to register a feed, then use the tools below. Responses are compact JSON. Page with `offset` until `next_offset` is null.

## Tools

| Tool | What it answers |
| --- | --- |
| `register_feed` | Load a feed-set URL or a static zip plus realtime URLs. Replaces the same `feed_id`. |
| `list_feeds` | Registered feeds and freshness. |
| `unregister_feed` | Drop a feed and its caches. |
| `refresh_feed` | Re-read now. Feed-set static files reload only when the generation changes. |
| `feed_health` | Counts, header timestamps, age, TTL, last error, generation. |
| `search_stops` | `stop_id`, code, or name. Default 20, max 100. |
| `search_routes` | `route_id`, short name, or long name. |
| `trip_status` | Where a trip is. `trip` is a `trip_id` or `trip_short_name` (train number). Optional `stop` fills delay and track. |
| `trip_schedule` | Stop sequence with scheduled and estimated times. |
| `departure_board` | Station board: static schedule plus realtime delay, cancellation, and track. |
| `vehicle_positions` | Live positions. Filter by route, trip, vehicle, or stop. |
| `trip_updates` | Delays and `assigned_stop_id`. A stop filter matches `stop_id` or the static stop at that `stop_sequence`. |
| `alerts` | Cause, effect, severity, periods, informed entities, and clipped text. |
| `decode_protobuf` | One FeedMessage from a path, http(s) URL, or base64 string. Does not register a feed. |

`feed_id` may be omitted when one feed is registered. `at_unix` pins the clock; omit it to use the current time. Delays are seconds, and positive means late. `track` is the static `platform_code` of `assigned_stop_id` when that stop exists, otherwise the scheduled platform.

Limits: list tools default to 20 rows and cap at 100. `trip_updates` keeps 8 stop updates per trip (max 40). Alert text defaults to 400 characters (max 2000). `decode_protobuf` returns 20 entities (max 100).

Up to 16 feeds can be registered. Realtime TTL defaults to 15 seconds (1–3600). Direct static zips refresh every 6 hours unless `static_ttl_secs` is `0`. Downloads are capped at 250 MB for a static zip and 80 MB for a realtime message.

## Feed-set documents

`register_feed` with `feed_set` reads a JSON document and uses only the URLs inside it. It does not invent `/v1/generations/{id}/...` paths.

```json
{
  "generation_id": "gen-1",
  "generated_at_unix": 1768494600,
  "static_version": "harbor-1",
  "urls": {
    "static_zip": "/v1/generations/gen-1/static.zip",
    "trip_updates": "/v1/generations/gen-1/trip-updates.pb",
    "vehicle_positions": "/v1/generations/gen-1/vehicle-positions.pb",
    "alerts": "/v1/generations/gen-1/alerts.pb"
  }
}
```

Resolution:

- `http://` and `https://` are used as written.
- A path that starts with `/` is joined to the manifest's origin. `https://feeds.example/v1/feed-set.json` plus `/v1/generations/gen-1/static.zip` becomes `https://feeds.example/v1/generations/gen-1/static.zip`.
- A relative path is resolved next to the manifest. From a file, that is the manifest's directory.
- A manifest loaded over HTTP rejects `file://` URLs, so a remote document cannot point the server at local files. Loopback HTTP is allowed, because a publisher may listen only on localhost.

The artifact names above match the Amtrak publisher (`static.zip`, `trip-updates.pb`, `vehicle-positions.pb`, `alerts.pb`). Other names work when the document says so. Track assignments follow the publisher's convention: `stop_time_properties.assigned_stop_id` such as `NHV:track:12`, often with `stop_sequence` set and `stop_id` omitted. Static GTFS can carry a parent station and platform stops whose `platform_code` is the track label. Stop updates match `stop_sequence` first, then `stop_id`. A delay carries forward from the previous stop until the next update; `NO_DATA`, `SKIPPED`, and an assigned platform do not. An absolute `time` wins over `delay`.

## Example: Harbor fixture

The checked-in Harbor Line fixture is a small feed-set with no network. Clock is 2026-01-15 11:30 America/New_York (`at_unix` 1768494600). Train 177 (`trip_short_name`) is the Northeast Corridor trip `177-BOS`, in transit to New Haven.

`trip_status` with `trip: "177"`, `stop: "NHV"`, `at_unix: 1768494600` returns, in part:

```json
{
  "trip_id": "177-BOS",
  "trip_short_name": "177",
  "route_short_name": "NEC",
  "delay_sec": 480,
  "delay_label": "8 min late",
  "vehicle": {
    "status": "IN_TRANSIT_TO",
    "stop_id": "NHV",
    "lat": 41.25,
    "lon": -72.95,
    "vehicle": { "label": "177" }
  },
  "at_stop": {
    "stop_id": "NHV",
    "scheduled_departure_local": "12:04",
    "estimated_departure_local": "12:12",
    "arrival_delay_sec": 420,
    "departure_delay_sec": 480,
    "assigned_stop_id": "NHV:track:12",
    "platform_code": "12",
    "track": "12"
  }
}
```

`departure_board` at New Haven for the next 120 minutes, ignoring anything that left more than 5 minutes ago, returns train 177 on track 12 at 12:12, then bus B1 at 12:30. Train 66 left at 11:15, so it is outside the window. Asking for the platform stop `NHV:track:12` still shows 177, because the board expands to the parent station and sibling platforms.

`alerts` with `route: "NEC"` returns one alert, header `Track work between NYP and NHV`, effect `SIGNIFICANT_DELAYS`. A local-bus detour in the same feed does not match.

`decode_protobuf` on `trip-updates.pb` shows the second stop update's `assigned_stop_id` of `NHV:track:12`.

## Example: MBTA

This was run from the development VM against the public MBTA feeds (no API key): `https://cdn.mbta.com/MBTA_GTFS.zip` and the realtime protobufs under `https://cdn.mbta.com/realtime/`. The static feed was Fall 2026 (`10,292` stops, `3,104,937` stop times, `117,449` trips). The dominant agency is MBTA in `America/New_York`. At fetch time the vehicle feed had 349 entities and was 1 second old; trip updates had 1,184 entities.

`search_stops` for `South Station` ranks the station `place-sstat` first.

`departure_board` at that station, `window_minutes: 90`, `limit: 5`, expanded 25 platforms (not pathway nodes) and returned 83 departures. The first page started with:

- Providence/Stoughton train 889, scheduled 22:45
- Red Line to Alewife, 22:47
- Silver Line Way SLW, 22:53
- SL2 from Design Center, reported 7 min early (`delay_sec: -398`)
- SL1 from Logan, `delay_label: "on time"` (`delay_sec: 22`)

`vehicle_positions` with `limit: 3` included bus 0803 on route 220, `IN_TRANSIT_TO` Washington St @ Foster St, occupancy `MANY_SEATS_AVAILABLE`.

`alerts` with `active_only: true` and `limit: 3` returned 3 of 97, including the Providence Station stairwell closure on the Providence/Stoughton Line.

Re-run that check locally with:

```bash
cargo test --test live_mbta -- --ignored --nocapture
```

CI does not run it. `cargo test` uses the Harbor fixture and loopback feed-set servers only.

## Tests and CI

```bash
cargo test --locked
```

GitHub Actions (`.github/workflows/ci.yml`) installs `protoc`, runs `cargo test --locked`, and builds the release binary. No network is required for the default tests.

## Layout

- `src/mcp.rs` — tool schemas and stdio / streamable HTTP serving
- `src/catalog.rs` — feed registry, TTL refresh, last-good cache
- `src/gtfs.rs` — static zip to in-memory SQLite
- `src/realtime.rs` — protobuf decode, delay labels, platform fields
- `src/query.rs` — trip status, boards, vehicles, alerts
- `src/source.rs` — feed-set URL resolution and capped downloads
- `tests/fixtures/harbor/` — tiny GTFS text files
- `proto/gtfs-realtime.proto` — vendored spec, Apache-2.0

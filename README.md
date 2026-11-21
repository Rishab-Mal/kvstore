# kvstore

A multithreaded, Redis-style key-value server in C++20. Clients connect over
TCP and send `GET` / `SET` / `DEL`. The server answers from a sharded in-memory
hash map, and every write goes into an append-only log that is replayed on
startup, so acknowledged writes survive a `kill -9`.

## Build

Linux or WSL2, g++ 12+ or clang++ 15+, CMake 3.20+, GoogleTest.

```bash
sudo apt install build-essential cmake libgtest-dev netcat-openbsd

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure

# Sanitizer builds
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DKV_SANITIZER=address && cmake --build build-asan -j
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DKV_SANITIZER=thread  && cmake --build build-tsan -j
setarch -R ./build-tsan/kv_tests   # setarch -R: TSan needs ASLR off on recent kernels
```

## Run

```bash
./build/kvserver --port 6380 --aof data.aof --fsync everysec
```

| Flag | Default | Meaning |
|---|---|---|
| `--port N` | 6380 | TCP port |
| `--bind ADDR` | 127.0.0.1 | IPv4 address to listen on |
| `--threads N` | 64 | worker threads, which is also the max number of concurrent clients |
| `--shards N` | 16 | independently locked store shards (`1` = one global lock) |
| `--aof PATH` / `--no-aof` | data.aof | append-only log, or memory only |
| `--fsync P` | everysec | `always`, `everysec`, or `no` |
| `--batch on\|off` | on | one `send` per batch of commands, or one per command |

```console
$ nc localhost 6380
SET greeting hello world
OK
GET greeting
VALUE hello world
DEL greeting
DELETED
GET greeting
NOT_FOUND
```

## Protocol

One command per `\n`-terminated line (a trailing `\r` is ignored, so telnet works).

| Request | Reply |
|---|---|
| `SET <key> <value>` | `OK` |
| `GET <key>` | `VALUE <value>` or `NOT_FOUND` |
| `DEL <key>` | `DELETED` or `NOT_FOUND` |
| anything else | `ERR <reason>` |

Command names are case-insensitive and keys are case-sensitive. Keys can't
contain whitespace. The value is everything after the key, so it can contain
spaces, but it can't start with whitespace or contain a newline. The on-disk
log is binary-safe; only the text protocol has these limits.

## Architecture

```
            accept loop (main thread)
                   │ new fd
                   ▼
   ┌──────── ThreadPool (N workers) ────────┐
   │ worker: recv → split lines → parse →   │
   │         execute → send replies         │
   └───────────────────┬────────────────────┘
                       │ get / set / del
                       ▼
   Store: shard[mix(hash(key)) % S], each { shared_mutex, unordered_map }
                       │ set / del (under the shard lock)
                       ▼
   Aof: append record → write() ─→ data.aof
        fsync policy: always (group commit) | everysec (background thread) | no
```

| File | Responsibility |
|---|---|
| `src/store.*` | Sharded hash map. `shared_lock` for reads, `unique_lock` for writes |
| `src/protocol.*` | `parse(line) → Command`, `execute(store, cmd) → reply`; parsing is separate from execution |
| `src/aof.*` | Log record encoding, CRC32, append, fsync policies, replay and torn-tail repair |
| `src/server.cpp` | Sockets, per-connection framing, thread pool wiring, clean shutdown on SIGINT/SIGTERM |
| `src/thread_pool.hpp` | Fixed workers, a task queue, a condition variable; drains the queue and joins on destruction |
| `src/fd.hpp` | RAII file descriptor, `write_all` / `send_all` |
| `bench/loadgen.cpp` | Multi-threaded load generator with latency percentiles |

### Log format

```
[op:1][key_len:4][key][val_len:4][val][crc32:4]     little-endian, CRC over all preceding bytes
```

On startup the log is replayed from the beginning. Replay stops at the first
record that is incomplete or fails its checksum, and the file is truncated
there, so a write torn by a crash is dropped instead of corrupting state.
Run `tests/crash_recovery.sh build/kvserver [policy]` for the kill -9 test.

## Benchmarks

`./build/loadgen --clients 64 --read-ratio 0.5 --requests 50000` prints
throughput, p50/p99/p99.9, and a ready-made table row. Methodology: Release
build, server and load generator on the same machine, every key preloaded,
64-byte values, 100K keys. Every configuration ran 3 times per session, for 5
sessions (15 runs each). Each row shows the median session, picked by ops/sec.

Machine: Intel Core i9-13900H (20 logical CPUs), 7 GiB RAM, WSL2 (kernel 6.6.87.2).
Server and load generator share the machine, so these are relative numbers,
not a claim about dedicated hardware. Runs of the same configuration varied
by up to ~30%, so compare ratios rather than exact figures.

### Baseline (step 5)

| Clients | Read/Write | Pipeline | fsync | ops/sec | p50 (µs) | p99 (µs) | p99.9 (µs) | ops/sec range (5 sessions) |
|---|---|---|---|---|---|---|---|---|
| 8 | 90/10 | 1 | everysec | 40,473 | 188.7 | 373.6 | 623.8 | 37.7K–44.5K |
| 64 | 90/10 | 1 | everysec | 339,569 | 87.6 | 1,365.5 | 4,118.1 | 301K–399K |
| 64 | 50/50 | 1 | everysec | 121,063 | 224.7 | 4,228.4 | 11,672.3 | 86.5K–132K |
| 16 | 0/100 | 1 | always | 1,294 | 11,615.7 | 38,619.7 | 54,641.7 | 1.04K–1.52K |
| 16 | 0/100 | 1 | everysec | 58,369 | 217.2 | 946.3 | 1,490.5 | 52.0K–60.3K |
| 16 | 0/100 | 1 | no | 59,300 | 214.9 | 919.9 | 1,389.2 | 55.4K–59.6K |

- `fsync=always` costs ~45x in write throughput, since every acknowledged write waits for the disk.
- `everysec` and `no` are within run-to-run noise of each other. Both `write()` before
  replying, and `everysec`'s fsync runs on a background thread, off the request path.

### Optimizations (step 6)

Each row changes one thing, and the flags reproduce the "before" case. Both
sides of each comparison ran back to back in the same session.

| Change | Workload | Before → after flag | ops/sec before | ops/sec after | p99 before (µs) | p99 after (µs) | Speedup range (5 sessions) |
|---|---|---|---|---|---|---|---|
| Sharded store (16 shards) | 50/50, 64 clients | `--shards 1` → `--shards 16` | 31,482 | 126,657 (**4.0x**) | 18,316.1 | 3,765.0 (**−79%**) | 3.0x–4.6x |
| Batched replies | 90/10, 16 clients, pipeline 16 | `--batch off` → `--batch on` | 364,714 | 442,537 (**1.21x**) | 1,670.0 | 1,856.0 (+11%) | 1.12x–1.42x |

- **Sharding:** with one lock, 64 clients doing 50% writes serialize behind it.
  16 shards gave a 3.0x–4.6x throughput gain in every session and cut p99 by
  66%–83%. This is the largest and most repeatable gain in the project.
- **Batching:** one `send` per batch of 16 replies instead of 16 separate sends
  raised throughput 1.12x–1.42x. It also made latency slightly worse in every
  session (p99 +3% to +21%): replies wait until the whole batch has been
  processed, where before each one went out as soon as it was ready. That's the
  classic throughput-versus-latency trade-off.

Profile with `perf record -g ./build/kvserver ...` while loadgen runs, then
`perf report`. For readable call stacks, build with
`-DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer`.

## Design decisions

**Writes are logged while the shard lock is held.** Two concurrent `SET`s to
the same key are applied in some order, and replay has to reproduce that order.
Appending under the shard's exclusive lock makes log order equal apply order for
each key. If the append fails, the map is left unchanged and the client gets `ERR`.

**Every write hits `write()` before `OK`; fsync is a separate policy.** Once
`write()` returns, the bytes are in the kernel's page cache, so they survive the
server process dying (`kill -9`) under every policy. fsync only matters when the
machine itself dies. `always` fsyncs before replying, `everysec` fsyncs from a
background thread (Redis's default trade-off, and the default here), and `no`
leaves it to the OS.

**`fsync=always` uses group commit.** Each record gets a sequence number. A
thread waiting for durability fsyncs everything written so far and publishes the
new high-water mark; threads whose records are already covered skip their own
fsync. A pipelined batch from one client also waits once, not once per command.

**Sharding.** With one `shared_mutex`, every writer serializes, and even readers
contend on the lock's shared counter. Splitting the map into independently
locked shards (chosen by a remixed hash, each shard aligned to its own cache
line) lets operations on different keys proceed in parallel.

**Batching.** A client that pipelines commands can deliver many in one `recv`.
The server executes every complete line in the buffer and sends all the replies
in one `send`, instead of one system call per reply.

**Framing.** TCP is a byte stream, so each connection keeps a buffer of
unparsed bytes: complete lines are executed and any partial line waits for the
next `recv`. A partial line that grows past 16 MiB gets `ERR line too long`
and the connection is closed.

**A torn append is cut off right away.** If a `write()` fails halfway through a
record, the log is truncated back to the last complete record. Otherwise later
records would land behind the broken one, and replay (which stops at the first
bad record) would silently lose all of them.

### Known limitation: one thread per connection

A connection holds a pool worker for its entire life, so `--threads N` means at
most N clients are served at once; client N+1 is accepted but waits until
someone disconnects. Idle connections still pin a thread and its stack. This is
the motivation for step 7: non-blocking sockets with `epoll`, where a few event
loop threads multiplex thousands of connections.

## Roadmap

- [ ] epoll event loop (one per core), benchmarked against the thread pool at 64 / 1K / 10K connections
- [ ] `SET key value EX seconds` with lazy plus sampled background expiry
- [ ] Log compaction: snapshot to a new file, buffer writes during the rewrite, atomic rename
- [ ] RESP protocol, so `redis-cli` and `redis-benchmark` can talk to the server

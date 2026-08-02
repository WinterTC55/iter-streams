# Redesigning the Web Streams API

**Date:** January 2026

This is a **prototype** of an alternative streams API and implementation designed to address
fundamental ergonomics and performance flaws in the Web Streams API model.

This is a **work in progress** and should not be considered a final design.
The API and implementation are evolving as we explore design tradeoffs and gather feedback.

**DO NOT USE THIS API IN PRODUCTION** — it is not stable and may change significantly.

## Why

The WHATWG Streams Standard[^1] ("Web streams") provides a foundation for streaming data on the web
platform, but suffers from significant usability issues stemming from its design predating async
iteration and from attempting to serve too many use cases with a single complex abstraction. This
document critiques Web Streams and presents design principles for a simpler, iterable-based
alternative.

A complete reference implementation is available in this repository demonstrating these principles.
See [DESIGN.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/DESIGN.md) for the API design and [API.md](API.md) for the complete API reference.

## Table of Contents

1. [Design Principles for Improvement](#1-design-principles-for-improvement)
2. [Detailed Design and Reference Implementation](#2-detailed-design-and-reference-implementation)
3. [Benchmarks](#3-benchmarks)

## 1. Design Principles for Improvement

The new streams API follows these principles:

### P1: Streams Are Just Iterables

No custom Stream class with hidden state. Streams are `AsyncIterable<Uint8Array[]>` or `Iterable<Uint8Array[]>` - standard JavaScript iteration protocols. The `for await...of` syntax is the idiomatic way to consume a stream.

```javascript
// Streams are standard async iterables
for await (const chunks of readable) {
  for (const chunk of chunks) {
    process(chunk);
  }
}
```

### P2: Transforms Are Pull-Through

Transforms only execute when the consumer pulls. No eager evaluation, no hidden buffering. Data flows on-demand from source through transforms to consumer.

```javascript
// Nothing executes until iteration begins
const output = Stream.pull(source, compress, encrypt);

// Transforms execute as we iterate
for await (const chunks of output) { /* ... */ }
```

### P3: Explicit Backpressure

Strict by default, reject on overflow. All buffering has explicit limits with configurable overflow policies:

- `'strict'` (default) - Writes reject when buffer full
- `'block'` - Writes wait until space available
- `'drop-oldest'` - Drop oldest buffered chunks to make room
- `'drop-newest'` - Drop incoming chunks when buffer full

```javascript
const { writer, readable } = Stream.push({
  highWaterMark: 10,
  backpressure: 'drop-oldest'
});
```

Pull-streams implement inherent backpressure by only producing data when the consumer requests it.

Push-streams enforce strictly following backpressure policies on writes by default.

### P4: Batched Chunks

Iterables yield `Uint8Array[]` (arrays of chunks) to amortize async overhead. This batching reduces promise creation per-chunk:

```javascript
for await (const chunks of readable) {  // chunks is Uint8Array[]
  for (const chunk of chunks) {         // chunk is Uint8Array
    process(chunk);
  }
}
```

Writers accept `Uint8Array[]` for batched/vectorized writes:

```javascript
await writer.writev([chunk1, chunk2, chunk3]); // Write multiple chunks at once
```

### P5: Explicit Multi-Consumer

No built-in `tee()` with hidden unbounded buffers. Instead, explicit multi-consumer patterns:

- `Stream.broadcast()` - Push model with writer pushing to all consumers
- `Stream.share()` - Pull model with shared source pulled on demand

Both require explicit buffer limits and backpressure policies:

```javascript
// Share with explicit buffer management
const shared = Stream.share(source, {
  highWaterMark: 100,
  backpressure: 'strict'
});
const consumer1 = shared.pull();
const consumer2 = shared.pull(decompress);
```

For both models, a single cursor-based queue manages data flow to multiple consumers, ensuring predictable memory usage.

### P6: Clean Sync/Async Separation

Complete parallel sync versions for CPU-bound workloads. No ambiguity about which path executes:

| Async | Sync |
|-------|------|
| `Stream.pull()` | `Stream.pullSync()` |
| `Stream.pipeTo()` | `Stream.pipeToSync()` |
| `Stream.bytes()` | `Stream.bytesSync()` |
| `Stream.text()` | `Stream.textSync()` |
| `Stream.share()` | `Stream.shareSync()` |

The design allows for a synchronous fast path when all components are synchronous, eliminating unnecessary promise overhead.

### P7: Non-Negative desiredSize

Unlike Web Streams, `desiredSize` is always >= 0 or null (closed). The API enforces strict backpressure semantics - no negative values indicating "over capacity."

### P8: Bytes Only

The API deals exclusively with bytes (`Uint8Array`). Strings are UTF-8 encoded automatically. No "value streams" - use async iterables directly for streaming arbitrary JS values.

### P9: Chunk-Oriented, Not Byte-Oriented

Operations work on chunks (contiguous byte sequences) rather than individual bytes. No BYOB (bring your own buffer), no min/max read sizes, no partial fill handling.

| Approach        | Overhead                                  | Complexity                           |
| --------------- | ----------------------------------------- | ------------------------------------ |
| Byte-oriented   | Per-byte processing, frequent allocations | High (BYOB requests, partial fills)  |
| Chunk-oriented  | Per-chunk processing, batch allocations   | Low (simple iteration over chunks)   |

### P10: No Transfer/Detach Semantics

The API does not automatically transfer or detach buffers. Use `ArrayBuffer.transfer()` explicitly when needed. This keeps the API simple and predictable while allowing developers to opt into transfer semantics when performance requires it.

### P11: No forced/hidden promise chains

Implementers may optimize away promise chains when operations complete synchronously. The API does not mandate promise creation in all cases, allowing for efficient synchronous fast paths.

---

## 2. Detailed Design and Reference Implementation

A complete reference implementation is available in this repository demonstrating the API design principles above. The implementation is tested (194 tests) and benchmarked against Web Streams.

### Documentation

| Document | Description |
|----------|-------------|
| [DESIGN.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/DESIGN.md) | Comprehensive API design document covering push streams, pull pipelines, transforms, consumers, multi-consumer patterns, and protocol extensibility |
| [API.md](API.md) | Complete API reference for the reference implementation |
| [USAGE.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/USAGE.md) | Guide to using the API with code examples |
| [REQUIREMENTS.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md) | Detailed list of testable assertions |
| [MIGRATION.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/MIGRATION.md) | Guide for migrating from Web Streams API to this API |
| [MIGRATION-NODEJS.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/MIGRATION-NODEJS.md) | Guide for migrating from Node.js streams to this API |
| [DESIGN-TRADEOFFS.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/DESIGN-TRADEOFFS.md) | Discussion of design tradeoffs and rationale |
| [COMPLETENESS-ANALYSIS.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/COMPLETENESS-ANALYSIS.md) | Analysis of feature completeness |
| [TRANSFER-INTEGRATION.md](https://github.com/WinterTC55/iter-streams/blob/main/docs/TRANSFER-INTEGRATION.md) | Design discussion: Transfer Protocol integration for ownership semantics |

### Code

| Resource | Description |
|----------|-------------|
| [src/](src/) | TypeScript source code (types.ts, push.ts, from.ts, pull.ts, consumers.ts, broadcast.ts, share.ts) |
| [samples/](samples/) | Sample files demonstrating API usage patterns |
| [benchmarks/](benchmarks/) | Benchmark suites comparing performance with Web Streams |

### Installing from npm

```bash
npm install new-streams
```

### Running the Reference Implementation

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests (194 tests)
npm test

# Run samples
npx tsx samples/01-basic-creation.ts

# Run benchmarks
npm run benchmark

# Run html samples/benchmark server
npm run html-samples
```

---

## 3. Benchmarks

Note that these numbers are illustrative and will vary by environment and
implementation status. They are provided here to demonstrate the performance
characteristics of the new streams API compared to Web Streams and Node.js
streams and should not be taken as definitive.

No assertion is made that these benchmarks represent real-world workloads.
There is no intention to claim that the new streams API is universally faster than
any specific Web streams or Node.js streams implementation in all scenarios.
The benchmarks were run on a MacBook Pro (M1 Pro, 16GB RAM) using Node.js v24.x.

```
──────────────────────────────────────────────────────────────────────
Running: 01-throughput.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Raw Throughput
Measuring data flow speed through streams
Comparing: New Streams vs Web Streams vs Node.js Streams
New API uses batched iteration (Uint8Array[]) for amortized overhead
(minimum 20 samples, 3 seconds per test)

Running: Large chunks...
Running: Medium chunks...
Running: Small chunks...
Running: Tiny chunks...
Running: Async iteration...
Running: Generator source...

==================================================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==================================================================================================================================
Scenario                         | New Stream         | Web Stream         | Node Stream        | New vs Web           | New vs Node
---------------------------------+--------------------+--------------------+--------------------+----------------------+---------------------
Large chunks (64KB x 500)        | 4.87 GB/s          | 6.03 GB/s          | 5.83 GB/s          | ~same                | ~same
Medium chunks (8KB x 2000)       | 5.72 GB/s          | 4.72 GB/s          | 3.83 GB/s          | ~same                | ~same
Small chunks (1KB x 5000)        | 4.64 GB/s          | 2.09 GB/s          | 779.10 MB/s        | 2.22x faster         | 5.96x faster
Tiny chunks (100B x 10000)       | 1.18 GB/s          | 275.86 MB/s        | 124.15 MB/s        | 4.28x faster         | 9.50x faster
Async iteration (8KB x 1000)     | 310.78 GB/s        | 19.05 GB/s         | 12.39 GB/s         | 16.31x faster        | 25.08x faster
Generator source (8KB x 1000)    | 19.71 GB/s         | 14.29 GB/s         | 9.88 GB/s          | ~same                | 1.99x faster
==================================================================================================================================

New Stream vs Web Stream: 3 faster, 0 slower, 3 within noise
New Stream vs Node Stream: 4 faster, 0 slower, 2 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 02-push-streams.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Push Stream Performance
Measuring concurrent write/read patterns
Comparing: New Streams vs Web Streams vs Node.js Streams
(minimum 15-20 samples, 3 seconds per test)

Running: Concurrent push (medium chunks)...
Running: Many small writes...
Running: Batch writes...
Running: Push + async iteration...

==================================================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==================================================================================================================================
Scenario                         | New Stream         | Web Stream         | Node Stream        | New vs Web           | New vs Node
---------------------------------+--------------------+--------------------+--------------------+----------------------+---------------------
Concurrent push (4KB x 1000)     | 174.92 MB/s        | 181.45 MB/s        | 166.52 MB/s        | ~same                | ~same
Many small writes (64B x 10000)  | 111.66 MB/s        | 85.41 MB/s         | 77.15 MB/s         | 1.31x faster         | ~same
Batch writes (512B x 20 x 200)   | 137.98 MB/s        | 145.09 MB/s        | 136.07 MB/s        | ~same                | ~same
Push + async iter (2KB x 1000)   | 162.86 MB/s        | 166.54 MB/s        | 162.72 MB/s        | ~same                | ~same
==================================================================================================================================

New Stream vs Web Stream: 1 faster, 0 slower, 3 within noise
New Stream vs Node Stream: 0 faster, 0 slower, 4 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 03-transforms.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Transform Performance
Measuring data transformation speed
Comparing: New Streams vs Web Streams vs Node.js Streams
(minimum 15-20 samples, 3 seconds per test)

Running: Identity transform...
Running: XOR transform...
Running: Expanding transform...
Running: Chained transforms...
Running: Async transform...

==================================================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==================================================================================================================================
Scenario                         | New Stream         | Web Stream         | Node Stream        | New vs Web           | New vs Node
---------------------------------+--------------------+--------------------+--------------------+----------------------+---------------------
Identity transform (8KB x 1000)  | 291.01 GB/s        | 4.89 GB/s          | 4.77 GB/s          | 59.46x faster        | 61.00x faster
XOR transform (8KB x 500)        | 1.45 GB/s          | 1.08 GB/s          | 1.12 GB/s          | ~same                | ~same
Expanding 1:2 (4KB x 500)        | 70.51 GB/s         | 4.38 GB/s          | 5.84 GB/s          | 16.08x faster        | 12.07x faster
Chained 3x (8KB x 500)           | 163.89 GB/s        | 2.02 GB/s          | 4.47 GB/s          | 81.24x faster        | 36.64x faster
Async transform (8KB x 300)      | 132.62 GB/s        | 4.31 GB/s          | 4.67 GB/s          | 30.78x faster        | 28.40x faster
==================================================================================================================================

New Stream vs Web Stream: 4 faster, 0 slower, 1 within noise
New Stream vs Node Stream: 4 faster, 0 slower, 1 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 04-pipelines.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Pipeline Performance
Measuring full pipeline throughput
Comparing: New Streams vs Web Streams vs Node.js Streams
(minimum 15-20 samples, 3 seconds per test)

Running: Simple pipeline...
Running: Pipeline with transform...
Running: Multi-stage pipeline...
Running: High-frequency small chunks...

==================================================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==================================================================================================================================
Scenario                         | New Stream         | Web Stream         | Node Stream        | New vs Web           | New vs Node
---------------------------------+--------------------+--------------------+--------------------+----------------------+---------------------
Simple pipeline (8KB x 1000)     | 408.26 GB/s        | 8.63 GB/s          | 48.04 GB/s         | 47.30x faster        | 8.50x faster
Pipeline + XOR (8KB x 1000)      | 1.47 GB/s          | 1.16 GB/s          | 1.13 GB/s          | ~same                | ~same
Multi-stage 3x (8KB x 500)       | 180.55 GB/s        | 2.09 GB/s          | 4.36 GB/s          | 86.51x faster        | 41.39x faster
High-freq (64B x 20000)          | 2.97 GB/s          | 207.93 MB/s        | 180.83 MB/s        | 14.30x faster        | 16.44x faster
==================================================================================================================================

New Stream vs Web Stream: 3 faster, 0 slower, 1 within noise
New Stream vs Node Stream: 3 faster, 0 slower, 1 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 05-tee-branching.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Share/Branching Performance
Measuring stream branching efficiency
New API: share() for pull-model multi-consumer
Web Streams: tee() for branching
(minimum 15-20 samples, 3 seconds per test)

Running: Single share (2 readers)...
Running: Share with transforms...
Running: Small chunks share...

==============================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==============================================================================================================
Scenario                         | New Stream             | Web Stream             | Difference         | Significance
---------------------------------+------------------------+------------------------+--------------------+---------------
Share 2 readers (4KB x 500)      | 2.53 GB/s              | 2.46 GB/s              | 1.03x faster       | within noise
Share + transforms (4KB x 500)   | 887.61 MB/s            | 539.06 MB/s            | 1.65x faster       | within noise
Small chunks share (256B x 5000  | 2.20 GB/s              | 168.88 MB/s            | 13.01x faster      | significant
==============================================================================================================

Summary: 1 faster, 0 slower, 2 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 06-consumption.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Consumption Methods
Measuring different ways to read stream data
Comparing: New Streams vs Web Streams vs Node.js Streams
(minimum 20 samples, 3 seconds per test)

Running: bytes() collection...
Running: text() decode...
Running: Async iteration...
Running: Direct iterator consumption...

==================================================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==================================================================================================================================
Scenario                         | New Stream         | Web Stream         | Node Stream        | New vs Web           | New vs Node
---------------------------------+--------------------+--------------------+--------------------+----------------------+---------------------
bytes() (16KB x 500)             | 4.47 GB/s          | 8.47 GB/s          | 7.03 GB/s          | ~same                | ~same
text() (1KB chunks)              | 1.68 GB/s          | 856.26 MB/s        | 857.42 MB/s        | ~same                | ~same
Async iteration (8KB x 1000)     | 370.00 GB/s        | 21.50 GB/s         | 23.20 GB/s         | 17.21x faster        | 15.95x faster
Iterator loop (8KB x 1000)       | 277.88 GB/s        | 27.84 GB/s         | 24.45 GB/s         | 9.98x faster         | 11.36x faster
==================================================================================================================================

New Stream vs Web Stream: 2 faster, 0 slower, 2 within noise
New Stream vs Node Stream: 2 faster, 0 slower, 2 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 07-sync-generators.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Sync vs Async Sources
Comparing array sources vs async generators
(minimum 20 samples, 3 seconds per test)

Running: Large chunks (sync vs async source)...
Running: Medium chunks (sync vs async source)...
Running: Small chunks (sync vs async source)...
Running: Tiny chunks (sync vs async source)...
Running: Many tiny chunks (extreme case)...

==============================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==============================================================================================================
Scenario                         | New Stream             | Web Stream             | Difference         | Significance
---------------------------------+------------------------+------------------------+--------------------+---------------
Large chunks (64KB x 500)        | 4.04 GB/s              | 4.15 GB/s              | 1.03x slower       | within noise
Medium chunks (8KB x 2000)       | 6.36 GB/s              | 7.28 GB/s              | 1.14x slower       | within noise
Small chunks (1KB x 5000)        | 5.30 GB/s              | 2.50 GB/s              | 2.12x faster       | within noise
Tiny chunks (100B x 10000)       | 2.37 GB/s              | 311.45 MB/s            | 7.61x faster       | significant
Many tiny (10B x 50000)          | 317.52 MB/s            | 29.33 MB/s             | 10.83x faster      | significant
==============================================================================================================

Summary: 2 faster, 0 slower, 3 within noise
Samples per benchmark: 100

──────────────────────────────────────────────────────────────────────
Running: 08-pipeline-sync.ts
──────────────────────────────────────────────────────────────────────

Benchmark: pullSync vs pull vs Web Streams
Comparing synchronous pipeline processing performance
(minimum 30 samples, 3 seconds per test)

Running: Passthrough (no transforms)...
Running: Single transform...
Running: Chain of 3 transforms...
Running: Many small chunks...
Running: Tiny chunks (extreme case)...

==================================================================================================================================
BENCHMARK RESULTS - Sync Pipeline Comparison
==================================================================================================================================
Scenario                     | pullSync           | pull (async)       | Web Streams        | Sync vs Async    | Sync vs Web
-----------------------------+--------------------+--------------------+--------------------+------------------+-----------------
Passthrough (8KB x 1000)     | 335.42 GB/s        | 293.19 GB/s        | 24.22 GB/s         | 1.1x faster      | 13.8x faster
Single transform (4KB x 100  | 224.82 MB/s        | 229.66 MB/s        | 194.17 MB/s        | 1.0x slower      | 1.2x faster
3 transforms (4KB x 500)     | 401.01 MB/s        | 398.66 MB/s        | 292.58 MB/s        | 1.0x faster      | 1.4x faster
Small chunks (256B x 10000)  | 16.82 GB/s         | 21.57 GB/s         | 182.21 MB/s        | 1.3x slower      | 92.3x faster
Tiny chunks (64B x 20000)    | 7.11 GB/s          | 7.19 GB/s          | 201.79 MB/s        | 1.0x slower      | 35.2x faster
==================================================================================================================================

Average speedup of pullSync vs pull: 1.0x
Average speedup of pullSync vs Web Streams: 28.8x

Note: pullSync returns a sync generator (no async overhead)
      pull and Web Streams use async iteration

──────────────────────────────────────────────────────────────────────
Running: 09-sync-async-comparison.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Sync vs Async API Comparison
Comparing sync path (fromSync + pullSync + bytesSync) vs async path
(minimum 30 samples, 3 seconds per test)

Running: bytes() consumption...
Running: text() consumption...
Running: Pipeline with identity transform...
Running: Iteration consumption...
Running: Many tiny chunks...

==================================================================================================================================
BENCHMARK RESULTS - Sync vs Async API Comparison
==================================================================================================================================
Scenario                     | Sync Path        | Async (sync src) | Async (async src) | Sync vs Async    | Sync vs AsyncGen
-----------------------------+------------------+------------------+------------------+------------------+-----------------
bytes() (8KB x 2000)         | 5.00 GB/s        | 4.76 GB/s        | 7.00 GB/s        | 1.0x faster      | 1.4x slower
text() (1KB chunks)          | 1.71 GB/s        | 2.03 GB/s        | 1.11 GB/s        | 1.2x slower      | 1.5x faster
Transform (4KB x 1000)       | 5.46 GB/s        | 6.02 GB/s        | 3.10 GB/s        | 1.1x slower      | 1.8x faster
Iteration (8KB x 1000)       | 281.11 GB/s      | 371.53 GB/s      | 24.27 GB/s       | 1.3x slower      | 11.6x faster
Tiny chunks (64B x 20000)    | 1.79 GB/s        | 2.11 GB/s        | 189.77 MB/s      | 1.2x slower      | 9.4x faster
==================================================================================================================================

Average speedup of sync path vs async (sync source): 0.9x
Average speedup of sync path vs async (async generator): 5.0x

Recommendation: Use sync APIs (fromSync, pullSync, bytesSync) when source data is synchronously available.

──────────────────────────────────────────────────────────────────────
Running: 10-advanced-features.ts
──────────────────────────────────────────────────────────────────────

Benchmark: Advanced Features Performance
Tests: broadcast vs share, merge, pipeTo, backpressure policies, stateful transforms
(minimum 15 samples, 2-3 seconds per test)


--- Section 1: broadcast() vs share() ---
Running: broadcast() with 2 consumers...
Running: broadcast/share with transforms...

--- Section 2: merge() ---
Running: merge() 2 streams...
Running: merge() 4 streams...

--- Section 3: pipeTo() ---
Running: pipeTo() async...
Running: pipeToSync()...

--- Section 4: Backpressure Policies ---
Running: backpressure policy comparison...

--- Section 5: Stateful Transforms ---
Running: stateful vs stateless transforms...

==============================================================================================================
BENCHMARK RESULTS (higher throughput = better)
==============================================================================================================
Scenario                         | New Stream             | Web Stream             | Difference         | Significance
---------------------------------+------------------------+------------------------+--------------------+---------------
broadcast vs share (2 consumers  | 1.10 GB/s              | 2.36 GB/s              | 2.15x slower       | within noise
broadcast vs share (w/ transfor  | 649.16 MB/s            | 879.53 MB/s            | 1.35x slower       | within noise
merge() 2 streams (4KB x 250 ea  | 5.78 GB/s              | 5.35 GB/s              | 1.08x faster       | within noise
pipeTo vs pipeTo (4KB x 500)     | 50.71 GB/s             | 4.44 GB/s              | 11.43x faster      | significant
pipeToSync vs pipeTo             | 92.68 GB/s             | 42.19 GB/s             | 2.20x faster       | within noise
Stateless vs Stateful transform  | 1.00 GB/s              | 3.40 GB/s              | 3.40x slower       | significant
==============================================================================================================

Summary: 1 faster, 1 slower, 4 within noise
Samples per benchmark: 100

================================================================================
Merge 4 Streams
================================================================================
Test                                     | Time               | Throughput
-----------------------------------------+--------------------+-------------------
New Stream merge(4)                      | 324.79µs           | 6.31 GB/s
================================================================================

================================================================================
Backpressure Policies (broadcast)
================================================================================
Test                                     | Time               | Throughput
-----------------------------------------+--------------------+-------------------
strict                                   | 296.41µs           | 3.45 GB/s
drop-oldest                              | 267.54µs           | 3.83 GB/s
drop-newest                              | 255.00µs           | 4.02 GB/s
================================================================================
```

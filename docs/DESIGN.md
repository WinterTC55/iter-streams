# Stream API Design Document

**Status:** Implemented (Reference Implementation)
**Date:** January 2026

This document describes the design of the new streams API based on iterables with explicit backpressure handling. For the API reference, see [API.md](API.md).

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Core Concepts](#2-core-concepts)
3. [Type System](#3-type-system)
4. [Push Stream](#4-push-stream)
5. [Stream Factories](#5-stream-factories)
6. [Pull Pipeline](#6-pull-pipeline)
7. [Write To](#7-write-to)
8. [Consumers](#8-consumers)
9. [Multi-Consumer](#9-multi-consumer)
10. [Protocol Extensibility](#10-protocol-extensibility)
11. [WebIDL Definitions](#11-webidl-definitions)

---

## 1. Design Principles

See the [README.md](README.md) for motivation and critique of the Web Streams API.

## 2. Core Concepts

### Batched Iteration Model

All stream iterables yield `Uint8Array[]` - arrays of chunks. This batching amortizes the overhead of async iteration:

```typescript
// Multiple chunks can be yielded per iteration
for await (const chunks of readable) {
  // chunks is Uint8Array[]
  for (const chunk of chunks) {
    // chunk is Uint8Array
    process(chunk);
  }
}
```

### Flush Signal

Transforms receive `null` as a flush signal indicating end-of-input:

```typescript
const transform = (chunks: Uint8Array[] | null) => {
  if (chunks === null) {
    // Flush - emit any buffered data
    return finalData;
  }
  // Normal processing
  return processedChunks;
};
```

### Writer vs Source

The API distinguishes between:
- **Writers** - Push data into a stream (`writer.write()`)
- **Sources** - Pull data from a stream (iteration)

`Stream.push()` creates a bonded pair connecting both models.

---

## 3. Type System

### Streamable Types

A **Streamable** is any object that implements iteration protocols and yields stream-compatible values:

```typescript
// Sync streamable - yields via Symbol.iterator
interface SyncStreamable {
  [Symbol.iterator](): Iterator<SyncStreamableYield>;
}

// Async streamable - yields via Symbol.asyncIterator
interface AsyncStreamable {
  [Symbol.asyncIterator](): AsyncIterator<AsyncStreamableYield>;
}

// Either sync or async
type Streamable = SyncStreamable | AsyncStreamable;
```

### Primitive Chunks

These types can be written directly:

```typescript
type PrimitiveChunk = string | ArrayBuffer | ArrayBufferView;
```

- `string` - UTF-8 encoded to `Uint8Array`
- `ArrayBuffer` - Wrapped as `Uint8Array` view (no copy)
- `ArrayBufferView` - Converted to `Uint8Array` view of same memory

### Transform Types

```typescript
// Options passed to transform functions by the pipeline
interface TransformCallbackOptions {
  /** Signal that fires when the pipeline is cancelled, errors, or
   *  the consumer stops iteration. */
  readonly signal: AbortSignal;
}

// Stateless transform - function called for each batch
type TransformFn = (
  chunks: Uint8Array[] | null,
  options: TransformCallbackOptions
) => AsyncTransformResult;

// Stateful transform - generator wrapping entire source
type StatefulTransformFn = (
  source: AsyncIterable<Uint8Array[] | null>,
  options: TransformCallbackOptions
) => AsyncIterable<TransformYield>;

// Transform object for stateful transforms
// Using an object (vs a plain function) indicates this is a stateful transform
interface TransformObject {
  transform: StatefulTransformFn;
}

// Function = stateless, Object = stateful
type Transform = TransformFn | TransformObject;
```

### Writer Interface

```typescript
interface WriteOptions {
  readonly signal?: AbortSignal;
}

interface Writer {
  readonly desiredSize: number | null;  // >= 0 or null (closed)

  write(chunk: Uint8Array | string, options?: WriteOptions): Promise<void>;
  writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void>;
  writeSync(chunk: Uint8Array | string): boolean;
  writevSync(chunks: (Uint8Array | string)[]): boolean;
  end(options?: WriteOptions): Promise<number>;
  endSync(): number;
  fail(reason?: any): Promise<void>;
  failSync(reason?: any): boolean;
}
```

---

## 4. Push Stream

`Stream.push()` creates a bonded writer and async iterable pair:

```typescript
function push(
  ...args: [...Transform[], PushStreamOptions?]
): { writer: Writer; readable: AsyncIterable<Uint8Array[]> }

interface PushStreamOptions {
  highWaterMark?: number;       // Max pending writes (default: 4)
  backpressure?: BackpressurePolicy;  // 'strict' | 'block' | 'drop-oldest' | 'drop-newest'
  signal?: AbortSignal;
}
```

**Behavior:**
- Default `highWaterMark: 4` provides moderate buffering for typical workloads
- Each `write()` or `writev()` call counts as a single slot
- Consumer termination propagates to writer (breaking iteration closes writer)

**Example:**
```typescript
const { writer, readable } = Stream.push({ highWaterMark: 10 });

// Producer
await writer.write("chunk 1");
await writer.write("chunk 2");
await writer.end();

// Consumer
for await (const chunks of readable) {
  for (const chunk of chunks) {
    console.log(new TextDecoder().decode(chunk));
  }
}
```

Backpressure in push streams is explicit and strict by default. Writers must await `write()` calls to avoid overwhelming the buffer.

The `backpressure` option allows alternative policies:
- `'strict'` (default) - Awaited writes wait for space; rejects when both slots buffer and pending writes queue are at capacity
- `'block'` - Async writes wait for buffer space; sync writes return false
- `'drop-oldest'` - Oldest buffered chunks are dropped to make room for new writes
- `'drop-newest'` - Incoming writes are dropped when buffer is full

The `highWaterMark` option controls the maximum number of pending writes allowed before backpressure is applied. Note that `highWaterMark` is specifically counted
towards write calls, not byte size. Why? Great question! Because it simplifies reasoning about backpressure in terms of discrete write operations rather than variable byte sizes, which can be more complex to manage. Also, since the data
is already in memory as `Uint8Array`, the overhead of managing byte sizes is less relevant and is not going to help save memory in typical use cases. For backpressure
signaling, what matters is whether the producer is overwhelming the consumer with too many write calls when data is not being consumed.

Developers can create their own implementations of `WriterInterface` to handle custom
backpressure strategies based on byte sizes if needed.

---

## 5. Stream Factories

### Stream.from()

Create an async iterable from various sources:

```typescript
function from(input: ByteInput | Streamable): AsyncIterable<Uint8Array[]>
```

**Inputs:**
- `string` - Single UTF-8 encoded chunk
- `ArrayBuffer` / `ArrayBufferView` - Single chunk
- `Iterable` / `AsyncIterable` - Normalized and wrapped

### Stream.fromSync()

Create a sync iterable:

```typescript
function fromSync(input: ByteInput | SyncStreamable): Iterable<Uint8Array[]>
```

**Example:**
```typescript
// From string
const readable = Stream.from("Hello, World!");

// From generator
const readable = Stream.from(async function* () {
  yield "chunk 1";
  yield "chunk 2";
}());

// From array (sync)
const syncReadable = Stream.fromSync(["a", "b", "c"]);
```

---

## 6. Pull Pipeline

### Stream.pull()

Pull-through async pipeline with variadic transforms:

```typescript
function pull(
  source: Streamable,
  ...args: [...Transform[], PullOptions?]
): AsyncIterable<Uint8Array[]>

interface PullOptions {
  signal?: AbortSignal;
}
```

**Behavior:**
- Transforms execute lazily when consumer pulls
- Sync sources work directly - no async wrapping overhead
- Error in transform N aborts the pipeline signal, notifying all transforms

### Stream.pullSync()

Sync pipeline for CPU-bound workloads:

```typescript
function pullSync(
  source: SyncStreamable,
  ...transforms: SyncTransform[]
): Iterable<Uint8Array[]>
```

**Example:**
```typescript
// Async pipeline
const output = Stream.pull(source, compress, encrypt, { signal });
for await (const chunks of output) { /* ... */ }

// Sync pipeline (no async overhead)
const syncOutput = Stream.pullSync(syncSource, transformSync);
for (const chunks of syncOutput) { /* ... */ }
```

---

## 7. Pipe To

### Stream.pipeTo()

Consume source and write to a writer with optional transforms:

```typescript
function pipeTo(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  ...args: [...Transform[], Writer, PipeToOptions?]
): Promise<number>  // Total bytes written

interface PipeToOptions {
  signal?: AbortSignal;
  preventClose?: boolean;   // Don't call writer.end() on completion
  preventFail?: boolean;    // Don't call writer.fail() on error
}
```

### Stream.pipeToSync()

Sync version:

```typescript
function pipeToSync(
  source: Iterable<Uint8Array[]>,
  ...args: [...SyncTransform[], SyncWriter, PipeToSyncOptions?]
): number
```

**Example:**
```typescript
// Pipe with transforms
const bytesWritten = await Stream.pipeTo(
  source,
  compress,
  encrypt,
  fileWriter,
  { signal: controller.signal }
);
```

---

## 8. Consumers

### Collection Methods

```typescript
// Collect all bytes
function bytes(source, options?: ConsumeOptions): Promise<Uint8Array>
function bytesSync(source, options?: ConsumeSyncOptions): Uint8Array

// Collect and decode as text
function text(source, options?: TextConsumeOptions): Promise<string>
function textSync(source, options?: TextConsumeSyncOptions): string

// Collect as ArrayBuffer
function arrayBuffer(source, options?: ConsumeOptions): Promise<ArrayBuffer>
function arrayBufferSync(source, options?: ConsumeSyncOptions): ArrayBuffer

interface ConsumeOptions {
  signal?: AbortSignal;
  limit?: number;  // Max bytes - throws if exceeded
}

interface TextConsumeOptions extends ConsumeOptions {
  encoding?: string;  // Default: 'utf-8'
}
```

### Tap Utility

Observe chunks without modifying:

```typescript
function tap(callback: TapCallbackAsync): Transform
function tapSync(callback: TapCallback): SyncTransform

type TapCallback = (chunks: Uint8Array[] | null) => void;
```

**Example:**
```typescript
// Hash while streaming
const hasher = crypto.createHash('sha256');
const output = Stream.pull(
  source,
  Stream.tap((chunks) => {
    if (chunks) for (const c of chunks) hasher.update(c);
  })
);
await Stream.pipeTo(output, destination);
console.log('Hash:', hasher.digest('hex'));
```

### Merge

Combine multiple sources by temporal order:

```typescript
function merge(
  ...args: [...AsyncIterable<Uint8Array[]>[], MergeOptions?]
): AsyncIterable<Uint8Array[]>
```

---

## 9. Multi-Consumer

### Broadcast (Push Model)

Creates a multi-consumer broadcast channel where a writer pushes to all consumers:

```typescript
function broadcast(options?: BroadcastOptions): {
  writer: Writer;
  channel: Broadcast;
}

interface Broadcast {
  push(...transforms?, options?): AsyncIterable<Uint8Array[]>;
  readonly consumerCount: number;
  readonly bufferSize: number;
  cancel(reason?: any): void;
}

interface BroadcastOptions {
  highWaterMark?: number;           // Default: 16
  backpressure?: BackpressurePolicy;  // Default: 'strict'
  signal?: AbortSignal;
}
```

**Buffer Model:**
- Single unified buffer shared by all consumers
- Each consumer has independent cursor position
- Buffer trimmed as slowest consumer advances
- Backpressure based on slowest consumer vs buffer limit

**Example:**
```typescript
const { writer, channel } = Stream.broadcast({ highWaterMark: 100 });

// Create consumers with different transforms
const consumer1 = channel.push();
const consumer2 = channel.push(decompress);

// Producer
for await (const chunk of source) {
  await writer.write(chunk);
}
await writer.end();

// Consumers receive independently
const [raw, decompressed] = await Promise.all([
  Stream.bytes(consumer1),
  Stream.bytes(consumer2)
]);
```

### Share (Pull Model)

Wraps a source for multi-consumer access with lazy pulling:

```typescript
function share(source, options?: ShareOptions): Share
function shareSync(source, options?: ShareSyncOptions): SyncShare

interface Share {
  pull(...transforms?, options?): AsyncIterable<Uint8Array[]>;
  readonly consumerCount: number;
  readonly bufferSize: number;
  cancel(reason?: any): void;
}
```

**Buffer Model:**
- Source pulled lazily on demand
- Buffer grows as consumers diverge
- Backpressure policy prevents unbounded growth:
  - `'strict'`: Fast consumer blocks until slow catches up
  - `'drop-oldest'`: Slow consumer misses data
  - `'drop-newest'`: All consumers miss data

**Example:**
```typescript
const shared = Stream.share(fileStream, { highWaterMark: 100 });

// Create consumers
const consumer1 = shared.pull();
const consumer2 = shared.pull(decompress);
const consumer3 = shared.pull(decompress, parse);

// All share the same source
const [raw, decompressed, parsed] = await Promise.all([
  Stream.bytes(consumer1),
  Stream.bytes(consumer2),
  Stream.bytes(consumer3)
]);
```

### Broadcast vs Share

| Aspect | `broadcast()` | `share()` |
|--------|---------------|-----------|
| Model | Push (writer -> consumers) | Pull (source -> consumers) |
| Data source | Writer pushes explicitly | Source pulled on demand |
| Create consumer | `channel.push()` | `share.pull()` |
| Sync version | No | Yes (`shareSync`) |
| Use case | Event sources, WebSocket | File, response body, iterables |

---

## 10. Protocol Extensibility

### ToStreamable / ToAsyncStreamable

Allow objects to convert themselves to streamable data:

```typescript
const toStreamable: unique symbol = Symbol.for('Stream.toStreamable');
const toAsyncStreamable: unique symbol = Symbol.for('Stream.toAsyncStreamable');

interface ToStreamable {
  [toStreamable](): SyncStreamableYield;
}

interface ToAsyncStreamable {
  [toAsyncStreamable](): AsyncStreamableYield | Promise<AsyncStreamableYield>;
}
```

**Example:**
```typescript
class JsonMessage {
  constructor(private data: object) {}

  [Symbol.for('Stream.toStreamable')]() {
    return JSON.stringify(this.data);
  }
}

// Used by Stream.from() to normalize input (not by writer.write())
const readable = Stream.from(new JsonMessage({ hello: "world" }));
const text = await Stream.text(readable);
```

### Broadcastable / Shareable

Allow objects to provide optimized multi-consumer implementations:

```typescript
const broadcastProtocol: unique symbol = Symbol.for('Stream.broadcastProtocol');
const shareProtocol: unique symbol = Symbol.for('Stream.shareProtocol');
const shareSyncProtocol: unique symbol = Symbol.for('Stream.shareSyncProtocol');

interface Broadcastable {
  [broadcastProtocol](options?: BroadcastOptions): Broadcast;
}

interface Shareable {
  [shareProtocol](options?: ShareOptions): Share;
}
```

---

## 11. WebIDL Definitions

The following WebIDL definitions describe the new streams API for potential standardization. These definitions capture the essential interfaces while allowing implementation flexibility.

### Core Types

```webidl
// Backpressure policy enumeration
enum BackpressurePolicy { "strict", "block", "drop-oldest", "drop-newest" };

// Byte chunk type - the fundamental unit of data
typedef Uint8Array ByteChunk;

// Batch of chunks - used for iteration to amortize async overhead
typedef sequence<ByteChunk> ChunkBatch;

// Input types that can be written to streams
typedef (Uint8Array or USVString) WritableChunk;
```

### Writer Interface

```webidl
dictionary WriteOptions {
  AbortSignal signal;
};

[Exposed=*]
interface Writer {
  // Backpressure indicator: >= 0 (slots available), null (closed/failed)
  readonly attribute long? desiredSize;

  // Async write operations
  Promise<undefined> write(WritableChunk chunk, optional WriteOptions options = {});
  Promise<undefined> writev(sequence<WritableChunk> chunks, optional WriteOptions options = {});

  // Sync write attempts - return true if successful, false if buffer full
  boolean writeSync(WritableChunk chunk);
  boolean writevSync(sequence<WritableChunk> chunks);

  // End stream - returns total bytes written
  Promise<unsigned long long> end(optional WriteOptions options = {});
  long long endSync();  // Returns -1 if cannot complete synchronously

  // Fail stream (put writer into error state)
  Promise<undefined> fail(optional any reason);
  boolean failSync(optional any reason);
};

[Exposed=*]
interface SyncWriter {
  readonly attribute long? desiredSize;

  boolean write(WritableChunk chunk);
  boolean writev(sequence<WritableChunk> chunks);
  unsigned long long end();
  undefined fail(optional any reason);
};
```

### Push Stream

```webidl
dictionary PushStreamOptions {
  unsigned long highWaterMark = 4;
  BackpressurePolicy backpressure = "strict";
  AbortSignal signal;
};

[Exposed=*]
interface PushStreamResult {
  readonly attribute Writer writer;
  readonly attribute ReadableByteStream readable;
};

// ReadableByteStream is an async iterable yielding ChunkBatch
[Exposed=*]
interface ReadableByteStream {
  async iterable<ChunkBatch>;
};

// SyncReadableByteStream is a sync iterable yielding ChunkBatch
[Exposed=*]
interface SyncReadableByteStream {
  iterable<ChunkBatch>;
};
```

### Transform Types

```webidl
// Options passed to transform functions by the pipeline
dictionary TransformCallbackOptions {
  required AbortSignal signal;
};

// Transform function signature (TypeScript-style for clarity)
// transform: (chunks: Uint8Array[] | null, options: TransformCallbackOptions) => TransformResult
callback TransformFunction = any (ChunkBatch? chunks, TransformCallbackOptions options);
callback StatefulTransformFunction = any (AsyncIterable source, TransformCallbackOptions options);

// TransformObject is always stateful (receives entire source as async iterable)
// Using an object vs a plain function indicates stateful transform
dictionary TransformObject {
  required StatefulTransformFunction transform;
};

// Function = stateless (called per batch), Object = stateful (receives entire source)
typedef (TransformFunction or TransformObject) Transform;
typedef (TransformFunction or TransformObject) SyncTransform;
```

### Pipeline Options

```webidl
dictionary PullOptions {
  AbortSignal signal;
};

dictionary PipeToOptions {
  AbortSignal signal;
  boolean preventClose = false;
  boolean preventFail = false;
};

dictionary PipeToSyncOptions {
  boolean preventClose = false;
  boolean preventFail = false;
};

dictionary ConsumeOptions {
  AbortSignal signal;
  unsigned long long limit;
};

dictionary ConsumeSyncOptions {
  unsigned long long limit;
};

dictionary TextConsumeOptions : ConsumeOptions {
  DOMString encoding = "utf-8";
};

dictionary TextConsumeSyncOptions : ConsumeSyncOptions {
  DOMString encoding = "utf-8";
};

dictionary MergeOptions {
  AbortSignal signal;
};
```

### Broadcast (Push-Model Multi-Consumer)

```webidl
dictionary BroadcastOptions {
  unsigned long highWaterMark = 16;
  BackpressurePolicy backpressure = "strict";
  AbortSignal signal;
};

[Exposed=*]
interface Broadcast {
  // Create consumer with optional transforms
  ReadableByteStream push(Transform... transforms, optional PullOptions options = {});

  readonly attribute unsigned long consumerCount;
  readonly attribute unsigned long bufferSize;

  undefined cancel(optional any reason);
};

[Exposed=*]
interface Share {
  // Create consumer with optional transforms
  ReadableByteStream pull(Transform... transforms, optional PullOptions options = {});

  readonly attribute unsigned long consumerCount;
  readonly attribute unsigned long bufferSize;

  undefined cancel(optional any reason);
};

[Exposed=*]
interface SyncShare {
  SyncReadableByteStream pull(SyncTransform... transforms);

  readonly attribute unsigned long consumerCount;
  readonly attribute unsigned long bufferSize;

  undefined cancel(optional any reason);
};
```

### Stream Namespace

```webidl
[Exposed=*]
namespace Stream {
  // Push stream factory
  PushStreamResult push(Transform... transforms, optional PushStreamOptions options = {});

  // Source factories
  ReadableByteStream from((USVString or BufferSource or object) input);
  SyncReadableByteStream fromSync((USVString or BufferSource or object) input);

  // Pull pipelines
  ReadableByteStream pull(object source, Transform... transforms, optional PullOptions options = {});
  SyncReadableByteStream pullSync(object source, SyncTransform... transforms);

  // Pipe to destination
  Promise<unsigned long long> pipeTo(
    object source,
    Transform... transforms,
    Writer destination,
    optional PipeToOptions options = {}
  );
  unsigned long long pipeToSync(
    object source,
    SyncTransform... transforms,
    SyncWriter destination,
    optional PipeToSyncOptions options = {}
  );

  // Async consumers
  Promise<Uint8Array> bytes(object source, optional ConsumeOptions options = {});
  Promise<USVString> text(object source, optional TextConsumeOptions options = {});
  Promise<ArrayBuffer> arrayBuffer(object source, optional ConsumeOptions options = {});

  // Sync consumers
  Uint8Array bytesSync(object source, optional ConsumeSyncOptions options = {});
  USVString textSync(object source, optional TextConsumeSyncOptions options = {});
  ArrayBuffer arrayBufferSync(object source, optional ConsumeSyncOptions options = {});

  // Combining
  ReadableByteStream merge(ReadableByteStream... sources, optional MergeOptions options = {});

  // Multi-consumer factories
  BroadcastResult broadcast(optional BroadcastOptions options = {});
  Share share(object source, optional ShareOptions options = {});
  SyncShare shareSync(object source, optional ShareSyncOptions options = {});

  // Observation transforms
  StatelessTransformFn tap(TapCallback callback);
  SyncStatelessTransformFn tapSync(SyncTapCallback callback);

  // Drain
  Promise<boolean>? ondrain(any drainable);

  // Async consumers (array)
  Promise<sequence<Uint8Array>> array(object source, optional ConsumeOptions options = {});
  sequence<Uint8Array> arraySync(object source, optional ConsumeSyncOptions options = {});

  // Duplex
  sequence<DuplexChannel> duplex(optional DuplexOptions options = {});
};

callback TapCallback = (undefined or Promise<undefined>) (ChunkBatch? chunks);
callback SyncTapCallback = undefined (ChunkBatch? chunks);
```

### Protocol Symbols

Protocol symbols are accessed via `Symbol.for()`, not as namespace attributes.
This allows third-party code to implement protocols without importing anything.

```
Symbol.for('Stream.toStreamable')       // Value conversion (sync)
Symbol.for('Stream.toAsyncStreamable')  // Value conversion (async)
Symbol.for('Stream.broadcastProtocol')  // Custom broadcast implementation
Symbol.for('Stream.shareProtocol')      // Custom share implementation
Symbol.for('Stream.shareSyncProtocol')  // Custom sync share implementation
Symbol.for('Stream.drainableProtocol')  // Drain notification
```



### Notes on WebIDL Representation

1. **Variadic Transforms**: The WebIDL uses `Transform... transforms` to represent variadic transform arguments. In practice, implementations detect the options object by checking for the absence of a `transform` property.

2. **Object Sources**: Sources are typed as `object` in WebIDL because they are structurally typed (must be iterable). The implementation checks for `Symbol.iterator` or `Symbol.asyncIterator`.

3. **Protocol Symbols**: The symbols are defined using `Symbol.for()` to allow third-party code to implement protocols without importing the symbols directly.

4. **Chunk Batching**: All iterables yield `sequence<Uint8Array>` (ChunkBatch) rather than individual chunks. This is a deliberate design choice to amortize async iteration overhead.

5. **Non-Negative desiredSize**: Unlike Web Streams where `desiredSize` can be negative, this API guarantees `desiredSize >= 0` or `null` (closed/failed).

---

## Related Documents

| Document | Description |
|----------|-------------|
| [API.md](../API.md) | Complete API reference for the reference implementation |
| [README.md](../README.md) | Motivation and criticism of Web Streams API |
| [MIGRATION.md](MIGRATION.md) | Guide for migrating from Web Streams API |
| [REQUIREMENTS.md](REQUIREMENTS.md) | Detailed requirements and test specifications |
| [REFACTOR-TODO.md](REFACTOR-TODO.md) | Implementation notes and session history |

# New Streams API Reference

A bytes-only stream API based on iterables with explicit backpressure handling.

## Table of Contents

- [Overview](#overview)
- [Stream Namespace](#stream-namespace)
- [Push Stream](#push-stream)
- [Stream Factories](#stream-factories)
- [Pull Pipeline](#pull-pipeline)
- [Write To](#write-to)
- [Consumers](#consumers)
- [Utilities](#utilities)
- [Multi-Consumer](#multi-consumer)
- [Types](#types)
- [Protocol Symbols](#protocol-symbols)

---

## Overview

This API treats streams as iterables that yield batched chunks (`Uint8Array[]`). Key principles:

1. **Streams are iterables** - No custom Stream class, just async/sync iterables
2. **Batched chunks** - Iterables yield `Uint8Array[]` to amortize async overhead
3. **Explicit backpressure** - Strict by default, configurable policies
4. **Pull-through transforms** - Transforms execute only when consumer pulls
5. **Clean sync/async separation** - Separate interfaces, no ambiguity

### Basic Usage

```typescript
import { Stream } from 'new-streams';

// Create a push stream
const { writer, readable } = Stream.push();

// Producer and consumer run concurrently. With strict backpressure
// (the default), writes block until the consumer reads.
const producing = (async () => {
  await writer.write("Hello, World!");
  await writer.end();
})();

const text = await Stream.text(readable);
console.log(text); // "Hello, World!"
await producing;
```

See [USAGE.md](https://github.com/jasnell/new-streams/blob/main/docs/USAGE.md) for more examples.

### Batched Iteration

All stream iterables yield `Uint8Array[]` (arrays of chunks):

```typescript
for await (const chunks of readable) {
  for (const chunk of chunks) {
    // Process individual chunk
    console.log(chunk.byteLength);
  }
}
```

This is true even for single-chunk sources yielding one chunk per batch.

---

## Stream Namespace

The `Stream` namespace provides unified access to all stream functions:

```typescript
import { Stream } from 'new-streams';

// Stream Creation
Stream.push()            // Create push stream with writer and transforms
Stream.duplex()          // Create connected pair of duplex channels
Stream.from()            // Create pull stream from source
Stream.fromSync()        // Create sync-only pull stream from source

// Pipelines & Transforms
Stream.pull()            // Pull pipeline with transforms
Stream.pullSync()        // Sync pull pipeline
Stream.pipeTo()          // Pipe to destination
Stream.pipeToSync()      // Sync pipe to destination

// Consumers (Terminal)
Stream.bytes()           // Collect as Uint8Array
Stream.text()            // Collect as string
Stream.arrayBuffer()     // Collect as ArrayBuffer
Stream.array()           // Collect as Uint8Array[]

// Sync Consumers (Terminal)
Stream.bytesSync()       // Sync collect as Uint8Array
Stream.textSync()        // Sync collect as string
Stream.arrayBufferSync() // Sync collect as ArrayBuffer
Stream.arraySync()       // Sync collect as Uint8Array[]

// Multi-Consumer
Stream.broadcast()       // Push-model multi-consumer
Stream.share()           // Pull-model multi-consumer
Stream.shareSync()       // Sync pull-model multi-consumer

// Utilities
Stream.merge()           // Merge multiple sources
Stream.tap()             // Observation transform
Stream.tapSync()         // Sync observation transform
Stream.ondrain()         // Wait for backpressure to clear
```

---

## Push Stream

### `Stream.push(...transforms?, options?)`

Creates a bonded writer and async iterable pair for push-based streaming. The
writer and readable are connected—data written to the writer flows to anyone
iterating the readable.

The `highWaterMark` controls the buffer size (slots) and, for `'strict'` mode,
also limits pending writes. With `'strict'` backpressure (the default),
properly awaited writes will wait for buffer space, while "fire-and-forget"
writes (not awaited) will throw when exceeding the pending limit.

[Transforms](#transform-types) are applied to data as it flows from writer to readable.
Specifically, transforms are only invoked when the consumer pulls data from the
iterable.

```typescript
function push(
  ...args: [...Transform[], PushStreamOptions?]
): { writer: Writer; readable: AsyncIterable<Uint8Array[]> }
```

Returns a [Writer](#writer-interface) and an async iterable readable.

**Options:**
- `highWaterMark?: number` - Buffer capacity in slots (default: 4)
- `backpressure?:` [BackpressurePolicy](#backpressure-policy) - Policy when buffer full (default: 'strict')
- `signal?: AbortSignal` - Cancellation signal

**Example:**
```typescript
// Basic push stream
const { writer, readable } = Stream.push();

// With transforms
const { writer, readable } = Stream.push(compress, encrypt);

// With options
const { writer, readable } = Stream.push({
  highWaterMark: 10,
  backpressure: 'drop-oldest'
});

// Producer and consumer must run concurrently. With strict backpressure
// (the default), awaited writes will block until the consumer reads.
const producing = (async () => {
  await writer.write("chunk1");
  await writer.write("chunk2");
  await writer.end();
})();

for await (const chunks of readable) {
  for (const chunk of chunks) {
    console.log(chunk);
  }
}

await producing;
```

Note that like the rest of the API, transforms are pull-driven — no data flows
until the consumer iterates the readable.

The returned `readable` is an async iterable yielding `Uint8Array[]` batches.
There is no sync variant of `Stream.push()` since push streams are inherently
async as it is waiting for data to be written via the writer.

### Writer Interface

The `Writer` interface is the API for producing data. It is an interface, not
a concrete class; any object conforming to this interface can serve as a writer.

Implementations should support `Symbol.asyncDispose` (calling `fail()` with no
argument), enabling `await using` syntax. Implementations may also support
`Symbol.dispose` for synchronous cleanup.

```typescript
interface WriteOptions {
  readonly signal?: AbortSignal;
}

interface Writer {
  readonly desiredSize: number | null;

  write(chunk: Uint8Array | string, options?: WriteOptions): Promise<undefined>;
  writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<undefined>;
  end(options?: WriteOptions): Promise<number>;  // Total bytes written
  fail(reason?: any): Promise<undefined>;

  // Synchronous variants (see try-fallback pattern below)
  writeSync(chunk: Uint8Array | string): boolean;
  writevSync(chunks: (Uint8Array | string)[]): boolean;
  endSync(): number;  // >= 0: total bytes written, -1: cannot end synchronously
  failSync(reason?: any): boolean;
}
```

**Method details:**

- `desiredSize`: Available slots in the internal buffer (`highWaterMark` minus
  occupied slots), always >= 0 or `null` (closed/errored). This reflects the
  slots buffer only; it does not account for writes in the pending writes queue.
  A value of 0 means the buffer is full regardless of how many writes are
  queued behind it.

- `write(chunk)`: Write a single chunk. Strings are UTF-8 encoded. Behavior
  when the buffer is full depends on the backpressure policy:
  - `"strict"`: If pending writes queue has room, waits for buffer space. If
    queue is also at capacity, rejects with `RangeError`.
  - `"block"`: Waits for buffer space (pending queue is unbounded).
  - `"drop-oldest"`: Discards oldest buffered batch, enqueues new data.
  - `"drop-newest"`: Silently discards the new data.

- `writev(chunks)`: Write multiple chunks as a single atomic batch. All-or-nothing:
  either all chunks are accepted or the entire write is rejected. The batch
  occupies a single slot for backpressure purposes.

- `end()`: Signal end of data. Returns total bytes written. Rejects with
  `TypeError` if already closed or errored.

- `fail(reason)`: Put the writer into an error state. Accepts any value as the
  reason (not limited to `Error`). No-op if already closed or errored.

**Sync variants and the try-fallback pattern:**

The sync methods are designed for a try-fallback pattern: attempt the sync
method first, and if it fails, fall back to the async version. The sync methods
never throw on backpressure; they return a failure indicator.

The `writeSync`/`writevSync` behavior depends on the backpressure policy:
- `"strict"`: Returns `false` when buffer is full.
- `"block"`: Enqueues the data and returns `false` as a backpressure signal
  (the data IS accepted, but the caller should slow down).
- `"drop-oldest"`: Discards oldest batch, enqueues new data, returns `true`.
- `"drop-newest"`: Silently discards new data, returns `true`.

The `endSync` method returns total bytes written (>= 0) on success, or -1
if it cannot end synchronously. The `failSync` method returns `true` on
success or `false` if the writer cannot transition.

```js
// Attempt to write synchronously first
if (!writer.writeSync(chunk)) {
  // If that fails, fall back to async write
  await writer.write(chunk);
}

// End with try-fallback pattern
if (writer.endSync() < 0) {
  await writer.end();
}
```

### SyncWriter Interface

The `SyncWriter` interface is a synchronous interface for producing data, used
by `Stream.pipeToSync()`. Like `Writer`, it is an interface, not a concrete
class. Implementations should support `Symbol.dispose` (calling `fail()` with
no argument), enabling `using` syntax.

```typescript
interface SyncWriter {
  readonly desiredSize: number | null;

  write(chunk: Uint8Array | string): boolean;
  writev(chunks: (Uint8Array | string)[]): boolean;
  end(): number;  // Total bytes written; throws on failure
  fail(reason?: any): void;
}
```

`SyncWriter` follows the same backpressure policies as `Writer`:
- `"block"`: `write()`/`writev()` enqueue and return `true` when buffer has
  space, or enqueue and return `false` when full (backpressure signal; data IS
  accepted).
- `"strict"`: Writes exceeding buffer capacity throw a `RangeError`.
- `"drop-oldest"`/`"drop-newest"`: Behave as with `Writer`. Writes never fail.
- `end()` throws `TypeError` if already closed or errored (no -1 fallback;
  there is no async counterpart to fall back to).
- `fail()` transitions to error state; no-op if already closed or errored.

The `WriteOptions.signal` allows per-operation cancellation. A cancelled write
is removed from the queue without putting the writer into an error state -
subsequent writes can still succeed:

```typescript
// Cancel a single slow write after 5 seconds
const ac = new AbortController();
setTimeout(() => ac.abort(), 5000);

try {
  await writer.write(largeChunk, { signal: ac.signal });
} catch (err) {
  if (err.name === 'AbortError') {
    // This write was cancelled, but writer is still open
    await writer.write(fallbackChunk);
  }
}
```

`Writer` is an interface not a concrete class; any object implementing this
interface can be used as a writer.

---

## Duplex Channels

### `Stream.duplex(options?)`

Create a pair of connected duplex channels for bidirectional communication.
Similar to Unix `socketpair()` - creates two endpoints where data written to
one endpoint's writer appears in the other endpoint's readable.

```typescript
function duplex(options?: DuplexOptions): [DuplexChannel, DuplexChannel]
```

**Options:**
- `highWaterMark?: number` - Buffer size for both directions (default: 4)
- `backpressure?:` [BackpressurePolicy](#backpressure-policy) - Policy for both directions (default: 'strict')
- `a?: DuplexDirectionOptions` - Options specific to the A→B direction (overrides shared options)
- `b?: DuplexDirectionOptions` - Options specific to the B→A direction (overrides shared options)
- `signal?: AbortSignal` - Cancellation signal for both channels

**DuplexDirectionOptions:**
- `highWaterMark?: number` - Buffer size for this direction
- `backpressure?:` [BackpressurePolicy](#backpressure-policy) - Policy for this direction

**DuplexChannel Interface:**
```typescript
interface DuplexChannel {
  readonly writer: Writer;                    // Write to the remote peer
  readonly readable: AsyncIterable<Uint8Array[]>;  // Read from the remote peer
  close(): Promise<undefined>;               // Close this end (idempotent)
}
```

**Example: Echo server**
```typescript
const [client, server] = Stream.duplex();

// Server echoes back what it receives
(async () => {
  await using srv = server;
  for await (const chunks of srv.readable) {
    await srv.writer.writev(chunks);
  }
})();

// Client sends and receives
{
  await using conn = client;
  await conn.writer.write('Hello');
  for await (const chunks of conn.readable) {
    console.log(new TextDecoder().decode(chunks[0])); // "Hello"
    break;
  }
} // Automatically closed here
```

**Example: Different buffer sizes per direction**
```typescript
// High-throughput server responses, low-throughput client requests
const [client, server] = Stream.duplex({
  a: { highWaterMark: 2 },   // Client→Server: small buffer
  b: { highWaterMark: 16 },  // Server→Client: larger buffer for responses
});
```

### Custom DuplexChannel Implementations

`Stream.duplex()` is a convenience function, but it's not the only way to create
duplex channels. Applications and runtimes can implement their own duplex channels
as long as they conform to the `DuplexChannel` interface.

For example, a runtime might provide a native socket implementation:

```typescript
class NativeSocket implements DuplexChannel {
  readonly writer: Writer;
  readonly readable: AsyncIterable<Uint8Array[]>;

  constructor(private handle: NativeHandle) {
    this.writer = new SocketWriter(handle);
    this.readable = new SocketReadable(handle);
  }

  async close(): Promise<void> {
    await this.handle.close();
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}
```

Or an application might wrap a WebSocket:

```typescript
function wrapWebSocket(ws: WebSocket): DuplexChannel {
  const { writer, readable } = Stream.push();

  ws.onmessage = (event) => {
    writer.writeSync(new Uint8Array(event.data));
  };
  ws.onclose = () => writer.endSync();
  ws.onerror = (e) => writer.failSync(new Error('WebSocket error'));

  return {
    writer: {
      // Delegate writes to WebSocket.send()
      async write(chunk) { ws.send(chunk); },
      writeSync(chunk) { ws.send(chunk); return true; },
      // ... implement other Writer methods
    },
    readable,
    async close() { ws.close(); },
    [Symbol.asyncDispose]() { return this.close(); },
  };
}
```

`DuplexChannel` is an interface, not a concrete class. Any object conforming to
this interface can serve as a duplex channel. Implementations should support
`Symbol.asyncDispose` for `await using` support. Calling `close()` ends the
writer (signaling end-of-stream to the peer) and stops iteration of the
readable. It is idempotent.

---

## Stream Factories

### `Stream.from(input)`

Create an async iterable from various types of sources. Even when the input is already a
[Streamable](#streamable-types), `from()` performs important normalization:

1. **Batch normalization** - Ensures consistent `Uint8Array[]` batch output format
2. **Type conversion** - Strings → UTF-8 encoded bytes, ArrayBuffer/ArrayBufferView → Uint8Array
3. **Recursive flattening** - Nested arrays and iterables are flattened to a single stream
4. **Protocol handling** - Invokes [`toStreamable`/`toAsyncStreamable`](#tostreamable--toasyncstreamable) on objects implementing those protocols
5. **Batching optimization** - Groups synchronous values together to amortize async iteration overhead

```typescript
function from(
  input: ByteInput | Streamable
): AsyncIterable<Uint8Array[]>
```

Throws `TypeError` if input is `null` or `undefined`.

**Supported inputs (in precedence order):**
- `string` - UTF-8 encoded
- `ArrayBuffer` - Wrapped as Uint8Array (zero-copy)
- `ArrayBufferView` - Converted to Uint8Array (zero-copy)
- Objects with `Symbol.for('Stream.toAsyncStreamable')` - Called and result recursively normalized
- Objects with `Symbol.for('Stream.toStreamable')` - Called and result recursively normalized
- Objects with `Symbol.asyncIterator` - Pulled and each value normalized to `Uint8Array[]` batches
- Objects with `Symbol.iterator` - Treated as async source and normalized

Implementations must guard against infinite recursion from protocol methods (e.g., an object
whose `toStreamable` returns itself).

**Example:**
```typescript
const readable = Stream.from("Hello, World!");
const readable = Stream.from(new Uint8Array([1, 2, 3]));
const readable = Stream.from(["chunk1", "chunk2"]);
const readable = Stream.from(asyncGenerator());
```

### `Stream.fromSync(input)`

Create a sync iterable from various sources. Throws `TypeError` if input is
`null` or `undefined`. Like `from()`, this normalizes inputs to the consistent
`Uint8Array[]` batch format, handling type conversions, recursive flattening,
and [protocol handling](#tostreamable--toasyncstreamable), but for synchronous
sources only. Async inputs (objects with `Symbol.asyncIterator` or
`Symbol.for('Stream.toAsyncStreamable')`) cause a `TypeError`.

```typescript
function fromSync(
  input: ByteInput | SyncStreamable
): Iterable<Uint8Array[]>
```

---

## Pull Pipeline

### `Stream.pull(source, ...transforms?, options?)`

Create a pull-through pipeline with [transforms](#transform-types). The pipeline is lazy—no data
flows until the consumer iterates. Each iteration pulls data through the
transform chain on demand. Transforms receive a `null` flush signal after all
source data has been consumed, allowing them to emit any buffered final output.

The pipeline always creates an internal `AbortController` whose signal is passed
to transforms via `TransformCallbackOptions`. If an external signal is provided,
the internal signal follows it. When aborted, on error, or when the consumer
stops iterating, the internal controller is aborted, notifying all transforms.
Aborting the external signal rejects pending and future pulls from the returned
async iterable with the signal's abort reason. If the pipeline aborts while
waiting for an upstream source iterator's `next()` result, the pipeline rejects
the consumer's pending pull and calls the source iterator's `return()` method
when available. This does not require canceling the promise returned by the
upstream `next()` call.

```typescript
function pull(
  source: Streamable,
  ...args: [...Transform[], PullOptions?]
): AsyncIterable<Uint8Array[]>
```

See [Streamable Types](#streamable-types) for supported source types.

**Options:**
- `signal?: AbortSignal` - Cancellation signal

**Example:**
```typescript
// No transforms
const output = Stream.pull(source);

// With transforms
const output = Stream.pull(source, compress, encrypt);

// With options
const output = Stream.pull(source, compress, { signal: controller.signal });

// Consume
for await (const chunks of output) {
  for (const chunk of chunks) {
    process(chunk);
  }
}
```

### `Stream.pullSync(source, ...transforms)`

Sync pull pipeline. Works like `pull()` but for synchronous sources and
transforms. Does not accept an options dictionary (there is no cancellation
signal for synchronous pipelines). All arguments after `source` must be sync
transforms.

```typescript
function pullSync(
  source: SyncStreamable,
  ...transforms: SyncTransform[]
): Iterable<Uint8Array[]>
```

### Transform Types

Transforms come in two forms, distinguished by whether they are a plain function
or an object:

- **Stateless (function)**: Called once per batch. Receives `Uint8Array[] | null`,
  returns transformed output. The `null` input is a flush signal indicating
  end-of-stream—return any buffered data or `null`.

- **Stateful (object)**: Wraps the entire source as a generator. Has full control
  over iteration and can maintain internal state across batches. Use for
  compression, encryption, or any transform needing to buffer across chunks.

```typescript
// Options passed to async transform functions by the pipeline.
// The pipeline always creates an internal AbortController whose signal
// is passed here. If an external signal was provided, the internal
// signal follows it.
interface TransformCallbackOptions {
  readonly signal: AbortSignal;
}

// Async stateless transform
type TransformFn = (
  chunks: Uint8Array[] | null,
  options: TransformCallbackOptions
) => any;  // Flexible return; see "Transform Return Types" below

// Async stateful transform
type StatefulTransformFn = (
  source: AsyncIterable<Uint8Array[] | null>,
  options: TransformCallbackOptions
) => AsyncIterable<any>;

// Sync stateless transform (no options parameter; no cancellation signal)
type SyncTransformFn = (
  chunks: Uint8Array[] | null
) => any;

// Sync stateful transform (no options parameter)
type SyncStatefulTransformFn = (
  source: Iterable<Uint8Array[] | null>
) => Iterable<any>;

// Transform object for stateful transforms
interface TransformObject {
  transform: StatefulTransformFn | SyncStatefulTransformFn;
}

// Function = stateless, Object = stateful
type Transform = TransformFn | TransformObject;
type SyncTransform = SyncTransformFn | TransformObject;
```

**Transform Return Types:**

Transforms have flexible return types for convenience:

- `null` - No output for this batch
- `Uint8Array[]` - Batch of chunks (most common)
- `Uint8Array` - Single chunk (wrapped into batch)
- `string` - UTF-8 encoded to Uint8Array
- `Iterable<...>` / `AsyncIterable<...>` - Flattened to chunks
- `Promise<...>` - Awaited (async transforms only)

**Transform Example:**
```typescript
// Stateless transform (plain function)
const uppercase: TransformFn = (chunks, { signal }) => {
  if (chunks === null) return null; // Flush signal
  return chunks.map(chunk => {
    const text = new TextDecoder().decode(chunk);
    return new TextEncoder().encode(text.toUpperCase());
  });
};

// Stateful transform (object with generator function).
// The signal fires when the pipeline is torn down (consumer breaks,
// error, or explicit cancellation). Use it to abort in-flight work.
const compress: TransformObject = {
  async *transform(source, { signal }) {
    const compressor = createCompressor();
    signal.addEventListener('abort', () => compressor.cancel());
    try {
      for await (const chunks of source) {
        if (chunks === null) {
          yield compressor.finish();
        } else {
          for (const chunk of chunks) {
            yield compressor.push(chunk);
          }
        }
      }
    } finally {
      // Clean up compressor resources on error/cancellation
      compressor.destroy();
    }
  }
};
```

---

## Pipe To

### `Stream.pipeTo(source, ...transforms?, writer, options?)`

Consume source and write to a [writer](#writer-interface) with optional [transforms](#transform-types). This is the primary way to pipe data to a destination. It handles:

- Applying transforms in order before writing
- Calling `writer.writev()` when available for batch efficiency
- Calling `writer.end()` on successful completion (unless `preventClose`)
- Calling `writer.fail()` on error (unless `preventFail`)
- Propagating the pipeline's `AbortSignal` to `writer.write()` and `writer.end()`
- Respecting cancellation via `AbortSignal`

Returns the total number of bytes written to the writer.

```typescript
function pipeTo(
  source: any,  // Any input Stream.from() can normalize
  ...args: [...Transform[], Writer, PipeToOptions?]
): Promise<number>  // Total bytes written
```

**Options:**
- `signal?: AbortSignal` - Cancellation signal
- `preventClose?: boolean` - Don't call writer.end() on completion
- `preventFail?: boolean` - Don't call writer.fail() on error

**Example:**
```typescript
// Direct pipe
const bytesWritten = await Stream.pipeTo(source, fileWriter);

// With transforms
const bytesWritten = await Stream.pipeTo(
  source,
  compress,
  encrypt,
  networkWriter
);

// With options
const bytesWritten = await Stream.pipeTo(
  source,
  compress,
  fileWriter,
  { signal: controller.signal }
);
```

### `Stream.pipeToSync(source, ...transforms?, writer, options?)`

Sync pipe to destination.

```typescript
function pipeToSync(
  source: any,  // Any input Stream.fromSync() can normalize
  ...args: [...SyncTransform[], SyncWriter, PipeToSyncOptions?]
): number
```

---

## Consumers

Terminal consumers that collect an entire stream into memory. These accept any
[Streamable](#streamable-types) source. Use the `limit` option to protect against
unbounded memory growth from untrusted sources.

### `Stream.bytes(source, options?)`

Collect all bytes from source into a single `Uint8Array`. Efficiently
concatenates all chunks, avoiding unnecessary copies when possible.

```typescript
function bytes(
  source: any,  // Any input Stream.from() can normalize
  options?: ConsumeOptions
): Promise<Uint8Array>
```

**Options:**
- `signal?: AbortSignal` - Cancellation signal
- `limit?: number` - Max bytes (throws `RangeError` if exceeded)

### `Stream.text(source, options?)`

Collect and decode as text. Uses `TextDecoder` with `fatal: true` by default,
throwing on invalid byte sequences.

```typescript
function text(
  source: any,  // Any input Stream.from() can normalize
  options?: TextConsumeOptions
): Promise<string>
```

**Options** (`TextConsumeOptions`):
- `signal?: AbortSignal` - Cancellation signal
- `limit?: number` - Max bytes (throws `RangeError` if exceeded)
- `encoding?: string` - Text encoding (default: `'utf-8'`). Only `'utf-8'` is
  required for conformance; implementations may support additional encodings as
  defined by the Encoding Standard. Unsupported encodings throw `RangeError`.

### `Stream.arrayBuffer(source, options?)`

Collect as ArrayBuffer. Returns the underlying buffer directly when possible,
or copies when the Uint8Array is a view of a larger buffer.

```typescript
function arrayBuffer(
  source: any,  // Any input Stream.from() can normalize
  options?: ConsumeOptions
): Promise<ArrayBuffer>
```

### `Stream.array(source, options?)`

Collect all chunks as an array of `Uint8Array`. Unlike `bytes()` which concatenates
all chunks into a single `Uint8Array`, `array()` preserves the original chunk
boundaries. This is useful when you need to process or forward chunks individually
while still collecting them all.

```typescript
function array(
  source: any,  // Any input Stream.from() can normalize
  options?: ConsumeOptions
): Promise<Uint8Array[]>
```

**Options:**
- `signal?: AbortSignal` - Cancellation signal
- `limit?: number` - Max bytes (throws `RangeError` if exceeded)

**Example:**
```typescript
// Collect chunks preserving boundaries
const chunks = await Stream.array(source);
console.log(`Received ${chunks.length} chunks`);

// Compare with bytes() which concatenates
const concatenated = await Stream.bytes(source);
console.log(`Total bytes: ${concatenated.byteLength}`);

// Forward chunks individually
for (const chunk of chunks) {
  await destination.write(chunk);
}
```

### Sync Variants

Synchronous versions for use with sync sources. Same algorithms as async
versions, but normalize via `Stream.fromSync()` instead of `Stream.from()`.

Sync consumers use `ConsumeSyncOptions` (with `limit` only, no `signal`) and
`TextConsumeSyncOptions` (with `limit` and `encoding`, no `signal`), since
there is no async cancellation in synchronous code.

```typescript
Stream.bytesSync(source, options?: ConsumeSyncOptions)
Stream.textSync(source, options?: TextConsumeSyncOptions)
Stream.arrayBufferSync(source, options?: ConsumeSyncOptions)
Stream.arraySync(source, options?: ConsumeSyncOptions)
```

---

## Utilities

### `Stream.tap(callback)`

Create a pass-through [transform](#transform-types) that observes chunks without modifying them.

Note that it's up to the callback to not modify the chunks in place, as that would affect downstream consumers. The key difference with `tap` is that return values are ignored
preventing the callback from replacing the chunks in the stream, but since the data
flows through by-reference, in-place modifications would still be visible downstream.

The intended use case is for logging, metrics, or side-effects based on the data flowing
without altering the stream itself.

```typescript
function tap(callback: TapCallbackAsync): StatelessTransformFn
function tapSync(callback: TapCallback): SyncStatelessTransformFn

type TapCallback = (chunks: Uint8Array[] | null) => void;
type TapCallbackAsync = (
  chunks: Uint8Array[] | null,
  options: TransformCallbackOptions
) => void | Promise<void>;
```

If the callback throws (or returns a rejected promise for async callbacks),
the error propagates to the consumer and the pipeline is torn down.

**Example:**
```typescript
// Log chunks as they flow through
const output = Stream.pull(
  source,
  Stream.tap((chunks) => {
    if (chunks === null) {
      console.log('Stream complete');
    } else {
      console.log('Received', chunks.length, 'chunks');
    }
  }),
  compress
);

// Hash while streaming
const hasher = crypto.createHash('sha256');
const output = Stream.pull(
  source,
  Stream.tap((chunks) => {
    if (chunks === null) {
      console.log('Hash:', hasher.digest('hex'));
    } else {
      for (const chunk of chunks) hasher.update(chunk);
    }
  })
);
```

### `Stream.merge(...sources, options?)`

Interleave multiple async sources into a single async iterable, yielding
batches in the order they become available. All data from all sources is
preserved; no batches are discarded. Each source has at most one pending
`.next()` call at a time. When multiple sources settle between consumer
pulls, their batches are queued and drained in settlement order.

This is not `Promise.race` semantics where "losing" values are discarded.
Every batch from every source is yielded exactly once.

When the merged stream is closed or cancelled, all source iterators are
cleaned up via `.return()`.

```typescript
function merge(
  ...args: [...any[], MergeOptions?]  // Sources normalized via Stream.from()
): AsyncIterable<Uint8Array[]>
```

**Options:**
- `signal?: AbortSignal` - Cancellation signal

**Example:**
```typescript
// Merge WebSocket feeds
const merged = Stream.merge(prices, news, alerts);

for await (const chunks of merged) {
  // Process in order of arrival
}
```

### `Stream.ondrain(drainable)`

Wait for a [Writer](#writer-interface)'s backpressure to clear. This is the primary mechanism
for integrating event-driven sources (like Node.js EventEmitters, WebSockets, etc.)
with the streams API.

Writers from `Stream.push()` and `Stream.broadcast()` implement the Drainable protocol.

```typescript
function ondrain(drainable: unknown): Promise<boolean> | null
```

**Returns:**
- `null` - Object doesn't implement the Drainable protocol, or drain is not applicable (e.g., `desiredSize` is `null`)
- `Promise<true>` - Resolves immediately if ready to write (`desiredSize > 0`), or resolves when backpressure clears
- `Promise<false>` - Resolves when writer closes while waiting (no more writes accepted)
- `Promise` rejects - If writer fails/errors while waiting

**Important:** Due to TOCTOU (time-of-check-time-of-use) races, callers should still
check `desiredSize` and await writes even after the drain promise resolves with `true`.
The buffer may fill again between the drain signal and your write.

**Example: Event source integration:**
```typescript
const { writer, readable } = Stream.push({ highWaterMark: 4 });

// Consume in background
const consumePromise = Stream.text(readable);

// Event source integration pattern
eventSource.on('data', async (chunk) => {
  // Check backpressure before writing
  if (writer.desiredSize === 0) {
    eventSource.pause();

    // await works on null (returns null), and null is falsy
    const canWrite = await Stream.ondrain(writer);
    if (!canWrite) {
      // Writer closed or doesn't support drain - stop the source
      eventSource.destroy();
      return;
    }

    eventSource.resume();
  }

  // Still await - desiredSize may have changed (TOCTOU)
  await writer.write(chunk);
});

eventSource.on('end', () => writer.end());
eventSource.on('error', (err) => writer.fail(err));
```

Note: `await null` returns `null`, which is falsy, so the pattern `const canWrite = await Stream.ondrain(x)`
works correctly even when the protocol isn't supported. Explicit `null` checking is only needed when you
must distinguish "protocol not supported" from "writer closed".

**Why use ondrain instead of polling desiredSize?**

Polling `desiredSize` in a loop wastes CPU cycles and may miss the moment when
space becomes available. `ondrain()` provides an efficient notification mechanism
that resolves exactly when the buffer has space, without busy-waiting.

```typescript
// BAD: Polling (wastes CPU)
while (writer.desiredSize === 0) {
  await new Promise(r => setTimeout(r, 10));
}

// GOOD: Efficient notification
const canWrite = await Stream.ondrain(writer);
if (!canWrite) return; // closed or unsupported
```

---

## Multi-Consumer

Two patterns for sharing a single source among multiple consumers:

- **[Broadcast](#streambroadcastoptions) (push model)**: Broadcast is effectively a multi-consumer version of `Stream.push()`, where data written to the writer is delivered to all subscribed consumers.

- **[Share](#streamsharesource-options) (pull model)**: Share is a multi-consumer version of `Stream.from()`/`Stream.pull()`, where a single source is consumed on-demand as consumers pull data.

### `Stream.broadcast(options?)`

Create a push-model multi-consumer channel. Data written to the [writer](#writer-interface) is
delivered to all consumers that have subscribed via `broadcast.push()`.
Data remains in the buffer and is available to consumers that attach before
it is overwritten. Late-joining consumers begin reading from the oldest entry
still in the buffer at the time they call `broadcast.push()`.

```typescript
function broadcast(options?: BroadcastOptions): {
  writer: Writer;
  broadcast: Broadcast;
}
```

**Options:**
- `highWaterMark?: number` - Max slots in buffer; in strict mode, also limits pending writes (default: 16)
- `backpressure?:` [BackpressurePolicy](#backpressure-policy) - Policy when buffer full (default: 'strict')
- `signal?: AbortSignal` - Cancellation signal

Backpressure is applied based on the slowest consumer and follows the same rules as `Stream.push()`. Because the backpressure is slowest-consumer based, be cautious when multiple consumers are
pulling at different rates, as a very slow consumer can cause the entire broadcast to back up.

**Broadcast Interface:**
```typescript
interface Broadcast {
  push(...transforms?, options?: PullOptions): AsyncIterable<Uint8Array[]>;
  readonly consumerCount: number;
  readonly bufferSize: number;

  cancel(reason?: any): void;
  // Supports Symbol.dispose (calls cancel())
}
```

**Example:**
```typescript
const { writer, broadcast } = Stream.broadcast({ highWaterMark: 100 });

// Create consumers before writing
const consumer1 = broadcast.push();
const consumer2 = broadcast.push(decompress);

// Producer and consumers must run concurrently. Awaited writes
// block when the buffer fills until consumers read.
const producing = (async () => {
  await writer.write("shared data");
  await writer.end();
})();

const [result1, result2] = await Promise.all([
  Stream.text(consumer1),
  Stream.text(consumer2)
]);
await producing;
```

### `Stream.share(source, options?)`

Create a pull-model multi-consumer wrapper. The source is consumed on-demand as
consumers pull data. Chunks are buffered so slower consumers can catch up, and
the buffer is trimmed as all consumers advance past buffered data.

The source iterator is created lazily on first pull and shared across all
consumers. Each consumer maintains its own cursor position in the buffer.

```typescript
function share(
  source: any,  // Any input Stream.from() can normalize
  options?: ShareOptions
): Share
```

**Options:**
- `highWaterMark?: number` - Max slots in buffer (default: 16)
- `backpressure?:` [BackpressurePolicy](#backpressure-policy) - Policy when buffer full (default: 'strict')
- `signal?: AbortSignal` - Cancellation signal

**Share Interface:**
```typescript
interface Share {
  pull(...transforms?, options?: PullOptions): AsyncIterable<Uint8Array[]>;
  readonly consumerCount: number;
  readonly bufferSize: number;
  cancel(reason?: any): void;
  // Supports Symbol.dispose (calls cancel())
}
```

**Example:**
```typescript
const shared = Stream.share(fileStream, { highWaterMark: 100 });

// Create consumers with different transforms
const consumer1 = shared.pull();
const consumer2 = shared.pull(decompress);
const consumer3 = shared.pull(decompress, parse);

// All share the same source, buffered
const [raw, decompressed, parsed] = await Promise.all([
  Stream.bytes(consumer1),
  Stream.bytes(consumer2),
  Stream.bytes(consumer3)
]);
```

### `Stream.shareSync(source, options?)`

Sync pull-model multi-consumer wrapper.
`
```typescript
function shareSync(
  source: any,  // Any input Stream.fromSync() can normalize
  options?: ShareSyncOptions
): SyncShare
```

---

## Types

### Backpressure Policy

Controls behavior when internal buffers reach their limit:

```typescript
type BackpressurePolicy =
  | 'strict'       // Catches ignored backpressure (default)
  | 'block'        // Async writes block until space; sync writes return false
  | 'drop-oldest'  // Drop oldest buffered chunks to make room for new data
  | 'drop-newest'; // Discard incoming chunks when buffer full
```

- **strict**: (Default) Catches when producers ignore backpressure. Allows
  properly awaited writes to proceed (waiting for space when buffer is full),
  but rejects when too many writes are pending without being awaited. The
  `highWaterMark` limits both the slots buffer AND the pending writes queue.
  Sync writes return `false` when the slots buffer is full. Async writes will
  reject when both the slots buffer and pending write buffers are full. Use
  this to ensure producers properly respect backpressure signals.

- **block**: Async writes wait for buffer space; sync writes return `false`.
  Unlike strict, the pending writes queue is unbounded, so "fire-and-forget"
  writes will accumulate without error. Use this for flow control where the
  producer should wait for consumers to catch up, but be careful to await
  writes to avoid memory issues. This is the mode that existing Node.js and
  Web streams APIs default to.

- **drop-oldest**: Prioritizes recent data. Old buffered data is discarded to
  make room. Useful for live feeds where stale data is less valuable.

- **drop-newest**: Prioritizes existing data. New writes are silently discarded
  when buffer is full. Useful when you want to process what you have without
  being overwhelmed.

### How highWaterMark Works

The `highWaterMark` controls flow between producer and consumer using a two-part
buffering system. Think of it like a bucket (slots) being filled through a hose
(pending writes), with a float valve that closes when the bucket is full:

```
                          highWaterMark (e.g., 3)
                                 |
    Producer                     v
       |                    +---------+
       v                    |         |
  [ write() ] ----+    +--->| slots   |---> Consumer pulls
  [ write() ]     |    |    | (bucket)|     for await (...)
  [ write() ]     v    |    +---------+
              +--------+         ^
              | pending|         |
              | writes |    float valve
              | (hose) |    (backpressure)
              +--------+
                   ^
                   |
          'strict' mode limits this too!
```

When both the slots and pending writes are full, backpressure is signaled.

> **Note:** `highWaterMark` is clamped to a minimum of 1. Implementations may
> also apply a reasonable upper limit; the specific maximum is
> implementation-defined but must be at least 1024.

**The Two Buffers:**

1. **Slots (the bucket)**: Data ready for the consumer, limited by `highWaterMark`
2. **Pending writes (the hose)**: Writes waiting when slots are full

**How policies use these buffers:**

| Policy | Slots limit | Pending writes limit |
|--------|-------------|---------------------|
| `strict` | `highWaterMark` | `highWaterMark` |
| `block` | `highWaterMark` | Unbounded |
| `drop-oldest` | `highWaterMark` | N/A (never waits) |
| `drop-newest` | `highWaterMark` | N/A (never waits) |

Important: The key rule of thumb is that with `strict` mode, the total number of
unacknowledged writes (in both slots + pending) can never exceed
`highWaterMark * 2`.

#### Strict Mode in Detail

Strict mode catches "fire-and-forget" patterns where the producer calls `write()`
without awaiting, which would cause unbounded memory growth:

```typescript
// BAD: Fire-and-forget (strict mode will catch this)
for (const item of hugeDataset) {
  writer.write(item);  // Not awaited! Queues up unboundedly
}

// GOOD: Properly awaited (requires a concurrent consumer reading
// from the readable, otherwise the first write that fills the
// buffer will block forever)
for (const item of hugeDataset) {
  await writer.write(item);  // Waits for consumer to read
}
```

**How strict mode works step-by-step** (with `highWaterMark: 3`):

```
Initial state:
  slots: []           (0/3)
  pendingWrites: []   (0/3)

Write #1 (awaited): Goes directly to slots
  slots: [chunk1]     (1/3)
  pendingWrites: []   (0/3)

Write #2 (awaited): Goes directly to slots
  slots: [chunk1, chunk2]  (2/3)
  pendingWrites: []        (0/3)

Write #3 (awaited): Goes directly to slots
  slots: [chunk1, chunk2, chunk3]  (3/3) FULL
  pendingWrites: []                (0/3)

Write #4 (awaited): Slots full, waits in pendingWrites
  slots: [chunk1, chunk2, chunk3]  (3/3) FULL
  pendingWrites: [write4]          (1/3)
  --> write4 is waiting (Promise not resolved)

Consumer reads: Drains slots, pending write moves to slots
  slots: [chunk4]     (1/3)
  pendingWrites: []   (0/3)
  --> write4's Promise resolves, producer continues
```

**Fire-and-forget detection:**

```
Write #1 (NOT awaited): Goes to slots
Write #2 (NOT awaited): Goes to slots
Write #3 (NOT awaited): Goes to slots (FULL)
Write #4 (NOT awaited): Waits in pendingWrites
Write #5 (NOT awaited): Waits in pendingWrites
Write #6 (NOT awaited): Waits in pendingWrites (pendingWrites at limit)
Write #7 (NOT awaited): THROWS! Too many pending writes
  --> "Backpressure violation: too many pending writes"
```

If you're properly awaiting writes, you can only have ONE
pending write at a time (yours), so you'll never hit the pendingWrites limit.

#### Block Mode in Detail

Block mode is simpler: it always waits for space, with no limit on pending writes:

```typescript
const { writer, readable } = Stream.push({
  highWaterMark: 3,
  backpressure: 'block'
});

// Start consumer concurrently
const consuming = (async () => {
  for await (const chunks of readable) {
    await processChunks(chunks);
  }
})();

// Dangerous without await - queues indefinitely, may exhaust memory:
for (const item of hugeDataset) {
  writer.write(item);
}

// Safe - awaited writes block until the consumer reads:
for (const item of hugeDataset) {
  await writer.write(item);
}
await writer.end();
await consuming;
```

Block mode trusts the producer to properly await writes. Use it when:
- You control the producer code and know it awaits properly
- You intentionally want to queue writes (e.g., small bounded datasets)
- You're migrating from systems that don't enforce backpressure

#### Choosing Between Strict and Block

| Use Case | Recommended Policy |
|----------|-------------------|
| Untrusted/third-party producers | `strict` |
| Development/debugging | `strict` |
| High-performance trusted code | `block` |
| Intentional queuing of small data | `block` |
| Real-time data (drop is acceptable) | `drop-oldest` or `drop-newest` |

**Example: Safe streaming with strict mode:**

```typescript
const { writer, readable } = Stream.push({
  highWaterMark: 16,
  backpressure: 'strict'  // Default, explicit for clarity
});

// Consumer
const consumer = (async () => {
  for await (const chunks of readable) {
    await processChunks(chunks);  // Slow consumer
  }
})();

// Producer - this pattern is safe with strict mode
async function produce() {
  for (const data of largeDataset) {
    await writer.write(data);  // Awaited = safe
  }
  await writer.end();
}

await Promise.all([consumer, produce()]);
```

### Streamable Types

```typescript
// Primitive chunk types
type PrimitiveChunk = string | ArrayBuffer | ArrayBufferView;

// Raw byte input (single value)
type ByteInput = string | ArrayBuffer | ArrayBufferView;

// Sync streamable - has Symbol.iterator
interface SyncStreamable {
  [Symbol.iterator](): Iterator<SyncStreamableYield>;
}

// Async streamable - has Symbol.asyncIterator
interface AsyncStreamable {
  [Symbol.asyncIterator](): AsyncIterator<AsyncStreamableYield>;
}

// Either sync or async
type Streamable = SyncStreamable | AsyncStreamable;
```

---

## Protocol Symbols

Protocol symbols allow user-defined objects to participate in the streaming API.
All symbols are created via `Symbol.for()`, so third-party code can implement
these protocols without importing anything:

```typescript
// These are well-known symbols, not imports
Symbol.for('Stream.toStreamable')
Symbol.for('Stream.toAsyncStreamable')
Symbol.for('Stream.broadcastProtocol')
Symbol.for('Stream.shareProtocol')
Symbol.for('Stream.shareSyncProtocol')
Symbol.for('Stream.drainableProtocol')
```

### ToStreamable / ToAsyncStreamable

Allow objects to convert themselves to streamable data.

```typescript
class JsonMessage {
  constructor(private data: object) {}

  [Symbol.for('Stream.toStreamable')]() {
    return JSON.stringify(this.data);
  }
}

// Used by Stream.from() and transforms to normalize inputs.
// Not used by writer.write() which only accepts Uint8Array | string.
const readable = Stream.from(new JsonMessage({ hello: "world" }));
const text = await Stream.text(readable);
// text === '{"hello":"world"}'
```

The return value of `toStreamable` can be any supported input type (string,
ArrayBuffer, iterable, etc). For async conversions, implement
`toAsyncStreamable`. When both are present on an object, async paths
prefer `toAsyncStreamable`.

### Broadcastable / Shareable

Allow objects to provide optimized multi-consumer implementations:

```typescript
class OptimizedSource {
  [Symbol.for('Stream.shareProtocol')](options?: ShareOptions): Share {
    return new OptimizedShare(this, options);
  }
}

// Stream.share() checks for the protocol before creating its own Share
const shared = Stream.share(new OptimizedSource());
```

### Drainable

Allow objects to signal when backpressure clears. Used by [Stream.ondrain()](#streamondraindrainable)
to efficiently wait for write capacity.

Writers from `Stream.push()` and `Stream.broadcast()` automatically implement this protocol.

```typescript
// Any object implementing this method
[Symbol.for('Stream.drainableProtocol')](): Promise<boolean> | null;
```

The method returns:
- `null` if drain is not applicable (e.g., writer is closed)
- `Promise<true>` resolving immediately if ready, or when backpressure clears
- `Promise<false>` if writer closes while waiting
- Promise rejects if writer fails while waiting

**Custom implementation example:**

```typescript
class CustomWriter {
  private drainPromises: Array<{
    resolve: (v: boolean) => void;
    reject: (e: Error) => void;
  }> = [];

  [Symbol.for('Stream.drainableProtocol')](): Promise<boolean> | null {
    if (this.desiredSize === null) return null;
    if (this.desiredSize > 0) return Promise.resolve(true);

    return new Promise((resolve, reject) => {
      this.drainPromises.push({ resolve, reject });
    });
  }

  // Call when buffer space becomes available
  private notifyDrain() {
    for (const p of this.drainPromises) {
      p.resolve(true);
    }
    this.drainPromises = [];
  }

  // Call when writer closes
  private notifyClose() {
    for (const p of this.drainPromises) {
      p.resolve(false);
    }
    this.drainPromises = [];
  }

  // Call when writer errors
  private notifyError(error: Error) {
    for (const p of this.drainPromises) {
      p.reject(error);
    }
    this.drainPromises = [];
  }
}
```

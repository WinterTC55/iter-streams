# Migration Guide: Web Streams to New Stream API

This guide helps you migrate from the standard Web Streams API (`ReadableStream`, `WritableStream`, `TransformStream`) to the new simplified Stream API.

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Core Concepts](#core-concepts)
3. [Creating Streams](#creating-streams)
4. [Reading Data](#reading-data)
5. [Writing Data](#writing-data)
6. [Transforms](#transforms)
7. [Piping](#piping)
8. [Branching (Multi-Consumer)](#branching-multi-consumer)
9. [Error Handling](#error-handling)
10. [Cancellation and Abort](#cancellation-and-abort)
11. [Common Patterns](#common-patterns)

---

## Quick Reference

| Task | Web Streams | New Stream API |
|------|-------------|----------------|
| Create from data | `new ReadableStream({...})` | `Stream.from(data)` |
| Create push stream | `new ReadableStream({start(ctrl){}})` | `Stream.push()` |
| Read all bytes | Manual loop + concat | `await Stream.bytes(readable)` |
| Read as text | Manual loop + TextDecoder | `await Stream.text(readable)` |
| Collect chunks | Manual loop + push to array | `await Stream.array(readable)` |
| Iterate chunks | `for await (const chunk of stream)` | `for await (const batch of readable)` |
| Transform | `stream.pipeThrough(new TransformStream({...}))` | `Stream.pull(source, transform)` |
| Pipe to writer | `stream.pipeTo(writable)` | `Stream.pipeTo(source, writer)` |
| Branch stream | `stream.tee()` | `Stream.share(source)` or `Stream.broadcast()` |

---

## Core Concepts

### Key Differences

1. **Bytes-only**: The new API is exclusively for byte streams (`Uint8Array`). No generic value streams.

2. **Batched chunks**: Iterables yield `Uint8Array[]` (arrays of chunks) instead of single chunks. This amortizes async overhead.

3. **No locking**: There's no reader/writer locking mechanism. You interact with streams directly.

4. **No controllers**: Instead of `controller.enqueue()`, use generators or the `Writer` interface.

5. **Static functions**: All operations are static functions on the `Stream` namespace, not methods on stream objects.

6. **Explicit resource management**: Supports `using`/`await using` for automatic cleanup.

### Conceptual Mapping

| Web Streams Concept | New Stream API |
|---------------------|----------------|
| `ReadableStream` | `AsyncIterable<Uint8Array[]>` |
| `WritableStream` | `Writer` interface |
| `TransformStream` | `Transform` function |
| `ReadableStreamDefaultReader` | Not needed - iterate directly |
| `WritableStreamDefaultWriter` | `Writer` (from `Stream.push()`) |
| `ReadableStreamDefaultController` | Generator `yield` or `Writer.write()` |
| `stream.tee()` | `Stream.share()` or `Stream.broadcast()` |
| `stream.pipeThrough()` | `Stream.pull(source, ...transforms)` |
| `stream.pipeTo()` | `Stream.pipeTo(source, writer)` |

### Batched Iteration

The new API yields batches (`Uint8Array[]`) instead of single chunks:

```javascript
// Web Streams - single chunk per iteration
for await (const chunk of webReadableStream) {
  process(chunk);
}

// New API - batch of chunks per iteration
for await (const batch of readable) {
  for (const chunk of batch) {
    process(chunk);
  }
}
```

This batching improves performance by reducing promise overhead.

---

## Creating Streams

### From Static Data

**Web Streams:**
```javascript
const stream = new ReadableStream({
  start(controller) {
    controller.enqueue(new TextEncoder().encode('Hello, World!'));
    controller.close();
  }
});
```

**New Stream API:**
```javascript
// Simple one-liner
const readable = Stream.from('Hello, World!');

// From bytes
const readable = Stream.from(new Uint8Array([1, 2, 3]));

// From multiple chunks (array is iterable)
const readable = Stream.from(['chunk1', 'chunk2', 'chunk3']);
```

### From Generator (Pull Source)

**Web Streams:**
```javascript
async function* generateData() {
  for (let i = 0; i < 10; i++) {
    yield new TextEncoder().encode(`Line ${i}\n`);
  }
}

// Convert generator to ReadableStream - verbose!
const stream = new ReadableStream({
  async start(controller) {
    for await (const chunk of generateData()) {
      controller.enqueue(chunk);
    }
    controller.close();
  }
});
```

**New Stream API:**
```javascript
// Generator function - yields strings (auto-encoded) or Uint8Arrays
async function* generateData() {
  for (let i = 0; i < 10; i++) {
    yield `Line ${i}\n`;  // Strings auto-encoded to UTF-8
  }
}

// Just wrap with Stream.from()
const readable = Stream.from(generateData());
```

### Push Source (Events, WebSocket, etc.)

**Web Streams:**
```javascript
const stream = new ReadableStream({
  start(controller) {
    websocket.onmessage = (e) => {
      controller.enqueue(new TextEncoder().encode(e.data));
    };
    websocket.onclose = () => controller.close();
    websocket.onerror = (e) => controller.error(e);
  }
});
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push();

websocket.onmessage = (e) => {
  writer.write(e.data);  // Strings auto-encoded
};
websocket.onclose = () => writer.end();
websocket.onerror = (e) => writer.fail(e);

// Consumer reads from 'readable'
const text = await Stream.text(readable);
```

### Push Source with Backpressure

**Web Streams:**
```javascript
const stream = new ReadableStream({
  start(controller) {
    source.ondata = (data) => {
      controller.enqueue(data);
      if (controller.desiredSize <= 0) {
        source.pause();
      }
    };
  },
  pull(controller) {
    source.resume();
  }
});
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push({
  highWaterMark: 10,        // Max pending writes
  backpressure: 'block'     // Wait when full (use 'strict' to reject instead)
});

source.ondata = async (data) => {
  await writer.write(data);  // Automatically waits when buffer full with 'block' policy
};

// Or check desiredSize for non-blocking control
source.ondata = (data) => {
  if (writer.desiredSize === 0) {
    source.pause();
    return;
  }
  writer.writeSync(data);
};
```

---

## Reading Data

### Collect All Bytes

**Web Streams:**
```javascript
const reader = stream.getReader();
const chunks = [];
try {
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
} finally {
  reader.releaseLock();
}

// Concatenate chunks manually
const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
const result = new Uint8Array(totalLength);
let offset = 0;
for (const chunk of chunks) {
  result.set(chunk, offset);
  offset += chunk.length;
}
```

**New Stream API:**
```javascript
const result = await Stream.bytes(readable);
```

### Read as Text

**Web Streams:**
```javascript
const reader = stream.getReader();
const decoder = new TextDecoder();
let text = '';
try {
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  text += decoder.decode();  // Flush
} finally {
  reader.releaseLock();
}
```

**New Stream API:**
```javascript
const text = await Stream.text(readable);

// With different encoding
const text = await Stream.text(readable, { encoding: 'utf-16le' });
```

### Async Iteration

**Web Streams:**
```javascript
for await (const chunk of stream) {
  process(chunk);
}
```

**New Stream API:**
```javascript
// Yields batches (Uint8Array[])
for await (const batch of readable) {
  for (const chunk of batch) {
    process(chunk);
  }
}
```

### With Size Limits

**Web Streams:**
```javascript
// Manual implementation required
let totalBytes = 0;
const maxBytes = 1024 * 1024;
for await (const chunk of stream) {
  totalBytes += chunk.length;
  if (totalBytes > maxBytes) {
    throw new Error('Size limit exceeded');
  }
}
```

**New Stream API:**
```javascript
// Built-in limit option
const bytes = await Stream.bytes(readable, { limit: 1024 * 1024 });
// Throws RangeError if limit exceeded
```

---

## Writing Data

### Basic Writing

**Web Streams:**
```javascript
const writable = new WritableStream({
  write(chunk) {
    console.log('Received:', chunk);
  }
});

const writer = writable.getWriter();
await writer.write(new TextEncoder().encode('Hello'));
await writer.close();
writer.releaseLock();
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push();

// Consume stream somewhere...
Stream.text(readable).then(console.log);

// Write data
await writer.write('Hello');  // Strings auto-encoded
await writer.end();  // Returns total bytes written
```

### Batch Writing

**Web Streams:**
```javascript
const writer = writable.getWriter();
for (const chunk of chunks) {
  await writer.write(chunk);
}
```

**New Stream API:**
```javascript
// Write all at once as a single atomic operation
await writer.writev(chunks);

// Or individually
for (const chunk of chunks) {
  await writer.write(chunk);
}
```

### Sync Writing (Performance)

**New Stream API only:**
```javascript
// Try sync write - returns false if buffer is full
if (writer.writeSync(chunk)) {
  // Success, no await needed
} else {
  // Buffer full, fall back to async
  await writer.write(chunk);
}
```

---

## Transforms

### Simple Transform

**Web Streams:**
```javascript
const upperCaseTransform = new TransformStream({
  transform(chunk, controller) {
    const text = new TextDecoder().decode(chunk);
    controller.enqueue(new TextEncoder().encode(text.toUpperCase()));
  }
});

const result = source
  .pipeThrough(upperCaseTransform);
```

**New Stream API:**
```javascript
// Transform is just a function!
const uppercase = (chunks) => {
  if (chunks === null) return null;  // Flush signal
  return chunks.map(chunk => {
    const text = new TextDecoder().decode(chunk);
    return new TextEncoder().encode(text.toUpperCase());
  });
};

const result = Stream.pull(source, uppercase);
```

### Stateful Transform (with flush)

**Web Streams:**
```javascript
const splitLines = new TransformStream({
  buffer: '',
  transform(chunk, controller) {
    this.buffer += new TextDecoder().decode(chunk);
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop();
    for (const line of lines) {
      controller.enqueue(new TextEncoder().encode(line));
    }
  },
  flush(controller) {
    if (this.buffer) {
      controller.enqueue(new TextEncoder().encode(this.buffer));
    }
  }
});
```

**New Stream API:**
```javascript
// Use a stateful generator transform (object = stateful)
const splitLines = {
  async *transform(source) {
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = '';
    
    for await (const chunks of source) {
      if (chunks === null) {
        // Flush signal - emit remaining buffer
        if (buffer) yield encoder.encode(buffer);
        continue;
      }
      
      for (const chunk of chunks) {
        buffer += decoder.decode(chunk, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          yield encoder.encode(line);
        }
      }
    }
  }
};

const lines = Stream.pull(source, splitLines);
```

### Transform with Cleanup

**Web Streams:**
```javascript
const transform = new TransformStream({
  transform(chunk, controller) {
    controller.enqueue(process(chunk));
  },
  cancel(reason) {
    cleanup(reason);
  }
});
```

**New Stream API (stateless with signal-based cleanup):**
```javascript
const transform = (chunks, { signal }) => {
  signal.addEventListener('abort', () => cleanup(signal.reason), { once: true });
  if (chunks === null) return null;
  return chunks.map(process);
};

const result = Stream.pull(source, transform);
```

### Chaining Transforms

**Web Streams:**
```javascript
const result = source
  .pipeThrough(transform1)
  .pipeThrough(transform2)
  .pipeThrough(transform3);
```

**New Stream API:**
```javascript
// Pass multiple transforms to pull()
const result = Stream.pull(source, transform1, transform2, transform3);

// Or to pipeTo()
const bytes = await Stream.pipeTo(source, transform1, transform2, writer);
```

---

## Piping

### Pipe to Writer

**Web Streams:**
```javascript
await readable.pipeTo(writable);
```

**New Stream API:**
```javascript
const bytesWritten = await Stream.pipeTo(source, writer);
```

### Pipe with Transforms

**Web Streams:**
```javascript
await source
  .pipeThrough(compress)
  .pipeThrough(encrypt)
  .pipeTo(destination);
```

**New Stream API:**
```javascript
const bytesWritten = await Stream.pipeTo(
  source,
  compress,
  encrypt,
  destinationWriter
);
```

### Pipe with Options

**Web Streams:**
```javascript
await readable.pipeTo(writable, {
  preventClose: true,
  preventAbort: true,
  signal: abortController.signal
});
```

**New Stream API:**
```javascript
await Stream.pipeTo(source, writer, {
  preventClose: true,
  preventFail: true,
  signal: abortController.signal
});
```

---

## Branching (Multi-Consumer)

### Basic Branch (Tee)

**Web Streams:**
```javascript
const [branch1, branch2] = stream.tee();
```

**New Stream API - Pull Model (share):**
```javascript
// share() - pull-based, source only advances when consumers pull
const shared = Stream.share(source);

const consumer1 = shared.pull();
const consumer2 = shared.pull();

// Both consumers receive the same data
const [result1, result2] = await Promise.all([
  Stream.text(consumer1),
  Stream.text(consumer2)
]);
```

**New Stream API - Push Model (broadcast):**
```javascript
// broadcast() - push-based, producer controls data flow
const { writer, channel } = Stream.broadcast();

const consumer1 = channel.push();
const consumer2 = channel.push();

// Producer pushes to all consumers
await writer.write('shared data');
await writer.end();

const [result1, result2] = await Promise.all([
  Stream.text(consumer1),
  Stream.text(consumer2)
]);
```

### Branch with Different Transforms

**Web Streams:**
```javascript
const [branch1, branch2] = stream.tee();
const processed1 = branch1.pipeThrough(transform1);
const processed2 = branch2.pipeThrough(transform2);
```

**New Stream API:**
```javascript
const shared = Stream.share(source);

// Each consumer can have different transforms
const processed1 = shared.pull(transform1);
const processed2 = shared.pull(transform2);
```

### Buffer Configuration

**New Stream API only:**
```javascript
// Configure buffer for slow consumers
const shared = Stream.share(source, {
  highWaterMark: 100,
  backpressure: 'drop-oldest'  // or 'strict', 'block', 'drop-newest'
});

const { writer, channel } = Stream.broadcast({
  highWaterMark: 100,
  backpressure: 'block'  // Wait for space (use 'strict' to reject)
});
```

---

## Error Handling

### Source Errors

**Web Streams:**
```javascript
const stream = new ReadableStream({
  pull(controller) {
    controller.error(new Error('Source failed'));
  }
});

try {
  for await (const chunk of stream) {}
} catch (e) {
  console.log('Error:', e);
}
```

**New Stream API:**
```javascript
const readable = Stream.from(async function* () {
  throw new Error('Source failed');
});

try {
  for await (const batch of readable) {}
} catch (e) {
  console.log('Error:', e);
}
```

### Writer Errors

**Web Streams:**
```javascript
const writer = writable.getWriter();
await writer.abort(new Error('Something went wrong'));
```

**New Stream API:**
```javascript
// Fail propagates error to consumer
await writer.fail(new Error('Something went wrong'));

// Or sync version
writer.failSync(new Error('Something went wrong'));
```

---

## Cancellation and Abort

### Cancel Consumption

**Web Streams:**
```javascript
const reader = stream.getReader();
await reader.cancel('No longer needed');
reader.releaseLock();
```

**New Stream API:**
```javascript
// Just break out of iteration
for await (const batch of readable) {
  if (shouldStop) break;  // Automatically cleans up
}

// Or use AbortSignal
const controller = new AbortController();
const bytes = await Stream.bytes(readable, { signal: controller.signal });

// Cancel from elsewhere
controller.abort();
```

### Using AbortSignal

**Web Streams:**
```javascript
const controller = new AbortController();
await readable.pipeTo(writable, { signal: controller.signal });
setTimeout(() => controller.abort(), 5000);
```

**New Stream API:**
```javascript
const controller = new AbortController();

// Works with all consuming operations
const bytes = await Stream.bytes(readable, { signal: controller.signal });
const text = await Stream.text(readable, { signal: controller.signal });
await Stream.pipeTo(source, writer, { signal: controller.signal });

// With timeout
const signal = AbortSignal.timeout(5000);
const bytes = await Stream.bytes(readable, { signal });
```

### Explicit Resource Management

**Web Streams:**
```javascript
const reader = stream.getReader();
try {
  // Use reader...
} finally {
  reader.releaseLock();
}
```

**New Stream API:**
```javascript
// Automatic cleanup with 'using' (sync dispose)
{
  using shared = Stream.share(source);
  // Use shared...
} // shared.cancel() called automatically

// Async cleanup with 'await using'
{
  const { writer, channel } = Stream.broadcast();
  await using _ = channel;  // Will cancel on scope exit
  // Use channel...
}
```

---

## Common Patterns

### Wrap Web Streams Fetch Response

```javascript
// Wrap a fetch response body for use with new API
const response = await fetch(url);

const readable = Stream.from(async function* () {
  const reader = response.body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      yield value;
    }
  } finally {
    reader.releaseLock();
  }
});

const text = await Stream.text(readable);
```

### Process Large File in Chunks

**Web Streams:**
```javascript
const reader = file.stream().getReader();
try {
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    await processChunk(value);
  }
} finally {
  reader.releaseLock();
}
```

**New Stream API:**
```javascript
const readable = Stream.from(file.stream());

for await (const batch of readable) {
  for (const chunk of batch) {
    await processChunk(chunk);
  }
}
```

### Progress Tracking

**Web Streams:**
```javascript
let loaded = 0;
const total = +response.headers.get('content-length');

const reader = response.body.getReader();
while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  loaded += value.length;
  onProgress(loaded / total);
}
```

**New Stream API:**
```javascript
let loaded = 0;
const total = +response.headers.get('content-length');

// Use tap() to observe without modifying
const readable = Stream.pull(
  Stream.from(response.body),
  Stream.tap(chunks => {
    if (chunks !== null) {
      for (const chunk of chunks) {
        loaded += chunk.length;
      }
      onProgress(loaded / total);
    }
  })
);

const bytes = await Stream.bytes(readable);
```

### Merge Multiple Sources

**Web Streams:**
```javascript
// No built-in merge - requires manual implementation
```

**New Stream API:**
```javascript
const merged = Stream.merge(stream1, stream2, stream3);

for await (const batch of merged) {
  // Chunks arrive in temporal order (whichever produces first)
}
```

### JSON Lines Processing

**Web Streams:**
```javascript
const reader = response.body.getReader();
const decoder = new TextDecoder();
let buffer = '';

while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  
  buffer += decoder.decode(value, { stream: true });
  const lines = buffer.split('\n');
  buffer = lines.pop();
  
  for (const line of lines) {
    if (line.trim()) {
      const record = JSON.parse(line);
      await processRecord(record);
    }
  }
}
```

**New Stream API:**
```javascript
// Object = stateful transform (receives entire source as async iterable)
const lineBuffer = {
  async *transform(source) {
    const decoder = new TextDecoder();
    let buffer = '';
    
    for await (const chunks of source) {
      if (chunks === null) {
        if (buffer.trim()) yield new TextEncoder().encode(buffer);
        continue;
      }
      
      for (const chunk of chunks) {
        buffer += decoder.decode(chunk, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (line.trim()) yield new TextEncoder().encode(line);
        }
      }
    }
  }
};

const lines = Stream.pull(Stream.from(response.body), lineBuffer);

for await (const batch of lines) {
  for (const lineBytes of batch) {
    const record = JSON.parse(new TextDecoder().decode(lineBytes));
    await processRecord(record);
  }
}
```

### Sync Processing (No Async Overhead)

**New Stream API only:**
```javascript
// For CPU-bound transforms, avoid async entirely
const source = Stream.fromSync(['chunk1', 'chunk2', 'chunk3']);

const uppercase = (chunks) => {
  if (chunks === null) return null;
  return chunks.map(chunk => {
    const text = new TextDecoder().decode(chunk);
    return new TextEncoder().encode(text.toUpperCase());
  });
};

const result = Stream.pullSync(source, uppercase);
const text = Stream.textSync(result);
```

---

## Summary

The new Stream API provides:

1. **Less boilerplate** - Common operations are one-liners
2. **No lock management** - No `getReader()`/`releaseLock()` dance
3. **Static functions** - All operations via `Stream.*` namespace
4. **Batched iteration** - `Uint8Array[]` reduces async overhead
5. **Simple transforms** - Functions instead of `TransformStream` objects
6. **Built-in combinators** - `merge()`, `share()`, `broadcast()`
7. **Better backpressure** - Configurable policies (strict, block, drop-oldest, drop-newest)
8. **Sync variants** - Full sync API for CPU-bound workloads
9. **Automatic cleanup** - `using`/`await using` support

When migrating, the main changes are:
- Replace `new ReadableStream({...})` with `Stream.from()` or `Stream.push()`
- Replace `stream.getReader()` loops with `Stream.bytes()` / `Stream.text()` or `for await`
- Replace `new TransformStream({...})` with plain functions
- Replace `stream.pipeThrough()` with `Stream.pull(source, ...transforms)`
- Replace `stream.pipeTo()` with `Stream.pipeTo(source, writer)`
- Replace `stream.tee()` with `Stream.share()` or `Stream.broadcast()`
- Remember iteration yields `Uint8Array[]` batches, not single chunks

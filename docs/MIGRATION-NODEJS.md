# Migration Guide: Node.js Streams to New Stream API

This guide helps you migrate from Node.js streams (`stream.Readable`, `stream.Writable`, `stream.Transform`, `stream.Duplex`) to the new simplified Stream API.

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Core Concepts](#core-concepts)
3. [Creating Readable Streams](#creating-readable-streams)
4. [Reading Data](#reading-data)
5. [Creating Writable Streams](#creating-writable-streams)
6. [Transforms](#transforms)
7. [Piping](#piping)
8. [Object Mode and Value Streams](#object-mode-and-value-streams)
9. [Branching (Multi-Consumer)](#branching-multi-consumer)
10. [Error Handling](#error-handling)
11. [Backpressure](#backpressure)
12. [Common Patterns](#common-patterns)

---

## Quick Reference

| Task | Node.js Streams | New Stream API |
|------|-----------------|----------------|
| Create from data | `Readable.from(data)` | `Stream.from(data)` |
| Create push stream | `new Readable({ read() {} })` | `Stream.push()` |
| Read all bytes | `stream.toArray()` + concat | `await Stream.bytes(readable)` |
| Read as text | Manual + `Buffer.concat().toString()` | `await Stream.text(readable)` |
| Collect chunks | `stream.toArray()` | `await Stream.array(readable)` |
| Iterate chunks | `for await (const chunk of stream)` | `for await (const batch of readable)` |
| Transform | `stream.pipe(transform)` | `Stream.pull(source, transform)` |
| Pipe to writer | `stream.pipe(writable)` | `Stream.pipeTo(source, writer)` |
| Branch stream | Not built-in (use `PassThrough`) | `Stream.share(source)` |
| Destroy/cleanup | `stream.destroy()` | `AbortSignal` or `break` from iteration |

---

## Core Concepts

### Key Differences

1. **Bytes-only**: The new API is exclusively for byte streams (`Uint8Array`). No object mode.

2. **Batched chunks**: Iterables yield `Uint8Array[]` (arrays of chunks) instead of single chunks.

3. **No event emitters**: No `'data'`, `'end'`, `'error'` events. Use iteration and try/catch.

4. **Pull-through transforms**: Transforms execute lazily when consumers pull, not eagerly when data arrives.

5. **Static functions**: All operations are static functions on the `Stream` namespace.

6. **No flowing/paused modes**: No mode switching - pull semantics are the only model.

7. **Uint8Array, not Buffer**: Uses standard `Uint8Array` instead of Node.js `Buffer`.

### Conceptual Mapping

| Node.js Streams Concept | New Stream API |
|-------------------------|----------------|
| `stream.Readable` | `AsyncIterable<Uint8Array[]>` |
| `stream.Writable` | `Writer` interface |
| `stream.Transform` | `Transform` function or object |
| `stream.Duplex` | Separate readable + writer |
| `stream.PassThrough` | Identity transform or `Stream.share()` |
| `readable.read()` | Async iteration |
| `writable.write()` | `Writer.write()` |
| `stream.pipe()` | `Stream.pull()` or `Stream.pipeTo()` |
| `stream.pipeline()` | `Stream.pipeTo(source, ...transforms, writer)` |
| `readable._read()` | Generator function |
| `writable._write()` | Not needed - iterate the readable |
| Object mode | Use async iterables directly |

### Batched Iteration

The new API yields batches (`Uint8Array[]`) instead of single chunks:

```javascript
// Node.js Streams - single chunk per iteration
for await (const chunk of nodeReadable) {
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

### Buffer vs Uint8Array

```javascript
// Node.js Buffer extends Uint8Array - no conversion needed!
const buf = Buffer.from('hello');
buf instanceof Uint8Array;  // true

// You can pass Buffer directly to the new API
const readable = Stream.from(buf);  // Works!

// Results are Uint8Array, convert to Buffer only if you need Buffer-specific methods
const bytes = await Stream.bytes(readable);  // Uint8Array
const buf2 = Buffer.from(bytes);             // Only if you need Buffer methods like toString('hex')
```

---

## Creating Readable Streams

### From Static Data

**Node.js Streams:**
```javascript
const { Readable } = require('stream');

// From array
const stream = Readable.from(['chunk1', 'chunk2']);

// From buffer
const stream = Readable.from(Buffer.from('Hello, World!'));
```

**New Stream API:**
```javascript
// Simple one-liner - strings auto-encoded to UTF-8
const readable = Stream.from('Hello, World!');

// From bytes
const readable = Stream.from(new Uint8Array([1, 2, 3]));

// From multiple chunks
const readable = Stream.from(['chunk1', 'chunk2', 'chunk3']);
```

### From Generator (Pull Source)

**Node.js Streams:**
```javascript
const { Readable } = require('stream');

async function* generateData() {
  for (let i = 0; i < 10; i++) {
    yield Buffer.from(`Line ${i}\n`);
  }
}

const stream = Readable.from(generateData());
```

**New Stream API:**
```javascript
// Generator function - yields strings (auto-encoded) or Uint8Arrays
async function* generateData() {
  for (let i = 0; i < 10; i++) {
    yield `Line ${i}\n`;  // Strings auto-encoded to UTF-8
  }
}

// Wrap with Stream.from()
const readable = Stream.from(generateData());
```

### Implementing _read() (On-Demand Data)

**Node.js Streams:**
```javascript
const { Readable } = require('stream');

class MyReadable extends Readable {
  constructor(options) {
    super(options);
    this.counter = 0;
  }

  _read(size) {
    if (this.counter < 10) {
      this.push(Buffer.from(`chunk ${this.counter++}\n`));
    } else {
      this.push(null);  // Signal end
    }
  }
}

const stream = new MyReadable();
```

**New Stream API:**
```javascript
// Use a generator - cleaner and more intuitive
function* generateChunks() {
  for (let i = 0; i < 10; i++) {
    yield `chunk ${i}\n`;
  }
}

const readable = Stream.from(generateChunks());
```

### Push Source (Events, Sockets, etc.)

**Node.js Streams:**
```javascript
const { Readable } = require('stream');

const stream = new Readable({
  read() {}  // No-op, we push when data arrives
});

socket.on('data', (data) => {
  stream.push(data);
});
socket.on('end', () => {
  stream.push(null);
});
socket.on('error', (err) => {
  stream.destroy(err);
});
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push();

socket.on('data', (data) => {
  writer.write(data);  // Strings and Buffers work
});
socket.on('end', () => {
  writer.end();
});
socket.on('error', (err) => {
  writer.fail(err);
});

// Consumer reads from 'readable'
const bytes = await Stream.bytes(readable);
```

### From Node.js Readable

```javascript
// Wrap a Node.js readable stream for use with new API
const nodeReadable = fs.createReadStream('file.txt');

const readable = Stream.from(async function* () {
  for await (const chunk of nodeReadable) {
    yield chunk;
  }
});

// Or more directly if it's async iterable:
const readable = Stream.from(nodeReadable);
```

---

## Reading Data

### Collect All Bytes

**Node.js Streams:**
```javascript
const chunks = [];
for await (const chunk of stream) {
  chunks.push(chunk);
}
const result = Buffer.concat(chunks);
```

**New Stream API:**
```javascript
const result = await Stream.bytes(readable);
// Returns Uint8Array - works directly since Buffer extends Uint8Array
// Convert to Buffer only if you need Buffer-specific methods:
const buffer = Buffer.from(result);  // e.g., for toString('hex'), compare(), etc.
```

### Read as Text

**Node.js Streams:**
```javascript
const chunks = [];
for await (const chunk of stream) {
  chunks.push(chunk);
}
const text = Buffer.concat(chunks).toString('utf8');
```

**New Stream API:**
```javascript
const text = await Stream.text(readable);

// With different encoding
const text = await Stream.text(readable, { encoding: 'utf-16le' });
```

### Using stream.Readable.toArray()

**Node.js Streams (v17+):**
```javascript
const chunks = await stream.toArray();
const result = Buffer.concat(chunks);
```

**New Stream API:**
```javascript
const result = await Stream.bytes(readable);
```

### Async Iteration

**Node.js Streams:**
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

**Node.js Streams:**
```javascript
// Manual implementation
let totalBytes = 0;
const maxBytes = 1024 * 1024;
const chunks = [];

for await (const chunk of stream) {
  totalBytes += chunk.length;
  if (totalBytes > maxBytes) {
    stream.destroy(new Error('Size limit exceeded'));
    throw new Error('Size limit exceeded');
  }
  chunks.push(chunk);
}
```

**New Stream API:**
```javascript
// Built-in limit option
const bytes = await Stream.bytes(readable, { limit: 1024 * 1024 });
// Throws RangeError if limit exceeded
```

### Sync Reading (In-Memory Data)

**New Stream API only:**
```javascript
// For synchronous sources, avoid async entirely
const source = Stream.fromSync(['chunk1', 'chunk2', 'chunk3']);
const bytes = Stream.bytesSync(source);
const text = Stream.textSync(source);
```

---

## Creating Writable Streams

### Basic Writable

**Node.js Streams:**
```javascript
const { Writable } = require('stream');

const writable = new Writable({
  write(chunk, encoding, callback) {
    console.log('Received:', chunk.toString());
    callback();  // Signal completion
  }
});

stream.pipe(writable);
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push();

// Start consuming (this drives the writer)
(async () => {
  for await (const batch of readable) {
    for (const chunk of batch) {
      console.log('Received:', new TextDecoder().decode(chunk));
    }
  }
})();

// Write data
await writer.write('Hello');
await writer.end();
```

### Implementing _write()

**Node.js Streams:**
```javascript
const { Writable } = require('stream');

class FileWriter extends Writable {
  constructor(fd) {
    super();
    this.fd = fd;
  }

  _write(chunk, encoding, callback) {
    fs.write(this.fd, chunk, callback);
  }

  _final(callback) {
    fs.close(this.fd, callback);
  }
}
```

**New Stream API:**
```javascript
// Implement the Writer interface
class FileWriter {
  constructor(fd) {
    this.fd = fd;
    this.bytesWritten = 0;
  }

  get desiredSize() {
    return 16;  // Always accept up to 16 pending writes
  }

  async write(chunk) {
    const data = typeof chunk === 'string'
      ? new TextEncoder().encode(chunk)
      : chunk;
    await fs.promises.write(this.fd, data);
    this.bytesWritten += data.byteLength;
  }

  async writev(chunks) {
    for (const chunk of chunks) {
      await this.write(chunk);
    }
  }

  writeSync(chunk) {
    // Return false if you can't accept sync writes
    return false;
  }

  writevSync(chunks) {
    return false;
  }

  async end() {
    await fs.promises.close(this.fd);
    return this.bytesWritten;
  }

  endSync() {
    return -1;  // Can't close synchronously
  }

  async fail(reason) {
    await fs.promises.close(this.fd);
  }

  failSync(reason) {
    return false;
  }
}

// Use with pipeTo
await Stream.pipeTo(source, new FileWriter(fd));
```

### writev for Batch Efficiency

**Node.js Streams:**
```javascript
const { Writable } = require('stream');

class BatchWriter extends Writable {
  constructor() {
    super({ writev: true });
  }

  _writev(chunks, callback) {
    // chunks is array of { chunk, encoding }
    const buffers = chunks.map(c => c.chunk);
    writeAllAtOnce(buffers, callback);
  }
}
```

**New Stream API:**
```javascript
class BatchWriter {
  // ... other methods ...

  async writev(chunks) {
    // chunks is array of Uint8Array | string
    const buffers = chunks.map(c =>
      typeof c === 'string' ? new TextEncoder().encode(c) : c
    );
    await writeAllAtOnce(buffers);
  }
}

// Stream.pipeTo automatically uses writev when available
await Stream.pipeTo(source, batchWriter);
```

---

## Transforms

### Simple Transform

**Node.js Streams:**
```javascript
const { Transform } = require('stream');

const upperCase = new Transform({
  transform(chunk, encoding, callback) {
    callback(null, chunk.toString().toUpperCase());
  }
});

readable.pipe(upperCase).pipe(writable);
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

**Node.js Streams:**
```javascript
const { Transform } = require('stream');

class LineTransform extends Transform {
  constructor() {
    super();
    this.buffer = '';
  }

  _transform(chunk, encoding, callback) {
    this.buffer += chunk.toString();
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop();

    for (const line of lines) {
      this.push(line + '\n');
    }
    callback();
  }

  _flush(callback) {
    if (this.buffer) {
      this.push(this.buffer);
    }
    callback();
  }
}
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
          yield encoder.encode(line + '\n');
        }
      }
    }
  }
};

const lines = Stream.pull(source, splitLines);
```

### Transform with Cleanup

**Node.js Streams:**
```javascript
const { Transform } = require('stream');

class ResourceTransform extends Transform {
  constructor() {
    super();
    this.resource = acquireResource();
  }

  _transform(chunk, encoding, callback) {
    callback(null, this.resource.process(chunk));
  }

  _destroy(err, callback) {
    this.resource.release();
    callback(err);
  }
}
```

**New Stream API:**
```javascript
const resourceTransform = {
  async *transform(source) {
    const resource = acquireResource();
    try {
      for await (const chunks of source) {
        if (chunks === null) continue;
        for (const chunk of chunks) {
          yield resource.process(chunk);
        }
      }
    } finally {
      resource.release();
    }
  },
};

const result = Stream.pull(source, resourceTransform);
```

### PassThrough Equivalent

**Node.js Streams:**
```javascript
const { PassThrough } = require('stream');

const pass = new PassThrough();
source.pipe(pass);
pass.pipe(dest1);
pass.pipe(dest2);  // Both destinations get the data
```

**New Stream API:**
```javascript
// For multi-consumer, use share()
const shared = Stream.share(source);

const consumer1 = shared.pull();
const consumer2 = shared.pull();

await Promise.all([
  Stream.pipeTo(consumer1, dest1),
  Stream.pipeTo(consumer2, dest2)
]);
```

### Chaining Transforms

**Node.js Streams:**
```javascript
readable
  .pipe(transform1)
  .pipe(transform2)
  .pipe(transform3)
  .pipe(writable);
```

**New Stream API:**
```javascript
// Pass multiple transforms to pull()
const result = Stream.pull(source, transform1, transform2, transform3);

// Or to pipeTo()
await Stream.pipeTo(source, transform1, transform2, transform3, writer);
```

---

## Piping

### Basic Pipe

**Node.js Streams:**
```javascript
readable.pipe(writable);
```

**New Stream API:**
```javascript
await Stream.pipeTo(source, writer);
```

### Pipeline (with error handling)

**Node.js Streams:**
```javascript
const { pipeline } = require('stream/promises');

await pipeline(
  readable,
  transform1,
  transform2,
  writable
);
```

**New Stream API:**
```javascript
await Stream.pipeTo(source, transform1, transform2, writer);
// Errors automatically propagate
```

### Pipeline with Abort

**Node.js Streams:**
```javascript
const { pipeline } = require('stream/promises');

const ac = new AbortController();

await pipeline(
  readable,
  transform,
  writable,
  { signal: ac.signal }
);
```

**New Stream API:**
```javascript
const ac = new AbortController();

await Stream.pipeTo(source, transform, writer, {
  signal: ac.signal
});
```

### Preventing Auto-Close

**Node.js Streams:**
```javascript
readable.pipe(writable, { end: false });
// writable.end() not called automatically
```

**New Stream API:**
```javascript
await Stream.pipeTo(source, writer, { preventClose: true });
// writer.end() not called automatically
```

---

## Object Mode and Value Streams

### Object Mode Readable

**Node.js Streams:**
```javascript
const { Readable } = require('stream');

const stream = Readable.from([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
], { objectMode: true });

for await (const obj of stream) {
  console.log(obj.name);
}
```

**New Stream API:**
```javascript
// The new API is bytes-only. For objects, use async iterables directly:
async function* objects() {
  yield { id: 1, name: 'Alice' };
  yield { id: 2, name: 'Bob' };
}

for await (const obj of objects()) {
  console.log(obj.name);
}

// Or encode as JSON lines for byte streaming:
const readable = Stream.from(async function* () {
  for (const obj of [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]) {
    yield JSON.stringify(obj) + '\n';
  }
});
```

### Object Mode Transform

**Node.js Streams:**
```javascript
const { Transform } = require('stream');

const mapTransform = new Transform({
  objectMode: true,
  transform(obj, encoding, callback) {
    callback(null, { ...obj, processed: true });
  }
});
```

**New Stream API:**
```javascript
// Use async iterables for object transformation
async function* mapObjects(source) {
  for await (const obj of source) {
    yield { ...obj, processed: true };
  }
}

// Usage:
const objects = [{ id: 1 }, { id: 2 }];
for await (const obj of mapObjects(objects)) {
  console.log(obj);  // { id: 1, processed: true }, { id: 2, processed: true }
}
```

---

## Branching (Multi-Consumer)

### Multiple Destinations

**Node.js Streams:**
```javascript
const { PassThrough } = require('stream');

const pass1 = new PassThrough();
const pass2 = new PassThrough();

source.pipe(pass1);
source.pipe(pass2);  // Warning: This doesn't work well!

// Better approach:
source.on('data', (chunk) => {
  pass1.write(chunk);
  pass2.write(chunk);
});
source.on('end', () => {
  pass1.end();
  pass2.end();
});
```

**New Stream API - Pull Model (share):**
```javascript
// share() - consumers pull from shared source
const shared = Stream.share(source);

const consumer1 = shared.pull();
const consumer2 = shared.pull();

const [result1, result2] = await Promise.all([
  Stream.text(consumer1),
  Stream.text(consumer2)
]);
```

**New Stream API - Push Model (broadcast):**
```javascript
// broadcast() - producer pushes to all consumers
const { writer, channel } = Stream.broadcast();

const consumer1 = channel.push();
const consumer2 = channel.push();

// Push data
await writer.write('shared data');
await writer.end();

const [result1, result2] = await Promise.all([
  Stream.text(consumer1),
  Stream.text(consumer2)
]);
```

### Different Processing per Branch

**Node.js Streams:**
```javascript
const { PassThrough } = require('stream');

// Awkward manual approach
const branches = [new PassThrough(), new PassThrough()];

source.on('data', (chunk) => {
  branches.forEach(b => b.write(chunk));
});

branches[0].pipe(compressTransform).pipe(compressedDest);
branches[1].pipe(hashTransform).pipe(hashDest);
```

**New Stream API:**
```javascript
const shared = Stream.share(source);

// Each consumer can have different transforms
const compressed = shared.pull(compress);
const hashed = shared.pull(hash);

await Promise.all([
  Stream.pipeTo(compressed, compressedDest),
  Stream.pipeTo(hashed, hashDest)
]);
```

### Buffer Configuration

**New Stream API only:**
```javascript
// Configure buffer for handling slow consumers
const shared = Stream.share(source, {
  highWaterMark: 100,
  backpressure: 'drop-oldest'  // or 'strict', 'block', 'drop-newest'
});
```

---

## Error Handling

### Stream Errors

**Node.js Streams:**
```javascript
readable.on('error', (err) => {
  console.error('Stream error:', err);
});

// Or with pipeline
const { pipeline } = require('stream/promises');
try {
  await pipeline(readable, writable);
} catch (err) {
  console.error('Pipeline error:', err);
}
```

**New Stream API:**
```javascript
// Errors propagate through iteration
try {
  for await (const batch of readable) {
    // process...
  }
} catch (err) {
  console.error('Stream error:', err);
}

// Or with consumers
try {
  const bytes = await Stream.bytes(readable);
} catch (err) {
  console.error('Stream error:', err);
}
```

### Writer Errors

**Node.js Streams:**
```javascript
writable.destroy(new Error('Write failed'));
```

**New Stream API:**
```javascript
await writer.fail(new Error('Write failed'));

// Or sync
writer.failSync(new Error('Write failed'));
```

### Transform Errors

**Node.js Streams:**
```javascript
const transform = new Transform({
  transform(chunk, encoding, callback) {
    if (badData) {
      callback(new Error('Bad data'));
    } else {
      callback(null, processedData);
    }
  }
});
```

**New Stream API:**
```javascript
// Just throw - errors propagate naturally
const transform = (chunks) => {
  if (chunks === null) return null;
  for (const chunk of chunks) {
    if (isBadData(chunk)) {
      throw new Error('Bad data');
    }
  }
  return chunks.map(process);
};

try {
  const result = await Stream.bytes(Stream.pull(source, transform));
} catch (err) {
  console.error('Transform error:', err);
}
```

---

## Backpressure

### Handling Backpressure (Writing)

**Node.js Streams:**
```javascript
const writable = getWritableStream();

for (const data of largeDataset) {
  const canContinue = writable.write(data);
  if (!canContinue) {
    // Wait for drain event
    await new Promise(resolve => writable.once('drain', resolve));
  }
}
writable.end();
```

**New Stream API:**
```javascript
const { writer, readable } = Stream.push({
  highWaterMark: 16,
  backpressure: 'block'  // Async writes wait when full
});

// Just await writes - backpressure is automatic
for (const data of largeDataset) {
  await writer.write(data);  // Waits when buffer is full
}
await writer.end();
```

### Detecting Backpressure

**Node.js Streams:**
```javascript
// Check if writable is ready
if (writable.writableNeedDrain) {
  await new Promise(resolve => writable.once('drain', resolve));
}

// Or check highWaterMark
const hwm = writable.writableHighWaterMark;
const current = writable.writableLength;
const hasRoom = current < hwm;
```

**New Stream API:**
```javascript
// Check desiredSize (always >= 0 or null)
if (writer.desiredSize === 0) {
  // Buffer full - next write will wait (block) or reject (strict)
}

if (writer.desiredSize === null) {
  // Writer is closed or errored
}

// Try sync write first, fall back to async
if (!writer.writeSync(data)) {
  await writer.write(data);
}
```

### Strict Backpressure (Catching Fire-and-Forget)

**New Stream API only:**
```javascript
// 'strict' mode catches when you forget to await writes
const { writer, readable } = Stream.push({
  highWaterMark: 4,
  backpressure: 'strict'  // Default
});

// BAD: Not awaiting writes
for (const data of largeDataset) {
  writer.write(data);  // Will throw after buffer fills!
}

// GOOD: Awaiting writes
for (const data of largeDataset) {
  await writer.write(data);  // Works correctly
}
```

---

## Common Patterns

### File Processing

**Node.js Streams:**
```javascript
const fs = require('fs');
const { pipeline } = require('stream/promises');

await pipeline(
  fs.createReadStream('input.txt'),
  transformStream,
  fs.createWriteStream('output.txt')
);
```

**New Stream API:**
```javascript
// Wrap Node.js streams
const source = Stream.from(fs.createReadStream('input.txt'));

// Create a Writer wrapper for the write stream
const dest = fs.createWriteStream('output.txt');
const writer = {
  desiredSize: 16,
  write(chunk) {
    return new Promise((resolve, reject) => {
      const ok = dest.write(chunk, (err) => err ? reject(err) : resolve());
      if (!ok) {
        dest.once('drain', resolve);
      }
    });
  },
  writev(chunks) {
    return Promise.all(chunks.map(c => this.write(c))).then(() => {});
  },
  writeSync: () => false,
  writevSync: () => false,
  end() {
    return new Promise(resolve => dest.end(resolve));
  },
  endSync: () => -1,
  fail(err) {
    dest.destroy(err);
    return Promise.resolve();
  },
  failSync: () => false
};

await Stream.pipeTo(source, transform, writer);
```

### HTTP Request/Response

**Node.js Streams:**
```javascript
const http = require('http');

http.createServer((req, res) => {
  // Read request body
  const chunks = [];
  req.on('data', chunk => chunks.push(chunk));
  req.on('end', () => {
    const body = Buffer.concat(chunks);

    // Process and respond
    res.writeHead(200);
    res.end(processBody(body));
  });
});
```

**New Stream API:**
```javascript
const http = require('http');

http.createServer(async (req, res) => {
  // Read request body (wrap Node.js stream)
  const readable = Stream.from(req);
  const body = await Stream.bytes(readable);

  // Process and respond
  res.writeHead(200);
  res.end(processBody(body));
});
```

### Progress Tracking

**Node.js Streams:**
```javascript
const { Transform } = require('stream');

let bytesProcessed = 0;

const progress = new Transform({
  transform(chunk, encoding, callback) {
    bytesProcessed += chunk.length;
    console.log(`Processed: ${bytesProcessed} bytes`);
    callback(null, chunk);
  }
});

source.pipe(progress).pipe(dest);
```

**New Stream API:**
```javascript
let bytesProcessed = 0;

const withProgress = Stream.pull(
  source,
  Stream.tap(chunks => {
    if (chunks !== null) {
      for (const chunk of chunks) {
        bytesProcessed += chunk.byteLength;
      }
      console.log(`Processed: ${bytesProcessed} bytes`);
    }
  })
);

await Stream.pipeTo(withProgress, writer);
```

### Compression

**Node.js Streams:**
```javascript
const zlib = require('zlib');

const compressed = source.pipe(zlib.createGzip());
```

**New Stream API:**
```javascript
// Wrap zlib transform
const gzip = {
  async *transform(source) {
    const zlibStream = zlib.createGzip();

    // Feed source into zlib
    (async () => {
      for await (const chunks of source) {
        if (chunks === null) {
          zlibStream.end();
          return;
        }
        for (const chunk of chunks) {
          zlibStream.write(chunk);
        }
      }
      zlibStream.end();
    })();

    // Yield compressed output
    for await (const chunk of zlibStream) {
      yield chunk;
    }
  }
};

const compressed = Stream.pull(source, gzip);
```

### Line-by-Line Processing

**Node.js Streams:**
```javascript
const readline = require('readline');

const rl = readline.createInterface({
  input: fs.createReadStream('file.txt'),
  crlfDelay: Infinity
});

for await (const line of rl) {
  processLine(line);
}
```

**New Stream API:**
```javascript
const lineBuffer = {
  async *transform(source) {
    const decoder = new TextDecoder();
    let buffer = '';

    for await (const chunks of source) {
      if (chunks === null) {
        if (buffer) yield new TextEncoder().encode(buffer);
        continue;
      }

      for (const chunk of chunks) {
        buffer += decoder.decode(chunk, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          yield new TextEncoder().encode(line);
        }
      }
    }
  }
};

const source = Stream.from(fs.createReadStream('file.txt'));
const lines = Stream.pull(source, lineBuffer);

for await (const batch of lines) {
  for (const lineBytes of batch) {
    const line = new TextDecoder().decode(lineBytes);
    processLine(line);
  }
}
```

### Timeout Handling

**Node.js Streams:**
```javascript
const { pipeline } = require('stream/promises');
const { setTimeout } = require('timers/promises');

const ac = new AbortController();

// Set timeout
setTimeout(5000).then(() => ac.abort());

try {
  await pipeline(source, dest, { signal: ac.signal });
} catch (err) {
  if (err.name === 'AbortError') {
    console.log('Stream timed out');
  }
}
```

**New Stream API:**
```javascript
// Use AbortSignal.timeout()
const signal = AbortSignal.timeout(5000);

try {
  await Stream.pipeTo(source, writer, { signal });
} catch (err) {
  if (err.name === 'AbortError') {
    console.log('Stream timed out');
  }
}

// Or with consumers
const bytes = await Stream.bytes(readable, {
  signal: AbortSignal.timeout(5000)
});
```

### Sync Processing (No Async Overhead)

**Node.js Streams:**
```javascript
// Node.js streams don't have a true sync mode
```

**New Stream API:**
```javascript
// For in-memory data, avoid async entirely
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

1. **Simpler mental model** - Pull-based only, no flowing/paused modes
2. **No event emitters** - Use async iteration and try/catch
3. **Static functions** - All operations via `Stream.*` namespace
4. **Batched iteration** - `Uint8Array[]` reduces async overhead
5. **Simple transforms** - Functions instead of Transform classes
6. **Better backpressure** - Configurable policies with non-negative desiredSize
7. **Built-in combinators** - `merge()`, `share()`, `broadcast()`
8. **Sync variants** - Full sync API for in-memory workloads
9. **Standard types** - `Uint8Array` instead of Node.js-specific `Buffer`
10. **Automatic cleanup** - Iteration handles cleanup, plus `using` support

When migrating from Node.js streams:

- Replace `Readable.from()` with `Stream.from()`
- Replace `new Readable({ read() {} })` with `Stream.push()`
- Replace event-based reading with async iteration or `Stream.bytes()`/`Stream.text()`
- Replace `Transform` classes with transform functions
- Replace `.pipe()` chains with `Stream.pull()` or `Stream.pipeTo()`
- Replace `pipeline()` with `Stream.pipeTo(source, ...transforms, writer)`
- Replace `PassThrough` for branching with `Stream.share()`
- Replace `destroy()` with `AbortSignal` or breaking from iteration
- Remember iteration yields `Uint8Array[]` batches, not single `Buffer`s
- For object mode, use async iterables directly instead of byte streams

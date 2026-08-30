/**
 * Web Streams Interoperability
 *
 * This file demonstrates adapting between the new Stream API and the standard
 * Web Streams API (ReadableStream, WritableStream, TransformStream).
 *
 * Unlike Node.js streams, Web Streams are available in browsers and have
 * slightly different semantics. This sample shows bidirectional conversion
 * with proper error propagation and lifecycle management.
 */

import { Stream, type Writer, type WriteOptions, type Transform } from '../src/index.js';
import { section, uppercaseTransform } from './util.js';

// Shared encoder/decoder instances for efficiency
const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ============================================================================
// Adapter: Web ReadableStream -> New Stream API
// ============================================================================

/**
 * Convert a Web ReadableStream to the new Stream API.
 *
 * Features:
 * - Proper backpressure handling (pull-based)
 * - Error propagation from ReadableStream to new Stream
 * - Cleanup on cancel (cancels the underlying ReadableStream)
 */
function fromReadableStream<T extends ArrayBufferView | ArrayBuffer | string>(
  readable: ReadableStream<T>
) {
  return Stream.from(
    (async function* () {
      const reader = readable.getReader();
      let completed = false;

      try {
        while (true) {
          const { value, done } = await reader.read();

          if (done) {
            completed = true;
            break;
          }

          // Handle different chunk types
          if (typeof value === 'string') {
            yield value;
          } else if (value instanceof ArrayBuffer) {
            yield new Uint8Array(value);
          } else if (ArrayBuffer.isView(value)) {
            yield new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
          } else {
            throw new Error(`Unexpected chunk type: ${typeof value}`);
          }
        }
      } finally {
        // If we didn't complete normally, cancel the underlying stream
        if (!completed) {
          await reader.cancel('Stream consumer cancelled');
        }
        // Release the reader lock
        reader.releaseLock();
      }
    })()
  );
}

// ============================================================================
// Adapter: New Stream API -> Web ReadableStream
// ============================================================================

/**
 * Convert a new Stream to a Web ReadableStream.
 *
 * Features:
 * - Proper backpressure (underlying source pull model)
 * - Error propagation
 * - Cleanup when ReadableStream is cancelled
 */
function toReadableStream(source: AsyncIterable<Uint8Array[]>): ReadableStream<Uint8Array> {
  const iterator = source[Symbol.asyncIterator]();
  let cancelled = false;

  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        if (cancelled) return;

        try {
          const { value: batch, done } = await iterator.next();

          if (done) {
            controller.close();
          } else {
            // Enqueue all chunks in the batch
            for (const chunk of batch) {
              controller.enqueue(chunk);
            }
          }
        } catch (error) {
          controller.error(error);
        }
      },

      async cancel(reason) {
        cancelled = true;
        // Call return() on the iterator to trigger cleanup
        if (iterator.return) {
          await iterator.return(undefined);
        }
      },
    },
    // Use a queuing strategy that provides some buffering
    new CountQueuingStrategy({ highWaterMark: 1 })
  );
}

// ============================================================================
// Adapter: Create Writer that writes to Web WritableStream
// ============================================================================

/**
 * Create a Writer that writes to a Web WritableStream.
 *
 * Features:
 * - Proper backpressure handling (waits for ready)
 * - Error propagation
 * - Proper close/fail handling
 */
function createWebWriter(writable: WritableStream<Uint8Array>): Writer {
  const webWriter = writable.getWriter();
  let closed = false;
  let byteCount = 0;

  return {
    get desiredSize(): number | null {
      return closed ? null : webWriter.desiredSize ?? 16384;
    },

    async write(chunk: Uint8Array | string, options?: WriteOptions): Promise<void> {
      if (closed) throw new Error('Writer is closed');
      options?.signal?.throwIfAborted();
      await webWriter.ready;
      const bytes = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
      await webWriter.write(bytes);
      byteCount += bytes.byteLength;
    },

    async writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void> {
      for (const chunk of chunks) {
        await this.write(chunk, options);
      }
    },

    writeSync(chunk: Uint8Array | string): boolean {
      // Web WritableStream doesn't support sync writes
      return false;
    },

    writevSync(chunks: (Uint8Array | string)[]): boolean {
      return false;
    },

    async end(options?: WriteOptions): Promise<number> {
      options?.signal?.throwIfAborted();
      closed = true;
      await webWriter.close();
      return byteCount;
    },

    endSync(): number {
      return -1; // Not supported
    },

    async fail(reason?: any): Promise<void> {
      closed = true;
      await webWriter.abort(reason);
    },

    failSync(reason?: any): boolean {
      return false; // Not supported
    },
  };
}

// ============================================================================
// Adapter: New Stream API -> Web WritableStream
// ============================================================================

/**
 * Create a Web WritableStream that writes to the new Stream API via a Writer.
 *
 * Returns both the WritableStream and the readable Stream that receives the data.
 */
function toWritableStream(options?: { highWaterMark?: number }): {
  writable: WritableStream<Uint8Array>;
  readable: AsyncIterable<Uint8Array[]>;
} {
  const { writer, readable } = Stream.push({
    highWaterMark: options?.highWaterMark ?? 16,
  });

  const writable = new WritableStream<Uint8Array>({
    async write(chunk) {
      await writer.write(chunk);
    },

    async close() {
      await writer.end();
    },

    async abort(reason) {
      await writer.fail(reason);
    },
  });

  return { writable, readable };
}

// ============================================================================
// Main examples
// ============================================================================

async function main() {
  // ============================================================================
  // Example 1: Web ReadableStream -> New Stream API
  // ============================================================================
  section('Example 1: Web ReadableStream -> New Stream API');

  {
    // Create a Web ReadableStream
    const webReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode('Hello '));
        controller.enqueue(encoder.encode('from '));
        controller.enqueue(encoder.encode('Web Streams!'));
        controller.close();
      },
    });

    // Convert to new Stream API
    const readable = fromReadableStream(webReadable);

    // Use new Stream API methods
    const text = await Stream.text(readable);
    console.log('Read from Web ReadableStream:', text);
  }

  // ============================================================================
  // Example 2: New Stream API -> Web ReadableStream
  // ============================================================================
  section('Example 2: New Stream API -> Web ReadableStream');

  {
    // Create a new Stream
    const readable = Stream.from('Hello from new Stream API!');

    // Convert to Web ReadableStream
    const webReadable = toReadableStream(readable);

    // Use Web Streams API to consume
    const reader = webReadable.getReader();
    const chunks: Uint8Array[] = [];

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    const localDecoder = new TextDecoder();
    const text = chunks.map((c) => localDecoder.decode(c, { stream: true })).join('');
    console.log('Read from Web ReadableStream:', text);
  }

  // ============================================================================
  // Example 3: Pipe new Stream to Web WritableStream
  // ============================================================================
  section('Example 3: New Stream -> Web WritableStream');

  {
    // Collect output in an array (simulating a destination)
    const output: Uint8Array[] = [];

    // Create a Web WritableStream
    const webWritable = new WritableStream<Uint8Array>({
      write(chunk) {
        output.push(chunk);
      },
    });

    // Create source stream
    const source = Stream.from('Data flowing to Web WritableStream');

    // Pipe to Web WritableStream using our adapter
    const writer = createWebWriter(webWritable);
    await Stream.pipeTo(source, writer);

    // Verify output
    const text = output.map((c) => decoder.decode(c)).join('');
    console.log('Written to Web WritableStream:', text);
  }

  // ============================================================================
  // Example 4: Web WritableStream -> New Stream (bidirectional)
  // ============================================================================
  section('Example 4: Web WritableStream -> New Stream');

  {
    // Create a writable that feeds into our Stream
    const { writable, readable } = toWritableStream();

    // Start consuming in background
    const textPromise = Stream.text(readable);

    // Write using Web Streams API
    const writer = writable.getWriter();
    await writer.write(encoder.encode('Written '));
    await writer.write(encoder.encode('via '));
    await writer.write(encoder.encode('Web WritableStream'));
    await writer.close();

    // Read using new Stream API
    const text = await textPromise;
    console.log('Read from new Stream:', text);
  }

  // ============================================================================
  // Example 5: Using Web TransformStream with new Stream
  // ============================================================================
  section('Example 5: Web TransformStream Integration');

  {
    // Create a Web TransformStream that uppercases text
    const webUppercaseTransform = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        const text = decoder.decode(chunk);
        const upper = text.toUpperCase();
        controller.enqueue(encoder.encode(upper));
      },
    });

    // Create source stream
    const source = Stream.from('transform me to uppercase');

    // Convert to Web ReadableStream, pipe through transform, convert back
    const webReadable = toReadableStream(source);
    const transformed = webReadable.pipeThrough(webUppercaseTransform);
    const result = fromReadableStream(transformed);

    const text = await Stream.text(result);
    console.log('Transformed result:', text);
  }

  // ============================================================================
  // Example 6: Error Propagation (Web -> New Stream)
  // ============================================================================
  section('Example 6: Error Propagation (Web -> New Stream)');

  {
    // Create a Web ReadableStream that errors
    const erroringReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode('some data'));
      },
      pull(controller) {
        controller.error(new Error('Intentional Web Stream error'));
      },
    });

    const readable = fromReadableStream(erroringReadable);

    try {
      await Stream.text(readable);
      console.log('ERROR: Should have thrown!');
    } catch (error) {
      console.log('Error correctly propagated:', (error as Error).message);
    }
  }

  // ============================================================================
  // Example 7: Error Propagation (New Stream -> Web)
  // ============================================================================
  section('Example 7: Error Propagation (New Stream -> Web)');

  {
    // Create a new Stream that errors
    async function* erroringSource() {
      yield 'some data';
      throw new Error('Intentional new Stream error');
    }

    const webReadable = toReadableStream(Stream.from(erroringSource()));
    const reader = webReadable.getReader();

    try {
      // First read succeeds
      const first = await reader.read();
      console.log('First chunk:', first.value ? decoder.decode(first.value) : 'none');

      // Second read throws
      await reader.read();
      console.log('ERROR: Should have thrown!');
    } catch (error) {
      console.log('Error correctly propagated:', (error as Error).message);
    }
  }

  // ============================================================================
  // Example 8: Cancellation Propagation
  // ============================================================================
  section('Example 8: Cancellation Propagation');

  {
    let cleanedUp = false;

    // Create a source stream that can detect cancellation
    async function* source() {
      try {
        for (let i = 0; i < 100; i++) {
          yield `chunk ${i}\n`;
        }
      } finally {
        cleanedUp = true;
      }
    }

    // Convert to Web ReadableStream
    const webReadable = toReadableStream(Stream.from(source()));
    const reader = webReadable.getReader();

    // Read a few chunks
    await reader.read();
    await reader.read();
    console.log('Read 2 chunks');

    // Cancel the stream
    await reader.cancel('Done reading');

    // Wait for cleanup
    await new Promise((r) => setTimeout(r, 50));

    console.log('Cancel propagated to underlying stream:', cleanedUp);
  }

  // ============================================================================
  // Example 9: Backpressure Handling
  // ============================================================================
  section('Example 9: Backpressure Handling');

  {
    let bytesProduced = 0;
    let bytesConsumed = 0;

    // Create a producer that tracks bytes
    async function* source() {
      for (let i = 0; i < 10; i++) {
        const chunk = `chunk ${i}\n`;
        bytesProduced += chunk.length;
        yield chunk;
      }
    }

    // Convert to Web ReadableStream
    const webReadable = toReadableStream(Stream.from(source()));
    const reader = webReadable.getReader();

    // Consumer with simulated slow processing
    let readsPerformed = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      readsPerformed++;
      bytesConsumed += value.length;

      // Simulate slow processing for first few reads
      if (readsPerformed <= 3) {
        await new Promise((r) => setTimeout(r, 20));
      }
    }

    console.log(`Bytes produced: ${bytesProduced}, Bytes consumed: ${bytesConsumed}`);
    console.log(`Read operations: ${readsPerformed}`);
    console.log('(Pull-based model: producer only runs when consumer requests data)');
  }

  // ============================================================================
  // Example 10: Full Pipeline (Web -> Transform -> Web)
  // ============================================================================
  section('Example 10: Full Pipeline (Web Source -> Transform -> Web Sink)');

  {
    // Source: Web ReadableStream
    const webSource = new ReadableStream<Uint8Array>({
      start(controller) {
        const lines = ['line one', 'line two', 'line three'];
        for (const line of lines) {
          controller.enqueue(encoder.encode(line + '\n'));
        }
        controller.close();
      },
    });

    // Destination: Web WritableStream (collecting output)
    const output: string[] = [];
    const webSink = new WritableStream<Uint8Array>({
      write(chunk) {
        output.push(decoder.decode(chunk));
      },
    });

    // Convert source to new Stream
    const inputStream = fromReadableStream(webSource);

    // Transform using new Stream API (add line numbers)
    let lineNum = 1;
    const addLineNumbers: Transform = (chunks) => {
      if (chunks === null) return null;
      return chunks.map((chunk) => {
        const text = decoder.decode(chunk);
        const lines = text.split('\n').filter((l) => l.length > 0);
        return encoder.encode(lines.map((l) => `${lineNum++}: ${l}\n`).join(''));
      });
    };

    // Pipe to Web WritableStream
    const writer = createWebWriter(webSink);
    await Stream.pipeTo(inputStream, addLineNumbers, writer);

    console.log('Transformed output:');
    console.log(output.join('').trim());
  }

  // ============================================================================
  // Example 11: Teeing with broadcast
  // ============================================================================
  section('Example 11: Tee to Web and New Stream');

  {
    const { writer, channel } = Stream.broadcast();

    // Create branches
    const webBranch = channel.push();
    const mainBranch = channel.push();

    // Convert branch to Web ReadableStream for web consumers
    const webReadable = toReadableStream(webBranch);

    // Start consuming both
    const mainPromise = Stream.text(mainBranch);

    const webPromise = (async () => {
      const reader = webReadable.getReader();
      const chunks: Uint8Array[] = [];
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
      return chunks.map((c) => decoder.decode(c)).join('');
    })();

    // Write data
    await writer.write('Data to send to multiple destinations');
    await writer.end();

    const [mainResult, webResult] = await Promise.all([mainPromise, webPromise]);

    console.log('Main stream result:', mainResult);
    console.log('Web branch result:', webResult);
    console.log('Both received same data:', mainResult === webResult);
  }

  // ============================================================================
  // Example 12: Streaming fetch() response (simulated)
  // ============================================================================
  section('Example 12: Processing fetch() Response Body (simulated)');

  {
    // Simulate a fetch() response body (which is a ReadableStream)
    const simulatedFetchBody = new ReadableStream<Uint8Array>({
      async start(controller) {
        // Simulate chunked response
        const chunks = [
          '{"id": 1, "name": "Alice"}\n',
          '{"id": 2, "name": "Bob"}\n',
          '{"id": 3, "name": "Charlie"}\n',
        ];

        for (const chunk of chunks) {
          await new Promise((r) => setTimeout(r, 10)); // Simulate network delay
          controller.enqueue(encoder.encode(chunk));
        }
        controller.close();
      },
    });

    // Convert to new Stream for processing
    const readable = fromReadableStream(simulatedFetchBody);

    // Process NDJSON using new Stream API
    console.log('Processing NDJSON response:');

    let buffer = '';
    for await (const batches of readable) {
      for (const chunk of batches) {
        buffer += decoder.decode(chunk);

        // Process complete lines
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // Keep incomplete line

        for (const line of lines) {
          if (line.trim()) {
            const obj = JSON.parse(line);
            console.log(`  Received: ${obj.name} (id=${obj.id})`);
          }
        }
      }
    }
  }

  // ============================================================================
  // Example 13: Round-trip conversion
  // ============================================================================
  section('Example 13: Round-trip Conversion');

  {
    const originalData = 'Round-trip test data: Hello, World!';

    // Start with new Stream
    const stream1 = Stream.from(originalData);

    // Convert to Web ReadableStream
    const webReadable = toReadableStream(stream1);

    // Convert back to new Stream
    const stream2 = fromReadableStream(webReadable);

    // Verify data integrity
    const result = await Stream.text(stream2);

    console.log('Original:', originalData);
    console.log('After round-trip:', result);
    console.log('Data preserved:', originalData === result);
  }

  // ============================================================================
  // Example 14: Composing Web and New Stream Transforms
  // ============================================================================
  section('Example 14: Composing Web + New Stream Transforms');

  {
    // Web TransformStream: add prefix
    const prefixTransform = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        const prefixed = new Uint8Array(chunk.length + 4);
        prefixed.set(encoder.encode('>>> '));
        prefixed.set(chunk, 4);
        controller.enqueue(prefixed);
      },
    });

    // Source data
    const source = Stream.from('compose multiple transforms');

    // Apply Web Transform first
    const webReadable = toReadableStream(source);
    const afterWebTransform = fromReadableStream(webReadable.pipeThrough(prefixTransform));

    // Then apply new Stream transform
    const result = await Stream.text(Stream.pull(afterWebTransform, uppercaseTransform()));

    console.log('Composed result:', result);
  }

  console.log('\n--- Web Streams Interop Examples Complete! ---\n');
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });

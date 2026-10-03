/**
 * Node.js Streams Interoperability
 *
 * This file demonstrates adapting between the new Stream API and Node.js streams.
 * Includes proper error propagation and lifecycle management.
 */

import { Stream, type Writer, type WriteOptions, type Transform } from '../src/index.js';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import { Readable, Writable, pipeline as nodePipeline } from 'node:stream';
import { promisify } from 'node:util';
import { section, uppercaseTransform } from './util.js';

const pipelineAsync = promisify(nodePipeline);

// Shared encoder instance for efficiency
const textEncoder = new TextEncoder();

// ============================================================================
// Adapter: Node.js Readable -> New Stream API
// ============================================================================

/**
 * Convert a Node.js Readable stream to the new Stream API.
 *
 * Features:
 * - Proper backpressure handling
 * - Error propagation from Node.js stream to new Stream
 * - Cleanup on cancel (destroys the Node.js stream)
 */
function fromNodeReadable(readable: Readable) {
  return Stream.from(
    (async function* () {
      // Track if we need to destroy the readable on cleanup
      const destroyed = false;

      try {
        // Use async iteration (Node.js 10+)
        for await (const chunk of readable) {
          // Node.js streams can emit strings or Buffers
          if (typeof chunk === 'string') {
            yield chunk;
          } else if (Buffer.isBuffer(chunk)) {
            // Convert Buffer to Uint8Array (zero-copy view)
            yield new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
          } else if (chunk instanceof Uint8Array) {
            yield chunk;
          } else {
            throw new Error(`Unexpected chunk type: ${typeof chunk}`);
          }
        }
      } finally {
        // Cleanup: destroy the readable if not already destroyed
        if (!destroyed && !readable.destroyed) {
          readable.destroy();
        }
      }
    })()
  );
}

// ============================================================================
// Adapter: Create Writer that writes to Node.js Writable
// ============================================================================

/**
 * Create a Writer that pipes to a Node.js Writable stream.
 *
 * Features:
 * - Proper backpressure handling (respects drain events)
 * - Error propagation from Node.js stream to writer
 * - Proper close/fail handling
 */
function createNodeWriter(writable: Writable): Writer {
  let error: Error | null = null;
  let drainResolve: (() => void) | null = null;
  let closed = false;
  let byteCount = 0;

  // Listen for errors from the writable
  writable.on('error', (err) => {
    error = err;
    // Resolve any pending drain wait
    if (drainResolve) {
      drainResolve();
      drainResolve = null;
    }
  });

  // Helper to wait for drain, optionally cancellable via signal
  const waitForDrain = (signal?: AbortSignal): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      drainResolve = resolve;
      const onDrain = () => {
        signal?.removeEventListener('abort', onAbort);
        drainResolve = null;
        resolve();
      };
      const onAbort = () => {
        writable.removeListener('drain', onDrain);
        drainResolve = null;
        reject(signal!.reason);
      };
      writable.once('drain', onDrain);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  };

  return {
    get desiredSize(): number | null {
      return closed || error ? null : 16384; // Reasonable default
    },

    async write(chunk: Uint8Array | string, options?: WriteOptions): Promise<void> {
      if (error) throw error;
      if (closed) throw new Error('Writer is closed');

      const bytes = typeof chunk === 'string' ? textEncoder.encode(chunk) : chunk;
      const buffer = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

      const canContinue = writable.write(buffer);
      byteCount += buffer.length;

      if (!canContinue) {
        await waitForDrain(options?.signal);
        if (error) throw error;
      }
    },

    async writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void> {
      for (const chunk of chunks) {
        await this.write(chunk, options);
      }
    },

    writeSync(chunk: Uint8Array | string): boolean {
      if (error || closed) return false;

      const bytes = typeof chunk === 'string' ? textEncoder.encode(chunk) : chunk;
      const buffer = Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);

      const canContinue = writable.write(buffer);
      byteCount += buffer.length;
      return canContinue;
    },

    writevSync(chunks: (Uint8Array | string)[]): boolean {
      for (const chunk of chunks) {
        if (!this.writeSync(chunk)) return false;
      }
      return true;
    },

    async end(options?: WriteOptions): Promise<number> {
      if (error) throw error;
      closed = true;

      return new Promise<number>((resolve, reject) => {
        if (options?.signal?.aborted) {
          reject(options.signal.reason);
          return;
        }
        const onAbort = () => reject(options!.signal!.reason);
        options?.signal?.addEventListener('abort', onAbort, { once: true });
        writable.end(() => {
          options?.signal?.removeEventListener('abort', onAbort);
          if (error) {
            reject(error);
          } else {
            resolve(byteCount);
          }
        });
      });
    },

    endSync(): number {
      closed = true;
      writable.end();
      return byteCount;
    },

    async fail(reason?: any): Promise<void> {
      closed = true;
      writable.destroy(reason instanceof Error ? reason : new Error(String(reason)));
    },

    failSync(reason?: any): boolean {
      closed = true;
      writable.destroy(reason);
      return true;
    },
  };
}

// ============================================================================
// Adapter: New Stream API -> Node.js Readable
// ============================================================================

/**
 * Convert a new Stream to a Node.js Readable.
 *
 * Features:
 * - Proper backpressure (pauses reading when downstream is slow)
 * - Error propagation
 * - Cleanup when Readable is destroyed
 */
function toNodeReadable(source: AsyncIterable<Uint8Array[]>): Readable {
  const iterator = source[Symbol.asyncIterator]();
  let reading = false;

  const readable = new Readable({
    async read() {
      if (reading) return;
      reading = true;

      try {
        const { value: batch, done } = await iterator.next();

        if (done) {
          this.push(null); // Signal EOF
        } else {
          // Process all chunks in the batch
          for (const chunk of batch) {
            const buffer = Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
            if (!this.push(buffer)) {
              break; // Downstream is full
            }
          }
        }
      } catch (error) {
        this.destroy(error as Error);
      } finally {
        reading = false;
      }
    },

    destroy(error, callback) {
      if (iterator.return) {
        iterator.return(undefined).then(
          () => callback(error),
          (err) => callback(err || error)
        );
      } else {
        callback(error);
      }
    },
  });

  return readable;
}

// ============================================================================
// Main examples
// ============================================================================

async function main() {
  // Create a temporary directory for our test files
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'stream-interop-'));
  const testFile = path.join(tmpDir, 'test.txt');
  const outputFile = path.join(tmpDir, 'output.txt');

  console.log('Temp directory:', tmpDir);

  try {
    // ============================================================================
    // Example 1: Read file with Node.js, process with new Stream API
    // ============================================================================
    section('Example 1: Node.js ReadStream -> New Stream API');

    // Create test file
    fs.writeFileSync(testFile, 'Hello from Node.js!\nLine 2\nLine 3\n');

    {
      // Open file with Node.js
      const nodeReadStream = fs.createReadStream(testFile, { encoding: undefined });

      // Convert to new Stream API
      const readable = fromNodeReadable(nodeReadStream);

      // Process with new Stream API
      const text = await Stream.text(readable);
      console.log('Read from file:', text.trim());

      // Node.js stream is automatically cleaned up
      console.log('Node.js stream destroyed:', nodeReadStream.destroyed);
    }

    // ============================================================================
    // Example 2: Write file using new Stream API -> Node.js WriteStream
    // ============================================================================
    section('Example 2: New Stream API -> Node.js WriteStream');

    {
      // Create Node.js write stream
      const nodeWriteStream = fs.createWriteStream(outputFile);

      // Create writer adapter
      const writer = createNodeWriter(nodeWriteStream);

      // Write using new Stream API
      await writer.write('Written via new Stream API!\n');
      await writer.write('Second line\n');
      await writer.write('Third line\n');
      await writer.end();

      // Verify
      const content = fs.readFileSync(outputFile, 'utf-8');
      console.log('File content:', content.trim());
      console.log('Node.js stream finished:', nodeWriteStream.writableFinished);
    }

    // ============================================================================
    // Example 3: Full pipeline - Node.js Read -> Transform -> Node.js Write
    // ============================================================================
    section('Example 3: Full Pipeline with Transforms');

    {
      // Write source file
      fs.writeFileSync(testFile, 'transform this text to uppercase');

      // Create Node.js streams
      const nodeReader = fs.createReadStream(testFile);
      const nodeWriter = fs.createWriteStream(outputFile);

      // Convert to new Stream API
      const inputStream = fromNodeReadable(nodeReader);
      const outputWriter = createNodeWriter(nodeWriter);

      // Pipe with transform
      const bytesWritten = await Stream.pipeTo(inputStream, uppercaseTransform(), outputWriter);

      console.log('Bytes processed:', bytesWritten);
      console.log('Output:', fs.readFileSync(outputFile, 'utf-8'));
    }

    // ============================================================================
    // Example 4: Error propagation from Node.js stream
    // ============================================================================
    section('Example 4: Error Propagation (Node.js -> New Stream)');

    {
      // Try to read a non-existent file
      const nonExistent = path.join(tmpDir, 'does-not-exist.txt');
      const nodeReader = fs.createReadStream(nonExistent);

      const readable = fromNodeReadable(nodeReader);

      try {
        await Stream.text(readable);
        console.log('ERROR: Should have thrown!');
      } catch (error) {
        console.log('Error correctly propagated:', (error as NodeJS.ErrnoException).code);
      }
    }

    // ============================================================================
    // Example 5: Error propagation to Node.js stream
    // ============================================================================
    section('Example 5: Error Propagation (New Stream -> Node.js)');

    {
      const nodeWriter = fs.createWriteStream(outputFile);
      const writer = createNodeWriter(nodeWriter);

      // Write some data
      await writer.write('before fail');

      // Fail the writer
      await writer.fail(new Error('Intentional failure'));

      console.log('Node.js stream destroyed:', nodeWriter.destroyed);
    }

    // ============================================================================
    // Example 6: Cancellation propagation
    // ============================================================================
    section('Example 6: Cancellation Propagation');

    {
      // Create a larger test file
      fs.writeFileSync(testFile, 'x'.repeat(10000));

      let nodeStreamDestroyed = false;
      const nodeReader = fs.createReadStream(testFile, { highWaterMark: 100 });
      nodeReader.on('close', () => {
        nodeStreamDestroyed = true;
      });

      const readable = fromNodeReadable(nodeReader);

      // Read partial data
      let bytesRead = 0;
      for await (const batches of readable) {
        for (const chunk of batches) {
          bytesRead += chunk.length;
          if (bytesRead >= 500) break;
        }
        if (bytesRead >= 500) break;
      }
      console.log('Read partial:', bytesRead, 'bytes');

      // Give time for cleanup to propagate
      await new Promise((r) => setTimeout(r, 50));

      // Node.js stream should be destroyed
      console.log('Node.js stream cleaned up after break:', nodeStreamDestroyed);
    }

    // ============================================================================
    // Example 7: Convert new Stream to Node.js Readable
    // ============================================================================
    section('Example 7: New Stream -> Node.js Readable');

    {
      // Create a new Stream
      async function* source() {
        yield 'chunk1\n';
        await new Promise((r) => setTimeout(r, 10));
        yield 'chunk2\n';
        await new Promise((r) => setTimeout(r, 10));
        yield 'chunk3\n';
      }

      // Convert to Node.js Readable
      const nodeReadable = toNodeReadable(Stream.from(source()));

      // Use with Node.js pipeline
      const nodeWriter = fs.createWriteStream(outputFile);

      await pipelineAsync(nodeReadable, nodeWriter);

      console.log('Written via Node.js pipeline:', fs.readFileSync(outputFile, 'utf-8').trim());
    }

    // ============================================================================
    // Example 8: Tee to Node.js stream while processing with broadcast
    // ============================================================================
    section('Example 8: Tee to File While Processing');

    {
      const { writer, channel } = Stream.broadcast();

      // Create branches
      const fileBranch = channel.push();
      const processBranch = channel.push();

      // Save to file in background
      const savePromise = (async () => {
        const nodeWriter = fs.createWriteStream(outputFile);
        const fileWriter = createNodeWriter(nodeWriter);
        await Stream.pipeTo(fileBranch, fileWriter);
      })();

      // Process in background
      const processPromise = (async () => {
        return await Stream.text(Stream.pull(processBranch, uppercaseTransform()));
      })();

      // Write data
      await writer.write('Data to process and also save to file');
      await writer.end();

      const processed = await processPromise;
      await savePromise;

      console.log('Processed:', processed);
      console.log('Saved to file:', fs.readFileSync(outputFile, 'utf-8'));
    }

    // ============================================================================
    // Example 9: Backpressure handling with slow consumer
    // ============================================================================
    section('Example 9: Backpressure Handling');

    {
      // Create a large test file
      fs.writeFileSync(testFile, 'X'.repeat(100000)); // 100KB

      const nodeReader = fs.createReadStream(testFile, { highWaterMark: 1024 });
      const readable = fromNodeReadable(nodeReader);

      let chunksProcessed = 0;
      const startTime = Date.now();

      // Slow consumer
      for await (const batches of readable) {
        for (const _ of batches) {
          chunksProcessed++;
          // Simulate slow processing
          if (chunksProcessed <= 3) {
            await new Promise((r) => setTimeout(r, 50));
          }
        }
      }

      const elapsed = Date.now() - startTime;
      console.log(`Processed ${chunksProcessed} chunks in ${elapsed}ms`);
      console.log('(Backpressure correctly propagated to Node.js stream)');
    }

    // ============================================================================
    // Example 10: Large file copy with progress
    // ============================================================================
    section('Example 10: File Copy with Progress');

    {
      // Create a moderately large file
      const largeFile = path.join(tmpDir, 'large.txt');
      const copyFile = path.join(tmpDir, 'copy.txt');

      const size = 50000; // 50KB
      fs.writeFileSync(largeFile, 'X'.repeat(size));

      const nodeReader = fs.createReadStream(largeFile);
      const nodeWriter = fs.createWriteStream(copyFile);

      const inputStream = fromNodeReadable(nodeReader);
      const outputWriter = createNodeWriter(nodeWriter);

      let bytesCopied = 0;
      let lastProgress = 0;

      // Progress transform
      const withProgress: Transform = (chunks) => {
        if (chunks === null) return null;

        for (const chunk of chunks) {
          bytesCopied += chunk.length;
        }
        const progress = Math.floor((bytesCopied / size) * 100);

        if (progress >= lastProgress + 25) {
          console.log(`  Progress: ${progress}%`);
          lastProgress = progress;
        }

        return chunks;
      };

      const totalBytes = await Stream.pipeTo(inputStream, withProgress, outputWriter);
      console.log(`  Complete: ${totalBytes} bytes copied`);

      // Verify
      const originalSize = fs.statSync(largeFile).size;
      const copySize = fs.statSync(copyFile).size;
      console.log(`  Verified: original=${originalSize}, copy=${copySize}`);
    }

    console.log('\n--- Examples complete! ---\n');
  } finally {
    // Clean up temp directory
    console.log('Cleaning up temp directory...');
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log('Done.');
  }
}

main().catch(console.error);

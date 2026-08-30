/**
 * Benchmark: Advanced Features Performance
 *
 * Tests features not covered by other benchmarks:
 * - broadcast() vs share() comparison
 * - merge() - combining multiple streams
 * - pipeTo() / pipeToSync() - writing to destinations
 * - Backpressure policies (strict, drop-oldest, drop-newest)
 * - Stateful transforms
 */

import { Stream } from '../src/index.js';
import type { Transform, Writer, SyncWriter, WriteOptions } from '../src/index.js';
import {
  benchmark,
  createComparison,
  BenchmarkComparison,
  BenchmarkResult,
  printComparison,
  generateChunks,
  formatBytesPerSec,
  formatTime,
} from './utils.js';

/**
 * Print a single benchmark result for non-comparison tests.
 */
function printSingleResults(title: string, results: BenchmarkResult[]): void {
  console.log('\n' + '='.repeat(80));
  console.log(title);
  console.log('='.repeat(80));

  const colWidths = [40, 18, 18];
  const headers = ['Test', 'Time', 'Throughput'];
  console.log(headers.map((h, i) => h.padEnd(colWidths[i])).join(' | '));
  console.log(colWidths.map((w) => '-'.repeat(w)).join('-+-'));

  for (const r of results) {
    const row = [
      r.name.substring(0, colWidths[0] - 1),
      formatTime(r.mean),
      r.bytesPerSec ? formatBytesPerSec(r.bytesPerSec) : 'N/A',
    ];
    console.log(row.map((v, i) => v.padEnd(colWidths[i])).join(' | '));
  }
  console.log('='.repeat(80));
}

async function runBenchmarks(): Promise<void> {
  const comparisons: BenchmarkComparison[] = [];
  const singleResults: { title: string; results: BenchmarkResult[] }[] = [];

  // ============================================================================
  // Section 1: broadcast() vs share() comparison
  // ============================================================================
  console.log('\n--- Section 1: broadcast() vs share() ---');

  // Scenario 1a: Push model (broadcast) - 2 consumers
  console.log('Running: broadcast() with 2 consumers...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    const broadcastResult = await benchmark(
      'broadcast()',
      async () => {
        const { writer, channel } = Stream.broadcast({ highWaterMark: 1000 });

        // Create two consumers
        const consumer1 = channel.push();
        const consumer2 = channel.push();

        // Producer task
        const producerTask = (async () => {
          for (const chunk of chunks) {
            await writer.write(chunk);
          }
          await writer.end();
        })();

        // Consumer tasks
        const [result1, result2] = await Promise.all([
          Stream.bytes(consumer1),
          Stream.bytes(consumer2),
          producerTask,
        ]);

        if (result1.length !== totalBytes || result2.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    const shareResult = await benchmark(
      'share()',
      async () => {
        const source = Stream.from(chunks);
        const shared = Stream.share(source, { highWaterMark: 100 });

        const consumer1 = shared.pull();
        const consumer2 = shared.pull();

        const [result1, result2] = await Promise.all([
          Stream.bytes(consumer1),
          Stream.bytes(consumer2),
        ]);

        if (result1.length !== totalBytes || result2.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    comparisons.push(createComparison('2 consumers (4KB x 500)', broadcastResult, shareResult, { label1: 'broadcast()', label2: 'share()' }));
  }

  // Scenario 1b: With transforms on consumers
  console.log('Running: broadcast/share with transforms...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    const xorTransform: Transform = (batch) => {
      if (batch === null) return null;
      return batch.map(chunk => {
        const result = new Uint8Array(chunk.length);
        for (let i = 0; i < chunk.length; i++) {
          result[i] = chunk[i] ^ 0x42;
        }
        return result;
      });
    };

    const broadcastResult = await benchmark(
      'broadcast()+transform',
      async () => {
        const { writer, channel } = Stream.broadcast({ highWaterMark: 1000 });

        const consumer1 = channel.push();
        const consumer2 = channel.push(xorTransform);

        const producerTask = (async () => {
          for (const chunk of chunks) {
            await writer.write(chunk);
          }
          await writer.end();
        })();

        const [result1, result2] = await Promise.all([
          Stream.bytes(consumer1),
          Stream.bytes(consumer2),
          producerTask,
        ]);

        if (result1.length !== totalBytes || result2.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    const shareResult = await benchmark(
      'share()+transform',
      async () => {
        const source = Stream.from(chunks);
        const shared = Stream.share(source, { highWaterMark: 100 });

        const consumer1 = shared.pull();
        const consumer2 = shared.pull(xorTransform);

        const [result1, result2] = await Promise.all([
          Stream.bytes(consumer1),
          Stream.bytes(consumer2),
        ]);

        if (result1.length !== totalBytes || result2.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    comparisons.push(createComparison('w/ transforms (4KB x 500)', broadcastResult, shareResult, { label1: 'broadcast()', label2: 'share()' }));
  }

  // ============================================================================
  // Section 2: merge() performance
  // ============================================================================
  console.log('\n--- Section 2: merge() ---');

  // Scenario 2a: Merge 2 streams
  console.log('Running: merge() 2 streams...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 250; // per stream
    const totalBytes = chunkSize * chunkCount * 2;
    const chunks1 = generateChunks(chunkSize, chunkCount);
    const chunks2 = generateChunks(chunkSize, chunkCount);

    const mergeResult = await benchmark(
      'New Stream merge',
      async () => {
        const source1 = Stream.from(chunks1);
        const source2 = Stream.from(chunks2);

        const merged = Stream.merge(source1, source2);
        const result = await Stream.bytes(merged);

        if (result.length !== totalBytes) {
          throw new Error(`Wrong size: ${result.length} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    // Web Streams doesn't have native merge, but we can use Promise.race pattern
    const webStreamResult = await benchmark(
      'Web Stream (manual)',
      async () => {
        let index1 = 0;
        let index2 = 0;

        const stream1 = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (index1 < chunks1.length) {
              controller.enqueue(chunks1[index1++]);
            } else {
              controller.close();
            }
          },
        });

        const stream2 = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (index2 < chunks2.length) {
              controller.enqueue(chunks2[index2++]);
            } else {
              controller.close();
            }
          },
        });

        // Manual merge using Promise.race
        const reader1 = stream1.getReader();
        const reader2 = stream2.getReader();
        const chunks: Uint8Array[] = [];

        let done1 = false;
        let done2 = false;
        let pending1: Promise<{ reader: 1; result: ReadableStreamReadResult<Uint8Array> }> | null = null;
        let pending2: Promise<{ reader: 2; result: ReadableStreamReadResult<Uint8Array> }> | null = null;

        while (!done1 || !done2) {
          if (!done1 && !pending1) {
            pending1 = reader1.read().then(result => ({ reader: 1, result }));
          }
          if (!done2 && !pending2) {
            pending2 = reader2.read().then(result => ({ reader: 2, result }));
          }

          const pending = [pending1, pending2].filter(Boolean) as Promise<{ reader: 1 | 2; result: ReadableStreamReadResult<Uint8Array> }>[];
          if (pending.length === 0) break;

          const { reader, result } = await Promise.race(pending);

          if (result.done) {
            if (reader === 1) {
              done1 = true;
              pending1 = null;
            } else {
              done2 = true;
              pending2 = null;
            }
          } else {
            chunks.push(result.value);
            if (reader === 1) {
              pending1 = null;
            } else {
              pending2 = null;
            }
          }
        }

        let totalLength = 0;
        for (const c of chunks) totalLength += c.length;
        const result = new Uint8Array(totalLength);
        let offset = 0;
        for (const c of chunks) {
          result.set(c, offset);
          offset += c.length;
        }
        if (result.length !== totalBytes) {
          throw new Error(`Wrong size: ${result.length} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    comparisons.push(createComparison('merge() 2 streams (4KB x 250 each)', mergeResult, webStreamResult));
  }

  // Scenario 2b: Merge 4 streams
  console.log('Running: merge() 4 streams...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 125; // per stream
    const totalBytes = chunkSize * chunkCount * 4;
    const allChunks = [
      generateChunks(chunkSize, chunkCount),
      generateChunks(chunkSize, chunkCount),
      generateChunks(chunkSize, chunkCount),
      generateChunks(chunkSize, chunkCount),
    ];

    const mergeResult = await benchmark(
      'New Stream merge(4)',
      async () => {
        const sources = allChunks.map(c => Stream.from(c));
        const merged = Stream.merge(...sources);
        const result = await Stream.bytes(merged);

        if (result.length !== totalBytes) {
          throw new Error(`Wrong size: ${result.length} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    singleResults.push({ title: 'Merge 4 Streams', results: [mergeResult] });
  }

  // ============================================================================
  // Section 3: pipeTo() / pipeToSync() performance
  // ============================================================================
  console.log('\n--- Section 3: pipeTo() ---');

  // Scenario 3a: Async pipeTo
  console.log('Running: pipeTo() async...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    // Create a mock writer that just counts bytes
    class MockWriter implements Writer {
      totalBytes = 0;
      closed = false;

      get desiredSize(): number | null {
        return this.closed ? null : 16;
      }

      async write(chunk: Uint8Array | string, _options?: WriteOptions): Promise<void> {
        const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
        this.totalBytes += data.byteLength;
      }

      async writev(chunks: (Uint8Array | string)[], _options?: WriteOptions): Promise<void> {
        for (const chunk of chunks) {
          await this.write(chunk);
        }
      }

      writeSync(chunk: Uint8Array | string): boolean {
        const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
        this.totalBytes += data.byteLength;
        return true;
      }

      writevSync(chunks: (Uint8Array | string)[]): boolean {
        for (const chunk of chunks) {
          this.writeSync(chunk);
        }
        return true;
      }

      async end(_options?: WriteOptions): Promise<number> {
        this.closed = true;
        return this.totalBytes;
      }

      endSync(): number {
        this.closed = true;
        return this.totalBytes;
      }

      async fail(): Promise<void> {
        this.closed = true;
      }

      failSync(): boolean {
        this.closed = true;
        return true;
      }
    }

    const pipeToResult = await benchmark(
      'New Stream pipeTo',
      async () => {
        const source = Stream.from(chunks);
        const mockWriter = new MockWriter();
        const bytes = await Stream.pipeTo(source, mockWriter);

        if (bytes !== totalBytes) {
          throw new Error(`Wrong size: ${bytes} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    // Web Streams pipeTo
    const webStreamResult = await benchmark(
      'Web Stream pipeTo',
      async () => {
        let index = 0;
        const source = new ReadableStream<Uint8Array>({
          pull(controller) {
            if (index < chunks.length) {
              controller.enqueue(chunks[index++]);
            } else {
              controller.close();
            }
          },
        });

        let totalWritten = 0;
        const sink = new WritableStream<Uint8Array>({
          write(chunk) {
            totalWritten += chunk.byteLength;
          },
        });

        await source.pipeTo(sink);

        if (totalWritten !== totalBytes) {
          throw new Error(`Wrong size: ${totalWritten} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    comparisons.push(createComparison('pipeTo vs pipeTo (4KB x 500)', pipeToResult, webStreamResult));
  }

  // Scenario 3b: Sync pipeToSync
  console.log('Running: pipeToSync()...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    class MockSyncWriter implements SyncWriter {
      totalBytes = 0;
      closed = false;

      get desiredSize(): number | null {
        return this.closed ? null : 16;
      }

      write(chunk: Uint8Array | string): void {
        const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
        this.totalBytes += data.byteLength;
      }

      writev(chunks: (Uint8Array | string)[]): void {
        for (const chunk of chunks) {
          this.write(chunk);
        }
      }

      end(): number {
        this.closed = true;
        return this.totalBytes;
      }

      fail(): void {
        this.closed = true;
      }
    }

    const pipeToSyncResult = await benchmark(
      'pipeToSync',
      async () => {
        const source = Stream.fromSync(chunks);
        const pipeline = Stream.pullSync(source);
        const mockWriter = new MockSyncWriter();
        const bytes = Stream.pipeToSync(pipeline, mockWriter);

        if (bytes !== totalBytes) {
          throw new Error(`Wrong size: ${bytes} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 2000 }
    );

    const pipeToAsyncResult = await benchmark(
      'pipeTo (async)',
      async () => {
        const source = Stream.from(chunks);

        class MockWriter implements Writer {
          totalBytes = 0;
          closed = false;

          get desiredSize(): number | null {
            return this.closed ? null : 16;
          }

          async write(chunk: Uint8Array | string, _options?: WriteOptions): Promise<void> {
            const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
            this.totalBytes += data.byteLength;
          }

          async writev(chunks: (Uint8Array | string)[], _options?: WriteOptions): Promise<void> {
            for (const chunk of chunks) {
              await this.write(chunk);
            }
          }

          writeSync(chunk: Uint8Array | string): boolean {
            const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
            this.totalBytes += data.byteLength;
            return true;
          }

          writevSync(chunks: (Uint8Array | string)[]): boolean {
            for (const chunk of chunks) {
              this.writeSync(chunk);
            }
            return true;
          }

          async end(_options?: WriteOptions): Promise<number> {
            this.closed = true;
            return this.totalBytes;
          }

          endSync(): number {
            this.closed = true;
            return this.totalBytes;
          }

          async fail(): Promise<void> {
            this.closed = true;
          }

          failSync(): boolean {
            this.closed = true;
            return true;
          }
        }

        const mockWriter = new MockWriter();
        const bytes = await Stream.pipeTo(source, mockWriter);

        if (bytes !== totalBytes) {
          throw new Error(`Wrong size: ${bytes} vs ${totalBytes}`);
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 2000 }
    );

    comparisons.push(createComparison('pipeToSync vs pipeTo (4KB x 500)', pipeToSyncResult, pipeToAsyncResult, { label1: 'pipeToSync()', label2: 'pipeTo() async' }));
  }

  // ============================================================================
  // Section 4: Backpressure policies
  // ============================================================================
  console.log('\n--- Section 4: Backpressure Policies ---');

  // Test different backpressure policies with broadcast
  console.log('Running: backpressure policy comparison...');
  {
    const chunkSize = 1024;
    const chunkCount = 1000;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    const policies: Array<'strict' | 'block' | 'drop-oldest' | 'drop-newest'> = [
      'strict',
      'block',
      'drop-oldest',
      'drop-newest',
    ];

    const policyResults: BenchmarkResult[] = [];

    for (const policy of policies) {
      const result = await benchmark(
        `${policy}`,
        async () => {
          const { writer, channel } = Stream.broadcast({
            highWaterMark: 50,
            backpressure: policy,
          });

          // Create a slow consumer (will cause backpressure)
          const consumer = channel.push();

          // Fast producer
          const producerTask = (async () => {
            for (const chunk of chunks) {
              // For strict policy, writes may reject if too many pending;
              // block policy waits indefinitely
              if (policy === 'strict') {
                try {
                  await writer.write(chunk);
                } catch {
                  // Backpressure violation (too many pending), just continue
                }
              } else {
                await writer.write(chunk);
              }
            }
            await writer.end();
          })();

          // Consume all available data
          let received = 0;
          for await (const batch of consumer) {
            for (const chunk of batch) {
              received += chunk.byteLength;
            }
          }

          await producerTask;

          // For non-strict policies, we may receive less data
          return received;
        },
        { totalBytes, minSamples: 15, minTimeMs: 2000 }
      );

      policyResults.push(result);
    }

    singleResults.push({ title: 'Backpressure Policies (broadcast)', results: policyResults });
  }

  // ============================================================================
  // Section 5: Stateful transforms
  // ============================================================================
  console.log('\n--- Section 5: Stateful Transforms ---');

  console.log('Running: stateful vs stateless transforms...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    // Stateless transform: XOR each byte
    const statelessTransform: Transform = (batch) => {
      if (batch === null) return null;
      return batch.map(chunk => {
        const result = new Uint8Array(chunk.length);
        for (let i = 0; i < chunk.length; i++) {
          result[i] = chunk[i] ^ 0x42;
        }
        return result;
      });
    };

    // Stateful transform: same XOR work, but via async generator (stateful API)
    const statefulTransform: Transform = {
      transform: async function* (source) {
        for await (const batch of source) {
          if (batch === null) continue;
          for (const chunk of batch) {
            const result = new Uint8Array(chunk.length);
            for (let i = 0; i < chunk.length; i++) {
              result[i] = chunk[i] ^ 0x42;
            }
            yield result;
          }
        }
      },
    };

    const statelessResult = await benchmark(
      'Stateless transform',
      async () => {
        const source = Stream.from(chunks);
        const pipeline = Stream.pull(source, statelessTransform);
        const result = await Stream.bytes(pipeline);

        if (result.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    const statefulResult = await benchmark(
      'Stateful transform',
      async () => {
        const source = Stream.from(chunks);
        const pipeline = Stream.pull(source, statefulTransform);
        const result = await Stream.bytes(pipeline);

        if (result.length !== totalBytes) {
          throw new Error('Wrong size');
        }
      },
      { totalBytes, minSamples: 15, minTimeMs: 3000 }
    );

    comparisons.push(createComparison('XOR transform (4KB x 500)', statelessResult, statefulResult, { label1: 'Stateless (fn)', label2: 'Stateful (gen)' }));
  }

  // ============================================================================
  // Section 6: Many-consumer broadcast (Item C: O(C²) → O(C))
  // ============================================================================
  console.log('\n--- Section 6: Many-Consumer Broadcast ---');

  console.log('Running: broadcast with many consumers...');
  {
    const chunkSize = 1024;
    const chunkCount = 200;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    const consumerCounts = [2, 10, 50, 100];
    const manyConsumerResults: BenchmarkResult[] = [];

    for (const numConsumers of consumerCounts) {
      const result = await benchmark(
        `broadcast ${numConsumers} consumers`,
        async () => {
          const { writer, channel } = Stream.broadcast({
            highWaterMark: 50,
            backpressure: 'block',
          });

          // Create N consumers
          const consumers: AsyncIterable<Uint8Array[]>[] = [];
          for (let i = 0; i < numConsumers; i++) {
            consumers.push(channel.push());
          }

          // Producer writes all chunks then ends
          const producerTask = (async () => {
            for (const chunk of chunks) {
              await writer.write(chunk);
            }
            await writer.end();
          })();

          // Each consumer drains to bytes
          const consumerTasks = consumers.map((c) => Stream.bytes(c));

          await Promise.all([producerTask, ...consumerTasks]);
        },
        { totalBytes: totalBytes * numConsumers, minSamples: 15, minTimeMs: 2000 }
      );

      manyConsumerResults.push(result);
    }

    singleResults.push({ title: 'Many-Consumer Broadcast', results: manyConsumerResults });
  }

  // ============================================================================
  // Section 7: Transform chain depth (Item A: stateless fusion)
  // ============================================================================
  console.log('\n--- Section 7: Transform Chain Depth ---');

  console.log('Running: transform chain depth comparison...');
  {
    const chunkSize = 4 * 1024;
    const chunkCount = 500;
    const totalBytes = chunkSize * chunkCount;
    const chunks = generateChunks(chunkSize, chunkCount);

    // Simple stateless XOR transform
    const xor: Transform = (batch) => {
      if (batch === null) return null;
      return batch.map(chunk => {
        const result = new Uint8Array(chunk.length);
        for (let i = 0; i < chunk.length; i++) {
          result[i] = chunk[i] ^ 0x42;
        }
        return result;
      });
    };

    const chainDepths = [1, 3, 5];
    const chainResults: BenchmarkResult[] = [];

    for (const depth of chainDepths) {
      const transforms: Transform[] = [];
      for (let i = 0; i < depth; i++) {
        transforms.push(xor);
      }

      const result = await benchmark(
        `${depth} stateless transform${depth > 1 ? 's' : ''}`,
        async () => {
          const source = Stream.from(chunks);
          const pipeline = Stream.pull(source, ...transforms);
          const result = await Stream.bytes(pipeline);

          if (result.length !== totalBytes) {
            throw new Error('Wrong size');
          }
        },
        { totalBytes, minSamples: 15, minTimeMs: 2000 }
      );

      chainResults.push(result);
    }

    singleResults.push({ title: 'Transform Chain Depth (stateless)', results: chainResults });
  }

  // ============================================================================
  // Print Results
  // ============================================================================

  // Print comparison table
  if (comparisons.length > 0) {
    printComparison(comparisons);
  }

  // Print single results
  for (const { title, results } of singleResults) {
    printSingleResults(title, results);
  }
}

// Main
console.log('Benchmark: Advanced Features Performance');
console.log('Tests: broadcast vs share, merge, pipeTo, backpressure policies, stateful transforms');
console.log('(minimum 15 samples, 2-3 seconds per test)\n');

runBenchmarks().catch(console.error);

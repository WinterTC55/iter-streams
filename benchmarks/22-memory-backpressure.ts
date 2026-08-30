/**
 * Memory Profiling: Backpressure Policy Comparison
 *
 * Measures how different backpressure policies (strict, block, drop-oldest,
 * drop-newest) affect memory usage and GC pressure across varying
 * highWaterMark values. Uses mitata for per-iteration heap/GC tracking.
 *
 * Scenarios:
 *   1. Push stream: write/read with each policy x HWM combination
 *   2. Broadcast: 2 consumers with each policy x HWM combination
 *   3. Slow consumer: fast producer, slow consumer under each policy
 *
 * Run with:
 *   node --expose-gc --import tsx/esm benchmarks/22-memory-backpressure.ts
 */

import { run, bench, do_not_optimize, summary, boxplot } from 'mitata';
import { Stream } from '../src/index.js';
import type { BackpressurePolicy, Transform } from '../src/index.js';
import { generateChunks } from './utils.js';

// ============================================================================
// Test data (pre-allocated to isolate streaming overhead)
// ============================================================================

const CHUNK_SIZE = 4 * 1024; // 4KB
const PUSH_CHUNKS = 2000;    // 2000 x 4KB = 8MB
const BCAST_CHUNKS = 500;    // 500 x 4KB = 2MB
const SLOW_CHUNKS = 500;     // 500 x 4KB = 2MB

const pushData = generateChunks(CHUNK_SIZE, PUSH_CHUNKS);
const bcastData = generateChunks(CHUNK_SIZE, BCAST_CHUNKS);
const slowData = generateChunks(CHUNK_SIZE, SLOW_CHUNKS);

const policies: BackpressurePolicy[] = ['strict', 'block', 'drop-oldest', 'drop-newest'];
const hwmValues = [1, 4, 16, 64, 256];

// ============================================================================
// Scenario 1: Push write/read - policy x HWM matrix
//
// Each policy handles a full producer/consumer cycle. Strict and block await
// each write (backpressure respected). Drop policies fire-and-forget with
// sync writes since they never block.
// ============================================================================

for (const policy of policies) {
  boxplot(() => {
    summary(() => {
      for (const hwm of hwmValues) {
        bench(`push ${policy} hwm=${hwm}`, function* () {
          yield async () => {
            const { writer, readable } = Stream.push({
              highWaterMark: hwm,
              backpressure: policy,
            });

            const producing = (async () => {
              if (policy === 'drop-oldest' || policy === 'drop-newest') {
                // Drop policies: use sync writes (never block)
                for (const chunk of pushData) writer.writeSync(chunk);
              } else {
                // strict/block: await each write for proper backpressure
                for (const chunk of pushData) await writer.write(chunk);
              }
              await writer.end();
            })();

            let total = 0;
            for await (const batch of readable) {
              for (const c of batch) total += c.byteLength;
            }
            await producing;
            do_not_optimize(total);
          };
        }).gc('inner');
      }
    });
  });
}

// ============================================================================
// Scenario 2: Broadcast 2 consumers - policy x HWM matrix
//
// Two consumers read from a broadcast with different policies. The broadcast
// buffer is governed by the slowest consumer, so policy mainly affects the
// writer's behavior when the buffer is full.
// ============================================================================

for (const policy of policies) {
  boxplot(() => {
    summary(() => {
      for (const hwm of hwmValues) {
        bench(`bcast ${policy} hwm=${hwm}`, function* () {
          yield async () => {
            const { writer, channel } = Stream.broadcast({
              highWaterMark: hwm,
              backpressure: policy,
            });
            const c1 = channel.push();
            const c2 = channel.push();

            const producing = (async () => {
              if (policy === 'drop-oldest' || policy === 'drop-newest') {
                for (const chunk of bcastData) writer.writeSync(chunk);
              } else {
                for (const chunk of bcastData) await writer.write(chunk);
              }
              await writer.end();
            })();

            const [r1, r2] = await Promise.all([
              Stream.bytes(c1),
              Stream.bytes(c2),
              producing,
            ]);
            do_not_optimize(r1.byteLength + r2.byteLength);
          };
        }).gc('inner');
      }
    });
  });
}

// ============================================================================
// Scenario 3: Slow consumer stress test
//
// Producer writes at full speed, consumer adds a microtask delay every N
// reads. This stresses the backpressure mechanism: strict should reject
// (we catch), block should build up pending writes, drop policies should
// shed load. Measures memory footprint under realistic mismatch.
// ============================================================================

const SLOW_DELAY_EVERY = 10; // Consumer yields every 10 reads

boxplot(() => {
  summary(() => {
    for (const policy of policies) {
      bench(`slow-consumer ${policy} hwm=16`, function* () {
        yield async () => {
          const { writer, readable } = Stream.push({
            highWaterMark: 16,
            backpressure: policy,
          });

          const producing = (async () => {
            for (const chunk of slowData) {
              if (policy === 'drop-oldest' || policy === 'drop-newest') {
                writer.writeSync(chunk);
              } else if (policy === 'strict') {
                try {
                  await writer.write(chunk);
                } catch {
                  // Strict may reject when full - that's expected
                  // Wait a tick and retry
                  await new Promise((r) => setTimeout(r, 0));
                  try { await writer.write(chunk); } catch { /* skip */ }
                }
              } else {
                // block
                await writer.write(chunk);
              }
            }
            await writer.end();
          })();

          let total = 0;
          let readCount = 0;
          for await (const batch of readable) {
            for (const c of batch) total += c.byteLength;
            readCount++;
            if (readCount % SLOW_DELAY_EVERY === 0) {
              // Simulate slow consumer
              await new Promise((r) => setTimeout(r, 0));
            }
          }
          await producing;
          do_not_optimize(total);
        };
      }).gc('inner');
    }
  });
});

// ============================================================================
// Scenario 4: HWM scaling - fixed policy, sweeping HWM
//
// For strict and block policies, measures how memory scales with increasing
// HWM. This isolates the buffer size cost from the policy mechanism cost.
// ============================================================================

for (const policy of ['strict', 'block'] as BackpressurePolicy[]) {
  boxplot(() => {
    summary(() => {
      for (const hwm of [1, 4, 16, 64, 256, 1024]) {
        bench(`hwm-scale ${policy} hwm=${hwm}`, function* () {
          yield async () => {
            const { writer, readable } = Stream.push({
              highWaterMark: hwm,
              backpressure: policy,
            });

            const producing = (async () => {
              for (const chunk of pushData) await writer.write(chunk);
              await writer.end();
            })();

            let total = 0;
            for await (const batch of readable) {
              for (const c of batch) total += c.byteLength;
            }
            await producing;
            do_not_optimize(total);
          };
        }).gc('inner');
      }
    });
  });
}

// ============================================================================

console.log('Memory Backpressure Profiling: Policy x HWM Comparison');
console.log('(heap and gc rows require --expose-gc)\n');
await run();

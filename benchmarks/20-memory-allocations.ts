/**
 * Memory Profiling: Per-Operation Allocations
 *
 * Measures heap allocations and GC pressure per streaming operation.
 * Compares New Streams vs Web Streams across five scenarios.
 *
 * Run with:
 *   node --expose-gc --import tsx/esm benchmarks/20-memory-allocations.ts
 */

import { run, bench, do_not_optimize, summary, boxplot } from 'mitata';
import { Stream } from '../src/index.js';
import type { Transform, Writer } from '../src/index.js';
import { generateChunks } from './utils.js';

// ============================================================================
// Test data (pre-allocated to isolate streaming machinery overhead)
// ============================================================================

const CHUNK_SIZE = 4 * 1024; // 4KB
const SMALL = 1000;          // 1000 chunks = 4MB
const LARGE = 10_000;        // 10000 chunks = 40MB
const BCAST = 500;           // 500 chunks = 2MB

const smallChunks = generateChunks(CHUNK_SIZE, SMALL);
const largeChunks = generateChunks(CHUNK_SIZE, LARGE);
const bcastChunks = generateChunks(CHUNK_SIZE, BCAST);

// ============================================================================
// Transforms (identical work in both APIs)
// ============================================================================

const xor: Transform = (chunks) => {
  if (chunks === null) return null;
  return chunks.map((c) => {
    const out = new Uint8Array(c.length);
    for (let i = 0; i < c.length; i++) out[i] = c[i] ^ 0x42;
    return out;
  });
};

function xorTS(): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream({
    transform(chunk, ctrl) {
      const out = new Uint8Array(chunk.length);
      for (let i = 0; i < chunk.length; i++) out[i] = chunk[i] ^ 0x42;
      ctrl.enqueue(out);
    },
  });
}

// ============================================================================
// Helpers
// ============================================================================

function webReadable(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(ctrl) {
      if (i >= chunks.length) { ctrl.close(); return; }
      ctrl.enqueue(chunks[i++]);
    },
  });
}

async function consumeWeb(stream: ReadableStream<Uint8Array>): Promise<number> {
  const reader = stream.getReader();
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    total += value!.byteLength;
  }
  return total;
}

async function consumeNew(readable: AsyncIterable<Uint8Array[]>): Promise<number> {
  let total = 0;
  for await (const batch of readable) {
    for (const c of batch) total += c.byteLength;
  }
  return total;
}

function nullWriter(): Writer {
  let total = 0;
  return {
    get desiredSize() { return 1; },
    async write(c: Uint8Array | string) {
      total += typeof c === 'string' ? c.length : c.byteLength;
    },
    async writev(cs: (Uint8Array | string)[]) {
      for (const c of cs) total += typeof c === 'string' ? c.length : c.byteLength;
    },
    async end() { return total; },
    async fail() {},
    writeSync(c: Uint8Array | string) {
      total += typeof c === 'string' ? c.length : c.byteLength;
      return true;
    },
    writevSync(cs: (Uint8Array | string)[]) {
      for (const c of cs) total += typeof c === 'string' ? c.length : c.byteLength;
      return true;
    },
    endSync() { return total; },
    failSync() { return true; },
  };
}

// ============================================================================
// Scenario 1: Push write/read (1000 x 4KB, HWM=100)
// ============================================================================

boxplot(() => {
  summary(() => {
    bench('push write/read (new)', function* () {
      yield async () => {
        const { writer, readable } = Stream.push({ highWaterMark: 100 });
        const producing = (async () => {
          for (const chunk of smallChunks) await writer.write(chunk);
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

    bench('push writeSync+ondrain (new)', function* () {
      yield async () => {
        const { writer, readable } = Stream.push({ highWaterMark: 100 });
        const producing = (async () => {
          for (const chunk of smallChunks) {
            if (!writer.writeSync(chunk)) {
              const canWrite = await Stream.ondrain(writer);
              if (!canWrite) break;
              writer.writeSync(chunk);
            }
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

    bench('push write/read (web)', function* () {
      yield async () => {
        let ctrl!: ReadableStreamDefaultController<Uint8Array>;
        const rs = new ReadableStream<Uint8Array>({ start(c) { ctrl = c; } });
        const producing = (async () => {
          for (const chunk of smallChunks) {
            ctrl.enqueue(chunk);
            await Promise.resolve();
          }
          ctrl.close();
        })();
        const total = await consumeWeb(rs);
        await producing;
        do_not_optimize(total);
      };
    }).gc('inner');
  });
});

// ============================================================================
// Scenario 2: Pull pipeline with XOR transform
// ============================================================================

boxplot(() => {
  summary(() => {
    bench('pull + transform (new)', function* () {
      yield async () => {
        const total = await consumeNew(
          Stream.pull(Stream.from(smallChunks), xor),
        );
        do_not_optimize(total);
      };
    }).gc('inner');

    bench('pull + transform (web)', function* () {
      yield async () => {
        const result = await consumeWeb(
          webReadable(smallChunks).pipeThrough(xorTS()),
        );
        do_not_optimize(result);
      };
    }).gc('inner');
  });
});

// ============================================================================
// Scenario 3: pipeTo with XOR transform
// ============================================================================

boxplot(() => {
  summary(() => {
    bench('pipeTo + transform (new)', function* () {
      yield async () => {
        const w = nullWriter();
        const total = await Stream.pipeTo(
          Stream.from(smallChunks), xor, w,
        );
        do_not_optimize(total);
      };
    }).gc('inner');

    bench('pipeTo + transform (web)', function* () {
      yield async () => {
        let total = 0;
        const ws = new WritableStream<Uint8Array>({
          write(chunk) { total += chunk.byteLength; },
        });
        await webReadable(smallChunks).pipeThrough(xorTS()).pipeTo(ws);
        do_not_optimize(total);
      };
    }).gc('inner');
  });
});

// ============================================================================
// Scenario 4: Broadcast / tee (2 consumers, 500 x 4KB)
// ============================================================================

boxplot(() => {
  summary(() => {
    bench('broadcast 2 consumers (new)', function* () {
      yield async () => {
        const { writer, channel } = Stream.broadcast({ highWaterMark: 100 });
        const c1 = channel.push();
        const c2 = channel.push();
        const producing = (async () => {
          for (const chunk of bcastChunks) await writer.write(chunk);
          await writer.end();
        })();
        const [r1, r2] = await Promise.all([
          consumeNew(c1),
          consumeNew(c2),
          producing,
        ]);
        do_not_optimize(r1 + r2);
      };
    }).gc('inner');

    bench('tee 2 consumers (web)', function* () {
      yield async () => {
        const [s1, s2] = webReadable(bcastChunks).tee();
        const [r1, r2] = await Promise.all([
          consumeWeb(s1),
          consumeWeb(s2),
        ]);
        do_not_optimize(r1 + r2);
      };
    }).gc('inner');
  });
});

// ============================================================================
// Scenario 5: Large volume pull (10000 x 4KB = 40MB)
// ============================================================================

boxplot(() => {
  summary(() => {
    bench('large pull 40MB (new)', function* () {
      yield async () => {
        const total = await consumeNew(
          Stream.pull(Stream.from(largeChunks), xor),
        );
        do_not_optimize(total);
      };
    }).gc('inner');

    bench('large pull 40MB (web)', function* () {
      yield async () => {
        const result = await consumeWeb(
          webReadable(largeChunks).pipeThrough(xorTS()),
        );
        do_not_optimize(result);
      };
    }).gc('inner');
  });
});

// ============================================================================

console.log('Memory Allocation Profiling: New Streams vs Web Streams');
console.log('(heap and gc rows require --expose-gc)\n');
await run();

/**
 * Memory Profiling: Sustained Load
 *
 * Measures peak RSS, peak heap, and GC pressure during sustained streaming
 * of large volumes. Compares New Streams vs Web Streams.
 *
 * Unlike per-op allocation benchmarks, these measure a single long operation
 * and track memory throughout -- capturing the high-water mark behavior
 * that per-iteration benchmarks miss.
 *
 * Run with:
 *   node --expose-gc --import tsx/esm benchmarks/21-memory-sustained.ts
 */

import { Stream } from '../src/index.js';
import type { Transform, Writer } from '../src/index.js';
import { generateChunks } from './utils.js';

// ============================================================================
// Configuration
// ============================================================================

const CHUNK_SIZE = 4 * 1024; // 4KB
const VOLUME_CHUNKS = 25_000; // 25000 x 4KB = 100MB
const WARMUP_ROUNDS = 2;
const MEASURE_ROUNDS = 5;
const RSS_SAMPLE_MS = 2;

const chunks = generateChunks(CHUNK_SIZE, VOLUME_CHUNKS);
const totalBytes = CHUNK_SIZE * VOLUME_CHUNKS;

// ============================================================================
// Transforms
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

function webReadable(data: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(ctrl) {
      if (i >= data.length) { ctrl.close(); return; }
      ctrl.enqueue(data[i++]);
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
// Measurement infrastructure
// ============================================================================

interface SustainedResult {
  name: string;
  elapsedMs: number;
  peakRSS: number;
  peakHeap: number;
  retained: number;
  gcCount: number;
  gcTimeMs: number;
  throughputMBps: number;
}

const gc = (globalThis as any).gc as (() => void) | undefined;

async function measureSustained(
  name: string,
  fn: () => Promise<void>,
): Promise<SustainedResult> {
  // Warmup
  for (let i = 0; i < WARMUP_ROUNDS; i++) await fn();

  const samples: SustainedResult[] = [];

  for (let round = 0; round < MEASURE_ROUNDS; round++) {
    gc?.();

    const baseline = process.memoryUsage();
    let peakRSS = 0;
    let peakHeap = 0;

    // Track peak memory during operation
    const interval = setInterval(() => {
      const mem = process.memoryUsage();
      const rss = mem.rss - baseline.rss;
      const heap = mem.heapUsed - baseline.heapUsed;
      if (rss > peakRSS) peakRSS = rss;
      if (heap > peakHeap) peakHeap = heap;
    }, RSS_SAMPLE_MS);

    // Track GC events
    let gcCount = 0;
    let gcTimeMs = 0;
    const obs = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        gcCount++;
        gcTimeMs += entry.duration;
      }
    });

    try {
      obs.observe({ type: 'gc', buffered: false });
    } catch {
      // GC observation not available
    }

    const start = performance.now();
    await fn();
    const elapsed = performance.now() - start;

    clearInterval(interval);
    obs.disconnect();

    // Final sample
    const final = process.memoryUsage();
    peakRSS = Math.max(peakRSS, final.rss - baseline.rss);
    peakHeap = Math.max(peakHeap, final.heapUsed - baseline.heapUsed);

    gc?.();
    const afterGC = process.memoryUsage();
    const retained = Math.max(0, afterGC.heapUsed - baseline.heapUsed);

    samples.push({
      name,
      elapsedMs: elapsed,
      peakRSS,
      peakHeap,
      retained,
      gcCount,
      gcTimeMs,
      throughputMBps: (totalBytes / (1024 * 1024)) / (elapsed / 1000),
    });
  }

  // Return median by elapsed time
  samples.sort((a, b) => a.elapsedMs - b.elapsedMs);
  return samples[Math.floor(samples.length / 2)];
}

// ============================================================================
// Formatting
// ============================================================================

function fmtMB(bytes: number): string {
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function fmtMs(ms: number): string {
  return `${ms.toFixed(1)} ms`;
}

function fmtPct(part: number, whole: number): string {
  if (whole === 0) return '0.0%';
  return `${((part / whole) * 100).toFixed(1)}%`;
}

function printResults(title: string, results: [SustainedResult, SustainedResult][]) {
  console.log('\n' + '='.repeat(96));
  console.log(title);
  console.log('='.repeat(96));

  const cols = [30, 10, 10, 10, 8, 10, 10];
  const headers = ['Scenario', 'Peak RSS', 'Peak Heap', 'Retained', 'GCs', 'GC Time', 'GC%'];
  console.log(headers.map((h, i) => h.padEnd(cols[i])).join(' '));
  console.log(cols.map((w) => '-'.repeat(w)).join(' '));

  for (const [newR, webR] of results) {
    console.log(
      [
        newR.name.padEnd(cols[0]),
        fmtMB(newR.peakRSS).padEnd(cols[1]),
        fmtMB(newR.peakHeap).padEnd(cols[2]),
        fmtMB(newR.retained).padEnd(cols[3]),
        String(newR.gcCount).padEnd(cols[4]),
        fmtMs(newR.gcTimeMs).padEnd(cols[5]),
        fmtPct(newR.gcTimeMs, newR.elapsedMs).padEnd(cols[6]),
      ].join(' '),
    );
    console.log(
      [
        webR.name.padEnd(cols[0]),
        fmtMB(webR.peakRSS).padEnd(cols[1]),
        fmtMB(webR.peakHeap).padEnd(cols[2]),
        fmtMB(webR.retained).padEnd(cols[3]),
        String(webR.gcCount).padEnd(cols[4]),
        fmtMs(webR.gcTimeMs).padEnd(cols[5]),
        fmtPct(webR.gcTimeMs, webR.elapsedMs).padEnd(cols[6]),
      ].join(' '),
    );
    console.log();
  }

  console.log('='.repeat(96));
}

// ============================================================================
// Scenarios
// ============================================================================

async function main() {
  console.log('Sustained Load Memory Profiling: New Streams vs Web Streams');
  console.log(`Volume: ${fmtMB(totalBytes)} (${VOLUME_CHUNKS} x ${CHUNK_SIZE / 1024}KB chunks)`);
  console.log(`Rounds: ${WARMUP_ROUNDS} warmup + ${MEASURE_ROUNDS} measured (median reported)`);
  if (!gc) console.log('WARNING: --expose-gc not set, GC timing may be inaccurate');
  console.log();

  const results: [SustainedResult, SustainedResult][] = [];

  // --- Scenario 1: Push write/read ---
  console.log('Running: Push write/read...');
  const pushNew = await measureSustained('push (new)', async () => {
    const { writer, readable } = Stream.push({ highWaterMark: 100 });
    const producing = (async () => {
      for (const chunk of chunks) await writer.write(chunk);
      await writer.end();
    })();
    let total = 0;
    for await (const batch of readable) {
      for (const c of batch) total += c.byteLength;
    }
    await producing;
    if (total !== totalBytes) throw new Error('Wrong size');
  });

  const pushWeb = await measureSustained('push (web)', async () => {
    let ctrl!: ReadableStreamDefaultController<Uint8Array>;
    const rs = new ReadableStream<Uint8Array>({ start(c) { ctrl = c; } });
    const producing = (async () => {
      for (const chunk of chunks) {
        ctrl.enqueue(chunk);
        await Promise.resolve();
      }
      ctrl.close();
    })();
    const total = await consumeWeb(rs);
    await producing;
    if (total !== totalBytes) throw new Error('Wrong size');
  });
  results.push([pushNew, pushWeb]);

  // --- Scenario 2: Pull + transform ---
  console.log('Running: Pull + transform...');
  const pullNew = await measureSustained('pull + xor (new)', async () => {
    const total = await consumeNew(Stream.pull(Stream.from(chunks), xor));
    if (total !== totalBytes) throw new Error('Wrong size');
  });

  const pullWeb = await measureSustained('pull + xor (web)', async () => {
    const total = await consumeWeb(webReadable(chunks).pipeThrough(xorTS()));
    if (total !== totalBytes) throw new Error('Wrong size');
  });
  results.push([pullNew, pullWeb]);

  // --- Scenario 3: pipeTo + transform ---
  console.log('Running: pipeTo + transform...');
  const pipeNew = await measureSustained('pipeTo + xor (new)', async () => {
    const w = nullWriter();
    await Stream.pipeTo(Stream.from(chunks), xor, w);
  });

  const pipeWeb = await measureSustained('pipeTo + xor (web)', async () => {
    let total = 0;
    const ws = new WritableStream<Uint8Array>({
      write(chunk) { total += chunk.byteLength; },
    });
    await webReadable(chunks).pipeThrough(xorTS()).pipeTo(ws);
  });
  results.push([pipeNew, pipeWeb]);

  // --- Scenario 4: Broadcast / tee ---
  // Use fewer chunks to keep runtime reasonable with 2 consumers
  const bcastCount = 12_500; // 50MB total, but each consumer gets full copy
  const bcastChunks = chunks.slice(0, bcastCount);
  const bcastBytes = CHUNK_SIZE * bcastCount;

  console.log('Running: Broadcast / tee (2 consumers)...');
  const bcastNew = await measureSustained('broadcast 2x (new)', async () => {
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
    if (r1 !== bcastBytes || r2 !== bcastBytes) {
      throw new Error('Wrong size');
    }
  });

  const bcastWeb = await measureSustained('tee 2x (web)', async () => {
    const [s1, s2] = webReadable(bcastChunks).tee();
    const [r1, r2] = await Promise.all([consumeWeb(s1), consumeWeb(s2)]);
    if (r1 !== bcastBytes || r2 !== bcastBytes) throw new Error('Wrong size');
  });
  results.push([bcastNew, bcastWeb]);

  // --- Scenario 5: Large volume pull (full 100MB) ---
  console.log('Running: Large volume pull...');
  const largeNew = await measureSustained('large pull (new)', async () => {
    const total = await consumeNew(Stream.pull(Stream.from(chunks), xor));
    if (total !== totalBytes) throw new Error('Wrong size');
  });

  const largeWeb = await measureSustained('large pull (web)', async () => {
    const total = await consumeWeb(webReadable(chunks).pipeThrough(xorTS()));
    if (total !== totalBytes) throw new Error('Wrong size');
  });
  results.push([largeNew, largeWeb]);

  // --- Print ---
  printResults(
    `Sustained Load (${fmtMB(totalBytes)}, ${CHUNK_SIZE / 1024}KB chunks, HWM=100)`,
    results,
  );
}

main().catch(console.error);

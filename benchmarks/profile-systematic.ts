/**
 * Systematic profiling of all major components
 */
import { Stream } from '../src/index.js';
import type { Transform } from '../src/index.js';
import { generateChunks } from './utils.js';

const chunkSize = 8 * 1024;
const chunkCount = 1000;
const totalBytes = chunkSize * chunkCount;
const chunks = generateChunks(chunkSize, chunkCount);

async function measure(name: string, iterations: number, fn: () => Promise<void> | void): Promise<number> {
  // Warmup
  for (let i = 0; i < 10; i++) await fn();
  
  const start = performance.now();
  for (let i = 0; i < iterations; i++) await fn();
  const time = performance.now() - start;
  
  const perIter = time / iterations;
  const throughput = totalBytes / (perIter / 1000);
  console.log(`${name.padEnd(40)} ${perIter.toFixed(2).padStart(8)}ms  ${(throughput / 1e9).toFixed(2).padStart(8)} GB/s`);
  return perIter;
}

async function main() {
  const iterations = 50;
  
  console.log('=== Systematic Performance Profile ===');
  console.log(`Data: ${chunkCount} x ${chunkSize/1024}KB = ${totalBytes/1024/1024}MB`);
  console.log(`Iterations: ${iterations}\n`);
  
  console.log('--- SOURCES ---');
  await measure('Raw array iteration', iterations, () => {
    for (const chunk of chunks) chunk.length;
  });
  
  await measure('Stream.from(Uint8Array[])', iterations, async () => {
    for await (const batch of Stream.from(chunks)) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  await measure('Stream.fromSync(Uint8Array[])', iterations, () => {
    for (const batch of Stream.fromSync(chunks)) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  // Test with generator source
  async function* asyncGen() {
    for (const chunk of chunks) yield chunk;
  }
  await measure('Stream.from(async generator)', iterations, async () => {
    for await (const batch of Stream.from(asyncGen())) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  function* syncGen() {
    for (const chunk of chunks) yield chunk;
  }
  await measure('Stream.from(sync generator)', iterations, async () => {
    for await (const batch of Stream.from(syncGen())) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  console.log('\n--- PULL PIPELINES ---');
  await measure('pull() no transforms', iterations, async () => {
    for await (const batch of Stream.pull(Stream.from(chunks))) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  const identity: Transform = (batch) => batch;
  await measure('pull() 1 identity', iterations, async () => {
    for await (const batch of Stream.pull(Stream.from(chunks), identity)) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  await measure('pull() 3 identity', iterations, async () => {
    for await (const batch of Stream.pull(Stream.from(chunks), identity, identity, identity)) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  await measure('pullSync() no transforms', iterations, () => {
    for (const batch of Stream.pullSync(Stream.fromSync(chunks))) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  await measure('pullSync() 1 identity', iterations, () => {
    const identitySync = (batch: Uint8Array[] | null) => batch;
    for (const batch of Stream.pullSync(Stream.fromSync(chunks), identitySync)) {
      for (const chunk of batch) chunk.length;
    }
  });
  
  console.log('\n--- CONSUMERS ---');
  await measure('bytes()', iterations, async () => {
    const result = await Stream.bytes(Stream.from(chunks));
    if (result.length !== totalBytes) throw new Error('Wrong');
  });
  
  await measure('bytesSync()', iterations, () => {
    const result = Stream.bytesSync(Stream.fromSync(chunks));
    if (result.length !== totalBytes) throw new Error('Wrong');
  });
  
  await measure('text() (100 x 1KB)', iterations, async () => {
    const textChunks = Array.from({length: 100}, () => new Uint8Array(1024).fill(65));
    const result = await Stream.text(Stream.from(textChunks));
    if (result.length !== 100 * 1024) throw new Error('Wrong');
  });
  
  console.log('\n--- PUSH STREAMS ---');
  await measure('push() write/read interleaved', iterations, async () => {
    const { writer, readable } = Stream.push<Uint8Array>();
    let total = 0;
    const readPromise = (async () => {
      for await (const batch of readable) {
        for (const chunk of batch) total += chunk.length;
      }
    })();
    for (const chunk of chunks) {
      await writer.write(chunk);
    }
    await writer.end();
    await readPromise;
    if (total !== totalBytes) throw new Error('Wrong');
  });
  
  await measure('push() batch write (writev)', iterations, async () => {
    const { writer, readable } = Stream.push<Uint8Array>();
    let total = 0;
    const readPromise = (async () => {
      for await (const batch of readable) {
        for (const chunk of batch) total += chunk.length;
      }
    })();
    // Write in batches of 100
    for (let i = 0; i < chunks.length; i += 100) {
      await writer.writev(chunks.slice(i, i + 100));
    }
    await writer.end();
    await readPromise;
    if (total !== totalBytes) throw new Error('Wrong');
  });
  
  console.log('\n--- SHARE/BROADCAST ---');
  await measure('share() single consumer', iterations, async () => {
    const shared = Stream.share(Stream.from(chunks));
    let total = 0;
    for await (const batch of shared.consume()) {
      for (const chunk of batch) total += chunk.length;
    }
    if (total !== totalBytes) throw new Error('Wrong');
  });
  
  await measure('broadcast() single consumer', iterations, async () => {
    const { writer, channel } = Stream.broadcast<Uint8Array>();
    let total = 0;
    const readPromise = (async () => {
      for await (const batch of channel.consume()) {
        for (const chunk of batch) total += chunk.length;
      }
    })();
    for (const chunk of chunks) {
      await writer.write(chunk);
    }
    await writer.end();
    await readPromise;
    if (total !== totalBytes) throw new Error('Wrong');
  });
}

main().catch(console.error);

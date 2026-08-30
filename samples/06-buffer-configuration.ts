/**
 * Buffer Configuration
 *
 * This file demonstrates buffer options including highWaterMark and backpressure policies.
 * The new API uses chunk-oriented backpressure (counting pending writes, not bytes).
 */

import { Stream } from '../src/index.js';
import { section } from './util.js';

// Shared decoder instance for efficiency
const decoder = new TextDecoder();

async function main() {
  // ============================================================================
  // Buffer basics - highWaterMark and desiredSize
  // ============================================================================
  section('Buffer basics - highWaterMark and desiredSize');

  {
    // highWaterMark is the max number of pending writes (chunk-oriented)
    const { writer, readable } = Stream.push({
      highWaterMark: 5  // Allow up to 5 pending writes
    });

    console.log('Initial desiredSize:', writer.desiredSize);

    writer.writeSync('chunk1');
    console.log('After 1 write, desiredSize:', writer.desiredSize);

    writer.writeSync('chunk2');
    writer.writeSync('chunk3');
    console.log('After 3 writes, desiredSize:', writer.desiredSize);

    await writer.end();

    // Consume the readable
    await Stream.bytes(readable);
  }

  // desiredSize becomes null when closed
  {
    const { writer, readable } = Stream.push({ highWaterMark: 5 });
    console.log('\nBefore close, desiredSize:', writer.desiredSize);
    await writer.end();
    console.log('After close, desiredSize:', writer.desiredSize);
    await Stream.bytes(readable);
  }

  // ============================================================================
  // backpressure: 'strict' - Catches backpressure violations (default)
  //
  // Strict mode allows properly awaited writes to proceed (waiting for space),
  // but rejects when too many writes are pending without being awaited.
  // This catches "fire-and-forget" patterns where producers ignore backpressure.
  //
  // - highWaterMark limits BOTH the slots buffer AND pending writes
  // - Sync writes return false when slots buffer is full  
  // - Async writes queue in pendingWrites, waiting for space
  // - If pendingWrites reaches highWaterMark, new async writes throw
  // ============================================================================
  section("backpressure: 'strict' - Catches backpressure violations");

  {
    const { writer, readable } = Stream.push({
      highWaterMark: 2,
      backpressure: 'strict'
    });

    // Start consuming in background
    const consumePromise = Stream.bytes(readable);

    writer.writeSync('chunk1'); // OK - goes to slots
    console.log('First write (writeSync): OK');

    const success = writer.writeSync('chunk2'); // OK - slots now full
    console.log('Second write (writeSync):', success ? 'OK' : 'rejected');

    const success2 = writer.writeSync('chunk3'); // Returns false - slots full
    console.log('Third write (writeSync):', success2 ? 'OK' : 'rejected - slots full');

    // Async writes wait for space when properly awaited
    // Consumer is running in background, so this will eventually succeed
    await writer.write('chunk3');
    console.log('Async write (awaited): OK - waited for space');

    await writer.end();
    await consumePromise;
  }

  // Demonstrate backpressure violation detection
  section("backpressure: 'strict' - Violation detection");

  {
    const { writer, readable } = Stream.push({
      highWaterMark: 2,
      backpressure: 'strict'
    });

    // Fill slots
    writer.writeSync('slot1');
    writer.writeSync('slot2');
    console.log('Slots filled (2 items)');

    // Fire-and-forget writes (not awaited) - these queue in pendingWrites
    const p1 = writer.write('pending1'); // pendingWrites.length = 1
    const p2 = writer.write('pending2'); // pendingWrites.length = 2 (at limit)
    console.log('Queued 2 pending writes (not awaited)');

    // This write exceeds the pendingWrites limit - VIOLATION!
    try {
      await writer.write('pending3');
    } catch (e) {
      console.log('Backpressure violation caught:', (e as Error).message);
    }

    // Clean up
    await writer.fail(new Error('cleanup'));
    await p1.catch(() => {});
    await p2.catch(() => {});
    await Stream.bytes(readable).catch(() => {});
  }

  // ============================================================================
  // backpressure: 'block' - Block writes until space available
  // ============================================================================
  section("backpressure: 'block' - Block until space");

  {
    const { writer, readable } = Stream.push({
      highWaterMark: 2,
      backpressure: 'block'
    });

    // Sync writes still return false when buffer is full
    writer.writeSync('chunk1');
    writer.writeSync('chunk2');
    const success = writer.writeSync('chunk3');
    console.log('Sync write when full:', success ? 'OK' : 'rejected - buffer full');

    // But async writes will wait for space
    const writePromise = writer.write('chunk3'); // This will wait

    // Consume one batch to make room
    const iter = readable[Symbol.asyncIterator]();
    await iter.next();

    // Now the async write can complete
    await writePromise;
    console.log('Async write completed after consumer made space');

    await writer.end();
    // Finish consuming
    for await (const _ of { [Symbol.asyncIterator]: () => iter }) {}
  }

  // ============================================================================
  // backpressure: 'drop-newest' - Discard data being written
  // ============================================================================
  section("backpressure: 'drop-newest' - Discard new data");

  {
    const { writer, readable } = Stream.push({
      highWaterMark: 2,
      backpressure: 'drop-newest'
    });

    writer.writeSync('KEPT-1');
    writer.writeSync('KEPT-2');
    console.log('Buffer filled with 2 chunks');

    // These will be silently dropped
    writer.writeSync('DROPPED-1');
    writer.writeSync('DROPPED-2');
    console.log('Wrote 2 more chunks (but they are dropped)');

    await writer.end();

    const result = await Stream.text(readable);
    console.log('Final content:', result);
    console.log('(Notice: dropped chunks are missing)');
  }

  // ============================================================================
  // backpressure: 'drop-oldest' - Discard oldest buffered data
  // ============================================================================
  section("backpressure: 'drop-oldest' - Discard old data");

  {
    const { writer, readable } = Stream.push({
      highWaterMark: 2,
      backpressure: 'drop-oldest'
    });

    writer.writeSync('OLD-1');
    writer.writeSync('OLD-2');
    console.log('Buffer filled with OLD-1, OLD-2');

    writer.writeSync('NEW-1'); // Drops OLD-1
    console.log('Wrote NEW-1 (OLD-1 dropped)');

    writer.writeSync('NEW-2'); // Drops OLD-2
    console.log('Wrote NEW-2 (OLD-2 dropped)');

    await writer.end();

    const result = await Stream.text(readable);
    console.log('Final content:', result);
    console.log('(Notice: only newest chunks remain)');
  }

  // ============================================================================
  // Broadcast with backpressure
  // ============================================================================
  section('Broadcast with backpressure');

  {
    const { writer, channel } = Stream.broadcast({
      highWaterMark: 3,
      backpressure: 'strict'
    });

    const consumer = channel.push();

    console.log('Broadcast highWaterMark: 3');

    // Fill buffer
    writer.writeSync('chunk1');
    writer.writeSync('chunk2');
    writer.writeSync('chunk3');
    console.log('Buffer filled with 3 chunks');

    // Check desiredSize
    console.log('desiredSize after filling:', writer.desiredSize);

    // Consume to free buffer
    const iter = consumer[Symbol.asyncIterator]();
    await iter.next();
    console.log('After consuming 1 batch, desiredSize:', writer.desiredSize);

    await writer.end();

    // Finish consuming
    for await (const _ of { [Symbol.asyncIterator]: () => iter }) {
      // consume remaining
    }
  }

  // ============================================================================
  // Share with backpressure policies
  // ============================================================================
  section('Share with backpressure policies');

  // drop-oldest in share
  {
    console.log("\n--- share with 'drop-oldest' ---");

    async function* source() {
      for (let i = 1; i <= 5; i++) {
        yield `chunk${i}`;
      }
    }

    const shared = Stream.share(Stream.from(source()), {
      highWaterMark: 2,
      backpressure: 'drop-oldest'
    });

    // Fast consumer
    const fast = shared.pull();
    // Slow consumer (starts late, may miss data)
    const slow = shared.pull();

    const fastResult = await Stream.text(fast);
    const slowResult = await Stream.text(slow);

    console.log('Fast consumer got:', fastResult);
    console.log('Slow consumer got:', slowResult);
    console.log('(Slow consumer may have missed data due to drop-oldest)');
  }

  // ============================================================================
  // Practical examples
  // ============================================================================
  section('Practical examples');

  // Rate limiting with async write using 'block' policy
  {
    console.log('\n--- Rate limiting producer with async writes (block policy) ---');

    const { writer, readable } = Stream.push({
      highWaterMark: 2,  // Small buffer
      backpressure: 'block'  // Use 'block' to wait for space instead of rejecting
    });

    // Fast producer - but limited by backpressure
    const producerDone = (async () => {
      for (let i = 0; i < 5; i++) {
        await writer.write(`msg${i}`);  // async write waits for space with 'block' policy
        console.log(`  Produced msg${i}`);
      }
      await writer.end();
    })();

    // Slow consumer
    for await (const chunks of readable) {
      for (const chunk of chunks) {
        console.log('  Consumed:', decoder.decode(chunk));
        await new Promise(r => setTimeout(r, 30)); // Slow processing
      }
    }

    await producerDone;
  }

  // Lossy streaming with drop-oldest (like video frames)
  {
    console.log('\n--- Lossy streaming (like video frames) ---');

    const { writer, readable } = Stream.push({
      highWaterMark: 3, // Keep only ~3 frames
      backpressure: 'drop-oldest'
    });

    // Fast producer (frames)
    (async () => {
      for (let i = 1; i <= 10; i++) {
        writer.writeSync(`frame${i.toString().padStart(2, '0')}`);
        await new Promise(r => setTimeout(r, 5));
      }
      await writer.end();
    })();

    // Slow consumer - starts late
    await new Promise(r => setTimeout(r, 60)); // Miss some frames

    const result = await Stream.text(readable);
    console.log('  Received (most recent frames):', result);
    console.log('  (Earlier frames were dropped)');
  }

  // Using writev for atomic batches
  {
    console.log('\n--- Using writev for atomic batches ---');

    const { writer, readable } = Stream.push({
      highWaterMark: 3,
      backpressure: 'block'  // Use 'block' so async writes wait for space
    });

    // writev counts as a single slot, regardless of chunk count
    await writer.writev(['header:', 'data1', 'data2', ':footer']);
    console.log('desiredSize after writev (4 chunks, 1 slot):', writer.desiredSize);

    await writer.writev(['another', 'batch']);
    console.log('desiredSize after second writev:', writer.desiredSize);

    await writer.end();

    const result = await Stream.text(readable);
    console.log('Result:', result);
  }

  console.log('\n--- Examples complete! ---\n');
}

main().catch(console.error);

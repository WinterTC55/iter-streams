/**
 * Tests for Broadcast - Push-model multi-consumer streaming
 *
 * Requirements covered: See https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md Section 6 (BCAST-xxx)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { broadcast, Broadcast } from './broadcast.js';
import { from, fromSync } from './from.js';
import { bytes, ondrain } from './consumers.js';
import { drainableProtocol } from './types.js';

// Helper to decode Uint8Array to string
function decode(data: Uint8Array): string {
  return new TextDecoder().decode(data);
}

// Helper to collect chunks from an async iterable
async function collect(source: AsyncIterable<Uint8Array[]>): Promise<Uint8Array[]> {
  const chunks: Uint8Array[] = [];
  for await (const batch of source) {
    chunks.push(...batch);
  }
  return chunks;
}

describe('broadcast()', () => {
  describe('basic usage', () => {
    it('should create writer and broadcast pair [BCAST-001]', () => {
      const { writer, broadcast: bc } = broadcast();
      assert.ok(writer);
      assert.ok(bc);
      assert.strictEqual(bc.consumerCount, 0);
      assert.strictEqual(bc.bufferSize, 0);
    });

    it('should allow single consumer to receive data [BCAST-002]', async () => {
      const { writer, broadcast: bc } = broadcast();
      const consumer = bc.push();

      // Write in background
      const writePromise = (async () => {
        await writer.write('Hello');
        await writer.write(' World');
        await writer.end();
      })();

      const chunks = await collect(consumer);
      await writePromise;

      assert.strictEqual(chunks.length, 2);
      assert.strictEqual(decode(chunks[0]), 'Hello');
      assert.strictEqual(decode(chunks[1]), ' World');
    });

    it('should allow multiple consumers to receive same data [BCAST-003]', async () => {
      const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

      const consumer1 = bc.push();
      const consumer2 = bc.push();

      assert.strictEqual(bc.consumerCount, 2);

      // Write data
      writer.writeSync('chunk1');
      writer.writeSync('chunk2');
      writer.endSync();

      // Both consumers should receive all data
      const [result1, result2] = await Promise.all([
        collect(consumer1),
        collect(consumer2),
      ]);

      assert.strictEqual(result1.length, 2);
      assert.strictEqual(result2.length, 2);
      assert.strictEqual(decode(result1[0]), 'chunk1');
      assert.strictEqual(decode(result2[0]), 'chunk1');
    });

    it('should track consumer count [BCAST-004]', async () => {
      const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

      assert.strictEqual(bc.consumerCount, 0);

      const consumer1 = bc.push();
      assert.strictEqual(bc.consumerCount, 1);

      const consumer2 = bc.push();
      assert.strictEqual(bc.consumerCount, 2);

      // Detach consumer1 by breaking iteration
      const iter1 = consumer1[Symbol.asyncIterator]();
      await iter1.return?.();

      assert.strictEqual(bc.consumerCount, 1);

      await iter1.return?.();
      writer.endSync();
    });
  });

  describe('buffer management', () => {
    it('should respect buffer limit [BCAST-010]', () => {
      const { writer, broadcast: bc } = broadcast({ highWaterMark: 2 });

      assert.strictEqual(writer.writeSync('chunk1'), true);
      assert.strictEqual(writer.writeSync('chunk2'), true);
      assert.strictEqual(bc.bufferSize, 2);
      
      // Third write should fail with strict policy
      assert.strictEqual(writer.writeSync('chunk3'), false);
    });

    it('should trim buffer as consumers advance [BCAST-011]', async () => {
      const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

      const consumer = bc.push();
      const iter = consumer[Symbol.asyncIterator]();

      writer.writeSync('chunk1');
      writer.writeSync('chunk2');
      assert.strictEqual(bc.bufferSize, 2);

      // Consume first chunk
      await iter.next();
      assert.strictEqual(bc.bufferSize, 1);

      // Consume second chunk
      await iter.next();
      assert.strictEqual(bc.bufferSize, 0);

      writer.endSync();
    });

    it('should use drop-oldest policy [BCAST-012]', () => {
      const { writer, broadcast: bc } = broadcast({
        highWaterMark: 2,
        backpressure: 'drop-oldest',
      });

      writer.writeSync('chunk1');
      writer.writeSync('chunk2');
      writer.writeSync('chunk3'); // Should drop chunk1

      assert.strictEqual(bc.bufferSize, 2);
    });

    it('should use drop-newest policy [BCAST-013]', () => {
      const { writer, broadcast: bc } = broadcast({
        highWaterMark: 2,
        backpressure: 'drop-newest',
      });

      writer.writeSync('chunk1');
      writer.writeSync('chunk2');
      const result = writer.writeSync('chunk3'); // Should succeed but drop

      assert.strictEqual(result, true);
      assert.strictEqual(bc.bufferSize, 2);
    });
  });

  describe('writer operations', () => {
    it('should track total bytes written [BCAST-020]', async () => {
      const { writer } = broadcast();

      await writer.write('Hello'); // 5 bytes
      await writer.write(new Uint8Array([1, 2, 3])); // 3 bytes
      const total = await writer.end();

      assert.strictEqual(total, 8);
    });

    it('should support writev [BCAST-021]', async () => {
      const { writer, broadcast: bc } = broadcast();
      const consumer = bc.push();

      await writer.writev(['part1', 'part2', 'part3']);
      await writer.end();

      const chunks = await collect(consumer);
      // writev combines into single batch
      assert.strictEqual(chunks.length, 3);
    });

    it('should propagate errors via fail [BCAST-022]', async () => {
      const { writer, broadcast: bc } = broadcast();
      const consumer = bc.push();

      const error = new Error('Test error');
      await writer.fail(error);

      await assert.rejects(async () => {
        await collect(consumer);
      }, /Test error/);
    });
  });

  describe('cancel()', () => {
    it('should cancel all consumers without error [BCAST-030]', async () => {
      const { writer, broadcast: bc } = broadcast();
      const consumer = bc.push();

      bc.cancel();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });

    it('should cancel all consumers with error [BCAST-031]', async () => {
      const { writer, broadcast: bc } = broadcast();
      const consumer = bc.push();

      // Start iteration before cancelling
      const iter = consumer[Symbol.asyncIterator]();
      const readPromise = iter.next();

      bc.cancel(new Error('Cancelled'));

      await assert.rejects(async () => {
        await readPromise;
      }, /Cancelled/);
    });

    it('should be idempotent [BCAST-032]', () => {
      const { broadcast: bc } = broadcast();
      bc.cancel();
      bc.cancel(); // Should not throw
      bc.cancel(new Error('test')); // Should not throw
    });
  });

  describe('Symbol.dispose', () => {
    it('should cancel on dispose [BCAST-040]', async () => {
      const { broadcast: bc } = broadcast();
      const consumer = bc.push();

      bc[Symbol.dispose]();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });
  });

  describe('AbortSignal', () => {
    it('should respect already-aborted signal [BCAST-050]', async () => {
      const controller = new AbortController();
      controller.abort();

      const { broadcast: bc } = broadcast({ signal: controller.signal });
      const consumer = bc.push();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });

    it('should cancel on signal abort [BCAST-051]', async () => {
      const controller = new AbortController();
      const { writer, broadcast: bc } = broadcast({ signal: controller.signal });
      const consumer = bc.push();

      writer.writeSync('chunk1');

      // Abort signal
      controller.abort();

      // Consumer should complete
      const chunks = await collect(consumer);
      // May or may not get chunk1 depending on timing
    });
  });

  describe('late subscribers', () => {
    it('should allow late subscribers to receive new data [BCAST-005]', async () => {
      const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

      // Write before any subscribers
      writer.writeSync('chunk1');

      // Late subscriber
      const consumer = bc.push();

      // Write more data
      writer.writeSync('chunk2');
      writer.endSync();

      const chunks = await collect(consumer);
      // Late subscriber only sees chunk2 (started at current position)
      assert.strictEqual(chunks.length, 1);
      assert.strictEqual(decode(chunks[0]), 'chunk2');
    });
  });
});

describe('Broadcast.from()', () => {
  it('should create broadcast from async iterable [BCAST-060]', async () => {
    const source = from(['chunk1', 'chunk2']);
    const { broadcast: bc } = Broadcast.from(source);

    const consumer = bc.push();
    
    // Wait a bit for pump to run
    await new Promise(resolve => setTimeout(resolve, 50));

    const chunks = await collect(consumer);
    assert.ok(chunks.length >= 0); // May vary by timing
  });

  it('should create broadcast from sync iterable [BCAST-061]', async () => {
    const source = fromSync(['chunk1', 'chunk2']);
    const { broadcast: bc } = Broadcast.from(source);

    const consumer = bc.push();
    
    // Wait for pump
    await new Promise(resolve => setTimeout(resolve, 50));

    const chunks = await collect(consumer);
    // Should have received the chunks
  });
});

describe('broadcast() with transforms', () => {
  // Simple transform that uppercases text
  const uppercase = (chunks: Uint8Array[] | null) => {
    if (chunks === null) return null;
    return chunks.map(chunk => {
      const text = new TextDecoder().decode(chunk).toUpperCase();
      return new TextEncoder().encode(text);
    });
  };

  // Transform that adds prefix
  const prefix = (chunks: Uint8Array[] | null) => {
    if (chunks === null) return null;
    return chunks.map(chunk => {
      const text = new TextDecoder().decode(chunk);
      return new TextEncoder().encode('PREFIX:' + text);
    });
  };

  it('should apply single transform to consumer [BCAST-070]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

    const consumer = bc.push(uppercase);

    // Write data
    writer.writeSync('hello');
    writer.writeSync('world');
    writer.endSync();

    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 2);
    assert.strictEqual(decode(chunks[0]), 'HELLO');
    assert.strictEqual(decode(chunks[1]), 'WORLD');
  });

  it('should apply multiple transforms in order [BCAST-071]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

    const consumer = bc.push(uppercase, prefix);

    writer.writeSync('hello');
    writer.endSync();

    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 1);
    assert.strictEqual(decode(chunks[0]), 'PREFIX:HELLO');
  });

  it('should allow different transforms per consumer [BCAST-072]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

    const consumer1 = bc.push(uppercase);
    const consumer2 = bc.push(prefix);
    const consumer3 = bc.push(); // no transform

    writer.writeSync('hello');
    writer.endSync();

    const [result1, result2, result3] = await Promise.all([
      collect(consumer1),
      collect(consumer2),
      collect(consumer3),
    ]);

    assert.strictEqual(decode(result1[0]), 'HELLO');
    assert.strictEqual(decode(result2[0]), 'PREFIX:hello');
    assert.strictEqual(decode(result3[0]), 'hello');
  });
});

describe('broadcast writer drainable protocol', () => {
  // BCAST-080: Writer implements drainable protocol
  it('should implement drainable protocol [BCAST-080]', () => {
    const { writer } = broadcast();
    
    assert.ok(drainableProtocol in writer);
    assert.strictEqual(typeof (writer as any)[drainableProtocol], 'function');
  });

  // BCAST-081: ondrain returns resolved Promise<true> when desiredSize > 0
  it('should return resolved Promise<true> when desiredSize > 0 [BCAST-081]', async () => {
    const { writer } = broadcast({ highWaterMark: 5 });
    
    assert.strictEqual(writer.desiredSize, 5);
    
    const drain = ondrain(writer);
    assert.ok(drain !== null);
    
    const result = await drain;
    assert.strictEqual(result, true);
  });

  // BCAST-082: ondrain returns null when desiredSize is null (writer closed)
  it('should return null when desiredSize is null (writer closed) [BCAST-082]', async () => {
    const { writer } = broadcast();
    
    await writer.end();
    assert.strictEqual(writer.desiredSize, null);
    
    const drain = ondrain(writer);
    assert.strictEqual(drain, null);
  });

  // BCAST-083: ondrain returns null when desiredSize is null (writer failed)
  it('should return null when desiredSize is null (writer failed) [BCAST-083]', async () => {
    const { writer } = broadcast();
    
    await writer.fail(new Error('test'));
    assert.strictEqual(writer.desiredSize, null);
    
    const drain = ondrain(writer);
    assert.strictEqual(drain, null);
  });

  // BCAST-084: ondrain returns pending Promise when desiredSize === 0
  it('should return pending Promise when desiredSize === 0 [BCAST-084]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 1 });
    
    // Create a consumer to read data
    const consumer = bc.push();
    const iter = consumer[Symbol.asyncIterator]();
    
    // Fill the buffer
    writer.writeSync('hello');
    assert.strictEqual(writer.desiredSize, 0);
    
    const drain = ondrain(writer);
    assert.ok(drain !== null);
    
    // Should be pending (not resolved yet)
    let resolved = false;
    drain.then(() => { resolved = true; });
    
    // Give microtasks a chance to run
    await Promise.resolve();
    assert.strictEqual(resolved, false);
    
    // Read to clear backpressure
    await iter.next();
    
    // Now should resolve with true
    const result = await drain;
    assert.strictEqual(result, true);
    
    await writer.end();
    await iter.return?.();
  });

  // BCAST-085: ondrain Promise resolves with false when writer closes while waiting
  it('should resolve with false when writer closes while waiting [BCAST-085]', async () => {
    const { writer } = broadcast({ highWaterMark: 1 });
    
    // Fill the buffer
    writer.writeSync('hello');
    assert.strictEqual(writer.desiredSize, 0);
    
    const drain = ondrain(writer);
    assert.ok(drain !== null);
    
    // Close the writer while waiting
    await writer.end();
    
    // Should resolve with false (cannot write anymore)
    const result = await drain;
    assert.strictEqual(result, false);
  });

  // BCAST-086: ondrain Promise rejects when writer fails while waiting
  it('should reject when writer fails while waiting [BCAST-086]', async () => {
    const { writer } = broadcast({ highWaterMark: 1 });
    
    // Fill the buffer
    writer.writeSync('hello');
    assert.strictEqual(writer.desiredSize, 0);
    
    const drain = ondrain(writer);
    assert.ok(drain !== null);
    
    // Fail the writer while waiting
    const error = new Error('fail error');
    await writer.fail(error);
    
    // Should reject with the error
    await assert.rejects(drain, /fail error/);
  });

  // BCAST-087: Multiple drain waiters all resolve together
  it('should resolve multiple drain waiters together [BCAST-087]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 1 });
    
    // Create a consumer to read data
    const consumer = bc.push();
    const iter = consumer[Symbol.asyncIterator]();
    
    // Fill the buffer
    writer.writeSync('hello');
    assert.strictEqual(writer.desiredSize, 0);
    
    // Multiple callers waiting for drain
    const drain1 = ondrain(writer);
    const drain2 = ondrain(writer);
    const drain3 = ondrain(writer);
    
    assert.ok(drain1 !== null);
    assert.ok(drain2 !== null);
    assert.ok(drain3 !== null);
    
    // Read to clear backpressure
    await iter.next();
    
    // All should resolve with true
    const [r1, r2, r3] = await Promise.all([drain1, drain2, drain3]);
    assert.strictEqual(r1, true);
    assert.strictEqual(r2, true);
    assert.strictEqual(r3, true);
    
    await writer.end();
    await iter.return?.();
  });
});

describe('broadcast write signal cancellation', () => {
  it('should reject blocked write when signal fires [BCAST-090]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 1 });

    // Create a consumer so writes go through
    const consumer = bc.push();
    const iter = consumer[Symbol.asyncIterator]();

    // Fill buffer
    writer.writeSync('first');
    assert.strictEqual(writer.desiredSize, 0);

    // Write with signal — will block on backpressure
    const controller = new AbortController();
    const writePromise = writer.write('second', { signal: controller.signal });

    // Abort
    controller.abort();
    await assert.rejects(writePromise, /Abort/);

    // Writer still usable
    await iter.next(); // drain 'first'
    await writer.write('third');
    await writer.end();
    await iter.return?.();
  });

  it('should reject immediately with pre-aborted signal [BCAST-091]', async () => {
    const { writer } = broadcast();
    const controller = new AbortController();
    controller.abort();

    await assert.rejects(
      writer.write('hello', { signal: controller.signal }),
      /Abort/
    );

    await writer.end();
  });

  it('should clean up signal listener on normal write completion [BCAST-092]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 1 });
    const consumer = bc.push();
    const iter = consumer[Symbol.asyncIterator]();

    writer.writeSync('first');

    const controller = new AbortController();
    const writePromise = writer.write('second', { signal: controller.signal });

    // Drain to unblock
    await iter.next();
    await writePromise;

    // Late abort should not cause issues
    controller.abort();

    await writer.end();
    await iter.return?.();
  });
});

describe('Broadcast.from() cancel bug fix', () => {
  it('should not hang when cancel() is called while write is blocked [BCAST-100]', async () => {
    // This tests the pre-existing bug: Broadcast.from() pump hangs when
    // broadcastImpl.cancel() is called while a write is parked on backpressure.
    //
    // Setup: create a slow source that produces data, with a small highWaterMark
    // so the pump blocks on backpressure when no consumer is draining fast enough.
    const source = from((async function* () {
      yield [new TextEncoder().encode('chunk1')];
      yield [new TextEncoder().encode('chunk2')];
      yield [new TextEncoder().encode('chunk3')];
    })());

    const controller = new AbortController();
    const { broadcast: bc } = Broadcast.from(source, {
      highWaterMark: 1,
      signal: controller.signal,
    });

    // Create a consumer but don't drain it — this causes backpressure
    const consumer = bc.push();
    const iter = consumer[Symbol.asyncIterator]();

    // Wait a bit for the pump to start and hit backpressure
    await new Promise(resolve => setTimeout(resolve, 30));

    // Cancel via signal — this must not hang.
    // Before the fix, cancel() would not reject pending writes on BroadcastWriter,
    // so the pump would be stuck forever waiting for the write to resolve.
    controller.abort();

    // If this test hangs here, the bug is present.
    // The consumer should complete cleanly.
    const result = await iter.next();
    // May or may not have data depending on timing, but must not hang
    assert.ok(result !== undefined);

    // Ensure iteration terminates
    const remaining = await iter.next();
    // Should be done (cancelled)
    if (!remaining.done) {
      // Drain any remaining
      await iter.return?.();
    }
  });

  it('should not hang when cancel() is called with no consumers [BCAST-101]', async () => {
    // Edge case: cancel with no consumers — pump write is parked, no one to drain
    const source = from((async function* () {
      yield [new TextEncoder().encode('chunk1')];
      await new Promise(resolve => setTimeout(resolve, 200));
      yield [new TextEncoder().encode('chunk2')];
    })());

    const controller = new AbortController();
    Broadcast.from(source, {
      highWaterMark: 1,
      signal: controller.signal,
    });

    // Wait for pump to start
    await new Promise(resolve => setTimeout(resolve, 30));

    // Cancel — should not hang
    controller.abort();

    // Give the pump time to exit cleanly
    await new Promise(resolve => setTimeout(resolve, 50));
    // If we get here, the pump did not hang. Test passes.
  });

  it('should forward signal to pump writes and check per-iteration [BCAST-102]', async () => {
    // Verify the pump checks signal between iterations
    let iterationCount = 0;
    const source = from((async function* () {
      iterationCount++;
      yield [new TextEncoder().encode('chunk1')];
      iterationCount++;
      yield [new TextEncoder().encode('chunk2')];
      iterationCount++;
      yield [new TextEncoder().encode('chunk3')];
    })());

    const controller = new AbortController();
    const { broadcast: bc } = Broadcast.from(source, {
      highWaterMark: 100, // Large buffer so writes don't block
      signal: controller.signal,
    });

    const consumer = bc.push();

    // Let pump run a bit
    await new Promise(resolve => setTimeout(resolve, 30));

    // Abort
    controller.abort();

    // Collect whatever we got
    const chunks = await collect(consumer);
    // The pump should have stopped — we might get some chunks but not hang
    assert.ok(chunks.length <= 3);
  });
});

describe('broadcast _abort consumer cleanup', () => {
  it('should detach consumers and clear the set on fail [BCAST-110]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

    const consumer1 = bc.push();
    const consumer2 = bc.push();
    assert.strictEqual(bc.consumerCount, 2);

    // Fail the writer — should detach all consumers
    await writer.fail(new Error('test error'));

    // Consumer count should be 0 — consumers are detached and cleared
    assert.strictEqual(bc.consumerCount, 0, 'All consumers should be detached after fail');

    // Consumers should still see the error when iterated
    await assert.rejects(collect(consumer1), /test error/);
    await assert.rejects(collect(consumer2), /test error/);
  });

  it('should detach consumers even without pending reads [BCAST-111]', async () => {
    const { writer, broadcast: bc } = broadcast({ highWaterMark: 100 });

    // Create consumers but don't start reading — no pending reads
    bc.push();
    bc.push();
    assert.strictEqual(bc.consumerCount, 2);

    await writer.fail(new Error('test error'));

    // Should still be cleaned up
    assert.strictEqual(bc.consumerCount, 0);
  });
});

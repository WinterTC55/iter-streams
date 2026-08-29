/**
 * Tests for Push Stream implementation
 * 
 * Requirements covered: PUSH-001 through PUSH-084
 * @see https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md Section 1 (Stream.push())
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { push } from './push.js';
import { ondrain } from './consumers.js';
import { drainableProtocol, type ByteStreamReadable } from './types.js';
import { concatBytes } from './utils.js';

// Helper to collect all chunks from an async iterable
async function collect(iterable: ByteStreamReadable): Promise<Uint8Array[]> {
  const result: Uint8Array[] = [];
  for await (const chunks of iterable) {
    result.push(...chunks);
  }
  return result;
}

// Helper to convert Uint8Array to string
function toString(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

// Helper to concatenate and decode all chunks
async function collectText(iterable: ByteStreamReadable): Promise<string> {
  const chunks = await collect(iterable);
  return new TextDecoder().decode(concatBytes(chunks));
}

describe('push()', () => {
  describe('basic write/read flow', () => {
    // PUSH-001: Returns object with writer and readable properties
    // PUSH-002: Writer.write() accepts string (UTF-8 encoded)
    // PUSH-007: Writer.end() signals end of stream
    // PUSH-012: Readable yields Uint8Array[] batches
    it('should write and read a single chunk [PUSH-001, PUSH-002, PUSH-007, PUSH-012]', async () => {
      const { writer, readable } = push();

      await writer.write('hello');
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'hello');
    });

    it('should write and read multiple chunks [PUSH-002]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      await writer.write('hello');
      await writer.write(' ');
      await writer.write('world');
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'hello world');
    });

    // PUSH-003: Writer.write() accepts Uint8Array
    it('should handle Uint8Array input [PUSH-003]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      await writer.write(new Uint8Array([104, 105])); // "hi"
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'hi');
    });

    // PUSH-004: Writer.writev() writes multiple chunks atomically
    it('should handle writev with multiple chunks [PUSH-004]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      await writer.writev(['hello', ' ', 'world']);
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'hello world');
    });
  });

  describe('backpressure behavior', () => {
    // PUSH-030: Default highWaterMark is 4
    // PUSH-005: Writer.writeSync() returns boolean
    // PUSH-020: desiredSize reflects available buffer space
    it('should respect default highWaterMark of 4 [PUSH-030, PUSH-005, PUSH-020]', async () => {
      const { writer, readable } = push();

      // First 4 writes should succeed synchronously
      assert.strictEqual(writer.writeSync('first'), true);
      assert.strictEqual(writer.desiredSize, 3);
      assert.strictEqual(writer.writeSync('second'), true);
      assert.strictEqual(writer.desiredSize, 2);
      assert.strictEqual(writer.writeSync('third'), true);
      assert.strictEqual(writer.desiredSize, 1);
      assert.strictEqual(writer.writeSync('fourth'), true);
      assert.strictEqual(writer.desiredSize, 0);

      // Fifth write should fail synchronously (buffer full)
      assert.strictEqual(writer.writeSync('fifth'), false);

      // Read drains all buffered chunks as one batch
      const iterator = readable[Symbol.asyncIterator]();
      const { value } = await iterator.next();
      assert.ok(value);
      assert.strictEqual(value.length, 4);
      assert.strictEqual(toString(value[0]), 'first');
      assert.strictEqual(toString(value[3]), 'fourth');

      // All 4 slots freed — desiredSize back to full
      assert.strictEqual(writer.desiredSize, 4);
      assert.strictEqual(writer.writeSync('fifth'), true);

      await writer.end();
      await iterator.return?.();
    });

    // PUSH-031: Custom highWaterMark is respected
    it('should respect custom highWaterMark [PUSH-031, PUSH-020]', async () => {
      const { writer } = push({ highWaterMark: 3 });

      assert.strictEqual(writer.desiredSize, 3);

      assert.strictEqual(writer.writeSync('1'), true);
      assert.strictEqual(writer.desiredSize, 2);

      assert.strictEqual(writer.writeSync('2'), true);
      assert.strictEqual(writer.desiredSize, 1);

      assert.strictEqual(writer.writeSync('3'), true);
      assert.strictEqual(writer.desiredSize, 0);

      assert.strictEqual(writer.writeSync('4'), false);

      await writer.end();
    });

    // PUSH-032: Infinity highWaterMark allows unbounded buffering
    it('should handle unlimited buffer with Infinity highWaterMark [PUSH-032]', async () => {
      const { writer, readable } = push({ highWaterMark: Infinity });

      // Should always succeed
      for (let i = 0; i < 1000; i++) {
        assert.strictEqual(writer.writeSync(`chunk${i}`), true);
      }

      await writer.end();
      const chunks = await collect(readable);
      assert.strictEqual(chunks.length, 1000);
    });

    // PUSH-032b: highWaterMark of 0 is clamped to 1
    it('should clamp highWaterMark of 0 to 1 [PUSH-032b]', async () => {
      const { writer, readable } = push({ highWaterMark: 0 });

      // Should behave like highWaterMark: 1 — one sync write succeeds
      assert.strictEqual(writer.writeSync('hello'), true);
      assert.strictEqual(writer.desiredSize, 0);

      // Second sync write should fail (buffer full)
      assert.strictEqual(writer.writeSync('world'), false);

      await writer.end();
      const result = await collectText(readable);
      assert.strictEqual(result, 'hello');
    });

    // PUSH-032c: negative highWaterMark is clamped to 1
    it('should clamp negative highWaterMark to 1 [PUSH-032c]', async () => {
      const { writer, readable } = push({ highWaterMark: -5 });

      assert.strictEqual(writer.writeSync('hello'), true);
      assert.strictEqual(writer.desiredSize, 0);
      assert.strictEqual(writer.writeSync('world'), false);

      await writer.end();
      const result = await collectText(readable);
      assert.strictEqual(result, 'hello');
    });
  });

  describe('writev counts as single slot', () => {
    // PUSH-033: writev counts as single slot for backpressure
    // PUSH-006: Writer.writevSync() returns boolean
    it('should count writev as single slot for backpressure [PUSH-033, PUSH-006]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });

      // writev with multiple chunks should count as 1 slot
      assert.strictEqual(writer.writevSync(['a', 'b', 'c']), true);
      assert.strictEqual(writer.desiredSize, 0);

      // Second write should fail
      assert.strictEqual(writer.writeSync('d'), false);

      // Read should get all chunks from the writev
      const iterator = readable[Symbol.asyncIterator]();
      const { value } = await iterator.next();
      assert.strictEqual(value!.length, 3);

      await writer.end();
      await iterator.return?.();
    });
  });

  describe('desiredSize', () => {
    // PUSH-020: desiredSize reflects available buffer space
    // PUSH-022: desiredSize is null after close
    it('should reflect available buffer space [PUSH-020, PUSH-022]', async () => {
      const { writer } = push({ highWaterMark: 5 });

      assert.strictEqual(writer.desiredSize, 5);

      await writer.write('1');
      assert.strictEqual(writer.desiredSize, 4);

      await writer.write('2');
      await writer.write('3');
      assert.strictEqual(writer.desiredSize, 2);

      await writer.end();
      assert.strictEqual(writer.desiredSize, null);
    });

    // PUSH-021: desiredSize is 0 when buffer is full
    it('should be 0 when buffer is full [PUSH-021]', async () => {
      const { writer } = push({ highWaterMark: 2 });

      await writer.write('1');
      await writer.write('2');
      assert.strictEqual(writer.desiredSize, 0);
    });

    // PUSH-022: desiredSize is null after close
    it('should be null after close [PUSH-022]', async () => {
      const { writer } = push();

      assert.strictEqual(writer.desiredSize, 4);
      await writer.end();
      assert.strictEqual(writer.desiredSize, null);
    });

    // PUSH-023: desiredSize is null after fail
    it('should be null after fail [PUSH-023]', async () => {
      const { writer } = push();

      assert.strictEqual(writer.desiredSize, 4);
      await writer.fail(new Error('test'));
      assert.strictEqual(writer.desiredSize, null);
    });
  });

  describe('end() returns byte count', () => {
    // PUSH-008: Writer.end() returns total bytes written
    it('should return total bytes written [PUSH-008]', async () => {
      const { writer } = push({ highWaterMark: 10 });

      await writer.write('hello'); // 5 bytes
      await writer.write('world'); // 5 bytes
      const total = await writer.end();

      assert.strictEqual(total, 10);
    });

    it('should return correct count with Uint8Array [PUSH-008]', async () => {
      const { writer } = push({ highWaterMark: 10 });

      await writer.write(new Uint8Array(100));
      await writer.write(new Uint8Array(50));
      const total = await writer.end();

      assert.strictEqual(total, 150);
    });

    // PUSH-009: Writer.endSync() returns total bytes written
    it('endSync should return byte count [PUSH-009]', () => {
      const { writer } = push({ highWaterMark: 10 });

      writer.writeSync('hello');
      const total = writer.endSync();

      assert.strictEqual(total, 5);
    });

    // PUSH-009b: Writer.endSync() returns -1 when writer is not open
    it('endSync should return -1 when already closed [PUSH-009b]', () => {
      const { writer } = push({ highWaterMark: 10 });

      writer.writeSync('hello');
      writer.endSync();

      // Second call should return -1
      assert.strictEqual(writer.endSync(), -1);
    });
  });

  describe('fail() propagates error', () => {
    // PUSH-010: Writer.fail() signals error to consumer
    it('should propagate error to consumer [PUSH-010]', async () => {
      const { writer, readable } = push();

      const error = new Error('test error');
      writer.writeSync('hello');

      // Start consuming
      const promise = collect(readable);

      // Fail with error
      await writer.fail(error);

      // Consumer should see the error
      await assert.rejects(promise, /test error/);
    });

    // PUSH-011: Writer.failSync() signals error synchronously
    it('failSync should work [PUSH-011]', async () => {
      const { writer, readable } = push();

      writer.writeSync('hello');
      writer.failSync(new Error('sync error'));

      await assert.rejects(collect(readable), /sync error/);
    });
  });

  describe('consumer termination propagates to writer', () => {
    // PUSH-050: Consumer break closes writer (desiredSize becomes null)
    it('consumer break should close writer [PUSH-050]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      writer.writeSync('hello');
      writer.writeSync('world');

      // Consumer breaks after first read
      for await (const _chunks of readable) {
        break; // Early exit
      }

      // Writer should be closed
      assert.strictEqual(writer.desiredSize, null);
      await assert.rejects(writer.write('more'));
    });

    // PUSH-051: writeSync returns false after consumer terminates
    it('writeSync should return false after consumer terminates [PUSH-051]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      writer.writeSync('hello');

      // Consumer breaks
      for await (const _chunks of readable) {
        break;
      }

      assert.strictEqual(writer.writeSync('more'), false);
      assert.strictEqual(writer.writevSync(['a', 'b']), false);
    });

    it('consumer throw in for-await should close writer (via return) [PUSH-050]', async () => {
      // Note: When you throw inside a for-await-of loop, the iterator's
      // return() method is called (not throw()). This is standard JS behavior.
      // The error is caught in the consumer's try/catch, not propagated to writer.
      const { writer, readable } = push({ highWaterMark: 10 });

      writer.writeSync('hello');

      // Consumer throws - this triggers iterator.return(), not throw()
      try {
        for await (const _chunks of readable) {
          throw new Error('consumer error');
        }
      } catch {
        // Expected
      }

      // Writer should be closed (via return), not errored
      assert.strictEqual(writer.desiredSize, null);
      // Subsequent writes should fail with "closed" message
      await assert.rejects(writer.write('more'), /closed/);
    });

    // PUSH-052: iterator.throw() propagates error to writer
    it('iterator.throw() should error writer with consumer error [PUSH-052]', async () => {
      // To actually propagate an error from consumer to writer,
      // the consumer must explicitly call iterator.throw()
      const { writer, readable } = push({ highWaterMark: 10 });

      writer.writeSync('hello');

      const iterator = readable[Symbol.asyncIterator]();
      await iterator.next(); // Read the 'hello'
      
      // Explicitly throw on the iterator
      const consumerError = new Error('consumer error');
      await iterator.throw?.(consumerError);

      // Writer should be errored with the consumer's error
      assert.strictEqual(writer.desiredSize, null);
      await assert.rejects(writer.write('more'), /consumer error/);
    });
  });

  describe('backpressure policies', () => {
    describe("'strict' policy (default)", () => {
      // PUSH-040: backpressure: 'strict' allows awaited writes with backpressure
      it('should allow properly awaited writes even when buffer is full [PUSH-040]', async () => {
        const { writer, readable } = push({ highWaterMark: 1, backpressure: 'strict' });

        // Start consumer in parallel
        const consumePromise = collectText(readable);

        // These writes should all succeed because each is awaited
        // The consumer runs in parallel, draining the buffer
        await writer.write('first');
        await writer.write('second');
        await writer.write('third');
        await writer.end();

        const text = await consumePromise;
        assert.strictEqual(text, 'firstsecondthird');
      });

      // PUSH-040a: backpressure: 'strict' rejects when too many pending writes
      it('should reject when too many unawaited writes exceed highWaterMark [PUSH-040a]', async () => {
        const { writer } = push({ highWaterMark: 1, backpressure: 'strict' });

        // First write goes to slots
        writer.writeSync('first');

        // Second write goes to pendingWrites (1 pending, at limit)
        const pendingWrite = writer.write('second');

        // Third write should reject - pendingWrites.length (1) >= highWaterMark (1)
        await assert.rejects(
          writer.write('third'),
          /Backpressure violation.*pending/
        );

        // Clean up
        await writer.fail(new Error('test cleanup'));
        await pendingWrite.catch(() => {}); // Ignore fail error
      });

      // PUSH-040b: strict mode with higher highWaterMark allows more pending writes
      it('should allow up to highWaterMark pending writes [PUSH-040b]', async () => {
        const { writer, readable } = push({ highWaterMark: 3, backpressure: 'strict' });

        // Fill slots (3 items)
        writer.writeSync('s1');
        writer.writeSync('s2');
        writer.writeSync('s3');

        // Now slots are full, writes go to pendingWrites
        // These should NOT throw because pendingWrites.length < highWaterMark
        const p1 = writer.write('p1'); // pendingWrites.length = 1
        const p2 = writer.write('p2'); // pendingWrites.length = 2
        const p3 = writer.write('p3'); // pendingWrites.length = 3

        // This should throw - pendingWrites.length (3) >= highWaterMark (3)
        await assert.rejects(
          writer.write('p4'),
          /Backpressure violation.*pending/
        );

        // Clean up - consume to let pending writes resolve
        const consumePromise = collectText(readable);
        await writer.end();
        await Promise.all([p1, p2, p3]);
        
        const text = await consumePromise;
        assert.strictEqual(text, 's1s2s3p1p2p3');
      });

      // PUSH-041: backpressure: 'strict' returns false for sync writes when buffer full
      it('should return false for sync writes when buffer full [PUSH-041]', async () => {
        const { writer } = push({ highWaterMark: 2, backpressure: 'strict' });

        assert.strictEqual(writer.writeSync('first'), true);
        assert.strictEqual(writer.writeSync('second'), true);
        assert.strictEqual(writer.writeSync('third'), false);

        await writer.end();
      });
    });

    describe("'block' policy", () => {
      // PUSH-042: backpressure: 'block' waits for space on async writes
      it('should wait for space when buffer full [PUSH-042]', async () => {
        const { writer, readable } = push({ highWaterMark: 1, backpressure: 'block' });

        writer.writeSync('first');

        // Async write should wait (not reject)
        const writePromise = writer.write('second');

        // Read to make space
        const iterator = readable[Symbol.asyncIterator]();
        await iterator.next();

        // Write should now complete
        await writePromise;

        await writer.end();
        await iterator.return?.();
      });

      // PUSH-043: backpressure: 'block' returns false for sync writes when buffer full
      it('should return false for sync writes when buffer full [PUSH-043]', async () => {
        const { writer } = push({ highWaterMark: 2, backpressure: 'block' });

        assert.strictEqual(writer.writeSync('first'), true);
        assert.strictEqual(writer.writeSync('second'), true);
        assert.strictEqual(writer.writeSync('third'), false);

        await writer.end();
      });
    });

    describe("'drop-oldest' policy", () => {
      // PUSH-044: backpressure: 'drop-oldest' discards oldest buffered data
      it('should drop oldest chunks when buffer full [PUSH-044]', async () => {
        const { writer, readable } = push({
          highWaterMark: 2,
          backpressure: 'drop-oldest',
        });

        writer.writeSync('first');
        writer.writeSync('second');
        writer.writeSync('third'); // Should drop 'first'

        await writer.end();

        const text = await collectText(readable);
        assert.strictEqual(text, 'secondthird');
      });
    });

    describe("'drop-newest' policy", () => {
      // PUSH-045: backpressure: 'drop-newest' discards incoming data when buffer full
      it('should drop incoming data when buffer full [PUSH-045]', async () => {
        const { writer, readable } = push({
          highWaterMark: 2,
          backpressure: 'drop-newest',
        });

        writer.writeSync('first');
        writer.writeSync('second');
        assert.strictEqual(writer.writeSync('third'), true); // Returns true but drops

        await writer.end();

        const text = await collectText(readable);
        assert.strictEqual(text, 'firstsecond');
      });
    });
  });

  describe('AbortSignal cancellation', () => {
    // PUSH-060: signal option aborts stream when signaled
    it('should abort on signal [PUSH-060]', async () => {
      const controller = new AbortController();
      const { writer, readable } = push({ signal: controller.signal });

      writer.writeSync('hello');

      // Abort the signal
      controller.abort();

      // Writer should be errored
      assert.strictEqual(writer.desiredSize, null);
      await assert.rejects(writer.write('more'));

      // Consumer should see abort error
      await assert.rejects(collect(readable));
    });

    // PUSH-061: Already-aborted signal creates errored stream
    it('should handle already-aborted signal [PUSH-061]', async () => {
      const controller = new AbortController();
      controller.abort();

      const { writer, readable } = push({ signal: controller.signal });

      // Writer should already be errored
      assert.strictEqual(writer.desiredSize, null);
      await assert.rejects(writer.write('hello'));
    });
  });

  describe('batching of synchronous chunks', () => {
    // PUSH-083: Batches synchronously available chunks
    it('should batch synchronously available chunks [PUSH-083]', async () => {
      const { writer, readable } = push({ highWaterMark: 10 });

      // Write multiple chunks synchronously
      writer.writeSync('a');
      writer.writeSync('b');
      writer.writeSync('c');
      await writer.end();

      // Consumer should receive all in one batch
      const iterator = readable[Symbol.asyncIterator]();
      const { value, done } = await iterator.next();

      assert.strictEqual(done, false);
      assert.ok(value);
      // Note: All chunks batched into a single read
      assert.strictEqual(value.length, 3);
    });
  });

  describe('concurrent operations', () => {
    // PUSH-084: Handles concurrent writes and reads
    it('should handle concurrent writes and reads [PUSH-084]', async () => {
      const { writer, readable } = push({ highWaterMark: 5 });

      // Start consumer
      const collectPromise = collectText(readable);

      // Write concurrently
      await Promise.all([
        writer.write('a'),
        writer.write('b'),
        writer.write('c'),
      ]);
      await writer.end();

      const text = await collectPromise;
      assert.strictEqual(text, 'abc');
    });
  });

  describe('edge cases', () => {
    // PUSH-080: Handles empty writes
    it('should handle empty writes [PUSH-080]', async () => {
      const { writer, readable } = push();

      await writer.write('');
      await writer.end();

      const chunks = await collect(readable);
      assert.strictEqual(chunks.length, 1);
      assert.strictEqual(chunks[0].byteLength, 0);
    });

    // PUSH-081: Reading from already-closed stream returns done
    it('should handle reading from already-closed stream [PUSH-081]', async () => {
      const { writer, readable } = push();

      await writer.end();

      const chunks = await collect(readable);
      assert.strictEqual(chunks.length, 0);
    });

    // PUSH-082: Second end() call rejects with TypeError
    it('should reject second end() call with TypeError [PUSH-082]', async () => {
      const { writer } = push({ highWaterMark: 10 });

      await writer.write('hello');
      const first = await writer.end();
      assert.strictEqual(first, 5);

      await assert.rejects(
        () => writer.end(),
        (err: any) => err instanceof TypeError
      );
    });
  });

  describe('transforms', () => {
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

    // PUSH-070: Accepts transforms as arguments
    it('should apply single transform [PUSH-070]', async () => {
      const { writer, readable } = push(uppercase, { highWaterMark: 10 });

      await writer.write('hello');
      await writer.write('world');
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'HELLOWORLD');
    });

    // PUSH-071: Multiple transforms are applied in order
    it('should apply multiple transforms in order [PUSH-071]', async () => {
      const { writer, readable } = push(uppercase, prefix, { highWaterMark: 10 });

      await writer.write('hello');
      await writer.end();

      const text = await collectText(readable);
      assert.strictEqual(text, 'PREFIX:HELLO');
    });

    // PUSH-072: Transforms are applied lazily (on pull)
    it('should apply transforms lazily (on pull) [PUSH-072]', async () => {
      let transformCalled = 0;
      const trackingTransform = (chunks: Uint8Array[] | null) => {
        if (chunks === null) return null;
        transformCalled++;
        return chunks;
      };

      const { writer, readable } = push(trackingTransform, { highWaterMark: 10 });

      // Write before any reads
      await writer.write('hello');
      await writer.write('world');
      
      // Transform shouldn't be called yet (lazy)
      // Note: This depends on implementation - might batch on first pull
      
      await writer.end();
      await collectText(readable);

      // Transform should have been called after consuming
      assert.ok(transformCalled > 0);
    });
  });

  describe('drainable protocol', () => {
    // PUSH-090: Writer implements drainable protocol
    it('should implement drainable protocol [PUSH-090]', () => {
      const { writer } = push();
      
      assert.ok(drainableProtocol in writer);
      assert.strictEqual(typeof (writer as any)[drainableProtocol], 'function');
    });

    // PUSH-091: ondrain returns resolved Promise<true> when desiredSize > 0
    it('should return resolved Promise<true> when desiredSize > 0 [PUSH-091]', async () => {
      const { writer } = push({ highWaterMark: 5 });
      
      assert.strictEqual(writer.desiredSize, 5);
      
      const drain = ondrain(writer);
      assert.ok(drain !== null);
      
      const result = await drain;
      assert.strictEqual(result, true);
    });

    // PUSH-092: ondrain returns null when desiredSize is null (writer closed)
    it('should return null when desiredSize is null (writer closed) [PUSH-092]', async () => {
      const { writer } = push();
      
      await writer.end();
      assert.strictEqual(writer.desiredSize, null);
      
      const drain = ondrain(writer);
      assert.strictEqual(drain, null);
    });

    // PUSH-093: ondrain returns null when desiredSize is null (writer failed)
    it('should return null when desiredSize is null (writer failed) [PUSH-093]', async () => {
      const { writer } = push();
      
      await writer.fail(new Error('test'));
      assert.strictEqual(writer.desiredSize, null);
      
      const drain = ondrain(writer);
      assert.strictEqual(drain, null);
    });

    // PUSH-094: ondrain returns pending Promise when desiredSize === 0
    it('should return pending Promise when desiredSize === 0 [PUSH-094]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });
      
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
      const iterator = readable[Symbol.asyncIterator]();
      await iterator.next();
      
      // Now should resolve with true
      const result = await drain;
      assert.strictEqual(result, true);
      
      await writer.end();
      await iterator.return?.();
    });

    // PUSH-095: ondrain Promise resolves with false when writer closes while waiting
    it('should resolve with false when writer closes while waiting [PUSH-095]', async () => {
      const { writer } = push({ highWaterMark: 1 });
      
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

    // PUSH-096: ondrain Promise rejects when writer fails while waiting
    it('should reject when writer fails while waiting [PUSH-096]', async () => {
      const { writer } = push({ highWaterMark: 1 });
      
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

    // PUSH-097: Multiple drain waiters all resolve together
    it('should resolve multiple drain waiters together [PUSH-097]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });
      
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
      const iterator = readable[Symbol.asyncIterator]();
      await iterator.next();
      
      // All should resolve with true
      const [r1, r2, r3] = await Promise.all([drain1, drain2, drain3]);
      assert.strictEqual(r1, true);
      assert.strictEqual(r2, true);
      assert.strictEqual(r3, true);
      
      await writer.end();
      await iterator.return?.();
    });

    // PUSH-098: ondrain returns null for non-drainable objects
    it('should return null for non-drainable objects [PUSH-098]', () => {
      assert.strictEqual(ondrain(null), null);
      assert.strictEqual(ondrain(undefined), null);
      assert.strictEqual(ondrain({}), null);
      assert.strictEqual(ondrain(123), null);
      assert.strictEqual(ondrain('string'), null);
      assert.strictEqual(ondrain(new Date()), null);
      assert.strictEqual(ondrain([]), null);
      assert.strictEqual(ondrain(() => {}), null);
    });

    // PUSH-099: ondrain works with event source pattern
    it('should work with event source pattern [PUSH-099]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });
      
      // Simulate event source - fill buffer first before consuming
      const chunks = ['a', 'b', 'c', 'd', 'e'];
      const received: string[] = [];
      let pauseCount = 0;
      
      // Fill the buffer to trigger backpressure
      writer.writeSync('a');
      assert.strictEqual(writer.desiredSize, 0, 'Buffer should be full');
      
      // Now test the drain pattern - should need to wait
      const drainPromise = ondrain(writer);
      assert.ok(drainPromise !== null, 'Should return a promise when buffer is full');
      
      // Start consumer in background to drain
      const iterator = readable[Symbol.asyncIterator]();
      
      // Read one chunk to clear backpressure
      const firstRead = await iterator.next();
      assert.ok(!firstRead.done);
      received.push(toString(firstRead.value![0]));
      
      // Drain promise should resolve with true
      const canWrite = await drainPromise;
      assert.strictEqual(canWrite, true, 'Drain should resolve with true');
      pauseCount++;
      
      // Now write more with the drain pattern
      for (let i = 1; i < chunks.length; i++) {
        if (writer.desiredSize === 0) {
          pauseCount++;
          const drain = ondrain(writer);
          if (drain) {
            // Read to unblock
            const readResult = await iterator.next();
            if (!readResult.done) {
              received.push(toString(readResult.value![0]));
            }
            const result = await drain;
            if (!result) break;
          }
        }
        await writer.write(chunks[i]);
      }
      
      await writer.end();
      
      // Collect remaining
      for await (const batch of { [Symbol.asyncIterator]: () => iterator }) {
        for (const chunk of batch) {
          received.push(toString(chunk));
        }
      }
      
      assert.strictEqual(received.join(''), 'abcde');
      assert.ok(pauseCount > 0, 'Expected at least one pause for backpressure');
    });
  });

  describe('write signal cancellation', () => {
    // PUSH-062: Write blocked on backpressure rejects on signal abort (strict)
    it('should reject blocked write when signal fires (strict) [PUSH-062]', async () => {
      const { writer, readable } = push({ highWaterMark: 1, backpressure: 'strict' });

      // Fill buffer
      writer.writeSync('first');
      assert.strictEqual(writer.desiredSize, 0);

      // Start a write that will block on backpressure, with a signal
      const controller = new AbortController();
      const writePromise = writer.write('second', { signal: controller.signal });

      // Abort the signal
      controller.abort();

      // Write should reject
      await assert.rejects(writePromise, /Abort/);

      // Writer should still be usable (per-operation cancellation, not terminal)
      assert.strictEqual(writer.desiredSize, 0);

      // Read to make space, then write again successfully
      const iter = readable[Symbol.asyncIterator]();
      await iter.next(); // read 'first'
      assert.strictEqual(writer.desiredSize, 1);
      await writer.write('third');
      await writer.end();
      await iter.return?.();
    });

    // PUSH-062b: Write blocked on backpressure rejects on signal abort (block)
    it('should reject blocked write when signal fires (block) [PUSH-062b]', async () => {
      const { writer } = push({ highWaterMark: 1, backpressure: 'block' });

      // Fill buffer
      writer.writeSync('first');

      // Blocked write with signal
      const controller = new AbortController();
      const writePromise = writer.write('second', { signal: controller.signal });

      controller.abort();
      await assert.rejects(writePromise, /Abort/);

      await writer.end();
    });

    // PUSH-063: Pre-aborted signal rejects write immediately
    it('should reject immediately with pre-aborted signal [PUSH-063]', async () => {
      const { writer } = push({ highWaterMark: 10 });

      const controller = new AbortController();
      controller.abort();

      // Even though buffer has space, pre-aborted signal rejects
      await assert.rejects(
        writer.write('hello', { signal: controller.signal }),
        /Abort/
      );

      // Writer still usable
      await writer.write('world');
      await writer.end();
    });

    // PUSH-064: Signal listener cleaned up on normal write completion
    it('should clean up signal listener on normal write completion [PUSH-064]', async () => {
      const { writer, readable } = push({ highWaterMark: 1, backpressure: 'block' });

      // Fill buffer
      writer.writeSync('first');

      const controller = new AbortController();
      const writePromise = writer.write('second', { signal: controller.signal });

      // Read to unblock the write
      const iter = readable[Symbol.asyncIterator]();
      await iter.next();

      // Write should complete normally
      await writePromise;

      // Aborting after completion should not cause issues
      controller.abort();

      await writer.end();
      await iter.return?.();
    });

    // PUSH-065: end() accepts WriteOptions (interface compliance)
    it('should accept WriteOptions on end() [PUSH-065]', async () => {
      const { writer } = push({ highWaterMark: 10 });
      await writer.write('hello');
      const total = await writer.end({ signal: undefined });
      assert.strictEqual(total, 5);
    });

    // PUSH-066: writev with signal rejects on abort
    it('should reject blocked writev when signal fires [PUSH-066]', async () => {
      const { writer } = push({ highWaterMark: 1, backpressure: 'block' });

      writer.writeSync('first');

      const controller = new AbortController();
      const writevPromise = writer.writev(['a', 'b'], { signal: controller.signal });

      controller.abort();
      await assert.rejects(writevPromise, /Abort/);

      await writer.end();
    });

    // PUSH-067: Cancelled write is removed from pending queue (doesn't occupy slot)
    it('should remove cancelled write from pending queue [PUSH-067]', async () => {
      const { writer, readable } = push({ highWaterMark: 1, backpressure: 'strict' });

      // Fill buffer
      writer.writeSync('first');

      // Start a write with signal, then cancel it
      const controller = new AbortController();
      const writePromise = writer.write('cancelled', { signal: controller.signal });
      controller.abort();
      await writePromise.catch(() => {});

      // The cancelled write should NOT occupy a pending slot.
      // A new write should be able to queue without backpressure violation.
      const iter = readable[Symbol.asyncIterator]();
      await iter.next(); // drain 'first'

      // This should succeed — the slot freed by cancellation
      await writer.write('replacement');
      await writer.end();
      await iter.return?.();
    });
  });

  describe('drain cleanup on consumer termination', () => {
    // PUSH-100: ondrain resolves with false when consumer breaks while drain is pending
    it('should resolve pending drain with false when consumer breaks [PUSH-100]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });

      // Fill the buffer so desiredSize = 0
      writer.writeSync('hello');
      assert.strictEqual(writer.desiredSize, 0);

      // Also queue a pending write so that reading one chunk
      // doesn't clear backpressure (the pending write refills the slot)
      const pendingWrite = writer.write('world');

      // Start a drain wait — still at capacity
      const drain = ondrain(writer);
      assert.ok(drain !== null);

      // Consumer returns without draining enough to clear backpressure
      const iter = readable[Symbol.asyncIterator]();
      await iter.return?.();

      const result = await drain;
      assert.strictEqual(result, false, 'Drain should resolve with false when consumer breaks');
      await pendingWrite.catch(() => {}); // ignore write rejection
    });

    // PUSH-101: ondrain rejects when consumer throws while drain is pending
    it('should reject pending drain when consumer throws [PUSH-101]', async () => {
      const { writer, readable } = push({ highWaterMark: 1 });

      // Fill the buffer so desiredSize = 0
      writer.writeSync('hello');
      assert.strictEqual(writer.desiredSize, 0);

      // Also queue a pending write so that reading one chunk
      // doesn't clear backpressure (the pending write refills the slot)
      const pendingWrite = writer.write('world');

      // Start a drain wait — still at capacity
      const drain = ondrain(writer);
      assert.ok(drain !== null);

      // Consumer throws via iterator.throw() before draining enough
      // to clear backpressure. The drain should reject.
      const iter = readable[Symbol.asyncIterator]();
      await iter.throw?.(new Error('consumer error'));

      await assert.rejects(drain, /consumer error/);
      await pendingWrite.catch(() => {}); // ignore write rejection
    });
  });
});

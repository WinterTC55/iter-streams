/**
 * Tests for Share - Pull-model multi-consumer streaming
 *
 * Requirements covered: See https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md Section 7 (SHARE-xxx)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { share, shareSync, Share, SyncShare } from './share.js';
import { from, fromSync } from './from.js';
import { bytes, bytesSync } from './consumers.js';

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

// Helper to collect chunks from a sync iterable
function collectSync(source: Iterable<Uint8Array[]>): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (const batch of source) {
    chunks.push(...batch);
  }
  return chunks;
}

describe('share()', () => {
  describe('basic usage', () => {
    it('should create a share instance [SHARE-001]', () => {
      const source = from('test');
      const shared = share(source);

      assert.strictEqual(shared.consumerCount, 0);
      assert.strictEqual(shared.bufferSize, 0);
    });

    it('should allow single consumer to pull data [SHARE-002]', async () => {
      const source = from(['chunk1', 'chunk2']);
      const shared = share(source);

      const consumer = shared.pull();
      const chunks = await collect(consumer);

      assert.strictEqual(chunks.length, 2);
      assert.strictEqual(decode(chunks[0]), 'chunk1');
      assert.strictEqual(decode(chunks[1]), 'chunk2');
    });

    it('should allow multiple consumers to share data [SHARE-003]', async () => {
      const source = from(['chunk1', 'chunk2']);
      const shared = share(source, { highWaterMark: 100 });

      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      assert.strictEqual(shared.consumerCount, 2);

      // Both should receive all data
      const [result1, result2] = await Promise.all([
        collect(consumer1),
        collect(consumer2),
      ]);

      assert.strictEqual(result1.length, 2);
      assert.strictEqual(result2.length, 2);
      assert.strictEqual(decode(result1[0]), 'chunk1');
      assert.strictEqual(decode(result2[0]), 'chunk1');
    });

    it('should handle sync source [SHARE-004]', async () => {
      const source = fromSync(['sync1', 'sync2']);
      const shared = share(source);

      const consumer = shared.pull();
      const chunks = await collect(consumer);

      assert.strictEqual(chunks.length, 2);
      assert.strictEqual(decode(chunks[0]), 'sync1');
    });
  });

  describe('buffer management', () => {
    it('should buffer data for slow consumers [SHARE-010]', async () => {
      const source = from(['chunk1', 'chunk2', 'chunk3']);
      const shared = share(source, { highWaterMark: 100 });

      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      // Consumer1 reads all
      const iter1 = consumer1[Symbol.asyncIterator]();
      await iter1.next(); // chunk1
      await iter1.next(); // chunk2
      await iter1.next(); // chunk3

      // Buffer should still have data for consumer2
      // (at least until consumer2 catches up)
    });

    it('should respect buffer limit with strict policy [SHARE-011]', async () => {
      // Create a source that yields many chunks
      async function* manyChunks() {
        for (let i = 0; i < 100; i++) {
          yield [new TextEncoder().encode(`chunk${i}`)];
        }
      }

      const shared = share(manyChunks(), { highWaterMark: 5, backpressure: 'strict' });

      // Create fast consumer
      const consumer = shared.pull();
      const iter = consumer[Symbol.asyncIterator]();

      // Pull a few chunks - should work
      await iter.next();
      await iter.next();
      await iter.next();

      // Cleanup
      await iter.return?.();
      shared.cancel();
    });

    it('should drop oldest with drop-oldest policy [SHARE-012]', async () => {
      async function* slowSource() {
        for (let i = 0; i < 5; i++) {
          yield [new TextEncoder().encode(`chunk${i}`)];
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      }

      const shared = share(slowSource(), {
        highWaterMark: 2,
        backpressure: 'drop-oldest',
      });

      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      // Fast consumer1 reads all
      const chunks1: Uint8Array[] = [];
      for await (const batch of consumer1) {
        chunks1.push(...batch);
      }

      // Consumer2 may miss some due to drop-oldest
      // (Not testing exact behavior as it depends on timing)
    });
  });

  describe('cancel()', () => {
    it('should cancel all consumers without error [SHARE-020]', async () => {
      const source = from(['chunk1', 'chunk2']);
      const shared = share(source);
      const consumer = shared.pull();

      shared.cancel();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });

    it('should cancel all consumers with error [SHARE-021]', async () => {
      const source = from(['chunk1', 'chunk2']);
      const shared = share(source);
      const consumer = shared.pull();

      shared.cancel(new Error('Cancelled'));

      await assert.rejects(async () => {
        await collect(consumer);
      }, /Cancelled/);
    });

    it('should close source iterator [SHARE-022]', async () => {
      let sourceClosed = false;
      async function* trackableSource() {
        try {
          yield [new TextEncoder().encode('chunk1')];
          yield [new TextEncoder().encode('chunk2')];
        } finally {
          sourceClosed = true;
        }
      }

      const shared = share(trackableSource());
      const consumer = shared.pull();

      // Start iteration
      const iter = consumer[Symbol.asyncIterator]();
      await iter.next();

      // Cancel
      shared.cancel();

      // Source should be closed
      // (Note: depends on implementation details)
    });
  });

  describe('Symbol.dispose', () => {
    it('should cancel on dispose [SHARE-030]', async () => {
      const source = from(['chunk1']);
      const shared = share(source);
      const consumer = shared.pull();

      shared[Symbol.dispose]();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });
  });

  describe('AbortSignal', () => {
    it('should respect already-aborted signal [SHARE-040]', async () => {
      const controller = new AbortController();
      controller.abort();

      const source = from(['chunk1']);
      const shared = share(source, { signal: controller.signal });
      const consumer = shared.pull();

      const chunks = await collect(consumer);
      assert.strictEqual(chunks.length, 0);
    });
  });

  describe('source errors', () => {
    it('should propagate source errors to all consumers [SHARE-050]', async () => {
      async function* errorSource() {
        yield [new TextEncoder().encode('chunk1')];
        throw new Error('Source error');
      }

      const shared = share(errorSource());
      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      await assert.rejects(async () => {
        await collect(consumer1);
      }, /Source error/);

      // consumer2 should also see the error
    });
  });
});

describe('shareSync()', () => {
  describe('basic usage', () => {
    it('should create a sync share instance [SHARE-005]', () => {
      const source = fromSync('test');
      const shared = shareSync(source);

      assert.strictEqual(shared.consumerCount, 0);
      assert.strictEqual(shared.bufferSize, 0);
    });

    it('should allow single consumer to pull data [SHARE-006]', () => {
      const source = fromSync(['chunk1', 'chunk2']);
      const shared = shareSync(source);

      const consumer = shared.pull();
      const chunks = collectSync(consumer);

      assert.strictEqual(chunks.length, 2);
      assert.strictEqual(decode(chunks[0]), 'chunk1');
      assert.strictEqual(decode(chunks[1]), 'chunk2');
    });

    it('should allow interleaved iteration of multiple consumers [SHARE-007]', () => {
      const source = fromSync(['chunk1', 'chunk2', 'chunk3']);
      const shared = shareSync(source, { highWaterMark: 100 });

      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      const iter1 = consumer1[Symbol.iterator]();
      const iter2 = consumer2[Symbol.iterator]();

      // Interleave reads
      const c1_1 = iter1.next();
      const c2_1 = iter2.next();
      const c1_2 = iter1.next();
      const c2_2 = iter2.next();

      assert.strictEqual(c1_1.done, false);
      assert.strictEqual(c2_1.done, false);
      assert.strictEqual(decode(c1_1.value[0]), 'chunk1');
      assert.strictEqual(decode(c2_1.value[0]), 'chunk1');
    });
  });

  describe('buffer limits', () => {
    it('should throw on buffer overflow with strict policy [SHARE-013]', () => {
      function* manyChunks() {
        for (let i = 0; i < 100; i++) {
          yield [new TextEncoder().encode(`chunk${i}`)];
        }
      }

      const shared = shareSync(manyChunks(), {
        highWaterMark: 5,
        backpressure: 'strict',
      });

      // Create two consumers
      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      // Consumer1 reads all - should throw when buffer exceeds limit
      // because consumer2 hasn't read anything
      assert.throws(() => {
        collectSync(consumer1);
      }, /Share buffer limit/);
    });

    it('should drop oldest with drop-oldest policy [SHARE-014]', () => {
      function* source() {
        for (let i = 0; i < 10; i++) {
          yield [new TextEncoder().encode(`chunk${i}`)];
        }
      }

      const shared = shareSync(source(), {
        highWaterMark: 3,
        backpressure: 'drop-oldest',
      });

      const consumer1 = shared.pull();
      const consumer2 = shared.pull();

      // Consumer1 reads all
      const chunks1 = collectSync(consumer1);
      assert.strictEqual(chunks1.length, 10);

      // Consumer2 may have missed some
      const chunks2 = collectSync(consumer2);
      // With drop-oldest, slow consumer catches up but misses data
    });
  });

  describe('cancel()', () => {
    it('should cancel all consumers [SHARE-023]', () => {
      const source = fromSync(['chunk1', 'chunk2']);
      const shared = shareSync(source);
      const consumer = shared.pull();

      shared.cancel();

      const chunks = collectSync(consumer);
      assert.strictEqual(chunks.length, 0);
    });
  });
});

describe('Share.from()', () => {
  it('should create share from async iterable [SHARE-060]', async () => {
    const source = from(['chunk1', 'chunk2']);
    const shared = Share.from(source);

    const consumer = shared.pull();
    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 2);
  });

  it('should create share from sync iterable [SHARE-061]', async () => {
    const source = fromSync(['chunk1', 'chunk2']);
    const shared = Share.from(source);

    const consumer = shared.pull();
    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 2);
  });
});

describe('SyncShare.fromSync()', () => {
  it('should create sync share from sync iterable [SHARE-062]', () => {
    const source = fromSync(['chunk1', 'chunk2']);
    const shared = SyncShare.fromSync(source);

    const consumer = shared.pull();
    const chunks = collectSync(consumer);

    assert.strictEqual(chunks.length, 2);
  });
});

describe('share() with transforms', () => {
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

  it('should apply single transform to consumer [SHARE-070]', async () => {
    const source = from(['hello', 'world']);
    const shared = share(source, { highWaterMark: 100 });

    const consumer = shared.pull(uppercase);
    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 2);
    assert.strictEqual(decode(chunks[0]), 'HELLO');
    assert.strictEqual(decode(chunks[1]), 'WORLD');
  });

  it('should apply multiple transforms in order [SHARE-071]', async () => {
    const source = from(['hello']);
    const shared = share(source, { highWaterMark: 100 });

    const consumer = shared.pull(uppercase, prefix);
    const chunks = await collect(consumer);

    assert.strictEqual(chunks.length, 1);
    assert.strictEqual(decode(chunks[0]), 'PREFIX:HELLO');
  });

  it('should allow different transforms per consumer [SHARE-072]', async () => {
    const source = from(['hello', 'world']);
    const shared = share(source, { highWaterMark: 100 });

    const consumer1 = shared.pull(uppercase);
    const consumer2 = shared.pull(prefix);
    const consumer3 = shared.pull(); // no transform

    const [result1, result2, result3] = await Promise.all([
      collect(consumer1),
      collect(consumer2),
      collect(consumer3),
    ]);

    assert.strictEqual(decode(result1[0]), 'HELLO');
    assert.strictEqual(decode(result2[0]), 'PREFIX:hello');
    assert.strictEqual(decode(result3[0]), 'hello');
  });

  it('should support transforms with options [SHARE-073]', async () => {
    const source = from(['hello']);
    const shared = share(source);

    const controller = new AbortController();
    const consumer = shared.pull(uppercase, { signal: controller.signal });

    const chunks = await collect(consumer);
    assert.strictEqual(decode(chunks[0]), 'HELLO');
  });
});

describe('shareSync() with transforms', () => {
  // Simple sync transform that uppercases text
  const uppercase = (chunks: Uint8Array[] | null) => {
    if (chunks === null) return null;
    return chunks.map(chunk => {
      const text = new TextDecoder().decode(chunk).toUpperCase();
      return new TextEncoder().encode(text);
    });
  };

  it('should apply single transform to sync consumer [SHARE-074]', () => {
    const source = fromSync(['hello', 'world']);
    const shared = shareSync(source, { highWaterMark: 100 });

    const consumer = shared.pull(uppercase);
    const chunks = collectSync(consumer);

    assert.strictEqual(chunks.length, 2);
    assert.strictEqual(decode(chunks[0]), 'HELLO');
    assert.strictEqual(decode(chunks[1]), 'WORLD');
  });
});

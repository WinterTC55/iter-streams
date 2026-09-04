/**
 * Tests for Convenience Consumers & Utilities
 *
 * Requirements covered: See https://github.com/jasnell/new-streams/blob/main/docs/REQUIREMENTS.md Sections 5, 8, 9 (BYTES-xxx, TEXT-xxx, ARRAYBUF-xxx, MERGE-xxx, TAP-xxx)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import v8 from 'node:v8';
import vm from 'node:vm';
import {
  bytes,
  bytesSync,
  text,
  textSync,
  arrayBuffer,
  arrayBufferSync,
  array,
  arraySync,
  dump,
  dumpSync,
  tap,
  tapSync,
  merge,
} from './consumers.js';
import { from, fromSync } from './from.js';
import { pull, pullSync } from './pull.js';

// Helper to create async source with delays
async function* delayedSource(items: string[], delayMs: number) {
  for (const item of items) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    yield [new TextEncoder().encode(item)];
  }
}

// Obtain a gc() without requiring the test runner to pass --expose-gc.
const gc: (() => void) | undefined = (() => {
  const existing = (globalThis as { gc?: () => void }).gc;
  if (existing) return existing;
  try {
    v8.setFlagsFromString('--expose-gc');
    const fn = vm.runInNewContext('gc') as () => void;
    v8.setFlagsFromString('--no-expose-gc');
    return fn;
  } catch {
    return undefined;
  }
})();

/**
 * Run gc() until condition() holds, or give up after maxCount attempts.
 * Returns whether the condition was met.
 */
async function gcUntil(condition: () => boolean, maxCount = 10) {
  if (!gc) return false;
  for (let i = 0; i < maxCount; i++) {
    await new Promise((resolve) => setImmediate(resolve));
    gc();
    if (condition()) return true;
  }
  return false;
}

describe('bytesSync()', () => {
  it('should collect all bytes from source [BYTES-003]', () => {
    const source = fromSync('Hello, World!');
    const result = bytesSync(source);
    assert.strictEqual(new TextDecoder().decode(result), 'Hello, World!');
  });

  it('should handle multiple chunks [BYTES-004]', () => {
    const source = fromSync(['chunk1', 'chunk2', 'chunk3']);
    const result = bytesSync(source);
    assert.strictEqual(new TextDecoder().decode(result), 'chunk1chunk2chunk3');
  });

  it('should handle empty source [BYTES-005]', () => {
    const source = fromSync('');
    const result = bytesSync(source);
    assert.strictEqual(result.byteLength, 0);
  });

  it('should respect byte limit [BYTES-008]', () => {
    const source = fromSync('Hello, World!');
    assert.throws(
      () => bytesSync(source, { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });

  it('should allow data within limit [BYTES-009]', () => {
    const source = fromSync('Hello');
    const result = bytesSync(source, { limit: 10 });
    assert.strictEqual(new TextDecoder().decode(result), 'Hello');
  });
});

describe('bytes()', () => {
  it('should collect all bytes from async source [BYTES-001]', async () => {
    const source = from('Hello, World!');
    const result = await bytes(source);
    assert.strictEqual(new TextDecoder().decode(result), 'Hello, World!');
  });

  it('should collect all bytes from sync source [BYTES-002]', async () => {
    const source = fromSync('Hello, Sync!');
    const result = await bytes(source);
    assert.strictEqual(new TextDecoder().decode(result), 'Hello, Sync!');
  });

  it('should respect AbortSignal [BYTES-006]', async () => {
    const controller = new AbortController();
    controller.abort();

    const source = from('test');
    await assert.rejects(
      async () => await bytes(source, { signal: controller.signal }),
      /Abort/
    );
  });

  it('should respect byte limit [BYTES-007]', async () => {
    const source = from('Hello, World!');
    await assert.rejects(
      async () => await bytes(source, { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });
});

describe('textSync()', () => {
  it('should decode UTF-8 by default [TEXT-004]', () => {
    const source = fromSync('Hello, UTF-8!');
    const result = textSync(source);
    assert.strictEqual(result, 'Hello, UTF-8!');
  });

  it('should handle multi-byte UTF-8 characters [TEXT-005]', () => {
    const source = fromSync('Hello, 世界! 🎉');
    const result = textSync(source);
    assert.strictEqual(result, 'Hello, 世界! 🎉');
  });

  it('should respect encoding option [TEXT-006]', () => {
    // ISO-8859-1 encoded bytes for "café"
    const bytes = new Uint8Array([99, 97, 102, 233]); // "café" in ISO-8859-1
    const source = {
      *[Symbol.iterator]() {
        yield [bytes];
      },
    };
    const result = textSync(source, { encoding: 'iso-8859-1' });
    assert.strictEqual(result, 'café');
  });

  it('should throw on invalid UTF-8 (fatal mode) [TEXT-007]', () => {
    // Invalid UTF-8 sequence
    const invalidBytes = new Uint8Array([0xff, 0xfe]);
    const source = {
      *[Symbol.iterator]() {
        yield [invalidBytes];
      },
    };
    assert.throws(() => textSync(source), TypeError);
  });

  it('should respect byte limit [TEXT-008]', () => {
    const source = fromSync('Hello, World!');
    assert.throws(
      () => textSync(source, { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });
});

describe('text()', () => {
  it('should decode UTF-8 by default [TEXT-001]', async () => {
    const source = from('Hello, UTF-8!');
    const result = await text(source);
    assert.strictEqual(result, 'Hello, UTF-8!');
  });

  it('should handle multi-byte UTF-8 characters [TEXT-002]', async () => {
    const source = from('Hello, 世界! 🎉');
    const result = await text(source);
    assert.strictEqual(result, 'Hello, 世界! 🎉');
  });

  it('should respect AbortSignal [TEXT-003]', async () => {
    const controller = new AbortController();
    controller.abort();

    const source = from('test');
    await assert.rejects(
      async () => await text(source, { signal: controller.signal }),
      /Abort/
    );
  });
});

describe('arrayBufferSync()', () => {
  it('should return ArrayBuffer [ARRAYBUF-003]', () => {
    const source = fromSync('test');
    const result = arrayBufferSync(source);
    assert.ok(result instanceof ArrayBuffer);
    assert.strictEqual(new TextDecoder().decode(result), 'test');
  });

  it('should respect byte limit [ARRAYBUF-004]', () => {
    const source = fromSync('Hello, World!');
    assert.throws(
      () => arrayBufferSync(source, { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });
});

describe('arrayBuffer()', () => {
  it('should return ArrayBuffer [ARRAYBUF-001]', async () => {
    const source = from('test');
    const result = await arrayBuffer(source);
    assert.ok(result instanceof ArrayBuffer);
    assert.strictEqual(new TextDecoder().decode(result), 'test');
  });

  it('should respect AbortSignal [ARRAYBUF-002]', async () => {
    const controller = new AbortController();
    controller.abort();

    const source = from('test');
    await assert.rejects(
      async () => await arrayBuffer(source, { signal: controller.signal }),
      /Abort/
    );
  });
});

describe('arraySync()', () => {
  it('should collect all chunks from source [ARRAY-003]', () => {
    const source = fromSync(['chunk1', 'chunk2', 'chunk3']);
    const result = arraySync(source);
    assert.strictEqual(result.length, 3);
    assert.strictEqual(new TextDecoder().decode(result[0]), 'chunk1');
    assert.strictEqual(new TextDecoder().decode(result[1]), 'chunk2');
    assert.strictEqual(new TextDecoder().decode(result[2]), 'chunk3');
  });

  it('should handle single chunk [ARRAY-004]', () => {
    const source = fromSync('Hello, World!');
    const result = arraySync(source);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(new TextDecoder().decode(result[0]), 'Hello, World!');
  });

  it('should handle empty source [ARRAY-005]', () => {
    const source = fromSync('');
    const result = arraySync(source);
    // Empty string produces one empty chunk
    assert.ok(result.length <= 1);
  });

  it('should respect byte limit [ARRAY-006]', () => {
    const source = fromSync(['chunk1', 'chunk2', 'chunk3']);
    assert.throws(
      () => arraySync(source, { limit: 10 }),
      /Stream exceeded byte limit of 10/
    );
  });

  it('should allow data within limit [ARRAY-007]', () => {
    const source = fromSync(['ab', 'cd']);
    const result = arraySync(source, { limit: 10 });
    assert.strictEqual(result.length, 2);
  });
});

describe('array()', () => {
  it('should collect all chunks from async source [ARRAY-001]', async () => {
    const source = from(['chunk1', 'chunk2', 'chunk3']);
    const result = await array(source);
    assert.strictEqual(result.length, 3);
    assert.strictEqual(new TextDecoder().decode(result[0]), 'chunk1');
    assert.strictEqual(new TextDecoder().decode(result[1]), 'chunk2');
    assert.strictEqual(new TextDecoder().decode(result[2]), 'chunk3');
  });

  it('should collect all chunks from sync source [ARRAY-002]', async () => {
    const source = fromSync(['chunk1', 'chunk2']);
    const result = await array(source);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(new TextDecoder().decode(result[0]), 'chunk1');
    assert.strictEqual(new TextDecoder().decode(result[1]), 'chunk2');
  });

  it('should respect AbortSignal [ARRAY-008]', async () => {
    const controller = new AbortController();
    controller.abort();

    const source = from(['chunk1', 'chunk2']);
    await assert.rejects(
      async () => await array(source, { signal: controller.signal }),
      /Abort/
    );
  });

  it('should respect byte limit [ARRAY-009]', async () => {
    const source = from(['chunk1', 'chunk2', 'chunk3']);
    await assert.rejects(
      async () => await array(source, { limit: 10 }),
      /Stream exceeded byte limit of 10/
    );
  });

  it('should preserve chunk boundaries [ARRAY-010]', async () => {
    // This is the key difference from bytes() - chunks are not concatenated
    const source = from(['Hello', ', ', 'World!']);
    const result = await array(source);
    assert.strictEqual(result.length, 3);
    
    // Each chunk is separate
    const decoded = result.map(chunk => new TextDecoder().decode(chunk));
    assert.deepStrictEqual(decoded, ['Hello', ', ', 'World!']);
    
    // Compare with bytes() which concatenates
    const source2 = from(['Hello', ', ', 'World!']);
    const bytesResult = await bytes(source2);
    assert.strictEqual(new TextDecoder().decode(bytesResult), 'Hello, World!');
  });
});

describe('dumpSync()', () => {
  it('should read a sync source to completion [DUMP-002, DUMP-012]', () => {
    let pulls = 0;
    function* gen() {
      for (let i = 0; i < 5; i++) {
        pulls++;
        yield [new Uint8Array([i])];
      }
    }
    dumpSync(gen());
    assert.strictEqual(pulls, 5);
  });

  it('should return undefined [DUMP-013]', () => {
    assert.strictEqual(dumpSync(fromSync('hello')), undefined);
  });

  it('should handle an empty source [DUMP-005]', () => {
    assert.strictEqual(dumpSync(fromSync([])), undefined);
  });

  it('should throw if the source throws mid-stream [DUMP-014]', () => {
    function* failing() {
      yield [new Uint8Array([1])];
      throw new Error('sync boom');
    }
    assert.throws(() => dumpSync(failing()), /sync boom/);
  });

  it('should respect byte limit [DUMP-015]', () => {
    const source = fromSync('Hello, World!');
    assert.throws(
      () => dumpSync(source, { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });

  it('should allow data within limit [DUMP-015]', () => {
    assert.strictEqual(dumpSync(fromSync('hello'), { limit: 100 }), undefined);
  });

  it('should throw TypeError on an async-only source [DUMP-016]', () => {
    async function* asyncGen() {
      yield [new Uint8Array([1])];
    }
    assert.throws(
      () => dumpSync(asyncGen() as unknown as Iterable<Uint8Array[]>),
      TypeError
    );
  });
});

describe('dump()', () => {
  it('should read an async source to completion [DUMP-001]', async () => {
    let pulls = 0;
    async function* gen() {
      for (let i = 0; i < 5; i++) {
        pulls++;
        yield [new Uint8Array([i])];
      }
    }
    await dump(gen());
    assert.strictEqual(pulls, 5);
  });

  it('should fulfill with undefined [DUMP-003]', async () => {
    assert.strictEqual(await dump(from('hello')), undefined);
  });

  it('should retain no data [DUMP-004]', async (t) => {
    if (!gc) return t.skip('gc unavailable');

    // Allocate a distinct buffer per chunk and keep only weak references.
    // Whatever the consumer retained is still reachable afterwards, and
    // whatever it discarded is collectable. A single pooled buffer would
    // prove nothing: it stays reachable either way.
    const kChunks = 64;

    async function measure(
      consumer: (s: AsyncIterable<Uint8Array[]>) => Promise<unknown>,
      keepResult: boolean
    ) {
      const refs: WeakRef<Uint8Array>[] = [];
      async function* source() {
        for (let i = 0; i < kChunks; i++) {
          const buf = new Uint8Array(64 * 1024);
          refs.push(new WeakRef(buf));
          yield [buf];
        }
      }
      const result = await consumer(source());
      let live = kChunks;
      const settled = await gcUntil(() => {
        live = refs.filter((ref) => ref.deref() !== undefined).length;
        return live === 0;
      });
      // Keep the collecting consumer's result reachable across the gc, so
      // what is measured is its retention rather than the result being
      // dropped.
      if (keepResult) assert.strictEqual((result as unknown[]).length, kChunks);
      return { settled, live };
    }

    const dumped = await measure(dump, false);
    assert.ok(
      dumped.settled,
      `dump() retained ${dumped.live}/${kChunks} chunks`
    );

    // Positive control: array() retains every chunk. Without this the test
    // could pass simply because the collector reclaimed everything anyway.
    // bytes() will not do here -- it concatenates into a new buffer, so the
    // original chunks become collectable for it too.
    const collected = await measure(array, true);
    assert.strictEqual(collected.live, kChunks);
  });

  it('should handle an empty source [DUMP-005]', async () => {
    assert.strictEqual(await dump(from([])), undefined);
  });

  it('should reject if the source errors mid-stream [DUMP-006]', async () => {
    async function* failing() {
      yield [new Uint8Array([1])];
      throw new Error('async boom');
    }
    await assert.rejects(async () => await dump(failing()), /async boom/);
  });

  it('should respect AbortSignal [DUMP-007, DUMP-008]', async () => {
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      async () => await dump(from('test'), { signal: controller.signal }),
      /Abort/
    );
  });

  it('should respect byte limit [DUMP-009]', async () => {
    await assert.rejects(
      async () => await dump(from('Hello, World!'), { limit: 5 }),
      /Stream exceeded byte limit of 5/
    );
  });

  it('should release the source on abrupt completion [DUMP-011]', async () => {
    let returned = false;
    const source = {
      async *[Symbol.asyncIterator]() {
        try {
          while (true) yield [new Uint8Array(64)];
        } finally {
          returned = true;
        }
      },
    };
    await assert.rejects(
      async () => await dump(source, { limit: 128 }),
      /Stream exceeded byte limit of 128/
    );
    assert.strictEqual(returned, true);
  });

  it('should accept a sync source [DUMP-002]', async () => {
    assert.strictEqual(await dump(fromSync('sync source')), undefined);
  });

  it('should not inspect chunks when no limit is set [DUMP-010]', async () => {
    // byteLength throws, so this only completes if dump() never reads it.
    const hostile = Object.defineProperty(new Uint8Array(4), 'byteLength', {
      get() {
        throw new Error('byteLength must not be read without a limit');
      },
    });
    async function* gen() {
      yield [hostile];
    }
    assert.strictEqual(await dump(gen()), undefined);

    // With a limit it must inspect, and therefore surface the getter's throw.
    async function* gen2() {
      yield [hostile];
    }
    await assert.rejects(
      async () => await dump(gen2(), { limit: 10 }),
      /byteLength must not be read/
    );
  });

  it('should observe without retaining when combined with tap()', async () => {
    let total = 0;
    const counter = tap((chunks) => {
      if (chunks !== null) {
        for (const chunk of chunks) total += chunk.byteLength;
      }
    });
    await dump(pull(from('hello world'), counter));
    assert.strictEqual(total, 11);
  });
});

describe('tapSync()', () => {
  it('should call callback with chunks [TAP-003]', () => {
    const observed: (Uint8Array[] | null)[] = [];
    const source = fromSync(['chunk1', 'chunk2']);
    const transform = tapSync((chunks) => {
      observed.push(chunks);
    });
    const output = pullSync(source, transform);
    const result = bytesSync(output);

    assert.strictEqual(new TextDecoder().decode(result), 'chunk1chunk2');
    // Should have received chunks and null (flush)
    assert.ok(observed.length >= 2);
    assert.strictEqual(observed[observed.length - 1], null);
  });

  it('should pass chunks through unchanged [TAP-004]', () => {
    const source = fromSync('test data');
    const transform = tapSync(() => {
      // No-op observer
    });
    const output = pullSync(source, transform);
    const result = bytesSync(output);

    assert.strictEqual(new TextDecoder().decode(result), 'test data');
  });
});

describe('tap()', () => {
  it('should call callback with chunks [TAP-001]', async () => {
    const observed: (Uint8Array[] | null)[] = [];
    const source = from(['chunk1', 'chunk2']);
    const transform = tap((chunks) => {
      observed.push(chunks);
    });
    const output = pull(source, transform);
    const result = await bytes(output);

    assert.strictEqual(new TextDecoder().decode(result), 'chunk1chunk2');
    // Should have received chunks and null (flush)
    assert.ok(observed.length >= 2);
    assert.strictEqual(observed[observed.length - 1], null);
  });

  it('should support async callback [TAP-002]', async () => {
    const delays: number[] = [];
    const source = from(['chunk1', 'chunk2']);
    const transform = tap(async (chunks) => {
      const start = Date.now();
      await new Promise((resolve) => setTimeout(resolve, 5));
      delays.push(Date.now() - start);
    });
    const output = pull(source, transform);
    const result = await bytes(output);

    assert.strictEqual(new TextDecoder().decode(result), 'chunk1chunk2');
    assert.ok(delays.length >= 2);
    assert.ok(delays.every((d) => d >= 4)); // Each delay should be ~5ms
  });
});

describe('merge()', () => {
  it('should handle empty sources [MERGE-001]', async () => {
    const output = merge();
    const result = await bytes(output);
    assert.strictEqual(result.byteLength, 0);
  });

  it('should handle single source [MERGE-002]', async () => {
    const source = from('single source');
    const output = merge(source);
    const result = await bytes(output);
    assert.strictEqual(new TextDecoder().decode(result), 'single source');
  });

  it('should merge multiple sources [MERGE-003]', async () => {
    const source1 = from('a');
    const source2 = from('b');
    const source3 = from('c');
    const output = merge(source1, source2, source3);
    const result = await bytes(output);
    // Order depends on timing, but all should be present
    const decoded = new TextDecoder().decode(result);
    assert.ok(decoded.includes('a'));
    assert.ok(decoded.includes('b'));
    assert.ok(decoded.includes('c'));
  });

  it('should yield in temporal order [MERGE-004]', async () => {
    // Source 1: fast (5ms delay)
    // Source 2: slow (50ms delay)
    const results: string[] = [];

    const fastSource = (async function* () {
      await new Promise((resolve) => setTimeout(resolve, 5));
      yield [new TextEncoder().encode('fast')];
    })();

    const slowSource = (async function* () {
      await new Promise((resolve) => setTimeout(resolve, 50));
      yield [new TextEncoder().encode('slow')];
    })();

    const output = merge(fastSource, slowSource);

    for await (const batch of output) {
      for (const chunk of batch) {
        results.push(new TextDecoder().decode(chunk));
      }
    }

    // Fast should come before slow
    assert.deepStrictEqual(results, ['fast', 'slow']);
  });

  it('should respect AbortSignal [MERGE-005]', async () => {
    const controller = new AbortController();
    controller.abort();

    const source = from('test');
    const output = merge(source, { signal: controller.signal });

    await assert.rejects(async () => {
      await bytes(output);
    }, /Abort/);
  });

  it('should handle source errors [MERGE-006]', async () => {
    const errorSource = (async function* () {
      yield [new TextEncoder().encode('before error')];
      throw new Error('Source failed');
    })();

    const goodSource = from('good');
    const output = merge(errorSource, goodSource);

    await assert.rejects(async () => {
      await bytes(output);
    }, /Source failed/);
  });

  it('should accept options as last argument [MERGE-007]', async () => {
    const source = from('test');
    const output = merge(source, { signal: undefined });
    const result = await bytes(output);
    assert.strictEqual(new TextDecoder().decode(result), 'test');
  });
});

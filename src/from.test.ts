/**
 * Tests for Stream Factories - from() and fromSync()
 *
 * Requirements covered: See https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md Section 2 (FROM-xxx)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { from, fromSync } from './from.js';
import { toStreamable, toAsyncStreamable } from './types.js';
import { concatBytes } from './utils.js';

// Helper to collect all bytes from an async iterable
async function collectBytes(source: AsyncIterable<Uint8Array[]>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const batch of source) {
    chunks.push(...batch);
  }
  return concatBytes(chunks);
}

// Helper to collect all bytes from a sync iterable
function collectBytesSync(source: Iterable<Uint8Array[]>): Uint8Array {
  const chunks: Uint8Array[] = [];
  for (const batch of source) {
    chunks.push(...batch);
  }
  return concatBytes(chunks);
}

// Helper to decode Uint8Array to string
function decode(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

describe('fromSync()', () => {
  describe('ByteInput handling', () => {
    it('should handle string input [FROM-004]', () => {
      const readable = fromSync('Hello, World!');
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should handle Uint8Array input [FROM-005]', () => {
      const input = new Uint8Array([72, 101, 108, 108, 111]); // "Hello"
      const readable = fromSync(input);
      const result = collectBytesSync(readable);
      assert.deepStrictEqual(result, input);
    });

    it('should handle ArrayBuffer input [FROM-006]', () => {
      const encoder = new TextEncoder();
      const input = encoder.encode('Test').buffer;
      const readable = fromSync(input);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'Test');
    });

    it('should handle other ArrayBufferView (Int8Array) [FROM-007]', () => {
      const input = new Int8Array([65, 66, 67]); // "ABC"
      const readable = fromSync(input);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'ABC');
    });

    it('should handle DataView [FROM-007]', () => {
      const buffer = new ArrayBuffer(3);
      const view = new DataView(buffer);
      view.setUint8(0, 88); // X
      view.setUint8(1, 89); // Y
      view.setUint8(2, 90); // Z
      const readable = fromSync(view);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'XYZ');
    });
  });

  describe('SyncStreamable handling', () => {
    it('should handle generator yielding strings [FROM-013]', () => {
      function* source() {
        yield 'Hello';
        yield ', ';
        yield 'World!';
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should handle generator yielding Uint8Arrays [FROM-013]', () => {
      function* source() {
        yield new Uint8Array([65, 66]); // AB
        yield new Uint8Array([67, 68]); // CD
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'ABCD');
    });

    it('should handle generator yielding mixed types [FROM-013]', () => {
      function* source() {
        yield 'Start:';
        yield new Uint8Array([65, 66]);
        yield ':End';
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'Start:AB:End');
    });

    it('should handle array input (arrays are iterable) [FROM-014]', () => {
      const source = ['line1\n', 'line2\n', 'line3\n'];
      const readable = fromSync(source);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'line1\nline2\nline3\n');
    });

    it('should flatten arrays yielded by generators [FROM-017]', () => {
      function* source() {
        yield ['a', 'b', 'c'];
        yield ['d', 'e'];
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'abcde');
    });

    it('should flatten nested iterables [FROM-016]', () => {
      function* inner() {
        yield 'nested1';
        yield 'nested2';
      }
      function* source() {
        yield 'before';
        yield inner();
        yield 'after';
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'beforenested1nested2after');
    });
  });

  describe('ToStreamable protocol', () => {
    it('should handle objects with toStreamable returning string [FROM-024]', () => {
      class JsonMessage {
        constructor(private data: object) {}
        [toStreamable]() {
          return JSON.stringify(this.data);
        }
      }

      function* source() {
        yield new JsonMessage({ hello: 'world' });
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), '{"hello":"world"}');
    });

    it('should handle objects with toStreamable returning array [FROM-025]', () => {
      class MultiPart {
        [toStreamable]() {
          return ['part1', '-', 'part2'];
        }
      }

      function* source() {
        yield new MultiPart();
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'part1-part2');
    });

    it('should handle nested ToStreamable [FROM-026]', () => {
      class Inner {
        [toStreamable]() {
          return 'INNER';
        }
      }
      class Outer {
        [toStreamable]() {
          return ['[', new Inner(), ']'];
        }
      }

      function* source() {
        yield new Outer();
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), '[INNER]');
    });
  });

  describe('String coercion fallback', () => {
    it('should handle URL (has custom toString) [FROM-030]', () => {
      function* source(): Generator<unknown> {
        yield new URL('https://example.com/path');
      }
      const readable = fromSync(source() as Iterable<string>);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), 'https://example.com/path');
    });

    it('should handle Date (has custom toString) [FROM-031]', () => {
      const date = new Date('2024-01-15T12:00:00Z');
      function* source(): Generator<unknown> {
        yield date;
      }
      const readable = fromSync(source() as Iterable<string>);
      const result = collectBytesSync(readable);
      // Date toString output varies by timezone, just check it's not empty
      assert.ok(decode(result).length > 0);
    });

    it('should handle objects with custom toString [FROM-032]', () => {
      class Point {
        constructor(public x: number, public y: number) {}
        toString() {
          return `${this.x},${this.y}`;
        }
      }

      function* source(): Generator<unknown> {
        yield new Point(10, 20);
      }
      const readable = fromSync(source() as Iterable<string>);
      const result = collectBytesSync(readable);
      assert.strictEqual(decode(result), '10,20');
    });
  });

  describe('Error handling', () => {
    it('should reject plain objects without custom toString [FROM-040]', () => {
      function* source(): Generator<unknown> {
        yield { foo: 'bar' };
      }
      const readable = fromSync(source() as Iterable<string>);
      assert.throws(
        () => collectBytesSync(readable),
        /Cannot convert value to streamable/
      );
    });

    it('should reject null [FROM-041]', () => {
      function* source(): Generator<unknown> {
        yield null;
      }
      const readable = fromSync(source() as Iterable<string>);
      assert.throws(
        () => collectBytesSync(readable),
        /Cannot convert value to streamable.*null/
      );
    });

    it('should reject undefined [FROM-042]', () => {
      function* source(): Generator<unknown> {
        yield undefined;
      }
      const readable = fromSync(source() as Iterable<string>);
      assert.throws(
        () => collectBytesSync(readable),
        /Cannot convert value to streamable/
      );
    });

    it('should reject numbers [FROM-043]', () => {
      function* source(): Generator<unknown> {
        yield 42;
      }
      const readable = fromSync(source() as Iterable<string>);
      assert.throws(
        () => collectBytesSync(readable),
        /Cannot convert value to streamable/
      );
    });

    it('should reject non-iterable input [FROM-044]', () => {
      assert.throws(
        () => fromSync(42 as unknown as string),
        /Input must be a ByteInput or SyncStreamable/
      );
    });
  });

  describe('Empty streams', () => {
    it('should handle empty string [FROM-052]', () => {
      const readable = fromSync('');
      const result = collectBytesSync(readable);
      assert.strictEqual(result.byteLength, 0);
    });

    it('should handle empty generator [FROM-053]', () => {
      function* source() {
        // yields nothing
      }
      const readable = fromSync(source());
      const result = collectBytesSync(readable);
      assert.strictEqual(result.byteLength, 0);
    });

    it('should handle empty array [FROM-054]', () => {
      const readable = fromSync([]);
      const result = collectBytesSync(readable);
      assert.strictEqual(result.byteLength, 0);
    });
  });
});

describe('from()', () => {
  describe('ByteInput handling', () => {
    it('should handle string input [FROM-001]', async () => {
      const readable = from('Hello, World!');
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should handle Uint8Array input [FROM-002]', async () => {
      const input = new Uint8Array([72, 101, 108, 108, 111]);
      const readable = from(input);
      const result = await collectBytes(readable);
      assert.deepStrictEqual(result, input);
    });

    it('should handle ArrayBuffer input [FROM-003]', async () => {
      const encoder = new TextEncoder();
      const input = encoder.encode('Test').buffer;
      const readable = from(input);
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'Test');
    });
  });

  describe('SyncStreamable handling', () => {
    it('should handle sync generator [FROM-011]', async () => {
      function* source() {
        yield 'Hello';
        yield ', ';
        yield 'World!';
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should handle array input [FROM-012]', async () => {
      const source = ['line1\n', 'line2\n'];
      const readable = from(source);
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'line1\nline2\n');
    });
  });

  describe('AsyncStreamable handling', () => {
    it('should handle async generator [FROM-010]', async () => {
      async function* source() {
        yield 'async1';
        yield 'async2';
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'async1async2');
    });

    it('should handle async generator with delays [FROM-010]', async () => {
      async function* source() {
        yield 'first';
        await new Promise(resolve => setTimeout(resolve, 10));
        yield 'second';
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'firstsecond');
    });

    it('should flatten nested async iterables [FROM-015]', async () => {
      async function* inner() {
        yield 'inner1';
        yield 'inner2';
      }
      async function* source() {
        yield 'before';
        yield inner();
        yield 'after';
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'beforeinner1inner2after');
    });
  });

  describe('ToAsyncStreamable protocol', () => {
    it('should handle objects with toAsyncStreamable returning promise [FROM-020]', async () => {
      class LazyData {
        [toAsyncStreamable]() {
          return Promise.resolve('lazy-data');
        }
      }

      async function* source() {
        yield new LazyData();
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'lazy-data');
    });

    it('should handle objects with toAsyncStreamable returning async iterable [FROM-021]', async () => {
      class StreamingData {
        [toAsyncStreamable]() {
          return (async function* () {
            yield 'stream1';
            yield 'stream2';
          })();
        }
      }

      async function* source() {
        yield new StreamingData();
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'stream1stream2');
    });

    it('should prefer toAsyncStreamable over toStreamable in async context [FROM-022]', async () => {
      class DualMode {
        [toStreamable]() {
          return 'sync-version';
        }
        [toAsyncStreamable]() {
          return 'async-version';
        }
      }

      async function* source() {
        yield new DualMode();
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'async-version');
    });
  });

  describe('ToStreamable protocol in async context', () => {
    it('should handle toStreamable when toAsyncStreamable not present [FROM-023]', async () => {
      class SyncOnly {
        [toStreamable]() {
          return 'sync-only-data';
        }
      }

      async function* source() {
        yield new SyncOnly();
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'sync-only-data');
    });
  });

  describe('Mixed sync/async', () => {
    it('should handle sync source in async from() [FROM-011]', async () => {
      function* source() {
        yield 'sync';
        yield 'chunks';
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'syncchunks');
    });

    it('should handle mixed yielded types [FROM-015]', async () => {
      async function* source() {
        yield 'string';
        yield new Uint8Array([65, 66]); // AB
        yield ['array', 'items'];
        yield (function* () { yield 'nested'; })();
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(decode(result), 'stringABarrayitemsnested');
    });
  });

  describe('Error handling', () => {
    it('should reject plain objects without custom toString [FROM-040]', async () => {
      async function* source(): AsyncGenerator<unknown> {
        yield { foo: 'bar' };
      }
      const readable = from(source() as AsyncIterable<string>);
      await assert.rejects(
        async () => await collectBytes(readable),
        /Cannot convert value to streamable/
      );
    });

    it('should reject non-iterable input [FROM-044]', () => {
      assert.throws(
        () => from(42 as unknown as string),
        /Input must be a ByteInput or Streamable/
      );
    });

    it('should propagate errors from async generators [FROM-045]', async () => {
      async function* source() {
        yield 'before';
        throw new Error('generator error');
      }
      const readable = from(source());
      await assert.rejects(
        async () => await collectBytes(readable),
        /generator error/
      );
    });
  });

  describe('Empty streams', () => {
    it('should handle empty string [FROM-050]', async () => {
      const readable = from('');
      const result = await collectBytes(readable);
      assert.strictEqual(result.byteLength, 0);
    });

    it('should handle empty async generator [FROM-051]', async () => {
      async function* source() {
        // yields nothing
      }
      const readable = from(source());
      const result = await collectBytes(readable);
      assert.strictEqual(result.byteLength, 0);
    });
  });
});

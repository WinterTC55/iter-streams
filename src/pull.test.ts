/**
 * Tests for Pull Pipeline - pull(), pullSync(), pipeTo(), pipeToSync()
 *
 * Requirements covered: See https://github.com/jasnell/new-streams/blob/main/docs/REQUIREMENTS.md Sections 3-4 (PULL-xxx, PIPE-xxx)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { pull, pullSync, pipeTo, pipeToSync } from './pull.js';
import { fromSync, from } from './from.js';
import type { Writer, SyncWriter, Transform, SyncTransform } from './types.js';
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

// Create a mock sync writer for testing
function createMockSyncWriter(): SyncWriter & { chunks: Uint8Array[]; closed: boolean; failed: Error | undefined } {
  const writer = {
    chunks: [] as Uint8Array[],
    closed: false,
    failed: undefined as Error | undefined,
    get desiredSize() {
      return this.closed ? null : 10;
    },
    write(chunk: Uint8Array | string) {
      if (this.closed) throw new Error('Writer is closed');
      const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
      this.chunks.push(data);
    },
    writev(chunks: (Uint8Array | string)[]) {
      for (const chunk of chunks) {
        this.write(chunk);
      }
    },
    end() {
      this.closed = true;
      return this.chunks.reduce((acc, c) => acc + c.byteLength, 0);
    },
    fail(reason?: Error) {
      this.failed = reason;
      this.closed = true;
    },
  };
  return writer;
}

// Create a mock async writer for testing
function createMockWriter(): Writer & { chunks: Uint8Array[]; closed: boolean; failed: Error | undefined } {
  const writer = {
    chunks: [] as Uint8Array[],
    closed: false,
    failed: undefined as Error | undefined,
    get desiredSize() {
      return this.closed ? null : 10;
    },
    async write(chunk: Uint8Array | string) {
      if (this.closed) throw new Error('Writer is closed');
      const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
      this.chunks.push(data);
    },
    async writev(chunks: (Uint8Array | string)[]) {
      for (const chunk of chunks) {
        await this.write(chunk);
      }
    },
    writeSync(chunk: Uint8Array | string) {
      if (this.closed) return false;
      const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
      this.chunks.push(data);
      return true;
    },
    writevSync(chunks: (Uint8Array | string)[]) {
      for (const chunk of chunks) {
        if (!this.writeSync(chunk)) return false;
      }
      return true;
    },
    async end() {
      this.closed = true;
      return this.chunks.reduce((acc, c) => acc + c.byteLength, 0);
    },
    endSync() {
      this.closed = true;
      return this.chunks.reduce((acc, c) => acc + c.byteLength, 0);
    },
    async fail(reason?: Error) {
      this.failed = reason;
      this.closed = true;
    },
    failSync(reason?: Error) {
      this.failed = reason;
      this.closed = true;
      return true;
    },
  };
  return writer;
}

describe('pullSync()', () => {
  describe('basic usage', () => {
    it('should pass through source without transforms [PULL-003]', () => {
      const source = fromSync('Hello, World!');
      const output = pullSync(source);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should apply a single transform [PULL-006]', () => {
      const source = fromSync('hello');
      const toUpper: SyncTransform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const output = pullSync(source, toUpper);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'HELLO');
    });

    it('should chain multiple transforms [PULL-007]', () => {
      const source = fromSync('hello');
      const toUpper: SyncTransform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const addExclaim: SyncTransform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c) + '!'));
      };
      const output = pullSync(source, toUpper, addExclaim);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'HELLO!');
    });
  });

  describe('transform output types', () => {
    it('should handle transform returning Uint8Array[] [PULL-010]', () => {
      const source = fromSync('test');
      const transform: SyncTransform = (chunks) => {
        if (chunks === null) return null;
        return [new Uint8Array([65, 66, 67])]; // ABC
      };
      const output = pullSync(source, transform);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'ABC');
    });

    it('should handle transform returning string (via generator) [PULL-011]', () => {
      const source = fromSync('test');
      const transform: SyncTransform = function* (chunks) {
        if (chunks === null) return;
        yield 'transformed';
      };
      const output = pullSync(source, transform);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'transformed');
    });

    it('should handle transform returning nested iterables [PULL-012]', () => {
      const source = fromSync('test');
      const transform: SyncTransform = function* (chunks) {
        if (chunks === null) return;
        yield 'a';
        yield (function* () {
          yield 'b';
          yield 'c';
        })();
        yield 'd';
      };
      const output = pullSync(source, transform);
      const result = collectBytesSync(output);
      assert.strictEqual(decode(result), 'abcd');
    });
  });

  describe('stateful transforms', () => {
    it('should support stateful transform object [PULL-030]', () => {
      const source = fromSync(['chunk1', 'chunk2']);
      // Object = stateful transform (receives entire source as iterable)
      const transform: SyncTransform = {
        transform: function* (sourceIter) {
          let count = 0;
          for (const chunks of sourceIter) {
            if (chunks === null) {
              yield `total:${count}`;
            } else {
              count += chunks.length;
              for (const chunk of chunks) {
                yield chunk;
              }
            }
          }
        },
      };
      const output = pullSync(source, transform);
      const result = collectBytesSync(output);
      // Note: Each array element becomes a separate batch, so count = 2
      assert.ok(decode(result).includes('total:'));
    });

    it('should yield separate batches per transform yield, not buffer all output', () => {
      const source = fromSync(['a', 'b', 'c']);
      const transform: SyncTransform = {
        transform: function* (sourceIter) {
          for (const chunks of sourceIter) {
            if (chunks === null) {
              yield 'done';
            } else {
              for (const chunk of chunks) {
                yield chunk;
              }
            }
          }
        },
      };
      const output = pullSync(source, transform);
      // Count the number of batches yielded
      const batches: Uint8Array[][] = [];
      for (const batch of output) {
        batches.push(batch);
      }
      // The transform yields 4 items (a, b, c, done) so we should get 4 batches,
      // not 1 giant batch containing everything
      assert.strictEqual(batches.length, 4);
      assert.strictEqual(decode(batches[0][0]), 'a');
      assert.strictEqual(decode(batches[1][0]), 'b');
      assert.strictEqual(decode(batches[2][0]), 'c');
      assert.strictEqual(decode(batches[3][0]), 'done');
    });
  });

  describe('flush signal', () => {
    it('should receive null flush signal at end [PULL-031]', () => {
      const source = fromSync('test');
      let flushed = false;
      const transform: SyncTransform = (chunks) => {
        if (chunks === null) {
          flushed = true;
          return null;
        }
        return chunks;
      };
      const output = pullSync(source, transform);
      collectBytesSync(output);
      assert.strictEqual(flushed, true);
    });
  });
});

describe('pull()', () => {
  describe('basic usage', () => {
    it('should pass through source without transforms [PULL-001]', async () => {
      const source = from('Hello, World!');
      const output = pull(source);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'Hello, World!');
    });

    it('should work with sync source [PULL-002]', async () => {
      const source = fromSync('Sync Source');
      const output = pull(source);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'Sync Source');
    });

    it('should apply a single transform [PULL-004]', async () => {
      const source = from('hello');
      const toUpper: Transform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const output = pull(source, toUpper);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'HELLO');
    });

    it('should chain multiple transforms [PULL-005]', async () => {
      const source = from('hello');
      const toUpper: Transform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const addExclaim: Transform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c) + '!'));
      };
      const output = pull(source, toUpper, addExclaim);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'HELLO!');
    });
  });

  describe('async transforms', () => {
    it('should handle async transform function [PULL-020]', async () => {
      const source = from('test');
      const asyncTransform: Transform = async (chunks) => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        if (chunks === null) return null;
        return chunks;
      };
      const output = pull(source, asyncTransform);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'test');
    });

    it('should handle transform returning promise [PULL-021]', async () => {
      const source = from('test');
      const transform: Transform = (chunks) => {
        return Promise.resolve(chunks);
      };
      const output = pull(source, transform);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'test');
    });

    it('should handle transform returning async generator [PULL-022]', async () => {
      const source = from('test');
      const transform: Transform = async function* (chunks) {
        if (chunks === null) return;
        yield 'async';
        await new Promise((resolve) => setTimeout(resolve, 5));
        yield 'gen';
      };
      const output = pull(source, transform);
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'asyncgen');
    });
  });

  describe('options', () => {
    it('should accept options as last argument [PULL-040]', async () => {
      const source = from('test');
      const transform: Transform = (chunks) => chunks;
      const output = pull(source, transform, { signal: undefined });
      const result = await collectBytes(output);
      assert.strictEqual(decode(result), 'test');
    });

    it('should respect AbortSignal [PULL-041]', async () => {
      const controller = new AbortController();
      const source = from(
        (async function* () {
          yield 'chunk1';
          await new Promise((resolve) => setTimeout(resolve, 100));
          yield 'chunk2';
        })()
      );

      const output = pull(source, { signal: controller.signal });

      // Abort after first chunk
      setTimeout(() => controller.abort(), 20);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Abort/);
    });

    it('should handle already-aborted signal [PULL-042]', async () => {
      const controller = new AbortController();
      controller.abort();
      const source = from('test');
      const output = pull(source, { signal: controller.signal });

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Abort/);
    });

    it('should reject a pending source next() when aborted', async () => {
      const controller = new AbortController();
      const reason = new DOMException('Aborted', 'AbortError');
      let returned = false;
      const source: AsyncIterable<Uint8Array[]> = {
        [Symbol.asyncIterator]() {
          return {
            next() {
              return new Promise<IteratorResult<Uint8Array[]>>(() => undefined);
            },
            return() {
              returned = true;
              return Promise.resolve({ done: true, value: undefined });
            },
          };
        },
      };

      const iterator = pull(source, { signal: controller.signal })[Symbol.asyncIterator]();
      const next = iterator.next();
      controller.abort(reason);

      await assert.rejects(next, { name: 'AbortError' });
      assert.strictEqual(returned, true);
    });

    it('should reject future next() calls after abort', async () => {
      const controller = new AbortController();
      const reason = new DOMException('Aborted', 'AbortError');
      const source = from('test');
      const iterator = pull(source, { signal: controller.signal })[Symbol.asyncIterator]();

      controller.abort(reason);

      await assert.rejects(iterator.next(), { name: 'AbortError' });
    });

    it('should reject future next() calls after abort for sync sources without transforms', async () => {
      const controller = new AbortController();
      const reason = new DOMException('Aborted', 'AbortError');
      const iterator = pull(['a', 'b'], { signal: controller.signal })[Symbol.asyncIterator]();

      const first = await iterator.next();
      assert.strictEqual(first.done, false);
      assert.strictEqual(decode(concatBytes(first.value)), 'a');

      controller.abort(reason);

      await assert.rejects(iterator.next(), { name: 'AbortError' });
    });
  });

  describe('error handling', () => {
    it('should fire signal on transforms when error occurs [PULL-032]', async () => {
      let signalAborted = false;
      const source = from('test');
      // Object = stateful transform (receives entire source as async iterable)
      const transform1: Transform = {
        async *transform(source, { signal }) {
          signal.addEventListener('abort', () => {
            signalAborted = true;
          }, { once: true });
          for await (const chunks of source) {
            if (chunks) yield chunks;
          }
        },
      };
      const transform2: Transform = () => {
        throw new Error('Transform error');
      };

      const output = pull(source, transform1, transform2);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Transform error/);

      assert.strictEqual(signalAborted, true);
    });
  });

  describe('transform signal', () => {
    it('should pass signal to stateless transforms', async () => {
      let receivedSignal: AbortSignal | undefined;
      const source = from('hello');
      const transform: Transform = (chunks, { signal }) => {
        receivedSignal = signal;
        return chunks;
      };

      const output = pull(source, transform);
      await collectBytes(output);

      assert.ok(receivedSignal, 'Transform should receive a signal');
      assert.ok(receivedSignal instanceof AbortSignal, 'Should be an AbortSignal');
    });

    it('should pass signal to stateful transforms', async () => {
      let receivedSignal: AbortSignal | undefined;
      const source = from('hello');
      const transform: Transform = {
        async *transform(source, { signal }) {
          receivedSignal = signal;
          for await (const chunks of source) {
            if (chunks) yield chunks;
          }
        },
      };

      const output = pull(source, transform);
      await collectBytes(output);

      assert.ok(receivedSignal, 'Transform should receive a signal');
      assert.ok(receivedSignal instanceof AbortSignal, 'Should be an AbortSignal');
    });

    it('should fire signal when user signal aborts', async () => {
      let transformSignalAborted = false;
      const controller = new AbortController();
      const source = from((async function* () {
        yield [new TextEncoder().encode('chunk1')];
        await new Promise(resolve => setTimeout(resolve, 50));
        yield [new TextEncoder().encode('chunk2')];
      })());

      const transform: Transform = (chunks, { signal }) => {
        signal.addEventListener('abort', () => {
          transformSignalAborted = true;
        }, { once: true });
        return chunks;
      };

      const output = pull(source, transform, { signal: controller.signal });

      // Abort after first chunk
      setTimeout(() => controller.abort(), 20);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Abort/);

      assert.strictEqual(transformSignalAborted, true);
    });

    it('should fire signal when consumer breaks', async () => {
      let transformSignalAborted = false;
      const source = from((async function* () {
        yield [new TextEncoder().encode('chunk1')];
        yield [new TextEncoder().encode('chunk2')];
        yield [new TextEncoder().encode('chunk3')];
      })());

      const transform: Transform = (chunks, { signal }) => {
        if (!transformSignalAborted) {
          signal.addEventListener('abort', () => {
            transformSignalAborted = true;
          }, { once: true });
        }
        return chunks;
      };

      const output = pull(source, transform);
      // Only consume one batch, then break
      for await (const _batch of output) {
        break;
      }

      assert.strictEqual(transformSignalAborted, true);
    });

    it('should fire signal when source throws', async () => {
      let transformSignalAborted = false;
      const source = from((async function* () {
        yield [new TextEncoder().encode('chunk1')];
        throw new Error('Source error');
      })());

      const transform: Transform = (chunks, { signal }) => {
        if (!transformSignalAborted) {
          signal.addEventListener('abort', () => {
            transformSignalAborted = true;
          }, { once: true });
        }
        return chunks;
      };

      const output = pull(source, transform);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Source error/);

      assert.strictEqual(transformSignalAborted, true);
    });

    it('should fire signal when a transform throws', async () => {
      let transform1SignalAborted = false;
      const source = from('test');

      const transform1: Transform = {
        async *transform(source, { signal }) {
          signal.addEventListener('abort', () => {
            transform1SignalAborted = true;
          }, { once: true });
          for await (const chunks of source) {
            if (chunks) yield chunks;
          }
        },
      };

      const transform2: Transform = () => {
        throw new Error('Transform 2 error');
      };

      const output = pull(source, transform1, transform2);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Transform 2 error/);

      assert.strictEqual(transform1SignalAborted, true);
    });

    it('should NOT fire signal on normal completion', async () => {
      let signalAborted = false;
      const source = from('hello');
      const transform: Transform = (chunks, { signal }) => {
        signal.addEventListener('abort', () => {
          signalAborted = true;
        }, { once: true });
        return chunks;
      };

      const output = pull(source, transform);
      await collectBytes(output);

      assert.strictEqual(signalAborted, false);
    });

    it('should allow transform to pass signal to sub-operation', async () => {
      let subOperationAborted = false;
      const controller = new AbortController();

      const source = from((async function* () {
        yield [new TextEncoder().encode('chunk1')];
        await new Promise(resolve => setTimeout(resolve, 50));
        yield [new TextEncoder().encode('chunk2')];
      })());

      const transform: Transform = async (chunks, { signal }) => {
        if (chunks === null) return null;
        // Simulate passing signal to a sub-operation
        await new Promise<void>((resolve, reject) => {
          if (signal.aborted) {
            subOperationAborted = true;
            reject(signal.reason);
            return;
          }
          signal.addEventListener('abort', () => {
            subOperationAborted = true;
            reject(signal.reason);
          }, { once: true });
          // Resolve after a short delay (simulating work)
          setTimeout(resolve, 5);
        });
        return chunks;
      };

      const output = pull(source, transform, { signal: controller.signal });
      setTimeout(() => controller.abort(), 20);

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Abort/);

      assert.strictEqual(subOperationAborted, true);
    });

    it('should throw immediately with pre-aborted signal without calling transforms', async () => {
      let transformCalled = false;
      const controller = new AbortController();
      controller.abort();

      const source = from('test');
      const transform: Transform = (chunks) => {
        transformCalled = true;
        return chunks;
      };

      const output = pull(source, transform, { signal: controller.signal });

      await assert.rejects(async () => {
        await collectBytes(output);
      }, /Abort/);

      assert.strictEqual(transformCalled, false);
    });

    it('should give each transform its own options object', async () => {
      const options1: object[] = [];
      const options2: object[] = [];
      const source = from('hello');

      const t1: Transform = (chunks, opts) => {
        options1.push(opts);
        return chunks;
      };
      const t2: Transform = (chunks, opts) => {
        options2.push(opts);
        return chunks;
      };

      const output = pull(source, t1, t2);
      await collectBytes(output);

      assert.ok(options1.length > 0, 'Transform 1 should have been called');
      assert.ok(options2.length > 0, 'Transform 2 should have been called');
      assert.notStrictEqual(options1[0], options2[0], 'Each transform should get its own options object');
    });
  });
});

describe('pipeToSync()', () => {
  describe('basic usage', () => {
    it('should write source to writer without transforms [WRITE-004]', () => {
      const source = fromSync('Hello, World!');
      const writer = createMockSyncWriter();
      const bytesWritten = pipeToSync(source, writer);

      assert.strictEqual(bytesWritten, 13);
      assert.strictEqual(writer.closed, true);
      assert.strictEqual(decode(concatBytes(writer.chunks)), 'Hello, World!');
    });

    it('should apply transforms before writing [WRITE-005]', () => {
      const source = fromSync('hello');
      const toUpper: SyncTransform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const writer = createMockSyncWriter();
      const bytesWritten = pipeToSync(source, toUpper, writer);

      assert.strictEqual(bytesWritten, 5);
      assert.strictEqual(decode(concatBytes(writer.chunks)), 'HELLO');
    });
  });

  describe('options', () => {
    it('should respect preventClose option [WRITE-010]', () => {
      const source = fromSync('test');
      const writer = createMockSyncWriter();
      pipeToSync(source, writer, { preventClose: true });

      assert.strictEqual(writer.closed, false);
    });

    it('should respect preventFail option [WRITE-011]', () => {
      const source = {
        *[Symbol.iterator]() {
          yield [new TextEncoder().encode('test')];
          throw new Error('Source error');
        },
      };
      const writer = createMockSyncWriter();

      assert.throws(() => {
        pipeToSync(source, writer, { preventFail: true });
      }, /Source error/);

      assert.strictEqual(writer.failed, undefined);
    });
  });

  describe('error handling', () => {
    it('should fail writer on source error [WRITE-020]', () => {
      const source = {
        *[Symbol.iterator]() {
          yield [new TextEncoder().encode('test')];
          throw new Error('Source error');
        },
      };
      const writer = createMockSyncWriter();

      assert.throws(() => {
        pipeToSync(source, writer);
      }, /Source error/);

      assert.ok(writer.failed);
    });

    it('should throw if no writer provided [WRITE-021]', () => {
      const source = fromSync('test');
      assert.throws(() => {
        pipeToSync(source);
      }, /pipeTo requires a writer argument/);
    });
  });
});

describe('pipeTo()', () => {
  describe('basic usage', () => {
    it('should write source to writer without transforms [WRITE-001]', async () => {
      const source = from('Hello, World!');
      const writer = createMockWriter();
      const bytesWritten = await pipeTo(source, writer);

      assert.strictEqual(bytesWritten, 13);
      assert.strictEqual(writer.closed, true);
    });

    it('should work with sync source [WRITE-002]', async () => {
      const source = fromSync('Sync Source');
      const writer = createMockWriter();
      const bytesWritten = await pipeTo(source, writer);

      assert.strictEqual(bytesWritten, 11);
    });

    it('should apply transforms before writing [WRITE-003]', async () => {
      const source = from('hello');
      const toUpper: Transform = (chunks) => {
        if (chunks === null) return null;
        return chunks.map((c) => new TextEncoder().encode(decode(c).toUpperCase()));
      };
      const writer = createMockWriter();
      const bytesWritten = await pipeTo(source, toUpper, writer);

      assert.strictEqual(bytesWritten, 5);
      assert.strictEqual(decode(concatBytes(writer.chunks)), 'HELLO');
    });
  });

  describe('options', () => {
    it('should respect preventClose option [WRITE-010]', async () => {
      const source = from('test');
      const writer = createMockWriter();
      await pipeTo(source, writer, { preventClose: true });

      assert.strictEqual(writer.closed, false);
    });

    it('should respect preventFail option [WRITE-011]', async () => {
      const source = from(
        (async function* () {
          yield [new TextEncoder().encode('test')];
          throw new Error('Source error');
        })()
      );
      const writer = createMockWriter();

      await assert.rejects(async () => {
        await pipeTo(source, writer, { preventFail: true });
      }, /Source error/);

      assert.strictEqual(writer.failed, undefined);
    });

    it('should respect AbortSignal [WRITE-012]', async () => {
      const controller = new AbortController();
      const source = from(
        (async function* () {
          yield [new TextEncoder().encode('chunk1')];
          await new Promise((resolve) => setTimeout(resolve, 100));
          yield [new TextEncoder().encode('chunk2')];
        })()
      );
      const writer = createMockWriter();

      // Abort after first chunk
      setTimeout(() => controller.abort(), 20);

      await assert.rejects(async () => {
        await pipeTo(source, writer, { signal: controller.signal });
      }, /Abort/);
    });
  });

  describe('error handling', () => {
    it('should fail writer on source error [WRITE-020]', async () => {
      const source = from(
        (async function* () {
          yield [new TextEncoder().encode('test')];
          throw new Error('Source error');
        })()
      );
      const writer = createMockWriter();

      await assert.rejects(async () => {
        await pipeTo(source, writer);
      }, /Source error/);

      assert.ok(writer.failed);
    });

    it('should throw if no writer provided [WRITE-021]', async () => {
      const source = from('test');
      await assert.rejects(async () => {
        await pipeTo(source);
      }, /pipeTo requires a writer argument/);
    });
  });

  describe('signal propagation to writes', () => {
    it('should pass signal to writer.write() and cancel blocked write [WRITE-025]', async () => {
      // Create a writer that blocks on write until signal fires
      let writeSignalReceived = false;
      const writerChunks: Uint8Array[] = [];
      const slowWriter: Writer = {
        get desiredSize() { return 10; },
        async write(chunk: Uint8Array | string, options?) {
          const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
          if (options?.signal) {
            // Simulate slow I/O that respects signal
            await new Promise<void>((resolve, reject) => {
              if (options.signal!.aborted) {
                writeSignalReceived = true;
                reject(options.signal!.reason);
                return;
              }
              const onAbort = () => {
                writeSignalReceived = true;
                reject(options.signal!.reason);
              };
              options.signal!.addEventListener('abort', onAbort, { once: true });
              // Simulate slow write that takes 200ms
              setTimeout(() => {
                options.signal!.removeEventListener('abort', onAbort);
                writerChunks.push(data);
                resolve();
              }, 200);
            });
          } else {
            writerChunks.push(data);
          }
        },
        async writev(chunks, options?) {
          for (const c of chunks) await this.write(c, options);
        },
        writeSync() { return false; },
        writevSync() { return false; },
        async end() { return writerChunks.reduce((a, c) => a + c.byteLength, 0); },
        endSync() { return -1; },
        async fail() {},
        failSync() { return true; },
      };

      const controller = new AbortController();
      const source = from((async function* () {
        yield [new TextEncoder().encode('chunk1')];
      })());

      // Abort after a short delay — the write will be in-flight
      setTimeout(() => controller.abort(), 30);

      await assert.rejects(async () => {
        await pipeTo(source, slowWriter, { signal: controller.signal });
      }, /Abort/);

      assert.strictEqual(writeSignalReceived, true);
    });

    it('should pass signal to writer.end() [WRITE-026]', async () => {
      let endSignalReceived = false;
      const writer: Writer = {
        get desiredSize() { return 10; },
        async write(chunk: Uint8Array | string) {},
        async writev(chunks: (Uint8Array | string)[]) {},
        writeSync() { return true; },
        writevSync() { return true; },
        async end(options?) {
          endSignalReceived = options?.signal !== undefined;
          return 0;
        },
        endSync() { return 0; },
        async fail() {},
        failSync() { return true; },
      };

      const source = from('test');
      await pipeTo(source, writer, { signal: new AbortController().signal });

      assert.strictEqual(endSignalReceived, true);
    });

    it('should not pass signal to writes when no user signal provided', async () => {
      let writeOptionsReceived: unknown = 'not-called';
      const writer: Writer = {
        get desiredSize() { return 10; },
        async write(chunk: Uint8Array | string, options?) {
          writeOptionsReceived = options;
        },
        async writev(chunks: (Uint8Array | string)[]) {},
        writeSync() { return true; },
        writevSync() { return true; },
        async end() { return 0; },
        endSync() { return 0; },
        async fail() {},
        failSync() { return true; },
      };

      const source = from('test');
      await pipeTo(source, writer);

      assert.strictEqual(writeOptionsReceived, undefined);
    });
  });

  describe('transform-writer', () => {
    it('should handle writer that is also a transform [WRITE-030]', async () => {
      const source = from('hello');
      const hashes: string[] = [];
      const writerChunks: Uint8Array[] = [];
      // TransformObject = stateful transform, receives entire source as async iterable
      const hashingWriter: Writer & { transform: (source: AsyncIterable<Uint8Array[] | null>) => AsyncGenerator<Uint8Array[]> } = {
        async *transform(source) {
          for await (const chunks of source) {
            if (chunks) {
              for (const c of chunks) {
                hashes.push(`hash:${c.byteLength}`);
              }
              yield chunks;
            }
          }
        },
        get desiredSize() {
          return 10;
        },
        async write(chunk: Uint8Array | string) {
          const data = typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
          writerChunks.push(data);
        },
        async writev(chunks: (Uint8Array | string)[]) {
          for (const chunk of chunks) {
            await this.write(chunk);
          }
        },
        writeSync() { return true; },
        writevSync() { return true; },
        async end() {
          return writerChunks.reduce((acc, c) => acc + c.byteLength, 0);
        },
        endSync() { return 0; },
        async fail() {},
        failSync() { return true; },
      };

      await pipeTo(source, hashingWriter);

      assert.ok(hashes.length > 0);
      assert.ok(hashes[0].startsWith('hash:'));
    });
  });
});

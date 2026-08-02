/**
 * Tests for Duplex Channel implementation
 *
 * Requirements covered: DUPLEX-001 through DUPLEX-012
 * @see https://github.com/WinterTC55/iter-streams/blob/main/docs/REQUIREMENTS.md Section 11 (Stream.duplex())
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { duplex } from './duplex.js';
import { bytes, text } from './consumers.js';

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

describe('duplex()', () => {
  describe('basic operation', () => {
    // DUPLEX-001: Returns tuple of two DuplexChannel instances
    it('should return a pair of connected channels [DUPLEX-001]', () => {
      const [channelA, channelB] = duplex();

      assert.ok(channelA);
      assert.ok(channelB);
      assert.ok(channelA.writer);
      assert.ok(channelA.readable);
      assert.ok(channelB.writer);
      assert.ok(channelB.readable);
      assert.strictEqual(typeof channelA.close, 'function');
      assert.strictEqual(typeof channelB.close, 'function');
    });

    // DUPLEX-002: Data written to A appears in B's readable
    it('should send data from A to B [DUPLEX-002]', async () => {
      const [channelA, channelB] = duplex();

      await channelA.writer.write('Hello from A');
      await channelA.close();

      const received = await text(channelB.readable);
      assert.strictEqual(received, 'Hello from A');
    });

    // DUPLEX-003: Data written to B appears in A's readable
    it('should send data from B to A [DUPLEX-003]', async () => {
      const [channelA, channelB] = duplex();

      await channelB.writer.write('Hello from B');
      await channelB.close();

      const received = await text(channelA.readable);
      assert.strictEqual(received, 'Hello from B');
    });

    // DUPLEX-004: Bidirectional communication works
    it('should support bidirectional communication [DUPLEX-004]', async () => {
      const [client, server] = duplex();

      // Server echoes back
      const serverTask = (async () => {
        for await (const chunks of server.readable) {
          await server.writer.writev(chunks);
        }
        await server.close();
      })();

      // Client sends and receives
      await client.writer.write('ping');
      await client.close();

      const response = await text(client.readable);
      assert.strictEqual(response, 'ping');

      await serverTask;
    });
  });

  describe('close behavior', () => {
    // DUPLEX-005: close() is idempotent
    it('should handle multiple close() calls [DUPLEX-005]', async () => {
      const [channelA, channelB] = duplex();

      await channelA.close();
      await channelA.close(); // Should not throw
      await channelA.close(); // Should not throw

      // B should see end of stream
      const chunks = await collect(channelB.readable);
      assert.strictEqual(chunks.length, 0);
    });

    // DUPLEX-006: Symbol.asyncDispose works
    it('should support Symbol.asyncDispose [DUPLEX-006]', async () => {
      const [channelA, channelB] = duplex();

      // Use async dispose
      {
        await channelA[Symbol.asyncDispose]();
      }

      // B should see end of stream
      const chunks = await collect(channelB.readable);
      assert.strictEqual(chunks.length, 0);
    });

    // DUPLEX-007: Closing one channel doesn't affect the other direction
    it('should allow independent closing [DUPLEX-007]', async () => {
      const [channelA, channelB] = duplex();

      // A sends data and closes
      await channelA.writer.write('from A');
      await channelA.close();

      // B can still send to A even after A closed its writer
      await channelB.writer.write('from B');
      await channelB.close();

      // Both should receive their respective data
      const receivedByB = await text(channelB.readable);
      const receivedByA = await text(channelA.readable);

      assert.strictEqual(receivedByB, 'from A');
      assert.strictEqual(receivedByA, 'from B');
    });
  });

  describe('options', () => {
    // DUPLEX-008: Respects highWaterMark option
    it('should respect highWaterMark option [DUPLEX-008]', async () => {
      const [channelA, channelB] = duplex({ highWaterMark: 2 });

      // Fill buffer
      assert.strictEqual(channelA.writer.writeSync('chunk1'), true);
      assert.strictEqual(channelA.writer.writeSync('chunk2'), true);
      // Buffer should be full
      assert.strictEqual(channelA.writer.desiredSize, 0);
      assert.strictEqual(channelA.writer.writeSync('chunk3'), false);

      await channelA.close();
    });

    // DUPLEX-009: Respects backpressure option
    it('should respect backpressure option [DUPLEX-009]', async () => {
      const [channelA, channelB] = duplex({
        highWaterMark: 1,
        backpressure: 'drop-newest',
      });

      // Write multiple - extras should be dropped
      channelA.writer.writeSync('first');
      channelA.writer.writeSync('second'); // Should be dropped
      channelA.writer.writeSync('third'); // Should be dropped

      await channelA.close();

      const received = await text(channelB.readable);
      assert.strictEqual(received, 'first');
    });

    // DUPLEX-010: Respects AbortSignal option
    it('should respect signal option [DUPLEX-010]', async () => {
      const controller = new AbortController();
      const [channelA, channelB] = duplex({ signal: controller.signal });

      channelA.writer.writeSync('hello');

      // Abort
      controller.abort();

      // Writers should be closed
      assert.strictEqual(channelA.writer.desiredSize, null);
      assert.strictEqual(channelB.writer.desiredSize, null);
    });

    // DUPLEX-013: Respects per-direction options
    it('should respect per-direction options [DUPLEX-013]', async () => {
      const [channelA, channelB] = duplex({
        highWaterMark: 1, // Default for both
        a: { highWaterMark: 3 }, // A→B direction has higher buffer
        b: { highWaterMark: 2 }, // B→A direction
      });

      // A→B direction (channelA.writer) should have highWaterMark of 3
      assert.strictEqual(channelA.writer.writeSync('1'), true);
      assert.strictEqual(channelA.writer.writeSync('2'), true);
      assert.strictEqual(channelA.writer.writeSync('3'), true);
      assert.strictEqual(channelA.writer.desiredSize, 0);
      assert.strictEqual(channelA.writer.writeSync('4'), false); // Full

      // B→A direction (channelB.writer) should have highWaterMark of 2
      assert.strictEqual(channelB.writer.writeSync('1'), true);
      assert.strictEqual(channelB.writer.writeSync('2'), true);
      assert.strictEqual(channelB.writer.desiredSize, 0);
      assert.strictEqual(channelB.writer.writeSync('3'), false); // Full

      await channelA.close();
      await channelB.close();
    });

    // DUPLEX-014: Per-direction options override shared options
    it('should allow per-direction backpressure policies [DUPLEX-014]', async () => {
      const [channelA, channelB] = duplex({
        highWaterMark: 1,
        backpressure: 'strict', // Default
        a: { backpressure: 'drop-newest' }, // A→B drops new data
      });

      // A→B uses drop-newest
      channelA.writer.writeSync('first');
      const result = channelA.writer.writeSync('second'); // Should succeed but drop
      assert.strictEqual(result, true);

      await channelA.close();

      // Only first chunk should be received
      const received = await text(channelB.readable);
      assert.strictEqual(received, 'first');
    });
  });

  describe('real-world patterns', () => {
    // DUPLEX-011: Request-response pattern
    it('should support request-response pattern [DUPLEX-011]', async () => {
      const [client, server] = duplex();

      // Simple request-response server
      const serverTask = (async () => {
        for await (const chunks of server.readable) {
          const request = decode(chunks[0]);
          if (request === 'GET /hello') {
            await server.writer.write('HTTP/1.1 200 OK\r\n\r\nHello!');
          } else {
            await server.writer.write('HTTP/1.1 404 Not Found\r\n\r\n');
          }
        }
        await server.close();
      })();

      // Client makes request
      await client.writer.write('GET /hello');
      await client.close();

      const response = await text(client.readable);
      assert.ok(response.includes('200 OK'));
      assert.ok(response.includes('Hello!'));

      await serverTask;
    });

    // DUPLEX-012: Multiple message exchange
    it('should support multiple message exchanges [DUPLEX-012]', async () => {
      const [channelA, channelB] = duplex({ highWaterMark: 10 });

      const messagesFromA: string[] = [];
      const messagesFromB: string[] = [];

      // A sends messages
      const taskA = (async () => {
        await channelA.writer.write('A1');
        await channelA.writer.write('A2');
        await channelA.writer.write('A3');
        await channelA.close();

        for await (const chunks of channelA.readable) {
          for (const chunk of chunks) {
            messagesFromB.push(decode(chunk));
          }
        }
      })();

      // B sends messages
      const taskB = (async () => {
        await channelB.writer.write('B1');
        await channelB.writer.write('B2');
        await channelB.close();

        for await (const chunks of channelB.readable) {
          for (const chunk of chunks) {
            messagesFromA.push(decode(chunk));
          }
        }
      })();

      await Promise.all([taskA, taskB]);

      assert.deepStrictEqual(messagesFromA, ['A1', 'A2', 'A3']);
      assert.deepStrictEqual(messagesFromB, ['B1', 'B2']);
    });
  });
});

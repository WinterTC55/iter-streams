/**
 * Resource Management and Cleanup
 *
 * This file demonstrates resource management patterns with the new streams API.
 *
 * Key concepts:
 * - Breaking from iteration triggers generator cleanup (finally blocks)
 * - AbortSignal can cancel long-running operations
 * - Broadcast and Share have .cancel() and Symbol.dispose
 * - Writers have explicit end()/fail() for cleanup
 */

import { Stream, type Writer } from '../src/index.js';
import { section, MemoryWriter } from './util.js';

// Shared decoder instance for efficiency
const decoder = new TextDecoder();

async function main() {
  // ============================================================================
  // Generator cleanup on break
  // ============================================================================
  section('Generator cleanup on break');

  {
    let cleanedUp = false;

    async function* source() {
      try {
        for (let i = 0; i < 100; i++) {
          yield `chunk${i}`;
          await new Promise((r) => setTimeout(r, 10));
        }
      } finally {
        cleanedUp = true;
        console.log('  Generator finally block executed!');
      }
    }

    const readable = Stream.from(source());

    let count = 0;
    for await (const batches of readable) {
      for (const _ of batches) {
        count++;
        if (count >= 3) break;
      }
      if (count >= 3) break; // Break from outer loop too
    }

    // Small delay to ensure cleanup runs
    await new Promise((r) => setTimeout(r, 50));

    console.log('Chunks read:', count);
    console.log('Generator cleaned up:', cleanedUp);
  }

  // ============================================================================
  // Writer cleanup - end() vs fail()
  // ============================================================================
  section('Writer cleanup - end() vs fail()');

  // Normal completion with end()
  {
    const { writer, readable } = Stream.push();

    // Start consumer in background
    const textPromise = Stream.text(readable);

    await writer.write('normal');
    await writer.write(' completion');
    const bytesWritten = await writer.end();

    const text = await textPromise;
    console.log('Normal end() - bytes:', bytesWritten, 'text:', text);
  }

  // Abnormal termination with fail()
  {
    const { writer, readable } = Stream.push();

    // Start consumer in background
    const textPromise = Stream.text(readable).catch((e) => `ERROR: ${(e as Error).message}`);

    await writer.write('partial data');
    await writer.fail(new Error('Something went wrong'));

    const result = await textPromise;
    console.log('After fail():', result);
  }

  // Sync writer cleanup
  {
    const writer = new MemoryWriter();

    writer.writeSync('sync');
    writer.writeSync(' data');
    const bytesWritten = writer.endSync();

    console.log('Sync writer - bytes:', bytesWritten, 'text:', writer.getText());
  }

  // ============================================================================
  // Broadcast with Symbol.dispose
  // ============================================================================
  section('Broadcast with Symbol.dispose');

  {
    const { writer, channel } = Stream.broadcast();

    // Check for Symbol.dispose
    console.log('Broadcast has Symbol.dispose:', Symbol.dispose in channel);

    // Create consumers
    const consumer1 = channel.push();
    const consumer2 = channel.push();

    console.log('Consumer count:', channel.consumerCount);

    // Start consuming in background
    const promise1 = Stream.text(consumer1).catch((e) => `Consumer1 error: ${(e as Error).message}`);
    const promise2 = Stream.text(consumer2).catch((e) => `Consumer2 error: ${(e as Error).message}`);

    // Write some data
    await writer.write('broadcast data');

    // Cancel all consumers using Symbol.dispose (same as .cancel())
    channel[Symbol.dispose]();

    // Or equivalently: channel.cancel();

    const [result1, result2] = await Promise.all([promise1, promise2]);
    console.log('Results after dispose:', { result1: result1.slice(0, 30), result2: result2.slice(0, 30) });
  }

  // Broadcast with `using` keyword (explicit resource management)
  {
    console.log('\n--- Broadcast with using ---');

    let consumerCleanedUp = false;

    {
      const { writer, channel } = Stream.broadcast();
      using _ = channel; // Will call Symbol.dispose on scope exit

      const consumer = channel.push();

      // Start consuming
      const textPromise = Stream.text(consumer).catch(() => {
        consumerCleanedUp = true;
        return 'cleaned up';
      });

      await writer.write('data in using block');

      console.log('  Leaving using block...');
      // channel[Symbol.dispose]() called automatically here
    }

    await new Promise((r) => setTimeout(r, 50));
    console.log('Consumer cleaned up:', consumerCleanedUp);
  }

  // ============================================================================
  // Share with Symbol.dispose
  // ============================================================================
  section('Share with Symbol.dispose');

  {
    async function* source() {
      for (let i = 0; i < 10; i++) {
        yield `item${i}`;
        await new Promise((r) => setTimeout(r, 20));
      }
    }

    const shared = Stream.share(Stream.from(source()));

    console.log('Share has Symbol.dispose:', Symbol.dispose in shared);

    // Create consumers
    const consumer1 = shared.pull();
    const consumer2 = shared.pull();

    console.log('Consumer count:', shared.consumerCount);

    // Read a few items
    let count = 0;
    for await (const batches of consumer1) {
      for (const _ of batches) {
        count++;
        if (count >= 2) break;
      }
      if (count >= 2) break;
    }

    console.log('Read', count, 'items from consumer1');

    // Cancel the shared source (affects remaining consumers)
    shared.cancel();

    console.log('Share cancelled');
  }

  // ============================================================================
  // Pattern: try/finally for cleanup
  // ============================================================================
  section('Pattern: try/finally for cleanup');

  {
    const { writer, readable } = Stream.push();

    // Consumer in background
    const textPromise = Stream.text(readable).catch(() => 'failed');

    try {
      await writer.write('data 1');
      await writer.write('data 2');

      // Simulate error
      throw new Error('Processing error');
    } catch (e) {
      console.log('Caught error:', (e as Error).message);
      // Fail writer on error
      await writer.fail(e as Error);
    } finally {
      // Cleanup always runs
      console.log('Finally block executed');
    }

    const result = await textPromise;
    console.log('Result:', result);
  }

  // ============================================================================
  // Pattern: AbortController for timeout-based cleanup
  // ============================================================================
  section('Pattern: AbortController for cleanup');

  {
    async function* slowSource() {
      for (let i = 0; i < 100; i++) {
        yield `chunk${i}`;
        await new Promise((r) => setTimeout(r, 30));
      }
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 100);

    try {
      const data = await Stream.bytes(Stream.from(slowSource()), {
        signal: controller.signal,
      });
      console.log('Got data:', data.byteLength, 'bytes');
    } catch (e) {
      console.log('Timed out:', (e as Error).name);
    } finally {
      clearTimeout(timeoutId); // Clean up timer
      console.log('Timer cleaned up');
    }
  }

  // ============================================================================
  // Pattern: Multiple resources cleanup
  // ============================================================================
  section('Pattern: Multiple resources cleanup');

  {
    const cleanup: (() => void)[] = [];

    async function* trackedSource(name: string) {
      let cleaned = false;
      cleanup.push(() => {
        if (!cleaned) console.log(`  WARNING: ${name} not cleaned up!`);
      });

      try {
        for (let i = 0; i < 5; i++) {
          yield `${name}-chunk${i}`;
        }
      } finally {
        cleaned = true;
        console.log(`  ${name} cleaned up`);
      }
    }

    // Process multiple sources
    const sources = ['source1', 'source2', 'source3'];

    for (const name of sources) {
      const readable = Stream.from(trackedSource(name));

      // Read only first chunk
      for await (const batches of readable) {
        for (const chunk of batches) {
          console.log('  Read:', decoder.decode(chunk));
          break;
        }
        break;
      }
    }

    // Small delay for cleanup
    await new Promise((r) => setTimeout(r, 50));

    // Verify all cleaned up
    console.log('Running cleanup checks...');
    cleanup.forEach((check) => check());
    console.log('All resources cleaned up!');
  }

  // ============================================================================
  // Pattern: Wrapping external resources
  // ============================================================================
  section('Pattern: Wrapping external resources');

  {
    // Simulate an external resource (e.g., file handle, database connection)
    class FakeFileHandle {
      private _closed = false;
      private _data = ['line1\n', 'line2\n', 'line3\n'];
      private _position = 0;

      read(): string | null {
        if (this._closed || this._position >= this._data.length) return null;
        return this._data[this._position++];
      }

      close(): void {
        if (!this._closed) {
          this._closed = true;
          console.log('  File handle closed');
        }
      }

      get closed(): boolean {
        return this._closed;
      }
    }

    // Wrap external resource in a generator with cleanup
    async function* fileStream(handle: FakeFileHandle) {
      try {
        let line: string | null;
        while ((line = handle.read()) !== null) {
          yield line;
        }
      } finally {
        handle.close();
      }
    }

    // Use the wrapped resource
    const handle = new FakeFileHandle();
    const readable = Stream.from(fileStream(handle));

    // Read only first line, then stop
    for await (const batches of readable) {
      for (const chunk of batches) {
        console.log('  Read:', decoder.decode(chunk).trim());
        break;
      }
      break;
    }

    // Small delay for cleanup
    await new Promise((r) => setTimeout(r, 50));

    console.log('Handle closed:', handle.closed);
  }

  // ============================================================================
  // Pattern: Cleanup with pipeTo
  // ============================================================================
  section('Pattern: Cleanup with pipeTo');

  {
    let sourceCleanedUp = false;

    async function* source() {
      try {
        for (let i = 0; i < 5; i++) {
          yield `chunk${i}`;
        }
      } finally {
        sourceCleanedUp = true;
        console.log('  Source cleaned up');
      }
    }

    const writer = new MemoryWriter();

    // pipeTo consumes the entire source and cleans up
    const bytesWritten = await Stream.pipeTo(Stream.from(source()), writer);

    console.log('Bytes written:', bytesWritten);
    console.log('Source cleaned up:', sourceCleanedUp);
    console.log('Writer content:', writer.getText());
  }

  // pipeTo with error in source
  {
    console.log('\n--- pipeTo with source error ---');

    async function* errorSource() {
      yield 'chunk1';
      throw new Error('Source error!');
    }

    const writer = new MemoryWriter();

    try {
      await Stream.pipeTo(Stream.from(errorSource()), writer);
    } catch (e) {
      console.log('Caught:', (e as Error).message);
    }

    // Writer should be failed, not closed normally
    console.log('Writer state - desiredSize:', writer.desiredSize);
  }

  console.log('\n--- Examples complete! ---\n');
}

main().catch(console.error);

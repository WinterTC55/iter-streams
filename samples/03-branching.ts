/**
 * Multi-Consumer Streaming (Branching)
 *
 * This file demonstrates broadcast() and share() for multi-consumer patterns.
 * The new API uses explicit multi-consumer primitives instead of implicit fork().
 */

import { Stream, Share, ShareInterface, SyncShare, Broadcast } from '../src/index.js';
import { section, uppercaseTransform } from './util.js';

// Shared decoder instance for efficiency
const decoder = new TextDecoder();

async function main() {
  // ============================================================================
  // Stream.broadcast() - Push-model multi-consumer
  // ============================================================================
  section('Stream.broadcast() - Push-model branching');

  // Create a broadcast - writer pushes to all consumers
  {
    const { writer, channel } = Stream.broadcast({ highWaterMark: 100 });

    // Create multiple consumers
    const consumer1 = channel.push();
    const consumer2 = channel.push();

    // Write data - both consumers will see it
    (async () => {
      await writer.write('shared data');
      await writer.end();
    })();

    // Both consumers see the same content
    const [result1, result2] = await Promise.all([
      Stream.text(consumer1),
      Stream.text(consumer2)
    ]);

    console.log('Consumer 1:', result1);
    console.log('Consumer 2:', result2);
  }

  // Multiple consumers with different transforms
  {
    const { writer, channel } = Stream.broadcast({ highWaterMark: 100 });

    // Consumer without transform
    const raw = channel.push();

    // Consumer with transform (transforms are applied lazily when consumer pulls)
    const transformed = channel.push(uppercaseTransform());

    (async () => {
      await writer.write('Hello World');
      await writer.end();
    })();

    const [rawResult, transformedResult] = await Promise.all([
      Stream.text(raw),
      Stream.text(transformed)
    ]);

    console.log('\nRaw consumer:', rawResult);
    console.log('Transformed:', transformedResult);
  }

  // ============================================================================
  // Stream.share() - Pull-model multi-consumer
  // ============================================================================
  section('Stream.share() - Pull-model branching');

  // Share a source among multiple consumers
  {
    // Create a source that we want to share
    async function* dataSource() {
      yield 'chunk 1';
      yield 'chunk 2';
      yield 'chunk 3';
    }

    const shared = Stream.share(Stream.from(dataSource()), { highWaterMark: 100 });

    // Create multiple consumers
    const consumer1 = shared.pull();
    const consumer2 = shared.pull();

    // Both consumers pull from the same shared source
    const [result1, result2] = await Promise.all([
      Stream.text(consumer1),
      Stream.text(consumer2)
    ]);

    console.log('Consumer 1:', result1);
    console.log('Consumer 2:', result2);
    console.log('Consumer count:', shared.consumerCount);
  }

  // ============================================================================
  // Stream.shareSync() - Sync pull-model multi-consumer
  // ============================================================================
  section('Stream.shareSync() - Sync pull-model branching');

  // Share a sync source among multiple sync consumers
  {
    function* syncDataSource() {
      yield 'sync chunk 1';
      yield 'sync chunk 2';
    }

    const shared = Stream.shareSync(Stream.fromSync(syncDataSource()), { highWaterMark: 100 });

    // Both consumers pull from the same shared source (sync)
    const result1 = Stream.textSync(shared.pull());
    const result2 = Stream.textSync(shared.pull());

    console.log('Sync Consumer 1:', result1);
    console.log('Sync Consumer 2:', result2);
  }

  // ============================================================================
  // Share.from() and Broadcast.from() - Create from Streamable
  // ============================================================================
  section('Share.from() / Broadcast.from() - Convenience factories');

  // Share.from() creates a Share from any Streamable
  {
    async function* dataSource() {
      yield 'data from generator';
    }

    // Share.from() wraps a Streamable in a Share
    const shared = Share.from(Stream.from(dataSource()));

    const result = await Stream.text(shared.pull());
    console.log('Share.from() result:', result);
  }

  // SyncShare.fromSync() creates a SyncShare from any SyncStreamable
  {
    function* syncSource() {
      yield 'sync data';
    }

    const shared = SyncShare.fromSync(Stream.fromSync(syncSource()));

    const result = Stream.textSync(shared.pull());
    console.log('SyncShare.fromSync() result:', result);
  }

  // Broadcast.from() creates a Broadcast that pumps from a Streamable
  {
    async function* dataSource() {
      yield 'broadcast data 1';
      yield 'broadcast data 2';
    }

    const { channel } = Broadcast.from(Stream.from(dataSource()));

    const consumer1 = channel.push();
    const consumer2 = channel.push();

    await new Promise(resolve => setTimeout(resolve, 50));
    const results = await Promise.all([
      Stream.text(consumer1),
      Stream.text(consumer2)
    ]);
    console.log('Broadcast.from() results:', results);
  }

  // ============================================================================
  // Shareable Protocol - Custom optimized Share implementations
  // ============================================================================
  section('Shareable Protocol - Custom multi-consumer objects');

  // Objects can implement shareProtocol to provide optimized Share behavior
  {
    // Example: A shareable data source that tracks access
    class TrackingShareable {
      private accessCount = 0;

      constructor(private data: string) {}

      // Implement the share protocol.
      // Return any object that implements ShareInterface.
      [Stream.shareProtocol]() : ShareInterface {
        this.accessCount++;
        console.log(`  Share created (access #${this.accessCount})`);

        // Return a Share for the data
        return Stream.share(Stream.from(this.data));
      }

      getAccessCount() {
        return this.accessCount;
      }
    }

    const trackable = new TrackingShareable('shared content');

    // Share.from() detects the protocol and calls it
    const shared1 = Share.from(trackable);
    const shared2 = Share.from(trackable);

    console.log('  Total accesses:', trackable.getAccessCount());

    const results = await Promise.all([
      Stream.text(shared1.pull()),
      Stream.text(shared2.pull())
    ]);
    console.log('  Results match:', results[0] === results[1]);
  }

  // Objects can also implement shareSyncProtocol for sync sharing
  {
    class SyncShareable {
      [Stream.shareSyncProtocol]() {
        return Stream.shareSync(Stream.fromSync(['sync', ' ', 'shareable']));
      }
    }

    const obj = new SyncShareable();
    const shared = SyncShare.fromSync(obj);
    const result = Stream.textSync(shared.pull());
    console.log('SyncShareable result:', result);
  }

  // Note: The Broadcastable protocol (Stream.broadcastProtocol) allows objects to provide
  // optimized Broadcast implementations. This is an advanced pattern typically used for:
  // - Native bindings that share underlying resources
  // - Custom data structures with efficient multi-consumer support
  // - Platform-specific optimizations (e.g., shared memory, memory-mapped files)
  //
  // For most use cases, Broadcast.from(streamable) is sufficient.

  // ============================================================================
  // Backpressure policies
  // ============================================================================
  section('Backpressure policies');

  // Broadcast with 'strict' - uses writeSync to check buffer space
  {
    const { writer, channel } = Stream.broadcast({
      highWaterMark: 3,
      backpressure: 'strict'
    });

    const consumer = channel.push();

    console.log("'strict' policy with writeSync:");

    // Check desiredSize before writing
    for (let i = 0; i < 3; i++) {
      if (writer.desiredSize && writer.desiredSize > 0) {
        writer.writeSync(`chunk${i}`);
        console.log(`  Wrote chunk${i}, desiredSize: ${writer.desiredSize}`);
      }
    }
    await writer.end();

    const result = await Stream.text(consumer);
    console.log('  Consumer got:', result);
  }

  // Share with 'drop-oldest' - drops old chunks when buffer full
  {
    console.log("\n'drop-oldest' policy - old chunks may be lost");

    const shared = Stream.share(
      Stream.from(['a', 'b', 'c', 'd', 'e']),
      { highWaterMark: 2, backpressure: 'drop-oldest' }
    );

    // Fast consumer reads first
    const fastResult = await Stream.text(shared.pull());

    // Then slow consumer reads (may have missed some)
    // (This one is "slow" because we wait before pulling
    // until after the fast consumer is done)
    const slowResult = await Stream.text(shared.pull());

    console.log('  Fast consumer got:', fastResult.length, 'chars');
    console.log('  Slow consumer got:', slowResult.length, 'chars');
  }

  // ============================================================================
  // Using tap() for observation without branching
  // ============================================================================
  section('Stream.tap() - Observation without branching');

  // Tap lets you observe chunks without creating a separate consumer
  {
    const observed: string[] = [];

    const readable = Stream.pull(
      Stream.from(['hello', ' ', 'world']),
      Stream.tap((chunks) => {
        if (chunks) {
          for (const chunk of chunks) {
            observed.push(decoder.decode(chunk));
          }
        }
      })
    );

    // Consume the stream
    const result = await Stream.text(readable);

    console.log('Result:', result);
    console.log('Observed chunks:', observed);
  }

  // Hash while streaming
  {
    let totalBytes = 0;

    const readable = Stream.pull(
      Stream.from('Some data to measure'),
      Stream.tap((chunks) => {
        if (chunks) {
          for (const chunk of chunks) {
            totalBytes += chunk.byteLength;
          }
        }
      })
    );

    await Stream.bytes(readable);
    console.log('\nTotal bytes observed:', totalBytes);
  }

  console.log('\n--- Examples complete! ---\n');
}

main().catch(console.error);

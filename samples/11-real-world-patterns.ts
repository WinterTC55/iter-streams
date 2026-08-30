/**
 * Real-World Patterns
 *
 * This file demonstrates practical, real-world usage patterns for the new streams API.
 */

import { Stream, type Transform } from '../src/index.js';
import { section, uppercaseTransform, MemoryWriter } from './util.js';

// Shared encoder/decoder instances for efficiency
const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function main() {
  // ============================================================================
  // Pattern: Streaming JSON Lines (NDJSON)
  // ============================================================================
  section('Pattern: Streaming JSON Lines');

  {
    const jsonlData = `{"event":"start","ts":1}
{"event":"data","value":42}
{"event":"data","value":99}
{"event":"end","ts":2}`;

    // Stateful JSON Lines parser transform
    const createJsonlParser = (): Transform => {
      let buffer = '';

      return {

        transform: async function* (source: AsyncIterable<Uint8Array[] | null>) {
          for await (const batch of source) {
            if (batch === null) {
              // Flush remaining buffer
              if (buffer.trim()) {
                const obj = JSON.parse(buffer);
                obj.processed = true;
                yield encoder.encode(JSON.stringify(obj) + '\n');
              }
              continue;
            }

            for (const chunk of batch) {
              buffer += decoder.decode(chunk);
              const lines = buffer.split('\n');
              buffer = lines.pop() || '';

              for (const line of lines) {
                if (line.trim()) {
                  const obj = JSON.parse(line);
                  obj.processed = true;
                  yield encoder.encode(JSON.stringify(obj) + '\n');
                }
              }
            }
          }
        },
      };
    };

    const processed = Stream.pull(Stream.from(jsonlData), createJsonlParser());

    console.log('Processed JSON Lines:');
    for await (const batches of processed) {
      for (const chunk of batches) {
        const text = decoder.decode(chunk).trim();
        if (text) console.log('  ' + text);
      }
    }
  }

  // ============================================================================
  // Pattern: CSV Processing Pipeline
  // ============================================================================
  section('Pattern: CSV Processing');

  {
    const csvData = `name,age,city
Alice,30,NYC
Bob,25,LA
Charlie,35,Chicago`;

    // Stateful CSV parser
    const createCsvParser = (): Transform => {
      let lineBuffer = '';
      let headers: string[] = [];

      return {

        transform: async function* (source: AsyncIterable<Uint8Array[] | null>) {
          for await (const batch of source) {
            if (batch === null) {
              // Flush remaining buffer
              if (lineBuffer.trim()) {
                const values = lineBuffer.trim().split(',');
                const obj: Record<string, string> = {};
                headers.forEach((h, i) => (obj[h] = values[i]));
                yield encoder.encode(JSON.stringify(obj) + '\n');
              }
              continue;
            }

            for (const chunk of batch) {
              lineBuffer += decoder.decode(chunk);
              const lines = lineBuffer.split('\n');
              lineBuffer = lines.pop() || '';

              for (const line of lines) {
                if (!line.trim()) continue;
                const values = line.split(',');

                if (headers.length === 0) {
                  headers = values;
                  continue; // Skip header row
                }

                const obj: Record<string, string> = {};
                headers.forEach((h, i) => (obj[h] = values[i]));
                yield encoder.encode(JSON.stringify(obj) + '\n');
              }
            }
          }
        },
      };
    };

    const records = Stream.pull(Stream.from(csvData), createCsvParser());

    console.log('Parsed CSV records:');
    for await (const batches of records) {
      for (const chunk of batches) {
        const text = decoder.decode(chunk).trim();
        if (text) console.log('  ' + text);
      }
    }
  }

  // ============================================================================
  // Pattern: Rate-Limited Processing
  // ============================================================================
  section('Pattern: Rate-Limited Processing');

  {
    // Create a slow generator
    async function* slowSource() {
      for (let i = 0; i < 10; i++) {
        yield `item${i}`;
      }
    }

    const { writer, readable } = Stream.push({ highWaterMark: 2 });

    // Background producer
    const producerPromise = (async () => {
      for (let i = 0; i < 10; i++) {
        const success = writer.writeSync(`item${i}`);
        if (!success) {
          await writer.write(`item${i}`);
        }
      }
      await writer.end();
    })();

    // Slow consumer with rate limiting
    console.log('Processing with rate limiting:');
    let count = 0;
    for await (const batches of readable) {
      for (const chunk of batches) {
        const text = decoder.decode(chunk);
        console.log(`  Processed: ${text}`);
        await new Promise((r) => setTimeout(r, 20)); // Slow processing
        if (++count >= 5) break;
      }
      if (count >= 5) break;
    }

    console.log('  (Stopped after 5 items)');
    await producerPromise.catch(() => {}); // May error due to early stop
  }

  // ============================================================================
  // Pattern: Tee for Logging and Processing with broadcast()
  // ============================================================================
  section('Pattern: Tee for Logging');

  {
    // Use broadcast for multi-consumer pattern
    const { writer, channel } = Stream.broadcast();

    // Create branches
    const logBranch = channel.push();
    const processBranch = channel.push();

    // Log in background
    const logPromise = (async () => {
      const data = await Stream.text(logBranch);
      console.log(`  [AUDIT] Data received: "${data.substring(0, 20)}..."`);
    })();

    // Process in background
    const processPromise = (async () => {
      const transformed = Stream.pull(processBranch, uppercaseTransform());
      const result = await Stream.text(transformed);
      console.log(`  [MAIN] Processed: "${result.substring(0, 20)}..."`);
    })();

    // Write data
    await writer.write('important data to process and log');
    await writer.end();

    await Promise.all([logPromise, processPromise]);
  }

  // ============================================================================
  // Pattern: Progress Tracking with tap()
  // ============================================================================
  section('Pattern: Progress Tracking');

  {
    async function* largeSource() {
      for (let i = 0; i < 10; i++) {
        await new Promise((r) => setTimeout(r, 30));
        yield `chunk${i.toString().padStart(2, '0')} `; // ~9 bytes each
      }
    }

    let bytesRead = 0;
    const totalExpected = 90;

    // Progress tracking with tap()
    const withProgress = Stream.pull(
      Stream.from(largeSource()),
      Stream.tap((chunks) => {
        if (chunks === null) {
          console.log('  Progress: 100% complete');
          return;
        }
        for (const chunk of chunks) {
          bytesRead += chunk.length;
        }
        const percent = Math.round((bytesRead / totalExpected) * 100);
        console.log(`  Progress: ${percent}% (${bytesRead}/${totalExpected} bytes)`);
      })
    );

    await Stream.bytes(withProgress);
  }

  // ============================================================================
  // Pattern: Chunked Upload Simulation
  // ============================================================================
  section('Pattern: Chunked Upload');

  {
    const data = 'This is a larger piece of data that we want to upload in chunks for reliability.';

    // Create chunks of ~20 bytes
    const chunks: string[] = [];
    for (let i = 0; i < data.length; i += 20) {
      chunks.push(data.substring(i, i + 20));
    }

    console.log(`  Uploading ${data.length} bytes in ${chunks.length} chunks...`);

    // Simulate upload with retries
    let uploadedChunks = 0;

    for (const chunk of chunks) {
      const readable = Stream.from(chunk);

      let retries = 0;
      while (retries < 3) {
        try {
          const bytes = await Stream.bytes(readable);
          await new Promise((r) => setTimeout(r, 20));

          // Random failure for demo
          if (Math.random() < 0.3 && retries === 0) {
            throw new Error('Network error');
          }

          uploadedChunks++;
          console.log(`  Chunk ${uploadedChunks}/${chunks.length} uploaded (${bytes.length} bytes)`);
          break;
        } catch (e) {
          retries++;
          console.log(`  Chunk ${uploadedChunks + 1} failed, retry ${retries}...`);
        }
      }
    }

    console.log(`  Upload complete: ${uploadedChunks} chunks`);
  }

  // ============================================================================
  // Pattern: Server-Sent Events (SSE) Parser
  // ============================================================================
  section('Pattern: SSE Parser');

  {
    const sseData = `event: message
data: {"type":"greeting","text":"Hello"}

event: update
data: {"count":1}

event: update
data: {"count":2}

event: done
data: {"status":"complete"}

`;

    // Stateful SSE parser
    const createSseParser = (): Transform => {
      let buffer = '';
      let currentEvent: { event?: string; data?: string } = {};

      return {

        transform: async function* (source: AsyncIterable<Uint8Array[] | null>) {
          for await (const batch of source) {
            if (batch !== null) {
              for (const chunk of batch) {
                buffer += decoder.decode(chunk);
              }
            }

            const lines = buffer.split('\n');
            buffer = batch === null ? '' : lines.pop() || '';

            for (const line of lines) {
              if (line.startsWith('event: ')) {
                currentEvent.event = line.substring(7);
              } else if (line.startsWith('data: ')) {
                currentEvent.data = line.substring(6);
              } else if (line === '' && currentEvent.data) {
                // End of event
                yield encoder.encode(
                  JSON.stringify({
                    event: currentEvent.event || 'message',
                    data: JSON.parse(currentEvent.data),
                  }) + '\n'
                );
                currentEvent = {};
              }
            }
          }
        },
      };
    };

    console.log('Parsed SSE events:');
    const events = Stream.pull(Stream.from(sseData), createSseParser());

    for await (const batches of events) {
      for (const chunk of batches) {
        const text = decoder.decode(chunk).trim();
        if (text) {
          const parsed = JSON.parse(text);
          console.log(`  [${parsed.event}] ${JSON.stringify(parsed.data)}`);
        }
      }
    }
  }

  // ============================================================================
  // Pattern: Streaming Hash Computation with share()
  // ============================================================================
  section('Pattern: Streaming Hash');

  {
    // Use share() for multi-consumer from single source
    const shared = Stream.share(Stream.from('Data to compute a running checksum on'));

    // Create two consumers
    const hashConsumer = shared.pull();
    const mainConsumer = shared.pull();

    // Compute hash in background
    let checksum = 0;
    const hashPromise = (async () => {
      for await (const batches of hashConsumer) {
        for (const chunk of batches) {
          for (const byte of chunk) {
            checksum = (checksum + byte) % 256;
          }
        }
      }
      return checksum;
    })();

    // Main processing
    const result = await Stream.text(mainConsumer);
    const hash = await hashPromise;

    console.log(`  Data: "${result}"`);
    console.log(`  Checksum: ${hash}`);
  }

  // ============================================================================
  // Pattern: Conditional Pipeline
  // ============================================================================
  section('Pattern: Conditional Processing');

  {
    const data = 'COMPRESS:This data should be "compressed"';
    const prefix = data.substring(0, 8);
    const payload = data.substring(9);

    console.log(`  Header: "${prefix}"`);

    // Conditional processing based on header
    let result: string;
    if (prefix === 'COMPRESS') {
      result = await Stream.text(Stream.pull(Stream.from(payload), uppercaseTransform()));
      console.log(`  Applied compression transform`);
    } else {
      result = payload;
      console.log(`  No transform applied`);
    }

    console.log(`  Result: "${result}"`);
  }

  // ============================================================================
  // Pattern: Graceful Shutdown
  // ============================================================================
  section('Pattern: Graceful Shutdown');

  {
    const controller = new AbortController();

    // Long-running stream processing
    const processor = async () => {
      let cleanedUp = false;

      async function* source() {
        try {
          let i = 0;
          while (true) {
            await new Promise((r) => setTimeout(r, 30));
            yield `tick${i++}`;
          }
        } finally {
          cleanedUp = true;
          console.log('  Stream cleanup completed');
        }
      }

      const readable = Stream.from(source());

      try {
        for await (const batches of readable) {
          if (controller.signal.aborted) {
            console.log('  Received shutdown signal, stopping...');
            break;
          }
          for (const chunk of batches) {
            console.log(`  Processing: ${decoder.decode(chunk)}`);
          }
        }
      } catch (e) {
        if ((e as Error).name !== 'AbortError') throw e;
      }

      // Wait a bit for cleanup
      await new Promise((r) => setTimeout(r, 50));
    };

    // Start processing
    const processorPromise = processor();

    // Simulate shutdown after some time
    setTimeout(() => {
      console.log('  Initiating shutdown...');
      controller.abort();
    }, 100);

    await processorPromise;
    console.log('  Shutdown complete');
  }

  // ============================================================================
  // Pattern: Pipeline with pipeTo
  // ============================================================================
  section('Pattern: Full Pipeline with pipeTo');

  {
    async function* dataSource() {
      yield 'Record 1\n';
      yield 'Record 2\n';
      yield 'Record 3\n';
    }

    const addTimestamp: Transform = (chunks) => {
      if (chunks === null) return null;
      const now = new Date().toISOString();
      return chunks.map((chunk) => {
        const text = decoder.decode(chunk);
        return encoder.encode(`[${now}] ${text}`);
      });
    };

    const writer = new MemoryWriter();

    // Full pipeline: source -> timestamp -> uppercase -> writer
    const bytesWritten = await Stream.pipeTo(
        Stream.from(dataSource()),
        addTimestamp,
        uppercaseTransform(),
        writer);

    console.log(`  Bytes written: ${bytesWritten}`);
    console.log('  Output:');
    writer
      .getText()
      .split('\n')
      .filter((l) => l)
      .forEach((line) => console.log('    ' + line));
  }

  console.log('\n--- Examples complete! ---\n');
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });

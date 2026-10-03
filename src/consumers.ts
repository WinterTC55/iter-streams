/**
 * Convenience Consumers & Utilities
 *
 * bytes(), text(), arrayBuffer() - collect entire stream
 * tap(), tapSync() - observe without modifying
 * merge() - temporal combining of sources
 */

import {
  type ConsumeOptions,
  type ConsumeSyncOptions,
  type TextConsumeOptions,
  type TextConsumeSyncOptions,
  type MergeOptions,
  type Transform,
  type TransformCallbackOptions,
  type SyncTransform,
  type ByteStreamReadable,
  type Drainable,
  drainableProtocol,
} from './types.js';

import { from, isAsyncIterable, isSyncIterable } from './from.js';
import { concatBytes } from './utils.js';

// =============================================================================
// Type Guards and Helpers
// =============================================================================

/**
 * Check if last argument is MergeOptions.
 */
function isMergeOptions(value: unknown): value is MergeOptions {
  return (
    value !== null &&
    typeof value === 'object' &&
    !isAsyncIterable(value) &&
    !isSyncIterable(value)
  );
}



// =============================================================================
// Sync Consumers
// =============================================================================

/**
 * Collect all bytes from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional limit
 * @returns Concatenated Uint8Array
 */
export function bytesSync(
  source: Iterable<Uint8Array[]>,
  options?: ConsumeSyncOptions
): Uint8Array {
  const limit = options?.limit;
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  for (const batch of source) {
    for (const chunk of batch) {
      if (limit !== undefined) {
        totalBytes += chunk.byteLength;
        if (totalBytes > limit) {
          throw new RangeError(`Stream exceeded byte limit of ${limit}`);
        }
      }
      chunks.push(chunk);
    }
  }

  return concatBytes(chunks);
}

/**
 * Collect and decode text from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional encoding and limit
 * @returns Decoded string
 */
export function textSync(
  source: Iterable<Uint8Array[]>,
  options?: TextConsumeSyncOptions
): string {
  const data = bytesSync(source, options);
  const decoder = new TextDecoder(options?.encoding ?? 'utf-8', {
    fatal: true,
    ignoreBOM: true,
  });
  return decoder.decode(data);
}

/**
 * Collect bytes as ArrayBuffer from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional limit
 * @returns ArrayBuffer
 */
export function arrayBufferSync(
  source: Iterable<Uint8Array[]>,
  options?: ConsumeSyncOptions
): ArrayBuffer {
  const data = bytesSync(source, options);
  // Return the underlying ArrayBuffer, or copy if it's a view of a larger buffer
  if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
    return data.buffer as ArrayBuffer;
  }
  return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
}

/**
 * Collect all chunks as an array from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional limit
 * @returns Array of Uint8Array chunks
 */
export function arraySync(
  source: Iterable<Uint8Array[]>,
  options?: ConsumeSyncOptions
): Uint8Array[] {
  const limit = options?.limit;
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;

  for (const batch of source) {
    for (const chunk of batch) {
      if (limit !== undefined) {
        totalBytes += chunk.byteLength;
        if (totalBytes > limit) {
          throw new RangeError(`Stream exceeded byte limit of ${limit}`);
        }
      }
      chunks.push(chunk);
    }
  }

  return chunks;
}

/**
 * Read a sync source to completion, discarding everything it yields.
 *
 * Unlike the other consumers, nothing is retained: peak memory is one batch
 * regardless of how much the source produces. Reading is also what releases a
 * source's backpressure budget, so a source whose payload is not wanted still
 * needs to be read rather than abandoned.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional limit
 */
export function dumpSync(
  source: Iterable<Uint8Array[]>,
  options?: ConsumeSyncOptions
): undefined {
  const limit = options?.limit;

  let totalBytes = 0;
  for (const batch of source) {
    // Fast path: with no limit there is no reason to look at the chunks at all.
    if (limit === undefined) continue;
    for (const chunk of batch) {
      totalBytes += chunk.byteLength;
      if (totalBytes > limit) {
        throw new RangeError(`Stream exceeded byte limit of ${limit}`);
      }
    }
  }
}

// =============================================================================
// Async Consumers
// =============================================================================

/**
 * Collect all bytes from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to concatenated Uint8Array
 */
export async function bytes(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  options?: ConsumeOptions
): Promise<Uint8Array> {
  const signal = options?.signal;
  const limit = options?.limit;

  // Check for abort
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException('Aborted', 'AbortError');
  }

  const chunks: Uint8Array[] = [];

  // Fast path: no signal and no limit - minimize per-iteration checks
  if (!signal && limit === undefined) {
    if (isAsyncIterable(source)) {
      for await (const batch of source) {
        for (const chunk of batch) {
          chunks.push(chunk);
        }
      }
    } else if (isSyncIterable(source)) {
      for (const batch of source) {
        for (const chunk of batch) {
          chunks.push(chunk);
        }
      }
    } else {
      throw new TypeError('Source must be iterable');
    }
    return concatBytes(chunks);
  }

  // Slow path: with signal or limit checks
  let totalBytes = 0;

  if (isAsyncIterable(source)) {
    for await (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      for (const chunk of batch) {
        if (limit !== undefined) {
          totalBytes += chunk.byteLength;
          if (totalBytes > limit) {
            throw new RangeError(`Stream exceeded byte limit of ${limit}`);
          }
        }
        chunks.push(chunk);
      }
    }
  } else if (isSyncIterable(source)) {
    for (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      for (const chunk of batch) {
        if (limit !== undefined) {
          totalBytes += chunk.byteLength;
          if (totalBytes > limit) {
            throw new RangeError(`Stream exceeded byte limit of ${limit}`);
          }
        }
        chunks.push(chunk);
      }
    }
  } else {
    throw new TypeError('Source must be iterable');
  }

  return concatBytes(chunks);
}

/**
 * Collect and decode text from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional encoding, signal, and limit
 * @returns Promise resolving to decoded string
 */
export async function text(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  options?: TextConsumeOptions
): Promise<string> {
  const data = await bytes(source, options);
  const decoder = new TextDecoder(options?.encoding ?? 'utf-8', {
    fatal: true,
    ignoreBOM: true,
  });
  return decoder.decode(data);
}

/**
 * Collect bytes as ArrayBuffer from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to ArrayBuffer
 */
export async function arrayBuffer(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  options?: ConsumeOptions
): Promise<ArrayBuffer> {
  const data = await bytes(source, options);
  // Return the underlying ArrayBuffer, or copy if it's a view of a larger buffer
  if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
    return data.buffer as ArrayBuffer;
  }
  return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer;
}

/**
 * Collect all chunks as an array from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to array of Uint8Array chunks
 */
export async function array(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  options?: ConsumeOptions
): Promise<Uint8Array[]> {
  const signal = options?.signal;
  const limit = options?.limit;

  // Check for abort
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException('Aborted', 'AbortError');
  }

  const chunks: Uint8Array[] = [];

  // Fast path: no signal and no limit - minimize per-iteration checks
  if (!signal && limit === undefined) {
    if (isAsyncIterable(source)) {
      for await (const batch of source) {
        for (const chunk of batch) {
          chunks.push(chunk);
        }
      }
    } else if (isSyncIterable(source)) {
      for (const batch of source) {
        for (const chunk of batch) {
          chunks.push(chunk);
        }
      }
    } else {
      throw new TypeError('Source must be iterable');
    }
    return chunks;
  }

  // Slow path: with signal or limit checks
  let totalBytes = 0;

  if (isAsyncIterable(source)) {
    for await (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      for (const chunk of batch) {
        if (limit !== undefined) {
          totalBytes += chunk.byteLength;
          if (totalBytes > limit) {
            throw new RangeError(`Stream exceeded byte limit of ${limit}`);
          }
        }
        chunks.push(chunk);
      }
    }
  } else if (isSyncIterable(source)) {
    for (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      for (const chunk of batch) {
        if (limit !== undefined) {
          totalBytes += chunk.byteLength;
          if (totalBytes > limit) {
            throw new RangeError(`Stream exceeded byte limit of ${limit}`);
          }
        }
        chunks.push(chunk);
      }
    }
  } else {
    throw new TypeError('Source must be iterable');
  }

  return chunks;
}

/**
 * Read an async or sync source to completion, discarding everything it yields.
 *
 * Unlike the other consumers, nothing is retained: peak memory is one batch
 * regardless of how much the source produces. Reading is also what releases a
 * source's backpressure budget, and for some sources what releases resources
 * held on the producer's behalf, so a source whose payload is not wanted still
 * needs to be read rather than abandoned. bytes() achieves the same thing but
 * allocates the entire payload in order to throw it away.
 *
 * Ending for any reason other than normal completion - a source error, an
 * abort, or exceeding the limit - rejects. A partial read is never reported as
 * success.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to undefined
 */
export async function dump(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  options?: ConsumeOptions
): Promise<undefined> {
  const signal = options?.signal;
  const limit = options?.limit;

  // Check for abort
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException('Aborted', 'AbortError');
  }

  let totalBytes = 0;
  if (isAsyncIterable(source)) {
    for await (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      if (limit === undefined) continue;

      for (const chunk of batch) {
        totalBytes += chunk.byteLength;
        if (totalBytes > limit) {
          throw new RangeError(`Stream exceeded byte limit of ${limit}`);
        }
      }
    }
  } else if (isSyncIterable(source)) {
    for (const batch of source) {
      // Check for abort on each iteration
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      if (limit === undefined) continue;

      for (const chunk of batch) {
        totalBytes += chunk.byteLength;
        if (totalBytes > limit) {
          throw new RangeError(`Stream exceeded byte limit of ${limit}`);
        }
      }
    }
  } else {
    throw new TypeError('Source must be iterable');
  }
}

// =============================================================================
// Tap Utilities
// =============================================================================

/**
 * Callback type for tap - receives chunks or null (flush signal).
 */
export type TapCallback = (chunks: Uint8Array[] | null) => void;

/**
 * Async callback type for tap.
 */
export type TapCallbackAsync = (
  chunks: Uint8Array[] | null,
  options: TransformCallbackOptions
) => void | Promise<void>;

/**
 * Create a pass-through transform that observes chunks without modifying them.
 * Useful for logging, hashing, metrics, etc.
 *
 * @param callback - Called with each batch or null (flush signal)
 * @returns Transform that passes chunks through unchanged
 */
export function tap(callback: TapCallbackAsync): Transform {
  return async (chunks: Uint8Array[] | null, options: TransformCallbackOptions) => {
    await callback(chunks, options);
    return chunks;
  };
}

/**
 * Create a sync pass-through transform that observes chunks without modifying them.
 *
 * @param callback - Called with each batch or null (flush signal)
 * @returns SyncTransform that passes chunks through unchanged
 */
export function tapSync(callback: TapCallback): SyncTransform {
  return (chunks: Uint8Array[] | null) => {
    callback(chunks);
    return chunks;
  };
}

// =============================================================================
// Drain Utility
// =============================================================================

/**
 * Wait for a drainable object's backpressure to clear.
 * 
 * This is the primary way to integrate event-driven sources with the streams API.
 * Writers from Stream.push() and Stream.broadcast() implement the drainable protocol.
 * 
 * @param drainable - Object that may implement the Drainable protocol
 * @returns Promise<boolean> if object implements protocol, null if not supported
 *   - Promise resolves with `true` immediately if ready to write (desiredSize > 0)
 *   - Promise resolves with `true` when backpressure clears
 *   - Promise resolves with `false` if writer closes while waiting
 *   - Promise rejects if writer fails while waiting
 *   - Returns `null` if object doesn't implement the protocol or drain is not applicable
 * 
 * Note: Due to TOCTOU races, callers should still check desiredSize and
 * await writes even after the drain promise resolves with true.
 * 
 * @example
 * ```typescript
 * const { writer, readable } = Stream.push({ highWaterMark: 2 });
 * 
 * source.on('data', async (chunk) => {
 *   if (writer.desiredSize === 0) {
 *     source.pause();
 *     // await works on null (returns null), and null is falsy
 *     const canWrite = await Stream.ondrain(writer);
 *     if (!canWrite) {
 *       // Writer closed or protocol not supported - stop the source
 *       source.destroy();
 *       return;
 *     }
 *     source.resume();
 *   }
 *   await writer.write(chunk);
 * });
 * ```
 */
export function ondrain(drainable: unknown): Promise<boolean> | null {
  // Defensive: never throw synchronously
  // Return null for anything that doesn't implement the protocol
  if (
    drainable === null ||
    drainable === undefined ||
    typeof drainable !== 'object'
  ) {
    return null;
  }

  // Check if object has the drainable protocol
  if (
    !(drainableProtocol in drainable) ||
    typeof (drainable as Drainable)[drainableProtocol] !== 'function'
  ) {
    return null;
  }

  // Call the protocol method
  // This may return null (drain not applicable) or a Promise
  try {
    return (drainable as Drainable)[drainableProtocol]();
  } catch {
    // If the protocol method throws synchronously, return null
    return null;
  }
}

// =============================================================================
// Merge Utility
// =============================================================================

/**
 * Merge multiple async iterables by yielding values in temporal order.
 * Whichever source produces a value first gets yielded first.
 *
 * @param args - Variadic async iterables, with optional MergeOptions as last argument
 * @returns Async iterable yielding batches from any source in arrival order
 */
export function merge(
  ...args: (unknown | MergeOptions)[]
): ByteStreamReadable {
  // Parse arguments: last arg may be MergeOptions if it has a signal member
  // and no iteration protocols
  let rawSources: unknown[];
  let options: MergeOptions | undefined;

  if (args.length > 0 && isMergeOptions(args[args.length - 1])) {
    options = args[args.length - 1] as MergeOptions;
    rawSources = args.slice(0, -1);
  } else {
    rawSources = args;
  }

  // Normalize each source via from()
  const sources = rawSources.map(s => from(s as any));

  return {
    async *[Symbol.asyncIterator]() {
      const signal = options?.signal;

      // Check for abort
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException('Aborted', 'AbortError');
      }

      if (sources.length === 0) {
        return;
      }

      if (sources.length === 1) {
        // Single source - just yield from it
        for await (const batch of sources[0]) {
          if (signal?.aborted) {
            throw signal.reason ?? new DOMException('Aborted', 'AbortError');
          }
          yield batch;
        }
        return;
      }

      // Multiple sources - race them
      type IteratorState = {
        iterator: AsyncIterator<Uint8Array[]>;
        done: boolean;
        pending: Promise<{ index: number; result: IteratorResult<Uint8Array[]> }> | null;
      };

      const states: IteratorState[] = sources.map((source) => ({
        iterator: source[Symbol.asyncIterator](),
        done: false,
        pending: null,
      }));

      // Start all iterators
      const startIterator = (state: IteratorState, index: number) => {
        if (!state.done && !state.pending) {
          state.pending = state.iterator.next().then((result) => ({ index, result }));
        }
      };

      // Start all
      states.forEach(startIterator);

      try {
        while (true) {
          // Check for abort
          if (signal?.aborted) {
            throw signal.reason ?? new DOMException('Aborted', 'AbortError');
          }

          // Collect all pending promises
          const pending = states
            .map((state, _index) => (state.pending ? state.pending : null))
            .filter((p): p is NonNullable<typeof p> => p !== null);

          if (pending.length === 0) {
            // All done
            break;
          }

          // Wait for any to complete
          const { index, result } = await Promise.race(pending);

          // Clear the pending promise
          states[index].pending = null;

          if (result.done) {
            states[index].done = true;
          } else {
            yield result.value;
            // Start next iteration for this source
            startIterator(states[index], index);
          }
        }
      } finally {
        // Clean up: return all iterators
        const returnPromises = states.map(async (state) => {
          if (!state.done && state.iterator.return) {
            try {
              await state.iterator.return();
            } catch {
              // Ignore return errors
            }
          }
        });
        await Promise.all(returnPromises);
      }
    },
  };
}

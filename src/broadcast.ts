/**
 * Broadcast - Push-model multi-consumer streaming
 *
 * Creates a broadcast channel where a single writer can push data to multiple
 * consumers. Each consumer has an independent cursor into a shared buffer.
 */

import {
  type Writer,
  type WriteOptions,
  type Broadcast as BroadcastInterface,
  type BackpressurePolicy,
  type BroadcastOptions,
  type BroadcastResult,
  type Transform,
  type PushStreamOptions,
  type PullOptions,
  type Streamable,
  type Broadcastable,
  type Drainable,
  broadcastProtocol,
  drainableProtocol,
} from './types.js';

import { isAsyncIterable, isSyncIterable } from './from.js';
import { pull as pullWithTransforms } from './pull.js';
import { allUint8Array } from './utils.js';
import { RingBuffer } from './ringbuffer.js';

// Shared TextEncoder instance
const encoder = new TextEncoder();

// Cached resolved promise to avoid allocating a new one on every sync fast-path.
const kResolvedPromise: Promise<void> = Promise.resolve();

// Non-exported symbol for internal cancel notification from BroadcastImpl to BroadcastWriter.
// Because this symbol is not exported, external code cannot call it.
const cancelWriter = Symbol('cancelWriter');

// =============================================================================
// Argument Parsing Helpers
// =============================================================================

/**
 * Check if a value is PushStreamOptions (object without transform property).
 */
function isPushStreamOptions(value: unknown): value is PushStreamOptions {
  return (
    value !== null &&
    typeof value === 'object' &&
    !('transform' in value) &&
    !('write' in value)
  );
}

/**
 * Parse variadic arguments for push().
 * Returns { transforms, options }
 */
function parsePushArgs(
  args: (Transform | PushStreamOptions)[]
): { transforms: Transform[]; options: PushStreamOptions | undefined } {
  if (args.length === 0) {
    return { transforms: [], options: undefined };
  }

  const last = args[args.length - 1];
  if (isPushStreamOptions(last)) {
    return {
      transforms: args.slice(0, -1) as Transform[],
      options: last,
    };
  }

  return { transforms: args as Transform[], options: undefined };
}

// =============================================================================
// Consumer State
// =============================================================================

interface ConsumerState {
  /** Position in buffer (index of next chunk to read) */
  cursor: number;
  /** Resolve function for pending read */
  resolve: ((value: IteratorResult<Uint8Array[]>) => void) | null;
  /** Reject function for pending read */
  reject: ((error: Error) => void) | null;
  /** Whether consumer has been detached */
  detached: boolean;
}

// =============================================================================
// Broadcast Implementation
// =============================================================================

class BroadcastImpl implements BroadcastInterface {
  private buffer = new RingBuffer<Uint8Array[]>();
  private bufferStart = 0; // Index of first chunk in buffer (for cursor mapping)
  private consumers: Set<ConsumerState> = new Set();
  private ended = false;
  private error: Error | null = null;
  private cancelled = false;

  /** Callback invoked when buffer space becomes available (for pending writes) */
  _onBufferDrained: (() => void) | null = null;

  /** Reference to writer for cancel notification */
  private writer?: { [cancelWriter](): void };

  constructor(private options: Required<BroadcastOptions>) {}

  /** Register the writer for cancel notification. */
  setWriter(w: { [cancelWriter](): void }): void {
    this.writer = w;
  }

  get backpressurePolicy(): BackpressurePolicy {
    return this.options.backpressure;
  }

  get highWaterMark(): number {
    return this.options.highWaterMark;
  }

  get consumerCount(): number {
    return this.consumers.size;
  }

  get bufferSize(): number {
    return this.buffer.length;
  }

  /**
   * Create a new consumer that receives data from this broadcast.
   * Optionally apply transforms to the consumer's data.
   */
  push(
    ...args: (Transform | PullOptions)[]
  ): AsyncIterable<Uint8Array[]> {
    const { transforms, options } = parsePushArgs(args);

    // Create raw consumer
    const rawConsumer = this.createRawConsumer();

    // If transforms provided, wrap with pull() pipeline
    if (transforms.length > 0) {
      if (options?.signal) {
        return pullWithTransforms(rawConsumer, ...transforms, { signal: options.signal });
      }
      return pullWithTransforms(rawConsumer, ...transforms);
    }

    return rawConsumer;
  }

  /**
   * Create a raw consumer iterable (internal helper).
   */
  private createRawConsumer(): AsyncIterable<Uint8Array[]> {
    const state: ConsumerState = {
      cursor: this.bufferStart + this.buffer.length, // Start at current position
      resolve: null,
      reject: null,
      detached: false,
    };

    this.consumers.add(state);

    const self = this;

    return {
      [Symbol.asyncIterator]() {
        return {
          async next(): Promise<IteratorResult<Uint8Array[]>> {
            if (state.detached) {
              // If detached due to an error, throw the error
              if (self.error) throw self.error;
              return { done: true, value: undefined };
            }

            // Check if data is available in buffer
            const bufferIndex = state.cursor - self.bufferStart;
            if (bufferIndex < self.buffer.length) {
              const chunk = self.buffer.get(bufferIndex);
              state.cursor++;
              self.tryTrimBuffer();
              return { done: false, value: chunk };
            }

            // Check if ended/errored
            if (self.error) {
              state.detached = true;
              self.consumers.delete(state);
              throw self.error;
            }

            if (self.ended || self.cancelled) {
              state.detached = true;
              self.consumers.delete(state);
              return { done: true, value: undefined };
            }

            // Wait for data
            return new Promise((resolve, reject) => {
              state.resolve = resolve;
              state.reject = reject;
            });
          },

          async return(): Promise<IteratorResult<Uint8Array[]>> {
            state.detached = true;
            state.resolve = null;
            state.reject = null;
            self.consumers.delete(state);
            self.tryTrimBuffer();
            return { done: true, value: undefined };
          },

          async throw(_error?: Error): Promise<IteratorResult<Uint8Array[]>> {
            state.detached = true;
            state.resolve = null;
            state.reject = null;
            self.consumers.delete(state);
            self.tryTrimBuffer();
            return { done: true, value: undefined };
          },
        };
      },
    };
  }

  /**
   * Cancel all consumers and reject pending writes on the writer.
   * Sets ended=true so that subsequent _abort() calls early-return.
   */
  cancel(reason?: Error): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.ended = true; // Prevents _abort() from redundantly iterating consumers

    if (reason) {
      this.error = reason;
    }

    // Reject pending writes on the writer so the pump doesn't hang
    this.writer?.[cancelWriter]();

    // Notify all waiting consumers
    for (const consumer of this.consumers) {
      if (consumer.resolve) {
        if (reason) {
          consumer.reject?.(reason);
        } else {
          consumer.resolve({ done: true, value: undefined });
        }
        consumer.resolve = null;
        consumer.reject = null;
      }
      consumer.detached = true;
    }
    this.consumers.clear();
  }

  [Symbol.dispose](): void {
    this.cancel();
  }

  // ==========================================================================
  // Internal Methods (called by Writer)
  // ==========================================================================

  /**
   * Write a chunk to the broadcast buffer.
   * Returns true if write was accepted, false if buffer is full (strict/block policy).
   */
  _write(chunk: Uint8Array[]): boolean {
    if (this.ended || this.cancelled) {
      return false;
    }

    // Check buffer limit
    if (this.buffer.length >= this.options.highWaterMark) {
      switch (this.options.backpressure) {
        case 'strict':
        case 'block':
          return false;
        case 'drop-oldest':
          // Drop oldest and advance all cursors
          this.buffer.shift();
          this.bufferStart++;
          for (const consumer of this.consumers) {
            if (consumer.cursor < this.bufferStart) {
              consumer.cursor = this.bufferStart;
            }
          }
          break;
        case 'drop-newest':
          // Don't add to buffer, but still notify (no-op for data)
          return true;
      }
    }

    // Add to buffer
    this.buffer.push(chunk);

    // Notify waiting consumers
    this._notifyConsumers();

    return true;
  }

  /**
   * Signal end of stream.
   */
  _end(): void {
    if (this.ended) return;
    this.ended = true;

    // Notify all waiting consumers
    for (const consumer of this.consumers) {
      if (consumer.resolve) {
        // First deliver any remaining buffered data
        const bufferIndex = consumer.cursor - this.bufferStart;
        if (bufferIndex < this.buffer.length) {
          const chunk = this.buffer.get(bufferIndex);
          consumer.cursor++;
          consumer.resolve({ done: false, value: chunk });
        } else {
          consumer.resolve({ done: true, value: undefined });
        }
        consumer.resolve = null;
        consumer.reject = null;
      }
    }
  }

  /**
   * Signal error. Notifies all consumers and detaches them.
   */
  _abort(reason: Error): void {
    if (this.ended || this.error) return;
    this.error = reason;
    this.ended = true;

    // Notify all waiting consumers and detach them
    for (const consumer of this.consumers) {
      if (consumer.reject) {
        consumer.reject(reason);
        consumer.resolve = null;
        consumer.reject = null;
      }
      consumer.detached = true;
    }
    this.consumers.clear();
  }

  /**
   * Get the slowest consumer's cursor position.
   */
  private getMinCursor(): number {
    let min = Infinity;
    for (const consumer of this.consumers) {
      if (consumer.cursor < min) {
        min = consumer.cursor;
      }
    }
    return min === Infinity ? this.bufferStart + this.buffer.length : min;
  }

  /**
   * Check if we can accept more writes (for desiredSize).
   */
  _getDesiredSize(): number | null {
    if (this.ended || this.cancelled) return null;
    const available = this.options.highWaterMark - this.buffer.length;
    return Math.max(0, available);
  }

  /**
   * Check if a write would be accepted (without actually writing).
   * Returns true if _write would accept a write, false otherwise.
   */
  _canWrite(): boolean {
    if (this.ended || this.cancelled) {
      return false;
    }
    // For strict and block policies, check if there's space
    // For drop-oldest and drop-newest, writes are always accepted
    if ((this.options.backpressure === 'strict' || this.options.backpressure === 'block') && this.buffer.length >= this.options.highWaterMark) {
      return false;
    }
    return true;
  }

  /**
   * Trim buffer from front if all consumers have advanced.
   * Notifies writer if buffer space becomes available.
   */
  private tryTrimBuffer(): void {
    const minCursor = this.getMinCursor();
    const trimCount = minCursor - this.bufferStart;
    if (trimCount > 0) {
      this.buffer.trimFront(trimCount);
      this.bufferStart = minCursor;

      // Notify writer that buffer space is available for pending writes
      if (this._onBufferDrained && this.buffer.length < this.options.highWaterMark) {
        this._onBufferDrained();
      }
    }
  }

  /**
   * Notify consumers that have pending reads.
   */
  private _notifyConsumers(): void {
    for (const consumer of this.consumers) {
      if (consumer.resolve) {
        const bufferIndex = consumer.cursor - this.bufferStart;
        if (bufferIndex < this.buffer.length) {
          const chunk = this.buffer.get(bufferIndex);
          consumer.cursor++;
          const resolve = consumer.resolve;
          consumer.resolve = null;
          consumer.reject = null;
          resolve({ done: false, value: chunk });
        }
      }
    }
  }
}

// =============================================================================
// Writer Implementation for Broadcast
// =============================================================================

/** Pending write waiting for buffer space */
interface PendingBroadcastWrite {
  chunk: Uint8Array[];
  resolve: () => void;
  reject: (error: Error) => void;
}

/** Pending drain waiting for backpressure to clear */
interface PendingBroadcastDrain {
  resolve: (canWrite: boolean) => void;
  reject: (error: Error) => void;
}

class BroadcastWriter implements Writer, Drainable {
  private totalBytes = 0;
  private closed = false;
  private aborted = false;
  /** Queue of pending writes waiting for buffer space (strict and block policies) */
  private pendingWrites = new RingBuffer<PendingBroadcastWrite>();
  /** Queue of pending drains waiting for backpressure to clear */
  private pendingDrains: PendingBroadcastDrain[] = [];

  constructor(private broadcast: BroadcastImpl) {
    // Register callback for when buffer space becomes available
    this.broadcast._onBufferDrained = () => {
      this.resolvePendingWrites();
      this.resolvePendingDrains(true);
    };
  }

  /**
   * Drainable protocol implementation.
   *
   * @returns null if desiredSize is null (writer closed/errored)
   * @returns Promise<true> immediately if desiredSize > 0
   * @returns Promise<true> when backpressure clears
   * @returns Promise<false> if writer closes while waiting
   * @throws if writer fails while waiting
   */
  [drainableProtocol](): Promise<boolean> | null {
    const desired = this.desiredSize;

    // If desiredSize is null, drain is not applicable
    if (desired === null) {
      return null;
    }

    // If there's already space, resolve immediately with true
    if (desired > 0) {
      return Promise.resolve(true);
    }

    // Buffer is full, wait for drain
    return new Promise((resolve, reject) => {
      this.pendingDrains.push({ resolve, reject });
    });
  }

  get desiredSize(): number | null {
    if (this.closed || this.aborted) return null;
    return this.broadcast._getDesiredSize();
  }

  write(chunk: Uint8Array | string, options?: WriteOptions): Promise<void> {
    // Fast path: no signal, writer open, buffer has space
    if (!options?.signal && !this.closed && !this.aborted && this.broadcast._canWrite()) {
      const converted = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
      this.broadcast._write([converted]);
      this.totalBytes += converted.byteLength;
      return kResolvedPromise;
    }
    return this.writev([chunk], options);
  }

  writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void> {
    // Fast path: no signal, writer open, buffer has space
    if (!options?.signal && !this.closed && !this.aborted && this.broadcast._canWrite()) {
      const converted = allUint8Array(chunks)
        ? chunks.slice()
        : chunks.map((c) => typeof c === 'string' ? encoder.encode(c) : c);
      this.broadcast._write(converted);
      for (const c of converted) {
        this.totalBytes += c.byteLength;
      }
      return kResolvedPromise;
    }
    return this._writevSlow(chunks, options);
  }

  private async _writevSlow(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void> {
    const signal = options?.signal;

    // Check for pre-aborted signal
    if (signal?.aborted) {
      throw signal.reason ?? new DOMException('Aborted', 'AbortError');
    }

    if (this.closed || this.aborted) {
      throw new Error('Writer is closed');
    }

    const converted = allUint8Array(chunks)
      ? chunks.slice()
      : chunks.map((c) => typeof c === 'string' ? encoder.encode(c) : c);

    // Try to write directly to buffer
    if (this.broadcast._write(converted)) {
      for (const c of converted) {
        this.totalBytes += c.byteLength;
      }
      return;
    }

    // Buffer is full - handle based on policy
    // Note: _canWrite() and _write() handle drop-* policies, so we only get here for strict/block
    const policy = this.broadcast.backpressurePolicy;
    const highWaterMark = this.broadcast.highWaterMark;

    if (policy === 'strict') {
      // In strict mode, highWaterMark limits pendingWrites (the "hose")
      // If too many writes are already pending, caller is ignoring backpressure
      if (this.pendingWrites.length >= highWaterMark) {
        throw new Error(
          'Backpressure violation: too many pending writes. ' +
          'Await each write() call to respect backpressure.'
        );
      }
      // Otherwise, queue this write and wait for space
      return this.createPendingWrite(converted, signal);
    }

    // 'block' policy - wait for space (unbounded pending writes)
    return this.createPendingWrite(converted, signal);
  }

  /**
   * Create a pending write promise, optionally racing against a signal.
   * Same pattern as PushQueue.createPendingWrite().
   */
  private createPendingWrite(chunk: Uint8Array[], signal?: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const entry: PendingBroadcastWrite = { chunk, resolve, reject };
      this.pendingWrites.push(entry);

      if (!signal) return;

      const onAbort = () => {
        const idx = this.pendingWrites.indexOf(entry);
        if (idx !== -1) this.pendingWrites.removeAt(idx);
        reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
      };

      // Wrap resolve/reject to clean up signal listener
      const origResolve = entry.resolve;
      const origReject = entry.reject;
      entry.resolve = () => {
        signal.removeEventListener('abort', onAbort);
        origResolve();
      };
      entry.reject = (reason: Error) => {
        signal.removeEventListener('abort', onAbort);
        origReject(reason);
      };

      signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  /**
   * Resolve pending writes when buffer has space.
   */
  private resolvePendingWrites(): void {
    while (this.pendingWrites.length > 0 && this.broadcast._canWrite()) {
      const pending = this.pendingWrites.shift()!;
      if (this.broadcast._write(pending.chunk)) {
        for (const c of pending.chunk) {
          this.totalBytes += c.byteLength;
        }
        pending.resolve();
      } else {
        // Couldn't write - put it back and stop
        this.pendingWrites.unshift(pending);
        break;
      }
    }
  }

  /**
   * Reject all pending writes with an error.
   */
  private rejectPendingWrites(error: Error): void {
    while (this.pendingWrites.length > 0) {
      const pending = this.pendingWrites.shift()!;
      pending.reject(error);
    }
  }

  /**
   * Resolve all pending drains with a value.
   */
  private resolvePendingDrains(canWrite: boolean): void {
    const drains = this.pendingDrains;
    this.pendingDrains = [];
    for (const pending of drains) {
      pending.resolve(canWrite);
    }
  }

  /**
   * Reject all pending drains with an error.
   */
  private rejectPendingDrains(error: Error): void {
    const drains = this.pendingDrains;
    this.pendingDrains = [];
    for (const pending of drains) {
      pending.reject(error);
    }
  }

  writeSync(chunk: Uint8Array | string): boolean {
    if (this.closed || this.aborted) return false;

    // Check if write would be accepted before converting
    if (!this.broadcast._canWrite()) {
      return false;
    }

    const converted = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
    if (this.broadcast._write([converted])) {
      this.totalBytes += converted.byteLength;
      return true;
    }
    return false;
  }

  writevSync(chunks: (Uint8Array | string)[]): boolean {
    if (this.closed || this.aborted) return false;

    // Check if write would be accepted before converting
    if (!this.broadcast._canWrite()) {
      return false;
    }

    const converted = allUint8Array(chunks)
      ? chunks.slice()
      : chunks.map((c) => typeof c === 'string' ? encoder.encode(c) : c);

    if (this.broadcast._write(converted)) {
      for (const c of converted) {
        this.totalBytes += c.byteLength;
      }
      return true;
    }
    return false;
  }

  end(_options?: WriteOptions): Promise<number> {
    // end() rejects with TypeError if already closed/errored
    if (this.closed || this.aborted) return Promise.reject(new TypeError('Writer is already closed or errored'));
    this.closed = true;
    this.broadcast._end();
    // Resolve pending drains with false - writer closed, no more writes accepted
    this.resolvePendingDrains(false);
    return Promise.resolve(this.totalBytes);
  }

  endSync(): number {
    if (this.closed) return -1;
    this.closed = true;
    this.broadcast._end();
    // Resolve pending drains with false - writer closed, no more writes accepted
    this.resolvePendingDrains(false);
    return this.totalBytes;
  }

  fail(reason?: any): Promise<void> {
    if (this.aborted || this.closed) return kResolvedPromise;
    this.aborted = true;
    this.closed = true;
    const error = reason ?? new Error('Failed');
    this.rejectPendingWrites(error);
    // Reject pending drains with the error
    this.rejectPendingDrains(error);
    this.broadcast._abort(error);
    return kResolvedPromise;
  }

  failSync(reason?: any): boolean {
    if (this.aborted || this.closed) return false;
    this.aborted = true;
    this.closed = true;
    const error = reason ?? new Error('Failed');
    this.rejectPendingWrites(error);
    // Reject pending drains with the error
    this.rejectPendingDrains(error);
    this.broadcast._abort(error);
    return true;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    return this.fail();
  }

  [Symbol.dispose](): void {
    this.failSync();
  }

  /**
   * Internal cancel notification from BroadcastImpl.cancel().
   * Rejects pending writes and marks writer closed so the pump can exit.
   */
  [cancelWriter](): void {
    if (this.closed) return;
    this.closed = true;
    this.rejectPendingWrites(
      new DOMException('Broadcast cancelled', 'AbortError')
    );
    this.resolvePendingDrains(false);
  }
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Create a broadcast channel for push-model multi-consumer streaming.
 *
 * @param options - Buffer limit and backpressure policy
 * @returns Writer and Broadcast pair
 */
export function broadcast(options?: BroadcastOptions): BroadcastResult {
  const opts: Required<BroadcastOptions> = {
    highWaterMark: Math.max(1, options?.highWaterMark ?? 16),
    backpressure: options?.backpressure ?? 'strict',
    signal: options?.signal as AbortSignal,
  };

  const broadcastImpl = new BroadcastImpl(opts);
  const writer = new BroadcastWriter(broadcastImpl);
  broadcastImpl.setWriter(writer);

  // Handle abort signal - cancel without error (clean shutdown)
  if (opts.signal) {
    if (opts.signal.aborted) {
      broadcastImpl.cancel();
    } else {
      opts.signal.addEventListener('abort', () => {
        broadcastImpl.cancel();
      }, { once: true });
    }
  }

  return { writer, channel: broadcastImpl };
}

/**
 * Check if value implements Broadcastable protocol.
 */
function isBroadcastable(value: unknown): value is Broadcastable {
  return (
    value !== null &&
    typeof value === 'object' &&
    broadcastProtocol in value &&
    typeof (value as Broadcastable)[broadcastProtocol] === 'function'
  );
}

/**
 * Namespace for Broadcast.from() static method.
 */
export const Broadcast = {
  /**
   * Get or create a Broadcast from a Broadcastable or Streamable.
   *
   * If input implements the broadcastProtocol, calls it.
   * Otherwise, creates a Broadcast and pumps from the source.
   */
  from(
    input: Broadcastable | Streamable,
    options?: BroadcastOptions
  ): BroadcastResult {
    // Check for protocol
    if (isBroadcastable(input)) {
      const bc = input[broadcastProtocol](options);
      // The protocol returns Broadcast, we need to create a writer
      // This is a simplification - in practice the protocol would return the full result
      return { writer: {} as Writer, channel: bc };
    }

    // Create broadcast and pump from source
    const result = broadcast(options);
    const signal = options?.signal;

    // Start pumping in background
    (async () => {
      try {
        if (isAsyncIterable(input)) {
          for await (const chunks of input as AsyncIterable<unknown>) {
            if (signal?.aborted) {
              throw signal.reason ?? new DOMException('Aborted', 'AbortError');
            }
            if (Array.isArray(chunks)) {
              await result.writer.writev(
                chunks as (Uint8Array | string)[],
                signal ? { signal } : undefined
              );
            }
          }
        } else if (isSyncIterable(input)) {
          for (const chunks of input as Iterable<unknown>) {
            if (signal?.aborted) {
              throw signal.reason ?? new DOMException('Aborted', 'AbortError');
            }
            if (Array.isArray(chunks)) {
              await result.writer.writev(
                chunks as (Uint8Array | string)[],
                signal ? { signal } : undefined
              );
            }
          }
        }
        await result.writer.end(signal ? { signal } : undefined);
      } catch (error) {
        await result.writer.fail(
          error instanceof Error ? error : new Error(String(error))
        );
      }
    })();

    return result;
  },
};

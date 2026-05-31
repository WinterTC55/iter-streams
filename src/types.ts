/**
 * New Streams API - Type Definitions
 * 
 * This file defines all types for the new streams API based on iterables
 * with explicit backpressure handling.
 */

// =============================================================================
// §1 Protocol Symbols
// These symbols allow objects to participate in streaming.
// Using Symbol.for() allows third-party code to implement protocols without
// importing these symbols directly - they can use Symbol.for('Stream.xyz').
// =============================================================================

/**
 * Symbol for sync value-to-streamable conversion protocol.
 * Objects implementing this can be written to streams or yielded from generators.
 * Works in both sync and async contexts.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.toStreamable')]() { ... }`
 */
export const toStreamable = Symbol.for('Stream.toStreamable');

/**
 * Symbol for async value-to-streamable conversion protocol.
 * Objects implementing this can be written to async streams.
 * Works in async contexts only.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.toAsyncStreamable')]() { ... }`
 */
export const toAsyncStreamable = Symbol.for('Stream.toAsyncStreamable');

/**
 * Symbol for Broadcastable protocol - object can provide a Broadcast.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.broadcastProtocol')]() { ... }`
 */
export const broadcastProtocol = Symbol.for('Stream.broadcastProtocol');

/**
 * Symbol for Shareable protocol - object can provide a Share.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.shareProtocol')]() { ... }`
 */
export const shareProtocol = Symbol.for('Stream.shareProtocol');

/**
 * Symbol for SyncShareable protocol - object can provide a SyncShare.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.shareSyncProtocol')]() { ... }`
 */
export const shareSyncProtocol = Symbol.for('Stream.shareSyncProtocol');

/**
 * Symbol for Drainable protocol - object can signal when backpressure clears.
 * Used to bridge event-driven sources that need drain notification.
 * 
 * Third-party code can implement this as: `[Symbol.for('Stream.drainableProtocol')]() { ... }`
 */
export const drainableProtocol = Symbol.for('Stream.drainableProtocol');

// =============================================================================
// §2 Primitive Types
// Basic chunk and input types used throughout the API
// =============================================================================

/**
 * Primitive chunk types that can be streamed directly.
 * - string: UTF-8 encoded to Uint8Array
 * - ArrayBuffer: wrapped as Uint8Array view (no copy)
 * - ArrayBufferView: includes Uint8Array, Int8Array, DataView, etc.
 */
export type PrimitiveChunk = string | ArrayBuffer | ArrayBufferView;

/**
 * Raw byte input for single-value factory functions.
 * Used by Stream.from() and Stream.fromSync() when input is not iterable.
 */
export type ByteInput = string | ArrayBuffer | ArrayBufferView;

// =============================================================================
// §3 ToStreamable / ToAsyncStreamable Protocols
// Forward declarations needed for StreamableChunk
// =============================================================================

/**
 * Sync protocol - returns sync-safe yields only.
 * Works in BOTH sync and async contexts.
 */
export interface ToStreamable {
  [toStreamable](): SyncStreamableYield;
}

/**
 * Async protocol - may return promises or async iterables.
 * Works in async contexts ONLY.
 */
export interface ToAsyncStreamable {
  [toAsyncStreamable](): AsyncStreamableYield | Promise<AsyncStreamableYield>;
}

// =============================================================================
// §4 Streamable Types
// Structural types for iterable sources
// =============================================================================

/**
 * A chunk can be a primitive or an object implementing conversion protocols.
 */
export type StreamableChunk = PrimitiveChunk | ToStreamable | ToAsyncStreamable;

/**
 * Sync streamable yields - chunks, arrays, or nested sync iterables (flattened).
 */
export type SyncStreamableYield =
  | StreamableChunk
  | StreamableChunk[]
  | Iterable<SyncStreamableYield>;

/**
 * Async streamable yields - can also include async iterables.
 */
export type AsyncStreamableYield =
  | StreamableChunk
  | StreamableChunk[]
  | Iterable<AsyncStreamableYield>
  | AsyncIterable<AsyncStreamableYield>;

/**
 * Sync streamable - has Symbol.iterator yielding stream-compatible values.
 */
export interface SyncStreamable {
  [Symbol.iterator](): Iterator<SyncStreamableYield>;
}

/**
 * Async streamable - has Symbol.asyncIterator yielding stream-compatible values.
 */
export interface AsyncStreamable {
  [Symbol.asyncIterator](): AsyncIterator<AsyncStreamableYield>;
}

/**
 * An object is Streamable if it implements at least one iteration protocol.
 */
export type Streamable = SyncStreamable | AsyncStreamable;

// =============================================================================
// §5 Writer Interfaces
// Async and sync writers with backpressure support
// =============================================================================

/**
 * Options for individual write operations.
 * Contains a signal to cancel the specific operation without affecting the writer.
 */
export interface WriteOptions {
  /** Signal to cancel this write operation. */
  readonly signal?: AbortSignal;
}

/**
 * Async writer interface with backpressure.
 * 
 * Writers use chunk-oriented backpressure by default: desiredSize counts pending
 * write/writev calls, not bytes. Individual implementations may use byte-oriented
 * or other strategies.
 */
export interface Writer {
  /**
   * Slots available before hitting highWaterMark.
   * - Always >= 0 (never negative, unlike Web Streams)
   * - 0 means buffer is full
   * - null if writer is closed/failed
   */
  readonly desiredSize: number | null;

  /** Write single chunk. Rejects if buffer full (strict policy). */
  write(chunk: Uint8Array | string, options?: WriteOptions): Promise<void>;

  /** Write multiple chunks atomically. Counts as 1 slot for backpressure. */
  writev(chunks: (Uint8Array | string)[], options?: WriteOptions): Promise<void>;

  /** Try to write synchronously. Returns true if successful, false if buffer full. */
  writeSync(chunk: Uint8Array | string): boolean;

  /** Try to writev synchronously. Returns true if successful, false if buffer full. */
  writevSync(chunks: (Uint8Array | string)[]): boolean;

  /** Signal end of stream. Returns total bytes written. */
  end(options?: WriteOptions): Promise<number>;

  /** Signal end synchronously. Returns total bytes written, or -1 if cannot complete sync. */
  endSync(): number;

  /** Put writer into terminal error state. Downstream sees error. Accepts any value as reason. */
  fail(reason?: any): Promise<void>;

  /** Put writer into terminal error state synchronously. Returns false if writer cannot transition. */
  failSync(reason?: any): boolean;
}

/**
 * Sync writer interface for producing data.
 * 
 * A false return from write/writev means the writer could not perform the
 * operation synchronously. Sync pipelines must treat that as a failed write.
 * - end() throws TypeError if already closed or errored
 * - fail() is a no-op if already closed or errored
 */
export interface SyncWriter {
  /**
   * Slots available in the internal buffer.
   * - Always >= 0 (never negative)
   * - 0 means buffer is full
   * - null if writer is closed/failed
   */
  readonly desiredSize: number | null;

  /** Write single chunk. Returns true if accepted, false if not completed synchronously. */
  write(chunk: Uint8Array | string): boolean;

  /** Write multiple chunks atomically. All-or-nothing. Returns true/false like write(). */
  writev(chunks: (Uint8Array | string)[]): boolean;

  /** Signal end of stream. Returns total bytes written. Throws TypeError if already closed/errored. */
  end(): number;

  /** Put writer into terminal error state. No-op if already closed or errored. */
  fail(reason?: any): void;
}

// =============================================================================
// §6 Writer-Iterable Pair Types
// Paired writer and iterable for bridging push and pull models
// =============================================================================

/**
 * Async iterable that yields batches of Uint8Array chunks.
 * This is the normalized output type used throughout the API.
 */
export interface ByteStreamReadable {
  [Symbol.asyncIterator](): AsyncIterator<Uint8Array[]>;
}

/**
 * Sync iterable that yields batches of Uint8Array chunks.
 */
export interface SyncByteStreamReadable {
  [Symbol.iterator](): Iterator<Uint8Array[]>;
}

/**
 * Async pair - for event sources, network connections, etc.
 * Data written to writer appears in readable.
 */
export interface WriterIterablePair {
  readonly writer: Writer;
  readonly readable: ByteStreamReadable;
}

/**
 * Sync pair - for in-memory channels, testing.
 */
export interface SyncWriterIterablePair {
  readonly writer: SyncWriter;
  readonly readable: SyncByteStreamReadable;
}

/**
 * A bidirectional channel for full-duplex communication.
 * Extends WriterIterablePair with close/dispose semantics.
 * 
 * Data written to `writer` is sent to the connected peer's `readable`.
 * Data from the peer appears in this channel's `readable`.
 */
export interface DuplexChannel extends WriterIterablePair, AsyncDisposable {
  /**
   * Close this end of the channel.
   * Signals end-of-stream to the connected peer.
   * Idempotent - multiple calls are safe.
   */
  close(): Promise<void>;
}

/**
 * Options for one direction of a duplex channel.
 */
export interface DuplexDirectionOptions {
  /**
   * High water mark for this direction's buffer.
    * Inherits from shared DuplexOptions if not specified.
   */
  highWaterMark?: number;

  /**
   * Backpressure policy for this direction.
   * @default 'strict'
   */
  backpressure?: BackpressurePolicy;
}

/**
 * Options for Stream.duplex().
 * 
 * Can specify shared options for both directions, or use `a` and `b` for
 * per-direction configuration. Per-direction options override shared options.
 */
export interface DuplexOptions {
  /**
   * High water mark for both directions (can be overridden per-direction).
    * @default 4
   */
  highWaterMark?: number;

  /**
   * Backpressure policy for both directions (can be overridden per-direction).
   * @default 'strict'
   */
  backpressure?: BackpressurePolicy;

  /**
   * Options specific to the A→B direction (first channel's writer to second channel's readable).
   */
  a?: DuplexDirectionOptions;

  /**
   * Options specific to the B→A direction (second channel's writer to first channel's readable).
   */
  b?: DuplexDirectionOptions;

  /**
   * AbortSignal to cancel both channels.
   */
  signal?: AbortSignal;
}

// =============================================================================
// §7 Push Stream Types
// Options and result types for Stream.push()
// =============================================================================

/**
 * Backpressure policy when buffer is full.
 */
export type BackpressurePolicy =
  | 'strict'       // Default: writes reject/throw when buffer full
  | 'block'        // Async writes block until space available; sync writes return false
  | 'drop-oldest'  // Drop oldest buffered chunks to make room
  | 'drop-newest'; // Drop incoming chunks when buffer full

/**
 * Options for Stream.push()
 */
export interface PushStreamOptions {
  /**
   * Max pending write/writev calls (chunk-oriented, not byte-oriented).
   * Default: 4.
   * Set to Infinity for unbounded buffering.
   */
  highWaterMark?: number;

  /**
   * Backpressure policy when buffer exceeds highWaterMark.
   * Default: 'strict'
   */
  backpressure?: BackpressurePolicy;

  /** Cancellation signal - aborts both writer and readable. */
  signal?: AbortSignal;
}

/**
 * Result of Stream.push() - a WriterIterablePair.
 */
export type PushStreamResult = WriterIterablePair;

// =============================================================================
// §8 Transform Types
// Types for transform functions in pipelines
// =============================================================================

/**
 * Transform output types - can yield nested iterables that get flattened.
 */
export type TransformYield = Uint8Array | string | Iterable<TransformYield>;

/**
 * Sync transform result - returned from transform functions.
 */
export type TransformResult =
  | Uint8Array[]
  | null
  | Iterable<TransformYield>
  | Generator<TransformYield, void, unknown>;

/**
 * Async transform result - can include promises and async iterables.
 */
export type AsyncTransformResult =
  | TransformResult
  | Promise<TransformResult>
  | AsyncIterable<TransformYield>
  | AsyncGenerator<TransformYield, void, unknown>;

/**
 * Options passed to transform functions by the pipeline.
 * Contains the pipeline's cancellation signal.
 */
export interface TransformCallbackOptions {
  /** Signal that fires when the pipeline is cancelled, errors, or the
   *  consumer stops iteration. */
  readonly signal: AbortSignal;
}

/**
 * Stateless transform - function called for each batch.
 * Receives chunks or null (flush signal), and pipeline options.
 */
export type TransformFn = (
  chunks: Uint8Array[] | null,
  options: TransformCallbackOptions
) => AsyncTransformResult;

/**
 * Stateful transform - generator that receives chunks and yields output.
 * Useful for compression, encryption, parsing, etc. that need state across chunks.
 */
export type StatefulTransformFn = (
  source: AsyncIterable<Uint8Array[] | null>,
  options: TransformCallbackOptions
) => AsyncIterable<TransformYield> | AsyncGenerator<TransformYield, void, unknown>;

/**
 * Transform object for stateful transforms.
 * Using an object (vs a plain function) indicates this is a stateful transform
 * that receives the entire source as an async iterable.
 */
export interface TransformObject {
  /** Stateful transform function that receives the entire source. */
  transform: StatefulTransformFn;
}

/**
 * Union type for transforms.
 * - Function: stateless, called once per batch
 * - Object: stateful, receives entire source as async iterable
 */
export type Transform = TransformFn | TransformObject;

// =============================================================================
// §9 Sync Transform Types
// Types for sync transform functions
// =============================================================================

/**
 * Sync transform result.
 */
export type SyncTransformResult =
  | Uint8Array[]
  | null
  | Iterable<TransformYield>
  | Generator<TransformYield, void, unknown>;

/**
 * Stateless sync transform.
 */
export type SyncTransformFn = (chunks: Uint8Array[] | null) => SyncTransformResult;

/**
 * Stateful sync transform.
 */
export type StatefulSyncTransformFn = (
  source: Iterable<Uint8Array[] | null>
) => Iterable<TransformYield> | Generator<TransformYield, void, unknown>;

/**
 * Sync transform object for stateful transforms.
 * Using an object (vs a plain function) indicates this is a stateful transform
 * that receives the entire source as an iterable.
 */
export interface SyncTransformObject {
  /** Stateful transform function that receives the entire source. */
  transform: StatefulSyncTransformFn;
}

/**
 * Union type for sync transforms.
 * - Function: stateless, called once per batch
 * - Object: stateful, receives entire source as iterable
 */
export type SyncTransform = SyncTransformFn | SyncTransformObject;

// =============================================================================
// §10 Pipeline and Consumer Options
// Options for pull(), pipeTo(), and consumer functions
// =============================================================================

/**
 * Options for Stream.pull()
 */
export interface PullOptions {
  /** Cancellation signal. */
  signal?: AbortSignal;
}

/**
 * Options for Stream.pipeTo()
 */
export interface PipeToOptions {
  /** Cancellation signal. */
  signal?: AbortSignal;

  /** If true, don't call writer.end() when source completes. */
  preventClose?: boolean;

  /** If true, don't call writer.fail() when source errors. */
  preventFail?: boolean;
}

/**
 * Options for Stream.pipeToSync()
 */
export interface PipeToSyncOptions {
  /** If true, don't call writer.end() when source completes. */
  preventClose?: boolean;

  /** If true, don't call writer.fail() when source errors. */
  preventFail?: boolean;
}

/**
 * Options for async consumer functions (bytes, text, arrayBuffer).
 */
export interface ConsumeOptions {
  /** Cancellation signal. */
  signal?: AbortSignal;

  /** Max bytes - throws if exceeded. */
  limit?: number;
}

/**
 * Options for sync consumer functions (bytesSync, textSync, arrayBufferSync).
 */
export interface ConsumeSyncOptions {
  /** Max bytes - throws if exceeded. */
  limit?: number;
}

/**
 * Options for text() - extends ConsumeOptions with encoding.
 */
export interface TextConsumeOptions extends ConsumeOptions {
  /**
   * Text encoding for decoding bytes. Any encoding supported by TextDecoder.
   * Default: 'utf-8'
   */
  encoding?: string;
}

/**
 * Options for textSync().
 */
export interface TextConsumeSyncOptions extends ConsumeSyncOptions {
  /** Text encoding. Default: 'utf-8' */
  encoding?: string;
}

/**
 * Options for Stream.merge()
 */
export interface MergeOptions {
  /** Cancellation signal. */
  signal?: AbortSignal;
}

// =============================================================================
// §11 Broadcast Types
// Multi-consumer broadcast channel (push model)
// =============================================================================

/**
 * Options for Stream.broadcast()
 */
export interface BroadcastOptions {
  /**
   * Max slots in buffer before backpressure is applied.
   * In strict mode, also limits pending writes to catch ignored backpressure.
   * Default: 16. Set to Infinity for unbounded (use with caution).
   */
  highWaterMark?: number;

  /**
   * Policy when buffer limit exceeded.
   * Default: 'strict'
   */
  backpressure?: BackpressurePolicy;

  /** Cancellation signal. */
  signal?: AbortSignal;
}

/**
 * Broadcast interface - multi-consumer channel.
 */
export interface Broadcast {
  /**
   * Create a new consumer.
   * Each call returns an independent async iterable from current buffer position.
   * Optional transforms are applied lazily when this consumer pulls.
   * 
   * Signature: push(...transforms, options?) where options is detected by
   * being an object without a 'transform' property.
   */
  push(...args: (Transform | PullOptions)[]): AsyncIterable<Uint8Array[]>;

  /** Number of currently attached consumers. */
  readonly consumerCount: number;

  /** Current buffer size (chunks waiting for slow consumers). */
  readonly bufferSize: number;

  /**
   * Cancel all branches.
   * If reason provided, branches see it as an error; otherwise clean completion.
   */
  cancel(reason?: any): void;

  /** Support for `using` - defers to cancel(). */
  [Symbol.dispose](): void;
}

/**
 * Result of Stream.broadcast() - writer + broadcast pair.
 */
export interface BroadcastResult {
  writer: Writer;
  broadcast: Broadcast;
}

// =============================================================================
// §12 Share Types
// Multi-consumer shared source (pull model)
// =============================================================================

/**
 * Options for Stream.share()
 */
export interface ShareOptions {
  /**
   * Max slots in buffer before backpressure is applied.
   * In strict mode, also limits pending writes to catch ignored backpressure.
   * Default: 16.
   */
  highWaterMark?: number;

  /**
   * Policy when buffer limit exceeded.
   * Default: 'strict'
   */
  backpressure?: BackpressurePolicy;

  /** Cancellation signal. */
  signal?: AbortSignal;
}

/**
 * Share interface - pull-model multi-consumer.
 */
export interface Share {
  /**
   * Create a consumer - signature matches Stream.pull() (minus source).
   * Variadic: transforms, then options (optional).
   * Options detected by being an object without a 'transform' property.
   */
  pull(...args: (Transform | PullOptions)[]): AsyncIterable<Uint8Array[]>;

  /** Number of currently attached consumers. */
  readonly consumerCount: number;

  /** Current buffer size. */
  readonly bufferSize: number;

  /**
   * Cancel all branches and close source.
   * If reason provided, branches see it as an error; otherwise clean completion.
   */
  cancel(reason?: any): void;

  /** Support for `using`. */
  [Symbol.dispose](): void;
}

/**
 * Options for Stream.shareSync()
 */
export interface ShareSyncOptions {
  /**
   * Max slots in buffer before backpressure is applied.
   * Default: 16.
   */
  highWaterMark?: number;
  backpressure?: BackpressurePolicy;
}

/**
 * Sync share interface.
 */
export interface SyncShare {
  /**
   * Create a consumer - signature matches Stream.pullSync() (minus source).
   * Variadic: transforms only (no options for sync).
   */
  pull(...transforms: SyncTransform[]): Iterable<Uint8Array[]>;

  readonly consumerCount: number;
  readonly bufferSize: number;

  /** Cancel all branches and close source. */
  cancel(reason?: any): void;

  /** Support for `using`. */
  [Symbol.dispose](): void;
}

// =============================================================================
// §13 Protocol Interfaces
// Allow custom types to participate in broadcast/share
// =============================================================================

/**
 * Broadcastable protocol - object can provide a Broadcast.
 */
export interface Broadcastable {
  [broadcastProtocol](options?: BroadcastOptions): Broadcast;
}

/**
 * Shareable protocol - object can provide a Share.
 */
export interface Shareable {
  [shareProtocol](options?: ShareOptions): Share;
}

/**
 * SyncShareable protocol - object can provide a SyncShare.
 */
export interface SyncShareable {
  [shareSyncProtocol](options?: ShareSyncOptions): SyncShare;
}

/**
 * Drainable protocol - object can signal when ready for more writes.
 * Used to bridge event-driven sources that need drain notification.
 * 
 * The returned Promise resolves with:
 * - `true` if backpressure is cleared and writes may be accepted
 * - `false` if the writer closed while waiting (no more writes accepted)
 * 
 * The Promise rejects if the writer fails/errors while waiting.
 * 
 * Returns `null` if drain is not applicable (e.g., desiredSize is already null).
 * 
 * Note: Due to TOCTOU races, callers should still check desiredSize and
 * await writes even after the drain promise resolves with true.
 */
export interface Drainable {
  [drainableProtocol](): Promise<boolean> | null;
}

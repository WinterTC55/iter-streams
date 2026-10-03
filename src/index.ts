/**
 * This is a bytes-only stream API that provides:
 * - Simpler semantics than Web Streams
 * - Async/sync iteration as the primary interface
 * - Batched chunks (Uint8Array[]) to amortize async overhead
 * - Explicit backpressure handling
 * - Protocol-based extensibility
 */

// Re-export all types
export type {
  // Primitive types
  PrimitiveChunk,
  ByteInput,

  // Streamable types
  StreamableChunk,
  SyncStreamableYield,
  AsyncStreamableYield,
  SyncStreamable,
  AsyncStreamable,
  Streamable,

  // Protocol interfaces
  ToStreamable,
  ToAsyncStreamable,
  Broadcastable,
  Shareable,
  SyncShareable,
  Drainable,

  // Writer interfaces
  WriteOptions,
  Writer,
  SyncWriter,
  ByteStreamReadable,
  SyncByteStreamReadable,
  WriterIterablePair,
  SyncWriterIterablePair,

  // Duplex types
  DuplexChannel,
  DuplexDirectionOptions,
  DuplexOptions,

  // Push stream types
  BackpressurePolicy,
  PushStreamOptions,
  PushStreamResult,

  // Transform types
  TransformYield,
  TransformResult,
  AsyncTransformResult,
  TransformCallbackOptions,
  TransformFn,
  StatefulTransformFn,
  TransformObject,
  Transform,

  // Sync transform types
  SyncTransformResult,
  SyncTransformFn,
  StatefulSyncTransformFn,
  SyncTransformObject,
  SyncTransform,

  // Options types
  PullOptions,
  PipeToOptions,
  PipeToSyncOptions,
  ConsumeOptions,
  ConsumeSyncOptions,
  TextConsumeOptions,
  TextConsumeSyncOptions,
  MergeOptions,

  // Broadcast types
  BroadcastOptions,
  Broadcast as BroadcastInterface,
  BroadcastResult,

  // Share types
  ShareOptions,
  Share as ShareInterface,
  ShareSyncOptions,
  SyncShare as SyncShareInterface,
} from './types.js';

// Export protocol symbols
export {
  toStreamable,
  toAsyncStreamable,
  broadcastProtocol,
  shareProtocol,
  shareSyncProtocol,
  drainableProtocol,
} from './types.js';

export { push } from './push.js';
export { duplex } from './duplex.js';
export { from, fromSync } from './from.js';
export { pull, pullSync, pipeTo, pipeToSync } from './pull.js';
export { broadcast, Broadcast } from './broadcast.js';
export { share, shareSync, Share, SyncShare } from './share.js';

// =============================================================================
// Stream Namespace - Unified API access point
// =============================================================================

import { push } from './push.js';
import { duplex } from './duplex.js';
import { from, fromSync } from './from.js';
import { pull, pullSync, pipeTo, pipeToSync } from './pull.js';
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
  ondrain,
} from './consumers.js';
import { broadcast } from './broadcast.js';
import { share, shareSync } from './share.js';
import {
  toStreamable,
  toAsyncStreamable,
  broadcastProtocol,
  shareProtocol,
  shareSyncProtocol,
  drainableProtocol,
} from './types.js';

/**
 * Stream namespace - unified access to all stream functions.
 *
 * @example
 * ```typescript
 * import { Stream } from 'new-streams';
 *
 * // Push stream
 * const { writer, readable } = Stream.push();
 * await writer.write("hello");
 * await writer.end();
 *
 * // Pull pipeline
 * const output = Stream.pull(readable, transform1, transform2);
 *
 * // Consume
 * const data = await Stream.bytes(output);
 * const content = await Stream.text(output);
 * ```
 */
export const Stream = {
  // Factories
  push,
  duplex,
  from,
  fromSync,

  // Pipelines
  pull,
  pullSync,

  // Pipe to destination
  pipeTo,
  pipeToSync,

  // Consumers (async)
  bytes,
  text,
  arrayBuffer,
  array,
  dump,

  // Consumers (sync)
  bytesSync,
  textSync,
  arrayBufferSync,
  arraySync,
  dumpSync,

  // Combining
  merge,

  // Multi-consumer (push model)
  broadcast,

  // Multi-consumer (pull model)
  share,
  shareSync,

  // Utilities
  tap,
  tapSync,

  // Drain utility for event source integration
  ondrain,

  // Protocol symbols (for custom implementations)
  toStreamable,
  toAsyncStreamable,
  broadcastProtocol,
  shareProtocol,
  shareSyncProtocol,
  drainableProtocol,
} as const;

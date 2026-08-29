/**
 * Pull Pipeline - pull(), pullSync(), pipeTo(), pipeToSync()
 *
 * Pull-through pipelines with transforms. Data flows on-demand from source
 * through transforms to consumer.
 */

import {
  type Streamable,
  type SyncStreamable,
  type ByteStreamReadable,
  type SyncByteStreamReadable,
  type Transform,
  type TransformObject,
  type TransformFn,
  type TransformCallbackOptions,
  type StatefulTransformFn,
  type TransformYield,
  type AsyncStreamableYield,
  type AsyncTransformResult,
  type SyncTransform,
  type SyncTransformObject,
  type SyncTransformFn,
  type StatefulSyncTransformFn,
  type SyncTransformResult,
  type PullOptions,
  type PipeToOptions,
  type PipeToSyncOptions,
  type Writer,
  type SyncWriter,
} from './types.js';

import {
  normalizeAsyncSource,
  normalizeSyncSource,
  isSyncIterable,
  isAsyncIterable,
} from './from.js';

import { isPullOptions, parsePullArgs } from './utils.js';

// Shared TextEncoder instance for string conversion
const encoder = new TextEncoder();

// =============================================================================
// Type Guards and Helpers
// =============================================================================

/**
 * Check if a value is a TransformObject (has transform property).
 */
function isTransformObject(value: unknown): value is TransformObject {
  return (
    value !== null &&
    typeof value === 'object' &&
    'transform' in value &&
    typeof (value as TransformObject).transform === 'function'
  );
}

/**
 * Check if a value is a SyncTransformObject.
 */
function isSyncTransformObject(value: unknown): value is SyncTransformObject {
  return isTransformObject(value);
}



/**
 * Check if a value is a Writer (has write method).
 */
function isWriter(value: unknown): value is Writer {
  return (
    value !== null &&
    typeof value === 'object' &&
    'write' in value &&
    typeof (value as Writer).write === 'function'
  );
}


/**
 * Parse variadic arguments for pipeTo/pipeToSync.
 * Returns { transforms, writer, options }
 */
function parsePipeToArgs<
  T extends Transform | SyncTransform,
  W extends Writer | SyncWriter,
  O extends PipeToOptions | PipeToSyncOptions
>(
  args: (T | W | O)[]
): { transforms: T[]; writer: W; options: O | undefined } {
  if (args.length === 0) {
    throw new TypeError('pipeTo requires a writer argument');
  }

  let options: O | undefined;
  let writerIndex = args.length - 1;

  // Check if last arg is options
  const last = args[args.length - 1];
  if (isPullOptions(last) && !isWriter(last)) {
    options = last as O;
    writerIndex = args.length - 2;
  }

  if (writerIndex < 0) {
    throw new TypeError('pipeTo requires a writer argument');
  }

  const writer = args[writerIndex];
  if (!isWriter(writer)) {
    throw new TypeError('pipeTo requires a writer argument with a write method');
  }

  return {
    transforms: args.slice(0, writerIndex) as T[],
    writer: writer as W,
    options,
  };
}

// =============================================================================
// Transform Output Flattening
// =============================================================================

/**
 * Flatten transform yield to Uint8Array chunks (sync).
 */
function* flattenTransformYieldSync(value: TransformYield): Generator<Uint8Array> {
  if (value instanceof Uint8Array) {
    yield value;
    return;
  }
  if (typeof value === 'string') {
    yield encoder.encode(value);
    return;
  }
  // Must be Iterable<TransformYield>
  if (isSyncIterable(value)) {
    for (const item of value) {
      yield* flattenTransformYieldSync(item as TransformYield);
    }
    return;
  }
  throw new TypeError(`Invalid transform yield type: ${typeof value}`);
}

/**
 * Flatten transform yield to Uint8Array chunks (async).
 */
async function* flattenTransformYieldAsync(
  value: TransformYield | AsyncIterable<TransformYield>
): AsyncGenerator<Uint8Array> {
  if (value instanceof Uint8Array) {
    yield value;
    return;
  }
  if (typeof value === 'string') {
    yield encoder.encode(value);
    return;
  }
  // Check for async iterable first
  if (isAsyncIterable(value)) {
    for await (const item of value) {
      yield* flattenTransformYieldAsync(item as TransformYield);
    }
    return;
  }
  // Must be sync Iterable<TransformYield>
  if (isSyncIterable(value)) {
    for (const item of value) {
      yield* flattenTransformYieldAsync(item as TransformYield);
    }
    return;
  }
  throw new TypeError(`Invalid transform yield type: ${typeof value}`);
}

/**
 * Process transform result (sync).
 */
function* processTransformResultSync(
  result: SyncTransformResult
): Generator<Uint8Array[]> {
  if (result === null) {
    return;
  }
  if (Array.isArray(result) && result.length > 0 && result[0] instanceof Uint8Array) {
    // Fast path: Uint8Array[]
    if (result.length > 0) {
      yield result as Uint8Array[];
    }
    return;
  }
  // Iterable or Generator
  if (isSyncIterable(result)) {
    const batch: Uint8Array[] = [];
    for (const item of result) {
      for (const chunk of flattenTransformYieldSync(item as TransformYield)) {
        batch.push(chunk);
      }
    }
    if (batch.length > 0) {
      yield batch;
    }
    return;
  }
  throw new TypeError(`Invalid transform result type`);
}

/**
 * Process transform result (async).
 */
async function* processTransformResultAsync(
  result: AsyncTransformResult
): AsyncGenerator<Uint8Array[]> {
  // Handle Promise
  if (result instanceof Promise) {
    const resolved = await result;
    yield* processTransformResultAsync(resolved);
    return;
  }
  if (result === null) {
    return;
  }
  if (Array.isArray(result) && (result.length === 0 || result[0] instanceof Uint8Array)) {
    // Fast path: Uint8Array[]
    if (result.length > 0) {
      yield result as Uint8Array[];
    }
    return;
  }
  // Check for async iterable/generator first
  if (isAsyncIterable(result)) {
    const batch: Uint8Array[] = [];
    for await (const item of result) {
      // Fast path: item is already Uint8Array
      if (item instanceof Uint8Array) {
        batch.push(item);
        continue;
      }
      // Slow path: flatten the item
      for await (const chunk of flattenTransformYieldAsync(item as TransformYield)) {
        batch.push(chunk);
      }
    }
    if (batch.length > 0) {
      yield batch;
    }
    return;
  }
  // Sync Iterable or Generator
  if (isSyncIterable(result)) {
    const batch: Uint8Array[] = [];
    for (const item of result) {
      // Fast path: item is already Uint8Array
      if (item instanceof Uint8Array) {
        batch.push(item);
        continue;
      }
      // Slow path: flatten the item
      for await (const chunk of flattenTransformYieldAsync(item as TransformYield)) {
        batch.push(chunk);
      }
    }
    if (batch.length > 0) {
      yield batch;
    }
    return;
  }
  throw new TypeError(`Invalid transform result type`);
}

// =============================================================================
// Sync Pipeline Implementation
// =============================================================================

/**
 * Apply a single stateless sync transform to a source.
 */
function* applyStatelessSyncTransform(
  source: Iterable<Uint8Array[] | null>,
  transform: SyncTransformFn
): Generator<Uint8Array[]> {
  for (const chunks of source) {
    const result = transform(chunks);
    yield* processTransformResultSync(result);
  }
}

/**
 * Apply a single stateful sync transform to a source.
 */
function* applyStatefulSyncTransform(
  source: Iterable<Uint8Array[] | null>,
  transform: StatefulSyncTransformFn
): Generator<Uint8Array[]> {
  const output = transform(source);
  for (const item of output) {
    const batch: Uint8Array[] = [];
    for (const chunk of flattenTransformYieldSync(item as TransformYield)) {
      batch.push(chunk);
    }
    if (batch.length > 0) {
      yield batch;
    }
  }
}

/**
 * Wrap sync source to add null flush signal at end.
 */
function* withFlushSignalSync(source: Iterable<Uint8Array[]>): Generator<Uint8Array[] | null> {
  for (const batch of source) {
    yield batch;
  }
  yield null; // Flush signal
}

/**
 * Create a sync pipeline from source through transforms.
 */
function* createSyncPipeline(
  source: SyncStreamable,
  transforms: SyncTransform[]
): Generator<Uint8Array[]> {
  // Normalize source
  let current: Iterable<Uint8Array[] | null> = withFlushSignalSync(normalizeSyncSource(source));

  // Apply transforms
  // Object = stateful, function = stateless
  for (const transform of transforms) {
    if (isSyncTransformObject(transform)) {
      current = applyStatefulSyncTransform(current, transform.transform);
    } else {
      current = applyStatelessSyncTransform(current, transform);
    }
  }

  // Yield results (filter out null from final output)
  for (const batch of current) {
    if (batch !== null) {
      yield batch;
    }
  }
}

// =============================================================================
// Async Pipeline Implementation
// =============================================================================

/**
 * Check if result is already a Uint8Array[] (fast path).
 */
function isUint8ArrayBatch(result: unknown): result is Uint8Array[] {
  if (!Array.isArray(result)) return false;
  if (result.length === 0) return true;
  return result[0] instanceof Uint8Array;
}

/**
 * Apply a single stateless async transform to a source.
 */
async function* applyStatelessAsyncTransform(
  source: AsyncIterable<Uint8Array[] | null>,
  transform: TransformFn,
  options: TransformCallbackOptions
): AsyncGenerator<Uint8Array[]> {
  for await (const chunks of source) {
    const result = transform(chunks, options);
    // Fast path: result is already Uint8Array[] (common case)
    if (result === null) continue;
    if (isUint8ArrayBatch(result)) {
      if (result.length > 0) {
        yield result;
      }
      continue;
    }
    // Handle Promise of Uint8Array[]
    if (result instanceof Promise) {
      const resolved = await result;
      if (resolved === null) continue;
      if (isUint8ArrayBatch(resolved)) {
        if (resolved.length > 0) {
          yield resolved;
        }
        continue;
      }
      // Fall through to slow path
      yield* processTransformResultAsync(resolved);
      continue;
    }
    // Fast path: sync generator/iterable - collect all yielded items into batches
    // This avoids the overhead of processTransformResultAsync for simple generators
    if (isSyncIterable(result) && !isAsyncIterable(result)) {
      const batch: Uint8Array[] = [];
      for (const item of result as Iterable<unknown>) {
        // Fast path: item is Uint8Array[] batch (common for generators that yield batches)
        if (isUint8ArrayBatch(item)) {
          batch.push(...(item as Uint8Array[]));
        } else if (item instanceof Uint8Array) {
          // Single Uint8Array
          batch.push(item);
        } else if (item !== null && item !== undefined) {
          // Other item types - flatten and add to batch
          for await (const chunk of flattenTransformYieldAsync(item as TransformYield)) {
            batch.push(chunk);
          }
        }
      }
      if (batch.length > 0) {
        yield batch;
      }
      continue;
    }
    // Slow path for other types (async iterables, complex nested structures)
    yield* processTransformResultAsync(result);
  }
}

/**
 * Apply a single stateful async transform to a source.
 */
async function* applyStatefulAsyncTransform(
  source: AsyncIterable<Uint8Array[] | null>,
  transform: StatefulTransformFn,
  options: TransformCallbackOptions
): AsyncGenerator<Uint8Array[]> {
  const output = transform(source, options);
  for await (const item of output) {
    const batch: Uint8Array[] = [];
    for await (const chunk of flattenTransformYieldAsync(item)) {
      batch.push(chunk);
    }
    if (batch.length > 0) {
      yield batch;
    }
  }
}

/**
 * Wrap async source to add null flush signal at end.
 */
async function* withFlushSignalAsync(
  source: AsyncIterable<Uint8Array[]>
): AsyncGenerator<Uint8Array[] | null> {
  for await (const batch of source) {
    yield batch;
  }
  yield null; // Flush signal
}

/**
 * Convert sync iterable to async iterable.
 */
async function* syncToAsync<T>(source: Iterable<T>): AsyncGenerator<T> {
  for (const item of source) {
    yield item;
  }
}

/**
 * Return the standard abort reason for a signal.
 */
function getAbortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException('Aborted', 'AbortError');
}

/**
 * Race an operation with an abort signal.
 */
function raceWithAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(getAbortReason(signal));
  }

  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(getAbortReason(signal));
    };

    signal.addEventListener('abort', onAbort, { once: true });

    operation.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

/**
 * Wrap an async iterable so pending next() calls reject on abort.
 */
async function* abortableAsyncIterable<T>(
  source: AsyncIterable<T>,
  signal: AbortSignal
): AsyncGenerator<T> {
  const iterator = source[Symbol.asyncIterator]();
  let completed = false;

  try {
    while (true) {
      const result = await raceWithAbort(Promise.resolve(iterator.next()), signal);
      if (result.done) {
        completed = true;
        return;
      }
      yield result.value;
    }
  } finally {
    if (!completed && typeof iterator.return === 'function') {
      try {
        const returned = Promise.resolve(iterator.return());
        if (!signal.aborted) {
          await returned;
        } else {
          returned.catch(() => undefined);
        }
      } catch {
        // Suppress return() failures during teardown.
      }
    }
  }
}

/**
 * Create an async pipeline from source through transforms.
 */
async function* createAsyncPipeline(
  source: Streamable,
  transforms: Transform[],
  signal?: AbortSignal
): AsyncGenerator<Uint8Array[]> {
  // Check for abort
  if (signal?.aborted) {
    throw getAbortReason(signal);
  }

  const controller = new AbortController();
  let abortHandler: (() => void) | undefined;
  if (signal) {
    abortHandler = () => {
      try {
        controller.abort(getAbortReason(signal));
      } catch { /* transform signal listeners may throw — suppress */ }
    };
    signal.addEventListener('abort', abortHandler);
  }

  // Normalize source to async
  let normalized: AsyncIterable<Uint8Array[]>;
  if (isAsyncIterable(source)) {
    normalized = normalizeAsyncSource(
      abortableAsyncIterable(
        source as AsyncIterable<AsyncStreamableYield>,
        controller.signal
      )
    );
  } else if (isSyncIterable(source)) {
    normalized = syncToAsync(normalizeSyncSource(source as SyncStreamable));
  } else {
    throw new TypeError('Source must be iterable');
  }

  // Fast path: no transforms, just yield normalized source directly
  if (transforms.length === 0) {
    let completed = false;
    try {
      for await (const batch of abortableAsyncIterable(normalized, controller.signal)) {
        yield batch;
      }
      completed = true;
    } finally {
      if (!completed && !controller.signal.aborted) {
        try {
          controller.abort(new DOMException('Aborted', 'AbortError'));
        } catch { /* transform signal listeners may throw — suppress */ }
      }
      if (signal && abortHandler) {
        signal.removeEventListener('abort', abortHandler);
      }
    }
    return;
  }

  // Add flush signal
  let current: AsyncIterable<Uint8Array[] | null> = withFlushSignalAsync(normalized);

  // Apply transforms — each gets its own options object
  // Object = stateful, function = stateless
  for (const transform of transforms) {
    const options: TransformCallbackOptions = { signal: controller.signal };
    if (isTransformObject(transform)) {
      current = applyStatefulAsyncTransform(current, transform.transform, options);
    } else {
      current = applyStatelessAsyncTransform(current, transform, options);
    }
  }

  // Yield results (filter out null from final output)
  let completed = false;
  try {
    for await (const batch of abortableAsyncIterable(current, controller.signal)) {
      if (batch !== null) {
        yield batch;
      }
    }
    completed = true;
  } catch (error) {
    if (!controller.signal.aborted) {
      try {
        controller.abort(error instanceof Error ? error : new Error(String(error)));
      } catch { /* transform signal listeners may throw — suppress */ }
    }
    throw error;
  } finally {
    if (!completed && !controller.signal.aborted) {
      try {
        controller.abort(new DOMException('Aborted', 'AbortError'));
      } catch { /* transform signal listeners may throw — suppress */ }
    }
    // Clean up user signal listener to prevent holding controller alive
    if (signal && abortHandler) {
      signal.removeEventListener('abort', abortHandler);
    }
  }
}

// =============================================================================
// Public API: pull() and pullSync()
// =============================================================================

/**
 * Create a sync pull-through pipeline with transforms.
 *
 * @param source - The sync streamable source
 * @param args - Variadic transforms
 * @returns A sync iterable yielding Uint8Array[] batches
 */
export function pullSync(
  source: SyncStreamable,
  ...transforms: SyncTransform[]
): SyncByteStreamReadable {
  return {
    *[Symbol.iterator]() {
      yield* createSyncPipeline(source, transforms);
    },
  };
}

/**
 * Create an async pull-through pipeline with transforms.
 *
 * @param source - The streamable source (sync or async)
 * @param args - Variadic transforms, with optional PullOptions as last argument
 * @returns An async iterable yielding Uint8Array[] batches
 */
export function pull(
  source: Streamable,
  ...args: (Transform | PullOptions)[]
): ByteStreamReadable {
  const { transforms, options } = parsePullArgs<Transform, PullOptions>(args);

  return {
    async *[Symbol.asyncIterator]() {
      yield* createAsyncPipeline(source, transforms, options?.signal);
    },
  };
}

// =============================================================================
// Public API: pipeTo() and pipeToSync()
// =============================================================================

/**
 * Write a sync source through transforms to a sync writer.
 *
 * @param source - The sync source yielding Uint8Array[] batches
 * @param args - Variadic transforms, writer (required), and optional options
 * @returns Total bytes written
 */
export function pipeToSync(
  source: Iterable<Uint8Array[]>,
  ...args: (SyncTransform | SyncWriter | PipeToSyncOptions)[]
): number {
  const { transforms, writer, options } = parsePipeToArgs<
    SyncTransform,
    SyncWriter,
    PipeToSyncOptions
  >(args);

  // Handle transform-writer: if writer has transform, apply it as last transform
  const finalTransforms = [...transforms];
  if (isTransformObject(writer)) {
    finalTransforms.push(writer as unknown as SyncTransform);
  }

  // Create pipeline
  const pipeline = finalTransforms.length > 0
    ? createSyncPipeline({ [Symbol.iterator]: () => source[Symbol.iterator]() }, finalTransforms)
    : source;

  let totalBytes = 0;

  try {
    for (const batch of pipeline) {
      for (const chunk of batch) {
        totalBytes += chunk.byteLength;
      }
      if ('writev' in writer && typeof writer.writev === 'function') {
        writer.writev(batch);
      } else {
        for (const chunk of batch) {
          writer.write(chunk);
        }
      }
    }

    if (!options?.preventClose) {
      writer.end();
    }
  } catch (error) {
    if (!options?.preventFail) {
      writer.fail(error);
    }
    throw error;
  }

  return totalBytes;
}

/**
 * Write an async source through transforms to a writer.
 *
 * @param source - The source yielding Uint8Array[] batches (sync or async)
 * @param args - Variadic transforms, writer (required), and optional options
 * @returns Promise resolving to total bytes written
 */
export async function pipeTo(
  source: AsyncIterable<Uint8Array[]> | Iterable<Uint8Array[]>,
  ...args: (Transform | Writer | PipeToOptions)[]
): Promise<number> {
  const { transforms, writer, options } = parsePipeToArgs<Transform, Writer, PipeToOptions>(
    args
  );

  // Handle transform-writer: if writer has transform, apply it as last transform
  const finalTransforms = [...transforms];
  if (isTransformObject(writer)) {
    finalTransforms.push(writer as unknown as Transform);
  }

  const signal = options?.signal;

  // Check for abort
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException('Aborted', 'AbortError');
  }

  let totalBytes = 0;
  const hasWritev = typeof writer.writev === 'function';

  // Helper to write a batch efficiently - avoids per-chunk await when possible
  // A new WriteOptions object is created per call to avoid mutation concerns.
  const writeBatch = async (batch: Uint8Array[]): Promise<void> => {
    if (hasWritev && batch.length > 1) {
      await writer.writev!(batch, signal ? { signal } : undefined);
      for (const chunk of batch) {
        totalBytes += chunk.byteLength;
      }
    } else {
      // Write chunks, collecting any promises
      const promises: Promise<void>[] = [];
      for (const chunk of batch) {
        const result = writer.write(chunk, signal ? { signal } : undefined);
        if (result !== undefined) {
          promises.push(result);
        }
        totalBytes += chunk.byteLength;
      }
      // Await all promises at once if any were returned
      if (promises.length > 0) {
        await Promise.all(promises);
      }
    }
  };

  try {
    // Fast path: no transforms - iterate directly without pipeline wrapper
    if (finalTransforms.length === 0) {
      if (isAsyncIterable(source)) {
        // Async source, no transforms - iterate directly
        for await (const batch of source) {
          if (signal?.aborted) {
            throw signal.reason ?? new DOMException('Aborted', 'AbortError');
          }
          await writeBatch(batch);
        }
      } else {
        // Sync source, no transforms - no need for async wrapper
        for (const batch of source as Iterable<Uint8Array[]>) {
          if (signal?.aborted) {
            throw signal.reason ?? new DOMException('Aborted', 'AbortError');
          }
          await writeBatch(batch);
        }
      }
    } else {
      // Slow path: has transforms - need pipeline
      const streamableSource: Streamable = isAsyncIterable(source)
        ? { [Symbol.asyncIterator]: () => source[Symbol.asyncIterator]() }
        : { [Symbol.iterator]: () => (source as Iterable<Uint8Array[]>)[Symbol.iterator]() };

      const pipeline = createAsyncPipeline(streamableSource, finalTransforms, signal);

      for await (const batch of pipeline) {
        if (signal?.aborted) {
          throw signal.reason ?? new DOMException('Aborted', 'AbortError');
        }
        await writeBatch(batch);
      }
    }

    if (!options?.preventClose) {
      await writer.end(signal ? { signal } : undefined);
    }
  } catch (error) {
    if (!options?.preventFail) {
      await writer.fail(error instanceof Error ? error : new Error(String(error)));
    }
    throw error;
  }

  return totalBytes;
}

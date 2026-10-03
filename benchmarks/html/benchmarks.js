/**
 * Shared benchmark implementations for all.html
 * These mirror the individual benchmark files but are extracted for reuse.
 */

const enc = new TextEncoder();
const dec = new TextDecoder();

/**
 * Generate valid UTF-8 text chunks for text-based benchmarks
 */
export function generateTextChunks(chunkSize, numChunks) {
  const chunks = [];
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ';
  for (let i = 0; i < numChunks; i++) {
    let text = '';
    for (let j = 0; j < chunkSize; j++) {
      text += chars[Math.floor(Math.random() * chars.length)];
    }
    chunks.push(enc.encode(text));
  }
  return chunks;
}

// =============================================================================
// Throughput Benchmarks
// =============================================================================

export async function newStreamPushThroughput(Stream, chunks) {
  const { writer, readable } = Stream.push();
  (async () => {
    for (const chunk of chunks) await writer.write(chunk);
    await writer.end();
  })();
  return await Stream.bytes(readable);
}

export async function webStreamPushThroughput(chunks) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  (async () => {
    for (const chunk of chunks) await writer.write(chunk);
    await writer.close();
  })();
  const reader = readable.getReader();
  const result = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value);
  }
  return result;
}

export async function newStreamFromArray(Stream, chunks) {
  return await Stream.bytes(Stream.from(chunks));
}

export async function webStreamFromArray(chunks) {
  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  const reader = readable.getReader();
  const result = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value);
  }
  return result;
}

// =============================================================================
// Transform Benchmarks
// =============================================================================

export function createNewUppercase() {
  return function* (chunks) {
    if (chunks === null) return;
    for (const chunk of chunks) {
      yield enc.encode(dec.decode(chunk).toUpperCase());
    }
  };
}

export function createWebUppercase() {
  return new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(enc.encode(dec.decode(chunk).toUpperCase()));
    }
  });
}

export async function newStreamSingleTransform(Stream, chunks) {
  const readable = Stream.pull(Stream.from(chunks), createNewUppercase());
  return await Stream.bytes(readable);
}

export async function webStreamSingleTransform(chunks) {
  let readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  readable = readable.pipeThrough(createWebUppercase());
  const reader = readable.getReader();
  const result = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value);
  }
  return result;
}

export async function newStreamTripleTransform(Stream, chunks) {
  const readable = Stream.pull(
    Stream.from(chunks),
    createNewUppercase(),
    createNewUppercase(),
    createNewUppercase()
  );
  return await Stream.bytes(readable);
}

export async function webStreamTripleTransform(chunks) {
  let readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  readable = readable
    .pipeThrough(createWebUppercase())
    .pipeThrough(createWebUppercase())
    .pipeThrough(createWebUppercase());
  const reader = readable.getReader();
  const result = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value);
  }
  return result;
}

// =============================================================================
// Consumption Benchmarks
// =============================================================================

export async function newStreamBytes(Stream, chunks) {
  return await Stream.bytes(Stream.from(chunks));
}

export async function webStreamBytes(chunks) {
  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  const reader = readable.getReader();
  const result = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value);
  }
  const totalLen = result.reduce((sum, arr) => sum + arr.length, 0);
  const bytes = new Uint8Array(totalLen);
  let offset = 0;
  for (const arr of result) {
    bytes.set(arr, offset);
    offset += arr.length;
  }
  return bytes;
}

export async function newStreamText(Stream, chunks) {
  return await Stream.text(Stream.from(chunks));
}

export async function webStreamText(chunks) {
  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  const reader = readable.getReader();
  const decoder = new TextDecoder();
  let text = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  text += decoder.decode();
  return text;
}

// =============================================================================
// Branching Benchmarks
// =============================================================================

export async function newStreamBroadcast2(Stream, chunks) {
  const { writer, channel } = Stream.broadcast({ highWaterMark: 1000 });
  const c1 = channel.push();
  const c2 = channel.push();
  (async () => {
    for (const chunk of chunks) await writer.write(chunk);
    await writer.end();
  })();
  // Use array() to avoid concat overhead
  const [a1, a2] = await Promise.all([Stream.array(c1), Stream.array(c2)]);
  return a1.length + a2.length;
}

export async function webStreamTee2(chunks) {
  const readable = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    }
  });
  const [branch1, branch2] = readable.tee();
  
  async function collect(r) {
    const reader = r.getReader();
    const result = [];
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      result.push(value);
    }
    return result;
  }
  
  const [r1, r2] = await Promise.all([collect(branch1), collect(branch2)]);
  return r1.length + r2.length;
}

// =============================================================================
// Sync Pipeline Benchmarks
// =============================================================================

function collectSync(source) {
  const result = [];
  for (const batch of source) {
    for (const chunk of batch) {
      result.push(chunk);
    }
  }
  const totalLen = result.reduce((sum, arr) => sum + arr.length, 0);
  const bytes = new Uint8Array(totalLen);
  let offset = 0;
  for (const arr of result) {
    bytes.set(arr, offset);
    offset += arr.length;
  }
  return bytes;
}

export function newStreamSyncPipeline(Stream, chunks) {
  const source = Stream.fromSync(chunks);
  const pipeline = Stream.pullSync(source, createNewUppercase());
  return collectSync(pipeline);
}

export async function newStreamAsyncPipeline(Stream, chunks) {
  const source = Stream.pull(Stream.from(chunks), createNewUppercase());
  return await Stream.bytes(source);
}

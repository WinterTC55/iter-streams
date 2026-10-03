# Transfer Protocol Integration with the New Streams API

**Status:** Design Discussion
**Date:** February 2026
**Related:** [Transfer Protocol Proposal](https://github.com/jasnell/proposal-transfer-protocol), [API.md](../API.md), [DESIGN.md](DESIGN.md)

This document explores how a **As-yet no-official-status** [TC-39 Transfer Protocol pre-proposal](https://github.com/jasnell/proposal-transfer-protocol) (`Symbol.transfer` / `Object.transfer()`) could apply to the new streams API. The Transfer Protocol would offer an ownership primitive that could complement the new streams API's intentional omission of the Web Streams locking model.

---

## Table of Contents

1. [Introduction & Motivation](#1-introduction--motivation)
2. [Transferable Types](#2-transferable-types)
3. [Transfer Semantics per Type](#3-transfer-semantics-per-type)
4. [API Integration Points — Alternatives](#4-api-integration-points--alternatives)
5. [Interaction with Existing Semantics](#5-interaction-with-existing-semantics)
6. [Transfer vs Locking](#6-transfer-vs-locking)
7. [Usage Patterns](#7-usage-patterns)
8. [Open Questions](#8-open-questions)
9. [Polyfill Feasibility](#9-polyfill-feasibility)

---

## 1. Introduction & Motivation

### The Ownership Gap

The new streams API treats readables as plain `AsyncIterable<Uint8Array[]>`. There is no `ReadableStream` class and no locking. This is an intentional design choice. The Web Streams locking model is replaced by standard JavaScript iteration protocols.

Iterables have no notion of exclusive access. Multiple consumers can share an iterator reference and interleave `.next()` calls:

```js
const { writer, readable } = Stream.push();

// Two consumers race on the same iterable, data split unpredictably
consumer1(readable);
consumer2(readable);
```

Passing sources to pipeline functions has the same issue:

```js
const source = asyncGenerator();
const pipeline = Stream.pull(source, compress);

// The caller still has a reference to source's iterator.
// This silently steals data from the pipeline:
await source.next();
```

Web Streams solves this with locking: `getReader()` acquires an exclusive lock, and nothing else can read until `releaseLock()` is called. Locking works well in practice for many use cases, but it is an acquire/release model that requires managing reader lifecycle. The new streams API intentionally avoids this model.

### Ownership vs Locking

A stream consumer needs a guarantee of exclusive ownership: no other code can interfere with its iteration. Locking achieves this. Transfer achieves it differently.

### The Transfer Protocol

The [Transfer Protocol proposal](https://github.com/jasnell/proposal-transfer-protocol) introduces a generalized ownership transfer mechanism for JavaScript:

- `Symbol.transfer` — a well-known symbol for the transfer protocol
- `Object.transfer(obj)` — moves ownership from `obj` to a new object, detaching the original
- Post-transfer, the original is permanently unusable (operational methods throw `TypeError`)

Transfer is a one-shot operation: state moves from the old owner to the new. Post-transfer, the original's operational methods throw `TypeError`.

This is a primitive the new streams API could use. Where Web Streams uses locking (acquire �� use → release), the new API could use transfer (move → use). Transfer is permanent and one-directional per call, though a callee can always transfer back via a return value (see Section 6).

### Relationship Between the Proposals

The Transfer Protocol would be a general-purpose TC-39 language proposal. The new streams API is a domain-specific streaming primitive. They are independently useful but complementary:

- The Transfer Protocol provides the **mechanism** (ownership move with detach)
- The new streams API provides the **types** that implement it (readables, writers, channels)
- Together, they address the same problem Web Streams locking solves, with different tradeoffs

The new streams API does not depend on the Transfer Protocol. It works without it, relying on the convention that consumers don't share iterators. But with the Transfer Protocol available, the convention becomes enforceable.

---

## 2. Transferable Types

This section identifies which types in the new streams API would implement `[Symbol.transfer]()` and what value transfer provides for each.

### Overview

| Type | Transferable? | Rationale |
|------|:---:|-----------|
| Push stream readable | Yes | Single-consumer async iterable; transfer enforces exclusive iteration |
| Writer (from `Stream.push()`) | Yes | Single-owner write endpoint; transfer moves write authority |
| DuplexChannel | Yes | Bundles writer + readable for one endpoint; single unit of ownership |
| Share consumer (from `share.pull()`) | Yes | Each consumer iterable is single-consumer |
| Broadcast consumer (from `channel.push()`) | Yes | Each consumer iterable is single-consumer |
| Share instance | Possible | Multi-consumer wrapper owns the source; transfer moves management authority |
| Broadcast instance | Possible | Less clear value; the writer side is the primary ownership concern |
| `WriterIterablePair` (from `Stream.push()`) | Possible | Atomic transfer of both writer and readable together |
| Iterables from `Stream.pull()` / `Stream.from()` | Inherited | These return async generators; transfer depends on `AsyncIterator.prototype[Symbol.transfer]()` from a possible TC-39 proposal |

### Why Not Everything?

Some types are intentionally excluded:

- **Transform functions/objects**: Transforms are stateless (functions) or stateful (generator-wrapping objects). They don't hold stream state. They process data that flows through them. Transferring a transform doesn't serve an ownership purpose.

- **Backpressure policies, options objects**: Pure configuration, not stateful resources.

- **The `Stream` namespace itself**: It's a collection of static functions, not a stateful object.

### Inherited Transferability

The iterables returned by `Stream.pull()` and `Stream.from()` are async generators. If the TC-39 Transfer Protocol proposal adds `[Symbol.transfer]()` to `AsyncIterator.prototype` (as specified), these iterables would become transferable automatically, with no changes to the streams API needed. This is a consequence of building on language primitives rather than custom classes.

Similarly, sync iterables from `Stream.pullSync()` and `Stream.fromSync()` would inherit from `Iterator.prototype[Symbol.transfer]()`.

---

## 3. Transfer Semantics per Type

### 3.1 Push Stream Readable

The readable returned by `Stream.push()` is an `AsyncIterable<Uint8Array[]>` backed by internal queue state (pending reads, buffer slots, consumer state).

**Transfer behavior:**

1. A new readable is created with the original's internal queue state (buffer contents, pending reads, consumer state, reference to the shared `PushQueue`).
2. The original is detached: its `[Symbol.asyncIterator]()` and any existing iterator's `.next()` throw `TypeError`.
3. The new owner can iterate the readable and receive all data (including anything already buffered).

**Post-transfer behavior:**

| Operation on detached readable | Behavior |
|--------------------------------|----------|
| `[Symbol.asyncIterator]()` | Throws `TypeError` |
| `for await...of` | Throws `TypeError` (via `[Symbol.asyncIterator]()`) |
| `[Symbol.transfer]()` | Throws `TypeError` (already detached) |

**Note on `[Symbol.asyncDispose]()`:** The `ByteStreamReadable` interface does not currently extend `AsyncDisposable`. If transfer support is added to readables, implementing `[Symbol.asyncDispose]()` as a no-op on detached instances would enable `await using` patterns (see Section 7.4). This would be a new requirement on the readable type. Alternatively, callers can wrap readables with `await using` only when they know the concrete type supports it (e.g., `DuplexChannel.readable`).

The readable is internally bonded to its writer via a shared `PushQueue`. Transfer moves the consumer end of that bond; the writer continues writing to the same queue. `ReadableStream` transfer across realms works the same way: the producer side stays, the consumer side moves.

### 3.2 Writer

The writer from `Stream.push()` implements the `Writer` interface and is backed by the same `PushQueue` as its readable.

**Transfer behavior:**

1. A new writer is created with the original's queue reference, byte count, and state.
2. The original is detached: all write methods throw `TypeError`.
3. The new owner can write to the stream and call `end()` / `fail()`.

**Post-transfer behavior:**

| Operation on detached writer | Behavior |
|------------------------------|----------|
| `write()` / `writev()` | Throws `TypeError` |
| `writeSync()` / `writevSync()` | Throws `TypeError` |
| `end()` / `endSync()` | Throws `TypeError` |
| `fail()` | No-op (cleanup method) |
| `failSync()` | No-op (cleanup method) |
| `desiredSize` | Returns `null` |
| `[Symbol.transfer]()` | Throws `TypeError` (already detached) |

**Design note:** `fail()` is treated as a cleanup method (no-op on detached) rather than an operational method (throws on detached). This follows the Transfer Protocol's rule: "operational methods throw, cleanup methods no-op." A detached writer has no resources to fail; the new owner holds the queue reference.

The distinction between `end()` and `fail()` reflects their intent: `end()` is a **completion signal** — it writes a final state to the queue and transitions the stream to "ended." This is an operational action with observable effects on the consumer. Calling `end()` on a detached writer is a logic error (the caller believes they still own the write channel) and should surface immediately via `TypeError`. In contrast, `fail()` is a **cleanup action** — it says "something went wrong, discard everything." It is expected to be called defensively (e.g., in `catch` blocks, `finally` blocks, or disposal paths) where the caller may not know whether the writer is still valid. A no-op on detach avoids forcing callers to guard every cleanup path with a detach check.

### 3.3 DuplexChannel

A `DuplexChannel` bundles a writer (sends to the peer) and a readable (receives from the peer), plus a `close()` method. It implements `AsyncDisposable`.

**Transfer behavior:**

1. A new `DuplexChannel` is created with both the writer and readable state transferred atomically.
2. The original is detached: writer, readable, and `close()` all become inert.
3. The new owner has full control of this endpoint of the duplex connection.

**Post-transfer behavior:**

| Operation on detached channel | Behavior |
|-------------------------------|----------|
| `writer.write()` / `writev()` / `writeSync()` / `writevSync()` / `end()` / `endSync()` | Throws `TypeError` |
| `writer.fail()` / `failSync()` | No-op (cleanup methods, per Section 3.2) |
| `writer.desiredSize` | Returns `null` |
| `readable[Symbol.asyncIterator]()` | Throws `TypeError` |
| `close()` | No-op (cleanup method) |
| `[Symbol.asyncDispose]()` | No-op (delegates to `close()`) |
| `[Symbol.transfer]()` | Throws `TypeError` (already detached) |

**Atomic transfer:** Both the writer and readable must transfer together. A `DuplexChannel` where the writer has been transferred but the readable has not (or vice versa) would be an inconsistent state. The `[Symbol.transfer]()` implementation must move both atomically.

**Interaction with the peer:** Transferring one endpoint does not affect the other. The peer channel continues to function normally: it writes to the same queue that the new owner will read from, and reads from the same queue that the new owner will write to.

### 3.4 Share Consumer / Broadcast Consumer

Each call to `share.pull()` or `channel.push()` returns an `AsyncIterable<Uint8Array[]>` representing one consumer's view of the shared/broadcast data.

**Transfer behavior:**

1. A new consumer iterable is created with the original's cursor position in the shared buffer.
2. The original is detached.
3. The new owner continues reading from where the original left off.

**Post-transfer behavior:** Same as Push Stream Readable (Section 3.1).

**Multi-consumer interaction:** Transferring one consumer does not affect other consumers. The `Share` or `Broadcast` instance continues tracking all consumers (including the new owner, which replaces the original in the consumer set). The buffer trimming logic (advance past data all consumers have read) continues to work correctly.

### 3.5 Share / Broadcast Instances

Transferring the `Share` or `Broadcast` instance itself moves management authority: the ability to create new consumers, cancel all consumers, and (for `Broadcast`) the associated writer.

**Transfer behavior:**

1. A new `Share`/`Broadcast` is created with the original's buffer, consumer set, and source iterator reference.
2. The original is detached.
3. The new owner can call `pull()` (Share) or `push()` (Broadcast), `cancel()`, and access `consumerCount`/`bufferSize`.
4. Existing consumers continue functioning because they reference the shared buffer, not the `Share`/`Broadcast` wrapper.

**Post-transfer behavior:**

| Operation on detached Share/Broadcast | Behavior |
|---------------------------------------|----------|
| `pull()` (Share) / `push()` (Broadcast) | Throws `TypeError` |
| `cancel()` | No-op (cleanup method) |
| `[Symbol.dispose]()` | No-op (delegates to `cancel()`) |
| `consumerCount` | Returns `0` |
| `bufferSize` | Returns `0` |
| `[Symbol.transfer]()` | Throws `TypeError` (already detached) |

The value here is less clear than for other types. The primary ownership concern for `Share` is the source iterator it wraps, and that ownership is already managed internally. Transferring a `Share` instance moves the ability to create consumers and call `cancel()`, but existing consumers are unaffected either way.

### 3.6 WriterIterablePair (Atomic Push Transfer)

`Stream.push()` returns `{ writer, readable }`. These could be transferred individually, but there may be value in atomic pair transfer: moving both the writer and readable together in a single operation.

This would require `Stream.push()` to return an object that implements `[Symbol.transfer]()` and transfers both the writer and readable atomically, similar to how `DuplexChannel` works. The current API returns a plain object; this would need to become a concrete type with transfer support.

Individual transfer of writer and readable may be sufficient. Atomic pair transfer is only needed when both are being handed off as a unit.

---

## 4. API Integration Points — Alternatives

Several API functions accept source iterables or writers. When they do, they implicitly assume exclusive access to those resources. This section examines three approaches for how these functions interact with the Transfer Protocol.

### The Functions in Question

| Function | Accepts | Ownership expectation |
|----------|---------|----------------------|
| `Stream.pull(source, ...transforms)` | Iterable source | Exclusive iteration of source |
| `Stream.pipeTo(source, ...transforms, writer, options?)` | Iterable source + Writer destination | Exclusive iteration of source, exclusive write to destination |
| `Stream.share(source, options)` | Iterable source | Exclusive iteration of source (shared among consumers) |
| `Stream.from(input)` | Various source types | Exclusive iteration of the resulting iterable |
| `Stream.merge(...sources)` | Multiple iterable sources | Exclusive iteration of each source |
| `Stream.bytes(source)` / `Stream.text(source)` / etc. | Iterable source | Exclusive iteration until completion |

### Alternative A: Opt-In Transfer

The API does not automatically transfer sources. Users are expected to transfer sources themselves when exclusive ownership is important.

```js
// User explicitly transfers the source
const source = asyncGenerator();
const pipeline = Stream.pull(Object.transfer(source), compress);
// source is now detached; exclusive ownership enforced

// Without transfer, the API works but doesn't enforce ownership
const source2 = asyncGenerator();
const pipeline2 = Stream.pull(source2, compress);
// source2 is still usable; ownership violation possible but not prevented
```

**Advantages:**

- Compatible with all iterators, including duck-typed `{ next() {} }` objects that don't implement `[Symbol.transfer]()`
- No change to the API surface: functions accept anything iterable
- Callers know exactly what they're opting into
- Aligns with the API's "explicit over implicit" design philosophy
- Zero overhead when transfer isn't needed

**Disadvantages:**

- Doesn't prevent ownership bugs unless the caller remembers to transfer
- Ownership expectations are only documented, not enforced
- Every API that accepts a source must document the ownership expectation separately

**Documentation pattern:**

```js
/**
 * Stream.pull(source, ...transforms, options?)
 *
 * Creates a pull-through pipeline. The source is consumed exclusively
 * by the pipeline ��� concurrent access to the source from other code
 * will cause unpredictable behavior. Consider using Object.transfer()
 * to enforce exclusive ownership:
 *
 *   const pipeline = Stream.pull(Object.transfer(source), compress);
 */
```

### Alternative B: Auto-Transfer

The API automatically calls `Object.transfer()` on source iterators when the source implements `[Symbol.transfer]()`.

```js
const source = asyncGenerator();
const pipeline = Stream.pull(source, compress);
// Internally: const iter = Object.transfer(source[Symbol.asyncIterator]());
// source is now detached; automatic exclusive ownership

source.next(); // throws TypeError: Iterator has been detached
```

**Advantages:**

- Prevents ownership bugs by default
- No ceremony for the caller
- Matches how `ReadableStream.from()` could work with the Transfer Protocol (Use Case 4 in the proposal)

**Disadvantages:**

- Throws `TypeError` for non-transferable iterators (duck-typed `{ next() {} }` objects without `[Symbol.transfer]()`)
- Breaks the assumption that passing an object to a function doesn't invalidate it
- Surprising behavior: calling `Stream.pull(source)` has the side effect of detaching `source`
- Difficult to opt out when you intentionally want to retain a reference (e.g., for later cancellation via `.return()`)
- Source iterables are not necessarily iterators (they may create a fresh iterator via `[Symbol.asyncIterator]()`); auto-transfer of the iterable vs. the iterator has different semantics

**The iterable vs. iterator distinction matters:**

```js
// source is an iterable (has [Symbol.asyncIterator])
// Each call to [Symbol.asyncIterator]() creates a fresh iterator
const source = {
  [Symbol.asyncIterator]() { return asyncGenerator(); }
};

// Auto-transferring the iterable detaches the iterable wrapper
// Auto-transferring the iterator detaches only the current iterator
// The iterable can create new iterators from a new [Symbol.asyncIterator]() call
// Which one should auto-transfer target?
```

### Alternative C: Opt-In via Parameter

The API accepts a `transfer` option that triggers automatic transfer of the source.

```js
// Explicitly opt in to transfer
const source = asyncGenerator();
const pipeline = Stream.pull(source, compress, { transfer: true });
// source is detached

// Without the option, no transfer
const source2 = asyncGenerator();
const pipeline2 = Stream.pull(source2, compress);
// source2 is still usable
```

**Advantages:**

- Transfer only happens when requested
- Compatible with all iterators when `transfer` is not set
- API enforces ownership when the caller asks for it
- Single point of documentation (the `transfer` option) rather than scattered `Object.transfer()` calls

**Disadvantages:**

- Adds a new option to multiple API functions (`PullOptions`, `PipeToOptions`, `ShareOptions`, `MergeOptions`, `ConsumeOptions`)
- Still has the iterable vs. iterator ambiguity
- Still throws for non-transferable sources when `transfer: true`
- An option that mutates the input is unusual. Options typically configure the operation's behavior, not its side effects on arguments

**Affected option types:**

```typescript
interface PullOptions {
  signal?: AbortSignal;
  transfer?: boolean;  // New: transfer-own the source iterator
}

interface PipeToOptions {
  signal?: AbortSignal;
  preventClose?: boolean;
  preventFail?: boolean;
  transfer?: boolean;  // New
}

interface ShareOptions {
  highWaterMark?: number;
  backpressure?: BackpressurePolicy;
  signal?: AbortSignal;
  transfer?: boolean;  // New
}

interface MergeOptions {
  signal?: AbortSignal;
  transfer?: boolean;  // New
}

interface ConsumeOptions {
  signal?: AbortSignal;
  limit?: number;
  transfer?: boolean;  // New
}
```

### Summary

| Criterion | A: Opt-In | B: Auto-Transfer | C: Via Parameter |
|-----------|-----------|-------------------|------------------|
| Default safety | Low (convention only) | High (enforced) | Medium (opt-in) |
| Compatibility | All iterators | Transferable only | All by default |
| API surface change | None | None | New option on 5+ types |
| Caller ceremony | `Object.transfer()` call | None | `{ transfer: true }` |
| Surprise factor | None | High (detaches input) | Low (explicit) |
| Framework adoption | Frameworks add `Object.transfer()` where needed | Frameworks work automatically | Frameworks pass `{ transfer: true }` |

---

## 5. Interaction with Existing Semantics

### 5.1 Transfer and Backpressure Policies

Transfer moves ownership of a writer or readable; backpressure policies govern how data flows between them. These are orthogonal concerns, but some interactions need consideration.

**Transferring a writer mid-flow:**

```js
const { writer, readable } = Stream.push({
  highWaterMark: 10,
  backpressure: 'strict'
});

// Writer has pending writes in the queue
await writer.write(chunk1);
await writer.write(chunk2);

// Transfer the writer — new owner inherits the queue state
const owned = Object.transfer(writer);

// owned.desiredSize reflects the current buffer state (8 remaining)
// Pending writes continue to be delivered to the consumer
await owned.write(chunk3);
```

The backpressure policy doesn't change on transfer. It's a property of the `PushQueue`, not the writer. The new owner inherits the same `highWaterMark` and policy.

**Transferring a readable with unconsumed data:**

```js
const { writer, readable } = Stream.push({ highWaterMark: 5 });
await writer.write("a");
await writer.write("b");
await writer.write("c");

// Transfer readable — new owner gets the buffered data
const owned = Object.transfer(readable);
const text = await Stream.text(owned); // includes "a", "b", "c"
```

Buffered data transfers with the readable. The writer's `desiredSize` continues to reflect the actual buffer state regardless of which reference holds the readable.

### 5.2 Transfer and AbortSignal

When a push stream or pipeline is created with a `signal` option, the `AbortSignal` is registered internally on the `PushQueue` or pipeline. Transfer doesn't affect this registration.

```js
const controller = new AbortController();
const { writer, readable } = Stream.push({ signal: controller.signal });

const owned = Object.transfer(readable);

// Signal still works; it aborts the queue, which affects the new owner
controller.abort();
// owned iteration will throw AbortError
```

This is the expected behavior: the signal is bound to the stream's lifecycle, not to a particular reference. Whoever owns the readable sees the abort.

### 5.3 Transfer and `Stream.ondrain()`

Writers from `Stream.push()` implement the `Drainable` protocol. After transfer, the drainable protocol should follow the writer:

```js
const { writer, readable } = Stream.push({ highWaterMark: 4 });
const owned = Object.transfer(writer);

// ondrain works on the new owner
const canWrite = await Stream.ondrain(owned);

// ondrain on the detached writer returns null (desiredSize is null)
const result = await Stream.ondrain(writer); // null
```

### 5.4 Transfer and Consumer Termination

When a consumer breaks out of a `for await...of` loop, the readable's iterator `.return()` is called, which signals to the writer that the consumer has terminated. This interaction is unaffected by transfer. The consumer termination signal travels through the `PushQueue` regardless of which reference the consumer holds.

```js
const { writer, readable } = Stream.push();
const owned = Object.transfer(readable);

// Consumer breaks out of the loop on the transferred readable
for await (const chunks of owned) {
  break; // Calls owned's iterator .return()
}

// Writer sees desiredSize become null; consumer terminated
writer.desiredSize; // null
```

### 5.5 Transfer and Multi-Consumer Patterns

Transferring a consumer from `share.pull()` or `channel.push()` affects only that consumer. Other consumers are unaffected.

```js
const shared = Stream.share(source, { highWaterMark: 100 });

const consumer1 = shared.pull();
const consumer2 = shared.pull(decompress);

// Transfer consumer1 — consumer2 is unaffected
const owned = Object.transfer(consumer1);

// The share still tracks the transferred consumer (now owned by `owned`)
shared.consumerCount; // still 2

// consumer1 is detached — cannot iterate
for await (const chunks of consumer1) { /* throws TypeError */ }

// owned works normally
for await (const chunks of owned) { /* receives data */ }
```

### 5.6 Transfer and `using` / `await using`

The Transfer Protocol specifies that `[Symbol.dispose]()` and `[Symbol.asyncDispose]()` must be no-ops on detached objects. This aligns with the new streams API's existing disposal semantics:

```js
{
  // DuplexChannel supports await using
  await using channel = server;

  // Transfer the channel to a handler
  const owned = Object.transfer(channel);
  handleConnection(owned);

  // At block exit, channel[Symbol.asyncDispose]() is called.
  // channel is detached. This is a silent no-op.
  // The new owner (owned) is responsible for cleanup.
}
```

`Share` and `Broadcast` work the same way via `[Symbol.dispose]()`:

```js
{
  using shared = Stream.share(source, { highWaterMark: 100 });

  const owned = Object.transfer(shared);
  processShared(owned);

  // At block exit, shared[Symbol.dispose]() is called.
  // Detached, so no-op. owned is responsible for cancel().
}
```

---

## 6. Transfer vs Locking

### Side-by-Side Comparison

| Aspect | Web Streams Locking | Transfer Protocol |
|--------|--------------------|--------------------|
| **Mechanism** | `getReader()` / `releaseLock()` | `Object.transfer()` (one-shot) |
| **Reversible?** | Yes (release lock) | No (permanent detach) |
| **Scope** | `ReadableStream` / `WritableStream` classes | Any object implementing `[Symbol.transfer]()` |
| **State model** | Binary (`locked` / unlocked) | Binary (functional / detached) |
| **Multiple acquire/release cycles** | Yes | No — transfer once |
| **Forgetting to release** | Stream permanently locked (mitigated by `using`) | N/A — no release step |
| **Error message** | "Cannot obtain lock — stream is locked" | "Iterator has been detached" / "Resource has been detached" |
| **Concurrent safety check** | Per-operation lock check | Per-operation detach check |
| **Works across realms** | Yes (stream transfer via `postMessage`) | Yes (iterator transfer via `postMessage`, per companion proposal) |
| **Composability** | Must manage reader lifecycle alongside streams | `Object.transfer()` is a single expression |
| **Standard scope** | WHATWG Streams spec | possible TC-39 language standard |

### What Locking Provides That Transfer Doesn't

1. **Temporary exclusive access**: Locking allows borrowing: acquire a reader, do some work, release it, acquire again later. Transfer is one-directional per call, but a callee can transfer *back* to the caller via a return value:

   ```js
   function borrowIt(iter) {
     const owned = Object.transfer(iter);
     // ... use owned exclusively ...
     return Object.transfer(owned);
   }

   let source = getSource();
   source = borrowIt(Object.transfer(source));
   // source is usable again (new object, same underlying state)
   ```

   This achieves the same borrow-and-return pattern as locking, but with different tradeoffs: ownership is explicit at every call site (no hidden lock state), the caller gets back a new object (identity changes, see point 3), and there is no way to forget to "release" because the callee must return a value to hand ownership back.

2. **Lock introspection**: `stream.locked` tells you whether a stream is currently locked. There is no equivalent for transfer. You can't check whether an object has been transferred without trying to use it (and catching the `TypeError`). The Transfer Protocol does define a `detached` getter convention, but it's not part of the formal protocol.

3. **Same-object reuse**: With locking, the same `ReadableStream` object persists; it's just locked/unlocked. With transfer, a new object is created each time. This matters for identity checks (`===` comparisons, `WeakMap` keys). That said, this is already established behavior in the language: `ArrayBuffer.prototype.transfer()` works the same way (returns a new `ArrayBuffer`, detaches the original), and code that works with `ArrayBuffer` transfer has adapted to the identity change.

More broadly, transfer-with-detach semantics have extensive precedent across web platform APIs. The structured clone algorithm's transfer list (`structuredClone(value, { transfer: [...] })`, `postMessage(value, { transfer: [...] })`) has supported transferable objects since the early days of the `Worker` API. `ArrayBuffer`, `MessagePort`, `ReadableStream`, `WritableStream`, `TransformStream`, `OffscreenCanvas`, `ImageBitmap`, and `AudioData` (among others) are all transferable today, and the source is neutered/detached after transfer. Developers working with workers, `OffscreenCanvas`, or WebCodecs are already accustomed to this model. The Transfer Protocol proposal generalizes the same semantics from a web platform mechanism to a language-level protocol.

### What Transfer Provides That Locking Doesn't

1. **No acquire/release lifecycle**: Transfer is a single operation. There is no lock to release and no reader object to manage. (Though note that Explicit Resource Management via `using` also simplifies locking lifecycle: `using reader = stream.getReader()` auto-releases at block exit.)

2. **Generality**: Transfer works on any object that implements `[Symbol.transfer]()`, not just streams. Any type with ownership semantics (writers, channels, etc.) can participate in the same protocol.

3. **Language-level primitive**: Transfer is (proposed as) part of ECMAScript itself, not a web platform API. It works in any JavaScript environment, including embedded engines without a web platform. The tradeoff is that a language-level primitive requires significantly more effort to standardize and ship than a platform API feature. Locking is available today in all major engines; the Transfer Protocol must go through the TC-39 proposal process before any of the patterns in this document become possible.

4. **Composability with `using`**: Transfer integrates with Explicit Resource Management. A `using` binding + transfer expresses "I own this resource for this scope, and I'm handing it off":

```js
async function handleRequest(streamIn) {
  // Transfer-own the input stream
  await using stream = Object.transfer(streamIn);
  // streamIn is detached; stream is owned by this scope
  return await Stream.text(stream);
  // stream[Symbol.asyncDispose]() at block exit
}
```

5. **Protection of the underlying iterator**: Web Streams' locking protects the *stream* but not the *iterator underneath*. Any code retaining a reference to the iterator passed to `ReadableStream.from()` can bypass the lock. Transfer detaches the iterator itself, so *when used*, the protection extends to the actual data source rather than just a wrapper around it. However, transfer is not a silver bullet — it only protects when the developer actually calls `Object.transfer()`. If a caller forgets to transfer, or chooses not to, the iterator remains shared and the same concurrent-access bugs are possible. Transfer makes correct ownership *expressible and enforceable*, but it does not make it automatic (unless Alternative B from Section 4 is adopted).

### Why This Document Focuses on Transfer

The new streams API's design makes transfer a more straightforward fit than locking for its specific architecture:

- Streams are iterables, not custom classes, so there is no `ReadableStream` object on which to hang a locking mechanism (though one could be added)
- The API already uses well-known symbol protocols (`[Symbol.dispose]()`, `[Symbol.asyncDispose]()`) for cleanup, and `[Symbol.transfer]()` follows the same pattern
- The Transfer Protocol targets iterators directly, which is the primitive the new streams API is built on

This does not mean locking is inferior in general. Locking is well-proven in Web Streams and provides capabilities transfer does not (reversibility, introspection; see above). The choice between them depends on the API's existing architecture and design goals.

---

## 7. Usage Patterns

### 7.1 API Ownership Contracts

Functions that consume a source should document their ownership expectations. With the Transfer Protocol, callers can enforce these expectations:

```js
// A function that takes exclusive ownership of a stream
async function processStream(source) {
  const owned = Object.transfer(source);
  // source is detached; no concurrent access possible

  const result = [];
  for await (const chunks of owned) {
    for (const chunk of chunks) {
      result.push(transform(chunk));
    }
  }
  return result;
}
```

### 7.2 Pipeline Construction and Handoff

Build a pipeline, then transfer it to a consumer:

```js
function buildPipeline(source) {
  // Create a pipeline from the source
  const pipeline = Stream.pull(source, decompress, parse);
  // Transfer the pipeline to enforce exclusive consumption
  return Object.transfer(pipeline);
  // pipeline is detached — only the returned value can be iterated
}

const output = buildPipeline(networkStream);
const data = await Stream.bytes(output);
```

Note: This requires `Stream.pull()` to return an iterator that implements `[Symbol.transfer]()`. If `Stream.pull()` returns an async generator, this is inherited from `AsyncIterator.prototype[Symbol.transfer]()` per the proposal.

### 7.3 Duplex Channel Handoff

Transfer a duplex channel endpoint to a handler:

```js
const [client, server] = Stream.duplex();

// Transfer the server endpoint to a handler function
handleConnection(Object.transfer(server));
// server is detached — only the handler can use it

// Client communicates with the handler
await client.writer.write("Hello");
for await (const chunks of client.readable) {
  console.log(new TextDecoder().decode(chunks[0]));
  break;
}
await client.close();
```

### 7.4 `using` / `await using` Integration

Transfer works with Explicit Resource Management:

```js
async function processConnection(channelIn) {
  // Take ownership and bind to this scope
  await using channel = Object.transfer(channelIn);
  // channelIn is detached

  for await (const chunks of channel.readable) {
    // Echo back
    await channel.writer.writev(chunks);
  }

  // channel[Symbol.asyncDispose]() called at block exit
  // Closes the channel (calls close())
}
```

The two-binding pattern for maximum safety (applicable to types that implement `AsyncDisposable`, such as `DuplexChannel`):

```js
async function safeProcess(channelIn) {
  await using channel = Object.transfer(channelIn);
  // channelIn is detached
  // If anything throws, channel[Symbol.asyncDispose]() cleans up

  for await (const chunks of channel.readable) {
    await channel.writer.writev(chunks);
  }
}
```

**Note:** This pattern requires the transferred type to implement `[Symbol.asyncDispose]()`. `DuplexChannel` supports this natively. Plain readables (`ByteStreamReadable`) do not currently implement `AsyncDisposable`. See the note in Section 3.1 about adding `[Symbol.asyncDispose]()` to readables. For plain readables, use a `try`/`finally` pattern instead:

```js
async function safeProcess(sourceIn) {
  const source = Object.transfer(sourceIn);
  // sourceIn is detached
  try {
    return await Stream.text(source);
  } finally {
    // Manual cleanup if needed (e.g., call .return() on the iterator)
    source[Symbol.asyncIterator]().return?.();
  }
}
```

### 7.5 Share with Transferred Source

Take exclusive ownership of a source, then share it among multiple consumers:

```js
async function processWithFanOut(sourceIn) {
  // Take ownership of the source
  const source = Object.transfer(sourceIn);

  // Create a shared wrapper — the share owns the source exclusively
  using shared = Stream.share(source, {
    highWaterMark: 50,
    backpressure: 'strict'
  });

  // Create consumers with different transforms
  const raw = shared.pull();
  const compressed = shared.pull(compress);
  const hashed = shared.pull(hash);

  // Process all consumers concurrently
  const [rawBytes, compressedBytes, hashResult] = await Promise.all([
    Stream.bytes(raw),
    Stream.bytes(compressed),
    Stream.bytes(hashed),
  ]);

  return { rawBytes, compressedBytes, hashResult };
}
```

### 7.6 Event Source with Transferred Writer

Transfer a writer to isolate write authority:

```js
function connectEventSource(url) {
  const { writer, readable } = Stream.push({
    highWaterMark: 16,
    backpressure: 'drop-oldest'
  });

  // Transfer the writer to the event handler scope
  const ownedWriter = Object.transfer(writer);
  // writer is detached — only ownedWriter can write

  const eventSource = new EventSource(url);
  eventSource.onmessage = async (event) => {
    if (ownedWriter.desiredSize === 0) {
      const canWrite = await Stream.ondrain(ownedWriter);
      if (!canWrite) return;
    }
    await ownedWriter.write(event.data);
  };
  eventSource.onerror = () => ownedWriter.fail(new Error('EventSource failed'));

  return readable;
}
```

### 7.7 Cross-Realm Transfer (Future)

If the Transfer Protocol's companion proposal for cross-realm iterator transfer via `postMessage()` is adopted, stream iterables could be transferred to workers:

```js
// Main thread
const source = Stream.pull(networkStream, decompress);
worker.postMessage(source, { transfer: [source] });
// source is detached in this realm

// Worker
self.onmessage = async (e) => {
  const source = e.data; // AsyncIterator in worker realm
  for await (const chunks of source) {
    processInParallel(chunks);
  }
};
```

This depends on both the Transfer Protocol and the cross-realm iterator transfer being standardized. The new streams API would benefit from this automatically, since its readables are standard async iterables.

---

## 8. Open Questions

### 8.1 `[Symbol.transfer]()` and the Writer Interface

The `Writer` interface (§5 of types.ts) is a structural type — any object with the right methods qualifies. `[Symbol.transfer]()` should not be part of this interface. Custom writer implementations (e.g., a logging writer, a metrics writer) should not be forced to implement ownership transfer just to satisfy the interface contract.

Writers returned by `Stream.push()` and `Stream.broadcast()` should implement `[Symbol.transfer]()` on their concrete types. Custom writers can opt in by implementing the protocol themselves.

### 8.2 Inherited Transfer for `Stream.pull()` / `Stream.from()` Output

If `AsyncIterator.prototype[Symbol.transfer]()` lands in TC-39, the async generators returned by `Stream.pull()` and `Stream.from()` get transfer for free. Until then, a polyfill can provide the same semantics. See Section 9.

### 8.3 No Atomic Pair Transfer for `Stream.push()` Result

`Stream.push()` returns `{ writer, readable }`. These should not be transferable as a unit. The writer and readable will typically have different consumers: one side produces data, the other consumes it. Transferring them individually reflects this. The return value of `Stream.push()` can remain a plain object.

### 8.4 Transfer and `Stream.merge()`

`Stream.merge()` accepts multiple sources and interleaves their output. Whether it should transfer-own its input sources depends on which integration alternative from Section 4 is chosen. Under Alternative A (opt-in), callers transfer sources themselves before passing them. Under Alternative B (auto-transfer), `merge()` would detach all source iterators internally. Under Alternative C (via parameter), `merge()` would accept a `transfer` option. No special handling for `merge()` is needed beyond whatever convention the rest of the API follows.

### 8.5 `detached` Property Convention

The Transfer Protocol proposal uses a `detached` getter as a convention (not a formal requirement). Should the new streams API's transferable types expose a `detached` property?

```js
const { writer, readable } = Stream.push();
const owned = Object.transfer(readable);

readable.detached; // true (if exposed)
owned.detached;    // false
```

This would aid debugging but adds API surface.

### 8.6 Relationship to `[Symbol.dispose]()` / `[Symbol.asyncDispose]()`

The Transfer Protocol specifies that disposal must no-op on detached objects. The new streams API already has disposal on `DuplexChannel`, `Share`, and `Broadcast`. These already follow the right pattern (`close()` / `cancel()` are idempotent). Implementing `[Symbol.transfer]()` needs to ensure the detached state is recognized by the existing disposal methods.

**Question:** Should the existing `[Symbol.dispose]()` / `[Symbol.asyncDispose]()` implementations check a `#detached` flag, or should they rely on the underlying state (e.g., a closed `PushQueue`) to no-op on its own?

Using the underlying state is simpler (no new flag) but may not cover all edge cases. A dedicated `#detached` flag is explicit and aligns with the Transfer Protocol's `[[Detached]]` internal slot pattern.

### 8.7 Non-Transferable Sources and Graceful Degradation

Many real-world iterators are plain objects without `[Symbol.transfer]()`. The new streams API must work with these regardless of which integration alternative is chosen.

**Question:** Should the API provide a utility for wrapping non-transferable iterators in transferable ones?

```js
// Hypothetical utility
const transferable = Stream.transferable(plainIterator);
const pipeline = Stream.pull(Object.transfer(transferable), compress);
```

This is similar to `Iterator.from()` in the TC-39 proposal but scoped to the streams API. It carries the same "leaky abstraction" concern: the wrapper is detached but the underlying plain object remains accessible.

---

## 9. Polyfill Feasibility

The Transfer Protocol depends on TC-39 standardization, but the core mechanism — detaching an object and moving its state to a new one — can be polyfilled in userland.

A polyfill for `Object.transfer()` would:

1. Check for `[Symbol.transfer]()` on the target (using `Symbol.for('Symbol.transfer')` as the polyfill symbol)
2. Call it, receiving the new object
3. Return the new object

The `[Symbol.transfer]()` method itself must be implemented per-type. For the new streams API, this means each transferable type (writer, readable, `DuplexChannel`, etc.) would implement a method that copies internal state to a new instance and sets a `#detached` flag on the original. Post-detach, operational methods check the flag and throw `TypeError`.

This is straightforward to implement. The main limitations of a polyfill compared to a native implementation:

- **No engine-level detach**: A polyfill uses a boolean flag and runtime checks. Native transfer could use internal slots and engine-level neutering (as `ArrayBuffer.prototype.transfer()` does), which is harder to bypass and has no per-operation overhead.
- **No `postMessage()` integration**: Cross-realm transfer via structured clone requires engine support. A polyfill cannot intercept `postMessage()` to add new transferable types.
- **Symbol identity**: A polyfill must use `Symbol.for('Symbol.transfer')` rather than a true well-known symbol. This works but diverges from the eventual spec, requiring a migration step when native support ships.

A polyfill would be sufficient to validate the patterns in this document and to provide ownership enforcement in same-realm use cases. Cross-realm transfer (Section 7.7) would remain blocked on native support.

---

## Related Documents

| Document | Description |
|----------|-------------|
| [Transfer Protocol Proposal](https://github.com/jasnell/proposal-transfer-protocol) | TC-39 proposal for `Symbol.transfer` / `Object.transfer()` |
| [Cross-Realm Iterator Transfer](https://github.com/jasnell/proposal-transfer-protocol/blob/main/CROSS-REALM-ITERATOR-TRANSFER.md) | Companion proposal for `postMessage()` iterator transfer |
| [DESIGN.md](DESIGN.md) | New streams API design document |
| [API.md](../API.md) | New streams API reference |
| [README.md](../README.md) | Motivation and design principles |

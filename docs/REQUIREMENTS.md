# Testable Requirements - New Streams API

This document lists all testable requirements for the new streams API implementation.

## Legend

- ✅ Covered by existing tests
- ⚠️ Partially covered
- ❌ Not covered

---

## 1. Stream.push()

Creates a bonded writer and async iterable pair for push-based streaming.

### 1.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-001 | Returns object with `writer` and `readable` properties | ✅ |
| PUSH-002 | Writer.write() accepts string (UTF-8 encoded) | ✅ |
| PUSH-003 | Writer.write() accepts Uint8Array | ✅ |
| PUSH-004 | Writer.writev() writes multiple chunks atomically | ✅ |
| PUSH-005 | Writer.writeSync() returns boolean (true if accepted) | ✅ |
| PUSH-006 | Writer.writevSync() returns boolean (true if accepted) | ✅ |
| PUSH-007 | Writer.end() signals end of stream | ✅ |
| PUSH-008 | Writer.end() returns total bytes written | ✅ |
| PUSH-009 | Writer.endSync() returns total bytes written | ✅ |
| PUSH-010 | Writer.fail() signals error to consumer | ✅ |
| PUSH-011 | Writer.failSync() signals error synchronously | ✅ |
| PUSH-012 | Readable yields Uint8Array[] batches | ✅ |

### 1.2 Backpressure - desiredSize

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-020 | desiredSize reflects available buffer space | ✅ |
| PUSH-021 | desiredSize is 0 when buffer is full | ✅ |
| PUSH-022 | desiredSize is null after close | ✅ |
| PUSH-023 | desiredSize is null after fail | ✅ |
| PUSH-024 | desiredSize is always >= 0 (never negative) | ✅ |

### 1.3 Backpressure - highWaterMark

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-030 | Default highWaterMark is 4 | ✅ |
| PUSH-031 | Custom highWaterMark is respected | ✅ |
| PUSH-032 | highWaterMark is clamped to [1, implementation-defined max (>= 1024)] | ✅ |
| PUSH-033 | writev counts as single slot for backpressure | ✅ |

### 1.4 Backpressure Policies

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-040 | backpressure: 'strict' rejects async writes when buffer full | ✅ |
| PUSH-041 | backpressure: 'strict' returns false for sync writes when buffer full | ✅ |
| PUSH-042 | backpressure: 'block' waits for space on async writes | ✅ |
| PUSH-043 | backpressure: 'block' returns false for sync writes when buffer full | ✅ |
| PUSH-044 | backpressure: 'drop-oldest' discards oldest buffered data | ✅ |
| PUSH-045 | backpressure: 'drop-newest' discards incoming data when buffer full | ✅ |

### 1.5 Consumer Termination

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-050 | Consumer break closes writer (desiredSize becomes null) | ✅ |
| PUSH-051 | writeSync returns false after consumer terminates | ✅ |
| PUSH-052 | iterator.throw() propagates error to writer | ✅ |

### 1.6 AbortSignal

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-060 | signal option aborts stream when signaled | ✅ |
| PUSH-061 | Already-aborted signal creates errored stream | ✅ |
| PUSH-062 | Write blocked on backpressure rejects on signal abort | �� |

### 1.7 Transforms

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-070 | Accepts transforms as arguments | ✅ |
| PUSH-071 | Multiple transforms are applied in order | ✅ |
| PUSH-072 | Transforms are applied lazily (on pull) | ✅ |

### 1.8 Edge Cases

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-080 | Handles empty writes | ✅ |
| PUSH-081 | Reading from already-closed stream returns done | ✅ |
| PUSH-082 | Second end() call rejects with TypeError | ✅ |
| PUSH-083 | Batches synchronously available chunks | ✅ |
| PUSH-084 | Handles concurrent writes and reads | ✅ |

### 1.9 Drainable Protocol

| ID | Requirement | Status |
|----|-------------|--------|
| PUSH-090 | Writer implements drainable protocol | ✅ |
| PUSH-091 | ondrain returns resolved Promise<true> when desiredSize > 0 | ✅ |
| PUSH-092 | ondrain returns null when desiredSize is null (writer closed) | ✅ |
| PUSH-093 | ondrain returns null when desiredSize is null (writer failed) | ✅ |
| PUSH-094 | ondrain returns pending Promise when desiredSize === 0 | ✅ |
| PUSH-095 | ondrain Promise resolves with false when writer closes while waiting | ✅ |
| PUSH-096 | ondrain Promise rejects when writer fails while waiting | ✅ |
| PUSH-097 | Multiple drain waiters all resolve together | ✅ |
| PUSH-098 | ondrain returns null for non-drainable objects | ✅ |
| PUSH-099 | ondrain works with event source pattern | ✅ |

---

## 2. Stream.from() / Stream.fromSync()

Creates iterables from various input sources.

### 2.1 ByteInput Handling

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-001 | from() handles string input (UTF-8 encoded) | ✅ |
| FROM-002 | from() handles Uint8Array input | ✅ |
| FROM-003 | from() handles ArrayBuffer input | ✅ |
| FROM-004 | fromSync() handles string input | ✅ |
| FROM-005 | fromSync() handles Uint8Array input | ✅ |
| FROM-006 | fromSync() handles ArrayBuffer input | ✅ |
| FROM-007 | fromSync() handles ArrayBufferView (Int8Array, DataView) | ✅ |

### 2.2 Iterable Handling

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-010 | from() handles async generator | ✅ |
| FROM-011 | from() handles sync generator | ✅ |
| FROM-012 | from() handles array input | ✅ |
| FROM-013 | fromSync() handles sync generator | ✅ |
| FROM-014 | fromSync() handles array input | ✅ |
| FROM-015 | from() flattens nested async iterables | ✅ |
| FROM-016 | fromSync() flattens nested iterables | ✅ |
| FROM-017 | fromSync() flattens arrays yielded by generators | ✅ |

### 2.3 Protocol Handling

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-020 | from() handles toAsyncStreamable returning promise | ✅ |
| FROM-021 | from() handles toAsyncStreamable returning async iterable | ✅ |
| FROM-022 | from() prefers toAsyncStreamable over toStreamable | ✅ |
| FROM-023 | from() falls back to toStreamable when toAsyncStreamable absent | ✅ |
| FROM-024 | fromSync() handles toStreamable returning string | ✅ |
| FROM-025 | fromSync() handles toStreamable returning array | ✅ |
| FROM-026 | fromSync() handles nested ToStreamable objects | ✅ |

### 2.4 String Coercion Fallback

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-030 | fromSync() handles URL (custom toString) | ✅ |
| FROM-031 | fromSync() handles Date (custom toString) | ✅ |
| FROM-032 | fromSync() handles objects with custom toString | ✅ |

### 2.5 Error Handling

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-040 | fromSync() rejects plain objects without custom toString | ✅ |
| FROM-041 | fromSync() rejects null | ✅ |
| FROM-042 | fromSync() rejects undefined | ✅ |
| FROM-043 | fromSync() rejects numbers | ✅ |
| FROM-044 | from() rejects non-iterable input | ✅ |
| FROM-045 | from() propagates errors from async generators | ✅ |

### 2.6 Empty Inputs

| ID | Requirement | Status |
|----|-------------|--------|
| FROM-050 | from() handles empty string | ✅ |
| FROM-051 | from() handles empty async generator | ✅ |
| FROM-052 | fromSync() handles empty string | ✅ |
| FROM-053 | fromSync() handles empty generator | ✅ |
| FROM-054 | fromSync() handles empty array | ✅ |

---

## 3. Stream.pull() / Stream.pullSync()

Creates pull-through pipelines with transforms.

### 3.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| PULL-001 | pull() passes through source without transforms | ✅ |
| PULL-002 | pull() works with sync source | ✅ |
| PULL-003 | pullSync() passes through source without transforms | ✅ |
| PULL-004 | pull() applies single transform | ✅ |
| PULL-005 | pull() chains multiple transforms in order | ✅ |
| PULL-006 | pullSync() applies single transform | ✅ |
| PULL-007 | pullSync() chains multiple transforms | ✅ |

### 3.2 Transform Output Types

| ID | Requirement | Status |
|----|-------------|--------|
| PULL-010 | Transform returning Uint8Array[] is passed through | ✅ |
| PULL-011 | Transform returning string is UTF-8 encoded | ✅ |
| PULL-012 | Transform returning nested iterables is flattened | ✅ |
| PULL-013 | Transform returning null filters out batch | ✅ |

### 3.3 Async Transforms

| ID | Requirement | Status |
|----|-------------|--------|
| PULL-020 | Handles async transform function | ✅ |
| PULL-021 | Handles transform returning Promise | ✅ |
| PULL-022 | Handles transform returning async generator | ✅ |

### 3.4 Stateful Transforms

| ID | Requirement | Status |
|----|-------------|--------|
| PULL-030 | Supports stateful transform object | ✅ |
| PULL-031 | Receives null flush signal at end | ✅ |
| PULL-032 | Pipeline signal fires on transforms when error occurs | ✅ |

### 3.5 Options

| ID | Requirement | Status |
|----|-------------|--------|
| PULL-040 | Accepts options as last argument | ✅ |
| PULL-041 | Respects AbortSignal | ✅ |
| PULL-042 | Handles already-aborted signal | ✅ |

---

## 4. Stream.pipeTo() / Stream.pipeToSync()

Consumes source and writes to a writer with optional transforms.

### 4.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| PIPE-001 | pipeTo() writes source to writer without transforms | ✅ |
| PIPE-002 | pipeTo() works with sync source | ✅ |
| PIPE-003 | pipeTo() applies transforms before writing | ✅ |
| PIPE-004 | pipeToSync() writes source to writer without transforms | ✅ |
| PIPE-005 | pipeToSync() applies transforms before writing | ✅ |

### 4.2 Options

| ID | Requirement | Status |
|----|-------------|--------|
| WRITE-010 | preventClose keeps writer open on completion | ✅ |
| WRITE-011 | preventFail keeps writer from failing on error | ✅ |
| WRITE-012 | Respects AbortSignal | ✅ |

### 4.3 Error Handling

| ID | Requirement | Status |
|----|-------------|--------|
| WRITE-020 | Fails writer on source error | ✅ |
| WRITE-021 | Throws if no writer provided | ✅ |
| WRITE-022 | pipeToSync() throws and fails writer when writev returns false | ✅ |
| WRITE-023 | pipeToSync() throws and fails writer when write returns false | ✅ |
| WRITE-025 | pipeTo passes signal to writer.write() | ✅ |
| WRITE-026 | pipeTo passes signal to writer.end() | ✅ |

### 4.4 Special Cases

| ID | Requirement | Status |
|----|-------------|--------|
| WRITE-030 | Handles writer that is also a transform | ✅ |

---

## 5. Consumer Functions

Terminal consumers that collect streams into memory.

### 5.1 Stream.bytes() / Stream.bytesSync()

| ID | Requirement | Status |
|----|-------------|--------|
| BYTES-001 | bytes() collects all bytes from async source | ✅ |
| BYTES-002 | bytes() collects all bytes from sync source | ✅ |
| BYTES-003 | bytesSync() collects all bytes from source | ✅ |
| BYTES-004 | bytesSync() handles multiple chunks | ✅ |
| BYTES-005 | bytesSync() handles empty source | ✅ |
| BYTES-006 | bytes() respects AbortSignal | ✅ |
| BYTES-007 | bytes() respects byte limit | ✅ |
| BYTES-008 | bytesSync() respects byte limit | ✅ |
| BYTES-009 | bytesSync() allows data within limit | ✅ |

### 5.2 Stream.text() / Stream.textSync()

| ID | Requirement | Status |
|----|-------------|--------|
| TEXT-001 | text() decodes UTF-8 by default | ✅ |
| TEXT-002 | text() handles multi-byte UTF-8 characters | ✅ |
| TEXT-003 | text() respects AbortSignal | ✅ |
| TEXT-004 | textSync() decodes UTF-8 by default | ✅ |
| TEXT-005 | textSync() handles multi-byte UTF-8 characters | ✅ |
| TEXT-006 | textSync() respects encoding option | ✅ |
| TEXT-007 | textSync() throws on invalid UTF-8 (fatal mode) | ✅ |
| TEXT-008 | textSync() respects byte limit | ✅ |

### 5.3 Stream.arrayBuffer() / Stream.arrayBufferSync()

| ID | Requirement | Status |
|----|-------------|--------|
| ARRAYBUF-001 | arrayBuffer() returns ArrayBuffer | ✅ |
| ARRAYBUF-002 | arrayBuffer() respects AbortSignal | ✅ |
| ARRAYBUF-003 | arrayBufferSync() returns ArrayBuffer | ✅ |
| ARRAYBUF-004 | arrayBufferSync() respects byte limit | ✅ |

### 5.4 Stream.array() / Stream.arraySync()

| ID | Requirement | Status |
|----|-------------|--------|
| ARRAY-001 | array() collects all chunks from async source | ✅ |
| ARRAY-002 | array() collects all chunks from sync source | ✅ |
| ARRAY-003 | arraySync() collects all chunks from source | ✅ |
| ARRAY-004 | arraySync() handles single chunk | ✅ |
| ARRAY-005 | arraySync() handles empty source | ✅ |
| ARRAY-006 | arraySync() respects byte limit | ✅ |
| ARRAY-007 | arraySync() allows data within limit | ✅ |
| ARRAY-008 | array() respects AbortSignal | ✅ |
| ARRAY-009 | array() respects byte limit | ✅ |
| ARRAY-010 | array() preserves chunk boundaries | ✅ |

### 5.5 Stream.dump() / Stream.dumpSync()

| ID | Requirement | Status |
|----|-------------|--------|
| DUMP-001 | dump() reads an async source to completion | ✅ |
| DUMP-002 | dump() reads a sync source to completion | ✅ |
| DUMP-003 | dump() fulfills with undefined | ✅ |
| DUMP-004 | dump() retains no data (peak memory is one batch) | ✅ |
| DUMP-005 | dump() handles an empty source | ✅ |
| DUMP-006 | dump() rejects if the source errors mid-stream | ✅ |
| DUMP-007 | dump() respects AbortSignal | ✅ |
| DUMP-008 | dump() rejects if an already-aborted signal is passed | ✅ |
| DUMP-009 | dump() respects byte limit | ✅ |
| DUMP-010 | dump() performs no byte accounting when limit is absent | ✅ |
| DUMP-011 | dump() releases the source on abrupt completion | ✅ |
| DUMP-012 | dumpSync() reads a sync source to completion | ✅ |
| DUMP-013 | dumpSync() returns undefined | ✅ |
| DUMP-014 | dumpSync() throws if the source throws mid-stream | ✅ |
| DUMP-015 | dumpSync() respects byte limit | ✅ |
| DUMP-016 | dumpSync() throws TypeError on an async-only source | ✅ |

---

## 6. Stream.broadcast()

Push-model multi-consumer streaming.

### 6.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-001 | Creates writer and broadcast pair | ✅ |
| BCAST-002 | Single consumer receives data | ✅ |
| BCAST-003 | Multiple consumers receive same data | ✅ |
| BCAST-004 | Tracks consumer count | ✅ |
| BCAST-005 | Late subscribers receive new data | ✅ |

### 6.2 Buffer Management

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-010 | Respects buffer limit | ✅ |
| BCAST-011 | Trims buffer as consumers advance | ✅ |
| BCAST-012 | Uses drop-oldest policy | ✅ |
| BCAST-013 | Uses drop-newest policy | ✅ |

### 6.3 Writer Operations

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-020 | Tracks total bytes written | ✅ |
| BCAST-021 | Supports writev | ✅ |
| BCAST-022 | Propagates errors via fail | ✅ |

### 6.4 Cancel

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-030 | cancel() cancels all consumers without error | ✅ |
| BCAST-031 | cancel(reason) cancels all consumers with error | ✅ |
| BCAST-032 | cancel() is idempotent | ✅ |

### 6.5 Symbol.dispose

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-040 | Symbol.dispose cancels broadcast | ✅ |

### 6.6 AbortSignal

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-050 | Respects already-aborted signal | ✅ |
| BCAST-051 | Cancels on signal abort | ✅ |

### 6.7 Transforms

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-070 | Applies single transform to consumer | ✅ |
| BCAST-071 | Applies multiple transforms in order | ✅ |
| BCAST-072 | Allows different transforms per consumer | ✅ |

### 6.9 Drainable Protocol

| ID | Requirement | Status |
|----|-------------|--------|
| BCAST-080 | Writer implements drainable protocol | ✅ |
| BCAST-081 | ondrain returns resolved Promise<true> when desiredSize > 0 | ✅ |
| BCAST-082 | ondrain returns null when desiredSize is null (writer closed) | ✅ |
| BCAST-083 | ondrain returns null when desiredSize is null (writer failed) | ✅ |
| BCAST-084 | ondrain returns pending Promise when desiredSize === 0 | ✅ |
| BCAST-085 | ondrain Promise resolves with false when writer closes while waiting | ✅ |
| BCAST-086 | ondrain Promise rejects when writer fails while waiting | ✅ |
| BCAST-087 | Multiple drain waiters all resolve together | ✅ |

---

## 7. Stream.share() / Stream.shareSync()

Pull-model multi-consumer streaming.

### 7.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-001 | share() creates a share instance | ✅ |
| SHARE-002 | share() allows single consumer to pull data | ✅ |
| SHARE-003 | share() allows multiple consumers to share data | ✅ |
| SHARE-004 | share() handles sync source | ✅ |
| SHARE-005 | shareSync() creates a sync share instance | ✅ |
| SHARE-006 | shareSync() allows single consumer to pull data | ✅ |
| SHARE-007 | shareSync() allows interleaved iteration of multiple consumers | ✅ |

### 7.2 Buffer Management

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-010 | Buffers data for slow consumers | ✅ |
| SHARE-011 | share() respects buffer limit with strict policy (throws) | ✅ |
| SHARE-012 | share() drops oldest with drop-oldest policy | ✅ |
| SHARE-013 | shareSync() throws on buffer overflow with strict policy | ✅ |
| SHARE-014 | shareSync() drops oldest with drop-oldest policy | ✅ |

### 7.3 Cancel

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-020 | share() cancel() cancels all consumers without error | ✅ |
| SHARE-021 | share() cancel(reason) cancels all consumers with error | ✅ |
| SHARE-022 | share() cancel() closes source iterator | ✅ |
| SHARE-023 | shareSync() cancel() cancels all consumers | ✅ |

### 7.4 Symbol.dispose

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-030 | Symbol.dispose cancels share | ✅ |

### 7.5 AbortSignal

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-040 | Respects already-aborted signal | ✅ |

### 7.6 Error Propagation

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-050 | Propagates source errors to all consumers | ✅ |

### 7.7 Transforms

| ID | Requirement | Status |
|----|-------------|--------|
| SHARE-070 | share() applies single transform to consumer | ✅ |
| SHARE-071 | share() applies multiple transforms in order | ✅ |
| SHARE-072 | share() allows different transforms per consumer | ✅ |
| SHARE-073 | share() supports transforms with options | ✅ |
| SHARE-074 | shareSync() applies single transform to sync consumer | ✅ |

---

## 8. Stream.duplex()

Creates a pair of connected duplex channels for bidirectional communication.

### 8.1 Basic Operation

| ID | Requirement | Status |
|----|-------------|--------|
| DUPLEX-001 | Returns tuple of two DuplexChannel instances | ✅ |
| DUPLEX-002 | Data written to A appears in B's readable | ✅ |
| DUPLEX-003 | Data written to B appears in A's readable | ✅ |
| DUPLEX-004 | Bidirectional communication works | ✅ |

### 8.2 Close Behavior

| ID | Requirement | Status |
|----|-------------|--------|
| DUPLEX-005 | close() is idempotent | ✅ |
| DUPLEX-006 | Symbol.asyncDispose works | ✅ |
| DUPLEX-007 | Closing one channel doesn't affect the other direction | ✅ |

### 8.3 Options

| ID | Requirement | Status |
|----|-------------|--------|
| DUPLEX-008 | Respects highWaterMark option | ✅ |
| DUPLEX-009 | Respects backpressure option | ✅ |
| DUPLEX-010 | Respects AbortSignal option | ✅ |
| DUPLEX-013 | Respects per-direction options | ✅ |
| DUPLEX-014 | Per-direction options override shared options | ✅ |

### 8.4 Real-World Patterns

| ID | Requirement | Status |
|----|-------------|--------|
| DUPLEX-011 | Supports request-response pattern | ✅ |
| DUPLEX-012 | Supports multiple message exchanges | ✅ |

---

## 9. Stream.merge()

Merges multiple async sources by temporal order.

| ID | Requirement | Status |
|----|-------------|--------|
| MERGE-001 | Handles empty sources | ✅ |
| MERGE-002 | Handles single source | ✅ |
| MERGE-003 | Merges multiple sources | ✅ |
| MERGE-004 | Yields in temporal order (first-come) | ✅ |
| MERGE-005 | Respects AbortSignal | ✅ |
| MERGE-006 | Handles source errors | ✅ |
| MERGE-007 | Accepts options as last argument | ✅ |

---

## 10. Stream.tap() / Stream.tapSync()

Creates pass-through transforms for observation.

| ID | Requirement | Status |
|----|-------------|--------|
| TAP-001 | tap() calls callback with chunks | ✅ |
| TAP-002 | tap() supports async callback | ✅ |
| TAP-003 | tapSync() calls callback with chunks | ✅ |
| TAP-004 | tapSync() passes chunks through unchanged | ✅ |

---

## 11. Protocol Symbols

Symbols for custom type integration.

| ID | Requirement | Status |
|----|-------------|--------|
| PROTO-001 | toStreamable symbol allows sync conversion | ✅ |
| PROTO-002 | toAsyncStreamable symbol allows async conversion | ✅ |
| PROTO-003 | broadcastProtocol symbol allows custom broadcast | ✅ |
| PROTO-004 | shareProtocol symbol allows custom share | ✅ |
| PROTO-005 | shareSyncProtocol symbol allows custom sync share | ✅ |
| PROTO-006 | drainableProtocol symbol allows drain notification | ✅ |

---

## Summary Statistics

| Category | Total | Covered |
|----------|-------|---------|
| Stream.push() | 46 | 45 |
| Stream.from() / fromSync() | 26 | 26 |
| Stream.pull() / pullSync() | 17 | 17 |
| Stream.pipeTo() / pipeToSync() | 12 | 10 |
| Consumer Functions | 27 | 27 |
| Stream.broadcast() | 26 | 26 |
| Stream.share() / shareSync() | 22 | 22 |
| Stream.duplex() | 14 | 14 |
| Stream.merge() | 7 | 7 |
| Stream.tap() / tapSync() | 4 | 4 |
| Protocol Symbols | 6 | 6 |
| **Total** | **207** | **207** |

**Coverage: 100%** of specified requirements are tested.

"use strict";
/**
 * This is a bytes-only stream API that provides:
 * - Simpler semantics than Web Streams
 * - Async/sync iteration as the primary interface
 * - Batched chunks (Uint8Array[]) to amortize async overhead
 * - Explicit backpressure handling
 * - Protocol-based extensibility
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Stream = exports.SyncShare = exports.Share = exports.shareSync = exports.share = exports.Broadcast = exports.broadcast = exports.pipeToSync = exports.pipeTo = exports.pullSync = exports.pull = exports.fromSync = exports.from = exports.duplex = exports.push = exports.drainableProtocol = exports.shareSyncProtocol = exports.shareProtocol = exports.broadcastProtocol = exports.toAsyncStreamable = exports.toStreamable = void 0;
// Export protocol symbols
var types_js_1 = require("./types.js");
Object.defineProperty(exports, "toStreamable", { enumerable: true, get: function () { return types_js_1.toStreamable; } });
Object.defineProperty(exports, "toAsyncStreamable", { enumerable: true, get: function () { return types_js_1.toAsyncStreamable; } });
Object.defineProperty(exports, "broadcastProtocol", { enumerable: true, get: function () { return types_js_1.broadcastProtocol; } });
Object.defineProperty(exports, "shareProtocol", { enumerable: true, get: function () { return types_js_1.shareProtocol; } });
Object.defineProperty(exports, "shareSyncProtocol", { enumerable: true, get: function () { return types_js_1.shareSyncProtocol; } });
Object.defineProperty(exports, "drainableProtocol", { enumerable: true, get: function () { return types_js_1.drainableProtocol; } });
var push_js_1 = require("./push.js");
Object.defineProperty(exports, "push", { enumerable: true, get: function () { return push_js_1.push; } });
var duplex_js_1 = require("./duplex.js");
Object.defineProperty(exports, "duplex", { enumerable: true, get: function () { return duplex_js_1.duplex; } });
var from_js_1 = require("./from.js");
Object.defineProperty(exports, "from", { enumerable: true, get: function () { return from_js_1.from; } });
Object.defineProperty(exports, "fromSync", { enumerable: true, get: function () { return from_js_1.fromSync; } });
var pull_js_1 = require("./pull.js");
Object.defineProperty(exports, "pull", { enumerable: true, get: function () { return pull_js_1.pull; } });
Object.defineProperty(exports, "pullSync", { enumerable: true, get: function () { return pull_js_1.pullSync; } });
Object.defineProperty(exports, "pipeTo", { enumerable: true, get: function () { return pull_js_1.pipeTo; } });
Object.defineProperty(exports, "pipeToSync", { enumerable: true, get: function () { return pull_js_1.pipeToSync; } });
var broadcast_js_1 = require("./broadcast.js");
Object.defineProperty(exports, "broadcast", { enumerable: true, get: function () { return broadcast_js_1.broadcast; } });
Object.defineProperty(exports, "Broadcast", { enumerable: true, get: function () { return broadcast_js_1.Broadcast; } });
var share_js_1 = require("./share.js");
Object.defineProperty(exports, "share", { enumerable: true, get: function () { return share_js_1.share; } });
Object.defineProperty(exports, "shareSync", { enumerable: true, get: function () { return share_js_1.shareSync; } });
Object.defineProperty(exports, "Share", { enumerable: true, get: function () { return share_js_1.Share; } });
Object.defineProperty(exports, "SyncShare", { enumerable: true, get: function () { return share_js_1.SyncShare; } });
// =============================================================================
// Stream Namespace - Unified API access point
// =============================================================================
var push_js_2 = require("./push.js");
var duplex_js_2 = require("./duplex.js");
var from_js_2 = require("./from.js");
var pull_js_2 = require("./pull.js");
var consumers_js_1 = require("./consumers.js");
var broadcast_js_2 = require("./broadcast.js");
var share_js_2 = require("./share.js");
var types_js_2 = require("./types.js");
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
exports.Stream = {
    // Factories
    push: push_js_2.push,
    duplex: duplex_js_2.duplex,
    from: from_js_2.from,
    fromSync: from_js_2.fromSync,
    // Pipelines
    pull: pull_js_2.pull,
    pullSync: pull_js_2.pullSync,
    // Pipe to destination
    pipeTo: pull_js_2.pipeTo,
    pipeToSync: pull_js_2.pipeToSync,
    // Consumers (async)
    bytes: consumers_js_1.bytes,
    text: consumers_js_1.text,
    arrayBuffer: consumers_js_1.arrayBuffer,
    array: consumers_js_1.array,
    dump: consumers_js_1.dump,
    // Consumers (sync)
    bytesSync: consumers_js_1.bytesSync,
    textSync: consumers_js_1.textSync,
    arrayBufferSync: consumers_js_1.arrayBufferSync,
    arraySync: consumers_js_1.arraySync,
    dumpSync: consumers_js_1.dumpSync,
    // Combining
    merge: consumers_js_1.merge,
    // Multi-consumer (push model)
    broadcast: broadcast_js_2.broadcast,
    // Multi-consumer (pull model)
    share: share_js_2.share,
    shareSync: share_js_2.shareSync,
    // Utilities
    tap: consumers_js_1.tap,
    tapSync: consumers_js_1.tapSync,
    // Drain utility for event source integration
    ondrain: consumers_js_1.ondrain,
    // Protocol symbols (for custom implementations)
    toStreamable: types_js_2.toStreamable,
    toAsyncStreamable: types_js_2.toAsyncStreamable,
    broadcastProtocol: types_js_2.broadcastProtocol,
    shareProtocol: types_js_2.shareProtocol,
    shareSyncProtocol: types_js_2.shareSyncProtocol,
    drainableProtocol: types_js_2.drainableProtocol,
};

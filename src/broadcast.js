"use strict";
/**
 * Broadcast - Push-model multi-consumer streaming
 *
 * Creates a broadcast channel where a single writer can push data to multiple
 * consumers. Each consumer has an independent cursor into a shared buffer.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Broadcast = void 0;
exports.broadcast = broadcast;
var types_js_1 = require("./types.js");
var from_js_1 = require("./from.js");
var pull_js_1 = require("./pull.js");
var utils_js_1 = require("./utils.js");
var ringbuffer_js_1 = require("./ringbuffer.js");
// Shared TextEncoder instance
var encoder = new TextEncoder();
// Cached resolved promise to avoid allocating a new one on every sync fast-path.
var kResolvedPromise = Promise.resolve();
// Non-exported symbol for internal cancel notification from BroadcastImpl to BroadcastWriter.
// Because this symbol is not exported, external code cannot call it.
var cancelWriter = Symbol('cancelWriter');
// =============================================================================
// Argument Parsing Helpers
// =============================================================================
/**
 * Check if a value is PushStreamOptions (object without transform property).
 */
function isPushStreamOptions(value) {
    return (value !== null &&
        typeof value === 'object' &&
        !('transform' in value) &&
        !('write' in value));
}
/**
 * Parse variadic arguments for push().
 * Returns { transforms, options }
 */
function parsePushArgs(args) {
    if (args.length === 0) {
        return { transforms: [], options: undefined };
    }
    var last = args[args.length - 1];
    if (isPushStreamOptions(last)) {
        return {
            transforms: args.slice(0, -1),
            options: last,
        };
    }
    return { transforms: args, options: undefined };
}
// =============================================================================
// Broadcast Implementation
// =============================================================================
var BroadcastImpl = /** @class */ (function () {
    function BroadcastImpl(options) {
        this.options = options;
        this.buffer = new ringbuffer_js_1.RingBuffer();
        this.bufferStart = 0; // Index of first chunk in buffer (for cursor mapping)
        this.consumers = new Set();
        this.ended = false;
        this.error = null;
        this.cancelled = false;
        /** Callback invoked when buffer space becomes available (for pending writes) */
        this._onBufferDrained = null;
    }
    /** Register the writer for cancel notification. */
    BroadcastImpl.prototype.setWriter = function (w) {
        this.writer = w;
    };
    Object.defineProperty(BroadcastImpl.prototype, "backpressurePolicy", {
        get: function () {
            return this.options.backpressure;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BroadcastImpl.prototype, "highWaterMark", {
        get: function () {
            return this.options.highWaterMark;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BroadcastImpl.prototype, "consumerCount", {
        get: function () {
            return this.consumers.size;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(BroadcastImpl.prototype, "bufferSize", {
        get: function () {
            return this.buffer.length;
        },
        enumerable: false,
        configurable: true
    });
    /**
     * Create a new consumer that receives data from this broadcast.
     * Optionally apply transforms to the consumer's data.
     */
    BroadcastImpl.prototype.push = function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        var _a = parsePushArgs(args), transforms = _a.transforms, options = _a.options;
        // Create raw consumer
        var rawConsumer = this.createRawConsumer();
        // If transforms provided, wrap with pull() pipeline
        if (transforms.length > 0) {
            if (options === null || options === void 0 ? void 0 : options.signal) {
                return pull_js_1.pull.apply(void 0, __spreadArray(__spreadArray([rawConsumer], transforms, false), [{ signal: options.signal }], false));
            }
            return pull_js_1.pull.apply(void 0, __spreadArray([rawConsumer], transforms, false));
        }
        return rawConsumer;
    };
    /**
     * Create a raw consumer iterable (internal helper).
     */
    BroadcastImpl.prototype.createRawConsumer = function () {
        var _a;
        var state = {
            cursor: this.bufferStart + this.buffer.length, // Start at current position
            resolve: null,
            reject: null,
            detached: false,
        };
        this.consumers.add(state);
        var self = this;
        return _a = {},
            _a[Symbol.asyncIterator] = function () {
                return {
                    next: function () {
                        return __awaiter(this, void 0, void 0, function () {
                            var bufferIndex, chunk;
                            return __generator(this, function (_a) {
                                if (state.detached) {
                                    // If detached due to an error, throw the error
                                    if (self.error)
                                        throw self.error;
                                    return [2 /*return*/, { done: true, value: undefined }];
                                }
                                bufferIndex = state.cursor - self.bufferStart;
                                if (bufferIndex < self.buffer.length) {
                                    chunk = self.buffer.get(bufferIndex);
                                    state.cursor++;
                                    self.tryTrimBuffer();
                                    return [2 /*return*/, { done: false, value: chunk }];
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
                                    return [2 /*return*/, { done: true, value: undefined }];
                                }
                                // Wait for data
                                return [2 /*return*/, new Promise(function (resolve, reject) {
                                        state.resolve = resolve;
                                        state.reject = reject;
                                    })];
                            });
                        });
                    },
                    return: function () {
                        return __awaiter(this, void 0, void 0, function () {
                            return __generator(this, function (_a) {
                                state.detached = true;
                                state.resolve = null;
                                state.reject = null;
                                self.consumers.delete(state);
                                self.tryTrimBuffer();
                                return [2 /*return*/, { done: true, value: undefined }];
                            });
                        });
                    },
                    throw: function (_error) {
                        return __awaiter(this, void 0, void 0, function () {
                            return __generator(this, function (_a) {
                                state.detached = true;
                                state.resolve = null;
                                state.reject = null;
                                self.consumers.delete(state);
                                self.tryTrimBuffer();
                                return [2 /*return*/, { done: true, value: undefined }];
                            });
                        });
                    },
                };
            },
            _a;
    };
    /**
     * Cancel all consumers and reject pending writes on the writer.
     * Sets ended=true so that subsequent _abort() calls early-return.
     */
    BroadcastImpl.prototype.cancel = function (reason) {
        var _a, _b;
        if (this.cancelled)
            return;
        this.cancelled = true;
        this.ended = true; // Prevents _abort() from redundantly iterating consumers
        if (reason) {
            this.error = reason;
        }
        // Reject pending writes on the writer so the pump doesn't hang
        (_a = this.writer) === null || _a === void 0 ? void 0 : _a[cancelWriter]();
        // Notify all waiting consumers
        for (var _i = 0, _c = this.consumers; _i < _c.length; _i++) {
            var consumer = _c[_i];
            if (consumer.resolve) {
                if (reason) {
                    (_b = consumer.reject) === null || _b === void 0 ? void 0 : _b.call(consumer, reason);
                }
                else {
                    consumer.resolve({ done: true, value: undefined });
                }
                consumer.resolve = null;
                consumer.reject = null;
            }
            consumer.detached = true;
        }
        this.consumers.clear();
    };
    BroadcastImpl.prototype[Symbol.dispose] = function () {
        this.cancel();
    };
    // ==========================================================================
    // Internal Methods (called by Writer)
    // ==========================================================================
    /**
     * Write a chunk to the broadcast buffer.
     * Returns true if write was accepted, false if buffer is full (strict/block policy).
     */
    BroadcastImpl.prototype._write = function (chunk) {
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
                    for (var _i = 0, _a = this.consumers; _i < _a.length; _i++) {
                        var consumer = _a[_i];
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
    };
    /**
     * Signal end of stream.
     */
    BroadcastImpl.prototype._end = function () {
        if (this.ended)
            return;
        this.ended = true;
        // Notify all waiting consumers
        for (var _i = 0, _a = this.consumers; _i < _a.length; _i++) {
            var consumer = _a[_i];
            if (consumer.resolve) {
                // First deliver any remaining buffered data
                var bufferIndex = consumer.cursor - this.bufferStart;
                if (bufferIndex < this.buffer.length) {
                    var chunk = this.buffer.get(bufferIndex);
                    consumer.cursor++;
                    consumer.resolve({ done: false, value: chunk });
                }
                else {
                    consumer.resolve({ done: true, value: undefined });
                }
                consumer.resolve = null;
                consumer.reject = null;
            }
        }
    };
    /**
     * Signal error. Notifies all consumers and detaches them.
     */
    BroadcastImpl.prototype._abort = function (reason) {
        if (this.ended || this.error)
            return;
        this.error = reason;
        this.ended = true;
        // Notify all waiting consumers and detach them
        for (var _i = 0, _a = this.consumers; _i < _a.length; _i++) {
            var consumer = _a[_i];
            if (consumer.reject) {
                consumer.reject(reason);
                consumer.resolve = null;
                consumer.reject = null;
            }
            consumer.detached = true;
        }
        this.consumers.clear();
    };
    /**
     * Get the slowest consumer's cursor position.
     */
    BroadcastImpl.prototype.getMinCursor = function () {
        var min = Infinity;
        for (var _i = 0, _a = this.consumers; _i < _a.length; _i++) {
            var consumer = _a[_i];
            if (consumer.cursor < min) {
                min = consumer.cursor;
            }
        }
        return min === Infinity ? this.bufferStart + this.buffer.length : min;
    };
    /**
     * Check if we can accept more writes (for desiredSize).
     */
    BroadcastImpl.prototype._getDesiredSize = function () {
        if (this.ended || this.cancelled)
            return null;
        var available = this.options.highWaterMark - this.buffer.length;
        return Math.max(0, available);
    };
    /**
     * Check if a write would be accepted (without actually writing).
     * Returns true if _write would accept a write, false otherwise.
     */
    BroadcastImpl.prototype._canWrite = function () {
        if (this.ended || this.cancelled) {
            return false;
        }
        // For strict and block policies, check if there's space
        // For drop-oldest and drop-newest, writes are always accepted
        if ((this.options.backpressure === 'strict' || this.options.backpressure === 'block') && this.buffer.length >= this.options.highWaterMark) {
            return false;
        }
        return true;
    };
    /**
     * Trim buffer from front if all consumers have advanced.
     * Notifies writer if buffer space becomes available.
     */
    BroadcastImpl.prototype.tryTrimBuffer = function () {
        var minCursor = this.getMinCursor();
        var trimCount = minCursor - this.bufferStart;
        if (trimCount > 0) {
            this.buffer.trimFront(trimCount);
            this.bufferStart = minCursor;
            // Notify writer that buffer space is available for pending writes
            if (this._onBufferDrained && this.buffer.length < this.options.highWaterMark) {
                this._onBufferDrained();
            }
        }
    };
    /**
     * Notify consumers that have pending reads.
     */
    BroadcastImpl.prototype._notifyConsumers = function () {
        for (var _i = 0, _a = this.consumers; _i < _a.length; _i++) {
            var consumer = _a[_i];
            if (consumer.resolve) {
                var bufferIndex = consumer.cursor - this.bufferStart;
                if (bufferIndex < this.buffer.length) {
                    var chunk = this.buffer.get(bufferIndex);
                    consumer.cursor++;
                    var resolve = consumer.resolve;
                    consumer.resolve = null;
                    consumer.reject = null;
                    resolve({ done: false, value: chunk });
                }
            }
        }
    };
    return BroadcastImpl;
}());
var BroadcastWriter = /** @class */ (function () {
    function BroadcastWriter(broadcast) {
        var _this = this;
        this.broadcast = broadcast;
        this.totalBytes = 0;
        this.closed = false;
        this.aborted = false;
        /** Queue of pending writes waiting for buffer space (strict and block policies) */
        this.pendingWrites = new ringbuffer_js_1.RingBuffer();
        /** Queue of pending drains waiting for backpressure to clear */
        this.pendingDrains = [];
        // Register callback for when buffer space becomes available
        this.broadcast._onBufferDrained = function () {
            _this.resolvePendingWrites();
            _this.resolvePendingDrains(true);
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
    BroadcastWriter.prototype[types_js_1.drainableProtocol] = function () {
        var _this = this;
        var desired = this.desiredSize;
        // If desiredSize is null, drain is not applicable
        if (desired === null) {
            return null;
        }
        // If there's already space, resolve immediately with true
        if (desired > 0) {
            return Promise.resolve(true);
        }
        // Buffer is full, wait for drain
        return new Promise(function (resolve, reject) {
            _this.pendingDrains.push({ resolve: resolve, reject: reject });
        });
    };
    Object.defineProperty(BroadcastWriter.prototype, "desiredSize", {
        get: function () {
            if (this.closed || this.aborted)
                return null;
            return this.broadcast._getDesiredSize();
        },
        enumerable: false,
        configurable: true
    });
    BroadcastWriter.prototype.write = function (chunk, options) {
        // Fast path: no signal, writer open, buffer has space
        if (!(options === null || options === void 0 ? void 0 : options.signal) && !this.closed && !this.aborted && this.broadcast._canWrite()) {
            var converted = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
            this.broadcast._write([converted]);
            this.totalBytes += converted.byteLength;
            return kResolvedPromise;
        }
        return this.writev([chunk], options);
    };
    BroadcastWriter.prototype.writev = function (chunks, options) {
        // Fast path: no signal, writer open, buffer has space
        if (!(options === null || options === void 0 ? void 0 : options.signal) && !this.closed && !this.aborted && this.broadcast._canWrite()) {
            var converted = (0, utils_js_1.allUint8Array)(chunks)
                ? chunks.slice()
                : chunks.map(function (c) { return typeof c === 'string' ? encoder.encode(c) : c; });
            this.broadcast._write(converted);
            for (var _i = 0, converted_1 = converted; _i < converted_1.length; _i++) {
                var c = converted_1[_i];
                this.totalBytes += c.byteLength;
            }
            return kResolvedPromise;
        }
        return this._writevSlow(chunks, options);
    };
    BroadcastWriter.prototype._writevSlow = function (chunks, options) {
        return __awaiter(this, void 0, void 0, function () {
            var signal, converted, _i, converted_2, c, policy, highWaterMark;
            var _a;
            return __generator(this, function (_b) {
                signal = options === null || options === void 0 ? void 0 : options.signal;
                // Check for pre-aborted signal
                if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                    throw (_a = signal.reason) !== null && _a !== void 0 ? _a : new DOMException('Aborted', 'AbortError');
                }
                if (this.closed || this.aborted) {
                    throw new Error('Writer is closed');
                }
                converted = (0, utils_js_1.allUint8Array)(chunks)
                    ? chunks.slice()
                    : chunks.map(function (c) { return typeof c === 'string' ? encoder.encode(c) : c; });
                // Try to write directly to buffer
                if (this.broadcast._write(converted)) {
                    for (_i = 0, converted_2 = converted; _i < converted_2.length; _i++) {
                        c = converted_2[_i];
                        this.totalBytes += c.byteLength;
                    }
                    return [2 /*return*/];
                }
                policy = this.broadcast.backpressurePolicy;
                highWaterMark = this.broadcast.highWaterMark;
                if (policy === 'strict') {
                    // In strict mode, highWaterMark limits pendingWrites (the "hose")
                    // If too many writes are already pending, caller is ignoring backpressure
                    if (this.pendingWrites.length >= highWaterMark) {
                        throw new Error('Backpressure violation: too many pending writes. ' +
                            'Await each write() call to respect backpressure.');
                    }
                    // Otherwise, queue this write and wait for space
                    return [2 /*return*/, this.createPendingWrite(converted, signal)];
                }
                // 'block' policy - wait for space (unbounded pending writes)
                return [2 /*return*/, this.createPendingWrite(converted, signal)];
            });
        });
    };
    /**
     * Create a pending write promise, optionally racing against a signal.
     * Same pattern as PushQueue.createPendingWrite().
     */
    BroadcastWriter.prototype.createPendingWrite = function (chunk, signal) {
        var _this = this;
        return new Promise(function (resolve, reject) {
            var entry = { chunk: chunk, resolve: resolve, reject: reject };
            _this.pendingWrites.push(entry);
            if (!signal)
                return;
            var onAbort = function () {
                var _a;
                var idx = _this.pendingWrites.indexOf(entry);
                if (idx !== -1)
                    _this.pendingWrites.removeAt(idx);
                reject((_a = signal.reason) !== null && _a !== void 0 ? _a : new DOMException('Aborted', 'AbortError'));
            };
            // Wrap resolve/reject to clean up signal listener
            var origResolve = entry.resolve;
            var origReject = entry.reject;
            entry.resolve = function () {
                signal.removeEventListener('abort', onAbort);
                origResolve();
            };
            entry.reject = function (reason) {
                signal.removeEventListener('abort', onAbort);
                origReject(reason);
            };
            signal.addEventListener('abort', onAbort, { once: true });
        });
    };
    /**
     * Resolve pending writes when buffer has space.
     */
    BroadcastWriter.prototype.resolvePendingWrites = function () {
        while (this.pendingWrites.length > 0 && this.broadcast._canWrite()) {
            var pending = this.pendingWrites.shift();
            if (this.broadcast._write(pending.chunk)) {
                for (var _i = 0, _a = pending.chunk; _i < _a.length; _i++) {
                    var c = _a[_i];
                    this.totalBytes += c.byteLength;
                }
                pending.resolve();
            }
            else {
                // Couldn't write - put it back and stop
                this.pendingWrites.unshift(pending);
                break;
            }
        }
    };
    /**
     * Reject all pending writes with an error.
     */
    BroadcastWriter.prototype.rejectPendingWrites = function (error) {
        while (this.pendingWrites.length > 0) {
            var pending = this.pendingWrites.shift();
            pending.reject(error);
        }
    };
    /**
     * Resolve all pending drains with a value.
     */
    BroadcastWriter.prototype.resolvePendingDrains = function (canWrite) {
        var drains = this.pendingDrains;
        this.pendingDrains = [];
        for (var _i = 0, drains_1 = drains; _i < drains_1.length; _i++) {
            var pending = drains_1[_i];
            pending.resolve(canWrite);
        }
    };
    /**
     * Reject all pending drains with an error.
     */
    BroadcastWriter.prototype.rejectPendingDrains = function (error) {
        var drains = this.pendingDrains;
        this.pendingDrains = [];
        for (var _i = 0, drains_2 = drains; _i < drains_2.length; _i++) {
            var pending = drains_2[_i];
            pending.reject(error);
        }
    };
    BroadcastWriter.prototype.writeSync = function (chunk) {
        if (this.closed || this.aborted)
            return false;
        // Check if write would be accepted before converting
        if (!this.broadcast._canWrite()) {
            return false;
        }
        var converted = typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
        if (this.broadcast._write([converted])) {
            this.totalBytes += converted.byteLength;
            return true;
        }
        return false;
    };
    BroadcastWriter.prototype.writevSync = function (chunks) {
        if (this.closed || this.aborted)
            return false;
        // Check if write would be accepted before converting
        if (!this.broadcast._canWrite()) {
            return false;
        }
        var converted = (0, utils_js_1.allUint8Array)(chunks)
            ? chunks.slice()
            : chunks.map(function (c) { return typeof c === 'string' ? encoder.encode(c) : c; });
        if (this.broadcast._write(converted)) {
            for (var _i = 0, converted_3 = converted; _i < converted_3.length; _i++) {
                var c = converted_3[_i];
                this.totalBytes += c.byteLength;
            }
            return true;
        }
        return false;
    };
    BroadcastWriter.prototype.end = function (_options) {
        // end() rejects with TypeError if already closed/errored
        if (this.closed || this.aborted)
            return Promise.reject(new TypeError('Writer is already closed or errored'));
        this.closed = true;
        this.broadcast._end();
        // Resolve pending drains with false - writer closed, no more writes accepted
        this.resolvePendingDrains(false);
        return Promise.resolve(this.totalBytes);
    };
    BroadcastWriter.prototype.endSync = function () {
        if (this.closed)
            return -1;
        this.closed = true;
        this.broadcast._end();
        // Resolve pending drains with false - writer closed, no more writes accepted
        this.resolvePendingDrains(false);
        return this.totalBytes;
    };
    BroadcastWriter.prototype.fail = function (reason) {
        if (this.aborted || this.closed)
            return kResolvedPromise;
        this.aborted = true;
        this.closed = true;
        var error = reason !== null && reason !== void 0 ? reason : new Error('Failed');
        this.rejectPendingWrites(error);
        // Reject pending drains with the error
        this.rejectPendingDrains(error);
        this.broadcast._abort(error);
        return kResolvedPromise;
    };
    BroadcastWriter.prototype.failSync = function (reason) {
        if (this.aborted || this.closed)
            return false;
        this.aborted = true;
        this.closed = true;
        var error = reason !== null && reason !== void 0 ? reason : new Error('Failed');
        this.rejectPendingWrites(error);
        // Reject pending drains with the error
        this.rejectPendingDrains(error);
        this.broadcast._abort(error);
        return true;
    };
    BroadcastWriter.prototype[Symbol.asyncDispose] = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this.fail()];
            });
        });
    };
    BroadcastWriter.prototype[Symbol.dispose] = function () {
        this.failSync();
    };
    /**
     * Internal cancel notification from BroadcastImpl.cancel().
     * Rejects pending writes and marks writer closed so the pump can exit.
     */
    BroadcastWriter.prototype[cancelWriter] = function () {
        if (this.closed)
            return;
        this.closed = true;
        this.rejectPendingWrites(new DOMException('Broadcast cancelled', 'AbortError'));
        this.resolvePendingDrains(false);
    };
    return BroadcastWriter;
}());
// =============================================================================
// Public API
// =============================================================================
/**
 * Create a broadcast channel for push-model multi-consumer streaming.
 *
 * @param options - Buffer limit and backpressure policy
 * @returns Writer and Broadcast pair
 */
function broadcast(options) {
    var _a, _b;
    var opts = {
        highWaterMark: Math.max(1, (_a = options === null || options === void 0 ? void 0 : options.highWaterMark) !== null && _a !== void 0 ? _a : 16),
        backpressure: (_b = options === null || options === void 0 ? void 0 : options.backpressure) !== null && _b !== void 0 ? _b : 'strict',
        signal: options === null || options === void 0 ? void 0 : options.signal,
    };
    var broadcastImpl = new BroadcastImpl(opts);
    var writer = new BroadcastWriter(broadcastImpl);
    broadcastImpl.setWriter(writer);
    // Handle abort signal - cancel without error (clean shutdown)
    if (opts.signal) {
        if (opts.signal.aborted) {
            broadcastImpl.cancel();
        }
        else {
            opts.signal.addEventListener('abort', function () {
                broadcastImpl.cancel();
            }, { once: true });
        }
    }
    return { writer: writer, channel: broadcastImpl };
}
/**
 * Check if value implements Broadcastable protocol.
 */
function isBroadcastable(value) {
    return (value !== null &&
        typeof value === 'object' &&
        types_js_1.broadcastProtocol in value &&
        typeof value[types_js_1.broadcastProtocol] === 'function');
}
/**
 * Namespace for Broadcast.from() static method.
 */
exports.Broadcast = {
    /**
     * Get or create a Broadcast from a Broadcastable or Streamable.
     *
     * If input implements the broadcastProtocol, calls it.
     * Otherwise, creates a Broadcast and pumps from the source.
     */
    from: function (input, options) {
        var _this = this;
        // Check for protocol
        if (isBroadcastable(input)) {
            var bc = input[types_js_1.broadcastProtocol](options);
            // The protocol returns Broadcast, we need to create a writer
            // This is a simplification - in practice the protocol would return the full result
            return { writer: {}, channel: bc };
        }
        // Create broadcast and pump from source
        var result = broadcast(options);
        var signal = options === null || options === void 0 ? void 0 : options.signal;
        // Start pumping in background
        (function () { return __awaiter(_this, void 0, void 0, function () {
            var _a, _b, _c, chunks, e_1_1, _i, _d, chunks, error_1;
            var _e, e_1, _f, _g;
            var _h, _j;
            return __generator(this, function (_k) {
                switch (_k.label) {
                    case 0:
                        _k.trys.push([0, 20, , 22]);
                        if (!(0, from_js_1.isAsyncIterable)(input)) return [3 /*break*/, 14];
                        _k.label = 1;
                    case 1:
                        _k.trys.push([1, 7, 8, 13]);
                        _a = true, _b = __asyncValues(input);
                        _k.label = 2;
                    case 2: return [4 /*yield*/, _b.next()];
                    case 3:
                        if (!(_c = _k.sent(), _e = _c.done, !_e)) return [3 /*break*/, 6];
                        _g = _c.value;
                        _a = false;
                        chunks = _g;
                        if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                            throw (_h = signal.reason) !== null && _h !== void 0 ? _h : new DOMException('Aborted', 'AbortError');
                        }
                        if (!Array.isArray(chunks)) return [3 /*break*/, 5];
                        return [4 /*yield*/, result.writer.writev(chunks, signal ? { signal: signal } : undefined)];
                    case 4:
                        _k.sent();
                        _k.label = 5;
                    case 5:
                        _a = true;
                        return [3 /*break*/, 2];
                    case 6: return [3 /*break*/, 13];
                    case 7:
                        e_1_1 = _k.sent();
                        e_1 = { error: e_1_1 };
                        return [3 /*break*/, 13];
                    case 8:
                        _k.trys.push([8, , 11, 12]);
                        if (!(!_a && !_e && (_f = _b.return))) return [3 /*break*/, 10];
                        return [4 /*yield*/, _f.call(_b)];
                    case 9:
                        _k.sent();
                        _k.label = 10;
                    case 10: return [3 /*break*/, 12];
                    case 11:
                        if (e_1) throw e_1.error;
                        return [7 /*endfinally*/];
                    case 12: return [7 /*endfinally*/];
                    case 13: return [3 /*break*/, 18];
                    case 14:
                        if (!(0, from_js_1.isSyncIterable)(input)) return [3 /*break*/, 18];
                        _i = 0, _d = input;
                        _k.label = 15;
                    case 15:
                        if (!(_i < _d.length)) return [3 /*break*/, 18];
                        chunks = _d[_i];
                        if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                            throw (_j = signal.reason) !== null && _j !== void 0 ? _j : new DOMException('Aborted', 'AbortError');
                        }
                        if (!Array.isArray(chunks)) return [3 /*break*/, 17];
                        return [4 /*yield*/, result.writer.writev(chunks, signal ? { signal: signal } : undefined)];
                    case 16:
                        _k.sent();
                        _k.label = 17;
                    case 17:
                        _i++;
                        return [3 /*break*/, 15];
                    case 18: return [4 /*yield*/, result.writer.end(signal ? { signal: signal } : undefined)];
                    case 19:
                        _k.sent();
                        return [3 /*break*/, 22];
                    case 20:
                        error_1 = _k.sent();
                        return [4 /*yield*/, result.writer.fail(error_1 instanceof Error ? error_1 : new Error(String(error_1)))];
                    case 21:
                        _k.sent();
                        return [3 /*break*/, 22];
                    case 22: return [2 /*return*/];
                }
            });
        }); })();
        return result;
    },
};

"use strict";
/**
 * Convenience Consumers & Utilities
 *
 * bytes(), text(), arrayBuffer() - collect entire stream
 * tap(), tapSync() - observe without modifying
 * merge() - temporal combining of sources
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
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = Object.create((typeof AsyncIterator === "function" ? AsyncIterator : Object).prototype), verb("next"), verb("throw"), verb("return", awaitReturn), i[Symbol.asyncIterator] = function () { return this; }, i;
    function awaitReturn(f) { return function (v) { return Promise.resolve(v).then(f, reject); }; }
    function verb(n, f) { if (g[n]) { i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; if (f) i[n] = f(i[n]); } }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.bytesSync = bytesSync;
exports.textSync = textSync;
exports.arrayBufferSync = arrayBufferSync;
exports.arraySync = arraySync;
exports.dumpSync = dumpSync;
exports.bytes = bytes;
exports.text = text;
exports.arrayBuffer = arrayBuffer;
exports.array = array;
exports.dump = dump;
exports.tap = tap;
exports.tapSync = tapSync;
exports.ondrain = ondrain;
exports.merge = merge;
var types_js_1 = require("./types.js");
var from_js_1 = require("./from.js");
var utils_js_1 = require("./utils.js");
// =============================================================================
// Type Guards and Helpers
// =============================================================================
/**
 * Check if last argument is MergeOptions.
 */
function isMergeOptions(value) {
    return (value !== null &&
        typeof value === 'object' &&
        !(0, from_js_1.isAsyncIterable)(value) &&
        !(0, from_js_1.isSyncIterable)(value));
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
function bytesSync(source, options) {
    var limit = options === null || options === void 0 ? void 0 : options.limit;
    var chunks = [];
    var totalBytes = 0;
    for (var _i = 0, source_1 = source; _i < source_1.length; _i++) {
        var batch = source_1[_i];
        for (var _a = 0, batch_1 = batch; _a < batch_1.length; _a++) {
            var chunk = batch_1[_a];
            if (limit !== undefined) {
                totalBytes += chunk.byteLength;
                if (totalBytes > limit) {
                    throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                }
            }
            chunks.push(chunk);
        }
    }
    return (0, utils_js_1.concatBytes)(chunks);
}
/**
 * Collect and decode text from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional encoding and limit
 * @returns Decoded string
 */
function textSync(source, options) {
    var _a;
    var data = bytesSync(source, options);
    var decoder = new TextDecoder((_a = options === null || options === void 0 ? void 0 : options.encoding) !== null && _a !== void 0 ? _a : 'utf-8', {
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
function arrayBufferSync(source, options) {
    var data = bytesSync(source, options);
    // Return the underlying ArrayBuffer, or copy if it's a view of a larger buffer
    if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
        return data.buffer;
    }
    return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
}
/**
 * Collect all chunks as an array from a sync source.
 *
 * @param source - Sync iterable yielding Uint8Array[] batches
 * @param options - Optional limit
 * @returns Array of Uint8Array chunks
 */
function arraySync(source, options) {
    var limit = options === null || options === void 0 ? void 0 : options.limit;
    var chunks = [];
    var totalBytes = 0;
    for (var _i = 0, source_2 = source; _i < source_2.length; _i++) {
        var batch = source_2[_i];
        for (var _a = 0, batch_2 = batch; _a < batch_2.length; _a++) {
            var chunk = batch_2[_a];
            if (limit !== undefined) {
                totalBytes += chunk.byteLength;
                if (totalBytes > limit) {
                    throw new RangeError("Stream exceeded byte limit of ".concat(limit));
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
function dumpSync(source, options) {
    var limit = options === null || options === void 0 ? void 0 : options.limit;
    var totalBytes = 0;
    for (var _i = 0, source_3 = source; _i < source_3.length; _i++) {
        var batch = source_3[_i];
        // Fast path: with no limit there is no reason to look at the chunks at all.
        if (limit === undefined)
            continue;
        for (var _a = 0, batch_3 = batch; _a < batch_3.length; _a++) {
            var chunk = batch_3[_a];
            totalBytes += chunk.byteLength;
            if (totalBytes > limit) {
                throw new RangeError("Stream exceeded byte limit of ".concat(limit));
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
function bytes(source, options) {
    return __awaiter(this, void 0, void 0, function () {
        var signal, limit, chunks, batch, _i, batch_4, chunk, e_1_1, _a, source_4, batch, _b, batch_5, chunk, totalBytes, batch, _c, batch_6, chunk, e_2_1, _d, source_5, batch, _e, batch_7, chunk;
        var _f, source_6, source_6_1, _g, source_7, source_7_1;
        var _h, e_1, _j, _k, _l, e_2, _m, _o;
        var _p, _q, _r;
        return __generator(this, function (_s) {
            switch (_s.label) {
                case 0:
                    signal = options === null || options === void 0 ? void 0 : options.signal;
                    limit = options === null || options === void 0 ? void 0 : options.limit;
                    // Check for abort
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_p = signal.reason) !== null && _p !== void 0 ? _p : new DOMException('Aborted', 'AbortError');
                    }
                    chunks = [];
                    if (!(!signal && limit === undefined)) return [3 /*break*/, 15];
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 13];
                    _s.label = 1;
                case 1:
                    _s.trys.push([1, 6, 7, 12]);
                    _f = true, source_6 = __asyncValues(source);
                    _s.label = 2;
                case 2: return [4 /*yield*/, source_6.next()];
                case 3:
                    if (!(source_6_1 = _s.sent(), _h = source_6_1.done, !_h)) return [3 /*break*/, 5];
                    _k = source_6_1.value;
                    _f = false;
                    batch = _k;
                    for (_i = 0, batch_4 = batch; _i < batch_4.length; _i++) {
                        chunk = batch_4[_i];
                        chunks.push(chunk);
                    }
                    _s.label = 4;
                case 4:
                    _f = true;
                    return [3 /*break*/, 2];
                case 5: return [3 /*break*/, 12];
                case 6:
                    e_1_1 = _s.sent();
                    e_1 = { error: e_1_1 };
                    return [3 /*break*/, 12];
                case 7:
                    _s.trys.push([7, , 10, 11]);
                    if (!(!_f && !_h && (_j = source_6.return))) return [3 /*break*/, 9];
                    return [4 /*yield*/, _j.call(source_6)];
                case 8:
                    _s.sent();
                    _s.label = 9;
                case 9: return [3 /*break*/, 11];
                case 10:
                    if (e_1) throw e_1.error;
                    return [7 /*endfinally*/];
                case 11: return [7 /*endfinally*/];
                case 12: return [3 /*break*/, 14];
                case 13:
                    if ((0, from_js_1.isSyncIterable)(source)) {
                        for (_a = 0, source_4 = source; _a < source_4.length; _a++) {
                            batch = source_4[_a];
                            for (_b = 0, batch_5 = batch; _b < batch_5.length; _b++) {
                                chunk = batch_5[_b];
                                chunks.push(chunk);
                            }
                        }
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    _s.label = 14;
                case 14: return [2 /*return*/, (0, utils_js_1.concatBytes)(chunks)];
                case 15:
                    totalBytes = 0;
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 28];
                    _s.label = 16;
                case 16:
                    _s.trys.push([16, 21, 22, 27]);
                    _g = true, source_7 = __asyncValues(source);
                    _s.label = 17;
                case 17: return [4 /*yield*/, source_7.next()];
                case 18:
                    if (!(source_7_1 = _s.sent(), _l = source_7_1.done, !_l)) return [3 /*break*/, 20];
                    _o = source_7_1.value;
                    _g = false;
                    batch = _o;
                    // Check for abort on each iteration
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_q = signal.reason) !== null && _q !== void 0 ? _q : new DOMException('Aborted', 'AbortError');
                    }
                    for (_c = 0, batch_6 = batch; _c < batch_6.length; _c++) {
                        chunk = batch_6[_c];
                        if (limit !== undefined) {
                            totalBytes += chunk.byteLength;
                            if (totalBytes > limit) {
                                throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                            }
                        }
                        chunks.push(chunk);
                    }
                    _s.label = 19;
                case 19:
                    _g = true;
                    return [3 /*break*/, 17];
                case 20: return [3 /*break*/, 27];
                case 21:
                    e_2_1 = _s.sent();
                    e_2 = { error: e_2_1 };
                    return [3 /*break*/, 27];
                case 22:
                    _s.trys.push([22, , 25, 26]);
                    if (!(!_g && !_l && (_m = source_7.return))) return [3 /*break*/, 24];
                    return [4 /*yield*/, _m.call(source_7)];
                case 23:
                    _s.sent();
                    _s.label = 24;
                case 24: return [3 /*break*/, 26];
                case 25:
                    if (e_2) throw e_2.error;
                    return [7 /*endfinally*/];
                case 26: return [7 /*endfinally*/];
                case 27: return [3 /*break*/, 29];
                case 28:
                    if ((0, from_js_1.isSyncIterable)(source)) {
                        for (_d = 0, source_5 = source; _d < source_5.length; _d++) {
                            batch = source_5[_d];
                            // Check for abort on each iteration
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_r = signal.reason) !== null && _r !== void 0 ? _r : new DOMException('Aborted', 'AbortError');
                            }
                            for (_e = 0, batch_7 = batch; _e < batch_7.length; _e++) {
                                chunk = batch_7[_e];
                                if (limit !== undefined) {
                                    totalBytes += chunk.byteLength;
                                    if (totalBytes > limit) {
                                        throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                                    }
                                }
                                chunks.push(chunk);
                            }
                        }
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    _s.label = 29;
                case 29: return [2 /*return*/, (0, utils_js_1.concatBytes)(chunks)];
            }
        });
    });
}
/**
 * Collect and decode text from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional encoding, signal, and limit
 * @returns Promise resolving to decoded string
 */
function text(source, options) {
    return __awaiter(this, void 0, void 0, function () {
        var data, decoder;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, bytes(source, options)];
                case 1:
                    data = _b.sent();
                    decoder = new TextDecoder((_a = options === null || options === void 0 ? void 0 : options.encoding) !== null && _a !== void 0 ? _a : 'utf-8', {
                        fatal: true,
                        ignoreBOM: true,
                    });
                    return [2 /*return*/, decoder.decode(data)];
            }
        });
    });
}
/**
 * Collect bytes as ArrayBuffer from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to ArrayBuffer
 */
function arrayBuffer(source, options) {
    return __awaiter(this, void 0, void 0, function () {
        var data;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, bytes(source, options)];
                case 1:
                    data = _a.sent();
                    // Return the underlying ArrayBuffer, or copy if it's a view of a larger buffer
                    if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
                        return [2 /*return*/, data.buffer];
                    }
                    return [2 /*return*/, data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)];
            }
        });
    });
}
/**
 * Collect all chunks as an array from an async or sync source.
 *
 * @param source - Iterable or async iterable yielding Uint8Array[] batches
 * @param options - Optional signal and limit
 * @returns Promise resolving to array of Uint8Array chunks
 */
function array(source, options) {
    return __awaiter(this, void 0, void 0, function () {
        var signal, limit, chunks, batch, _i, batch_8, chunk, e_3_1, _a, source_8, batch, _b, batch_9, chunk, totalBytes, batch, _c, batch_10, chunk, e_4_1, _d, source_9, batch, _e, batch_11, chunk;
        var _f, source_10, source_10_1, _g, source_11, source_11_1;
        var _h, e_3, _j, _k, _l, e_4, _m, _o;
        var _p, _q, _r;
        return __generator(this, function (_s) {
            switch (_s.label) {
                case 0:
                    signal = options === null || options === void 0 ? void 0 : options.signal;
                    limit = options === null || options === void 0 ? void 0 : options.limit;
                    // Check for abort
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_p = signal.reason) !== null && _p !== void 0 ? _p : new DOMException('Aborted', 'AbortError');
                    }
                    chunks = [];
                    if (!(!signal && limit === undefined)) return [3 /*break*/, 15];
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 13];
                    _s.label = 1;
                case 1:
                    _s.trys.push([1, 6, 7, 12]);
                    _f = true, source_10 = __asyncValues(source);
                    _s.label = 2;
                case 2: return [4 /*yield*/, source_10.next()];
                case 3:
                    if (!(source_10_1 = _s.sent(), _h = source_10_1.done, !_h)) return [3 /*break*/, 5];
                    _k = source_10_1.value;
                    _f = false;
                    batch = _k;
                    for (_i = 0, batch_8 = batch; _i < batch_8.length; _i++) {
                        chunk = batch_8[_i];
                        chunks.push(chunk);
                    }
                    _s.label = 4;
                case 4:
                    _f = true;
                    return [3 /*break*/, 2];
                case 5: return [3 /*break*/, 12];
                case 6:
                    e_3_1 = _s.sent();
                    e_3 = { error: e_3_1 };
                    return [3 /*break*/, 12];
                case 7:
                    _s.trys.push([7, , 10, 11]);
                    if (!(!_f && !_h && (_j = source_10.return))) return [3 /*break*/, 9];
                    return [4 /*yield*/, _j.call(source_10)];
                case 8:
                    _s.sent();
                    _s.label = 9;
                case 9: return [3 /*break*/, 11];
                case 10:
                    if (e_3) throw e_3.error;
                    return [7 /*endfinally*/];
                case 11: return [7 /*endfinally*/];
                case 12: return [3 /*break*/, 14];
                case 13:
                    if ((0, from_js_1.isSyncIterable)(source)) {
                        for (_a = 0, source_8 = source; _a < source_8.length; _a++) {
                            batch = source_8[_a];
                            for (_b = 0, batch_9 = batch; _b < batch_9.length; _b++) {
                                chunk = batch_9[_b];
                                chunks.push(chunk);
                            }
                        }
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    _s.label = 14;
                case 14: return [2 /*return*/, chunks];
                case 15:
                    totalBytes = 0;
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 28];
                    _s.label = 16;
                case 16:
                    _s.trys.push([16, 21, 22, 27]);
                    _g = true, source_11 = __asyncValues(source);
                    _s.label = 17;
                case 17: return [4 /*yield*/, source_11.next()];
                case 18:
                    if (!(source_11_1 = _s.sent(), _l = source_11_1.done, !_l)) return [3 /*break*/, 20];
                    _o = source_11_1.value;
                    _g = false;
                    batch = _o;
                    // Check for abort on each iteration
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_q = signal.reason) !== null && _q !== void 0 ? _q : new DOMException('Aborted', 'AbortError');
                    }
                    for (_c = 0, batch_10 = batch; _c < batch_10.length; _c++) {
                        chunk = batch_10[_c];
                        if (limit !== undefined) {
                            totalBytes += chunk.byteLength;
                            if (totalBytes > limit) {
                                throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                            }
                        }
                        chunks.push(chunk);
                    }
                    _s.label = 19;
                case 19:
                    _g = true;
                    return [3 /*break*/, 17];
                case 20: return [3 /*break*/, 27];
                case 21:
                    e_4_1 = _s.sent();
                    e_4 = { error: e_4_1 };
                    return [3 /*break*/, 27];
                case 22:
                    _s.trys.push([22, , 25, 26]);
                    if (!(!_g && !_l && (_m = source_11.return))) return [3 /*break*/, 24];
                    return [4 /*yield*/, _m.call(source_11)];
                case 23:
                    _s.sent();
                    _s.label = 24;
                case 24: return [3 /*break*/, 26];
                case 25:
                    if (e_4) throw e_4.error;
                    return [7 /*endfinally*/];
                case 26: return [7 /*endfinally*/];
                case 27: return [3 /*break*/, 29];
                case 28:
                    if ((0, from_js_1.isSyncIterable)(source)) {
                        for (_d = 0, source_9 = source; _d < source_9.length; _d++) {
                            batch = source_9[_d];
                            // Check for abort on each iteration
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_r = signal.reason) !== null && _r !== void 0 ? _r : new DOMException('Aborted', 'AbortError');
                            }
                            for (_e = 0, batch_11 = batch; _e < batch_11.length; _e++) {
                                chunk = batch_11[_e];
                                if (limit !== undefined) {
                                    totalBytes += chunk.byteLength;
                                    if (totalBytes > limit) {
                                        throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                                    }
                                }
                                chunks.push(chunk);
                            }
                        }
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    _s.label = 29;
                case 29: return [2 /*return*/, chunks];
            }
        });
    });
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
function dump(source, options) {
    return __awaiter(this, void 0, void 0, function () {
        var signal, limit, totalBytes, batch, _i, batch_12, chunk, e_5_1, _a, source_12, batch, _b, batch_13, chunk;
        var _c, source_13, source_13_1;
        var _d, e_5, _e, _f;
        var _g, _h, _j;
        return __generator(this, function (_k) {
            switch (_k.label) {
                case 0:
                    signal = options === null || options === void 0 ? void 0 : options.signal;
                    limit = options === null || options === void 0 ? void 0 : options.limit;
                    // Check for abort
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_g = signal.reason) !== null && _g !== void 0 ? _g : new DOMException('Aborted', 'AbortError');
                    }
                    totalBytes = 0;
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 13];
                    _k.label = 1;
                case 1:
                    _k.trys.push([1, 6, 7, 12]);
                    _c = true, source_13 = __asyncValues(source);
                    _k.label = 2;
                case 2: return [4 /*yield*/, source_13.next()];
                case 3:
                    if (!(source_13_1 = _k.sent(), _d = source_13_1.done, !_d)) return [3 /*break*/, 5];
                    _f = source_13_1.value;
                    _c = false;
                    batch = _f;
                    // Check for abort on each iteration
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_h = signal.reason) !== null && _h !== void 0 ? _h : new DOMException('Aborted', 'AbortError');
                    }
                    if (limit === undefined)
                        return [3 /*break*/, 4];
                    for (_i = 0, batch_12 = batch; _i < batch_12.length; _i++) {
                        chunk = batch_12[_i];
                        totalBytes += chunk.byteLength;
                        if (totalBytes > limit) {
                            throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                        }
                    }
                    _k.label = 4;
                case 4:
                    _c = true;
                    return [3 /*break*/, 2];
                case 5: return [3 /*break*/, 12];
                case 6:
                    e_5_1 = _k.sent();
                    e_5 = { error: e_5_1 };
                    return [3 /*break*/, 12];
                case 7:
                    _k.trys.push([7, , 10, 11]);
                    if (!(!_c && !_d && (_e = source_13.return))) return [3 /*break*/, 9];
                    return [4 /*yield*/, _e.call(source_13)];
                case 8:
                    _k.sent();
                    _k.label = 9;
                case 9: return [3 /*break*/, 11];
                case 10:
                    if (e_5) throw e_5.error;
                    return [7 /*endfinally*/];
                case 11: return [7 /*endfinally*/];
                case 12: return [3 /*break*/, 14];
                case 13:
                    if ((0, from_js_1.isSyncIterable)(source)) {
                        for (_a = 0, source_12 = source; _a < source_12.length; _a++) {
                            batch = source_12[_a];
                            // Check for abort on each iteration
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_j = signal.reason) !== null && _j !== void 0 ? _j : new DOMException('Aborted', 'AbortError');
                            }
                            if (limit === undefined)
                                continue;
                            for (_b = 0, batch_13 = batch; _b < batch_13.length; _b++) {
                                chunk = batch_13[_b];
                                totalBytes += chunk.byteLength;
                                if (totalBytes > limit) {
                                    throw new RangeError("Stream exceeded byte limit of ".concat(limit));
                                }
                            }
                        }
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    _k.label = 14;
                case 14: return [2 /*return*/];
            }
        });
    });
}
/**
 * Create a pass-through transform that observes chunks without modifying them.
 * Useful for logging, hashing, metrics, etc.
 *
 * @param callback - Called with each batch or null (flush signal)
 * @returns Transform that passes chunks through unchanged
 */
function tap(callback) {
    var _this = this;
    return function (chunks, options) { return __awaiter(_this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, callback(chunks, options)];
                case 1:
                    _a.sent();
                    return [2 /*return*/, chunks];
            }
        });
    }); };
}
/**
 * Create a sync pass-through transform that observes chunks without modifying them.
 *
 * @param callback - Called with each batch or null (flush signal)
 * @returns SyncTransform that passes chunks through unchanged
 */
function tapSync(callback) {
    return function (chunks) {
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
function ondrain(drainable) {
    // Defensive: never throw synchronously
    // Return null for anything that doesn't implement the protocol
    if (drainable === null ||
        drainable === undefined ||
        typeof drainable !== 'object') {
        return null;
    }
    // Check if object has the drainable protocol
    if (!(types_js_1.drainableProtocol in drainable) ||
        typeof drainable[types_js_1.drainableProtocol] !== 'function') {
        return null;
    }
    // Call the protocol method
    // This may return null (drain not applicable) or a Promise
    try {
        return drainable[types_js_1.drainableProtocol]();
    }
    catch (_a) {
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
function merge() {
    var _a;
    var args = [];
    for (var _i = 0; _i < arguments.length; _i++) {
        args[_i] = arguments[_i];
    }
    // Parse arguments: last arg may be MergeOptions if it has a signal member
    // and no iteration protocols
    var rawSources;
    var options;
    if (args.length > 0 && isMergeOptions(args[args.length - 1])) {
        options = args[args.length - 1];
        rawSources = args.slice(0, -1);
    }
    else {
        rawSources = args;
    }
    // Normalize each source via from()
    var sources = rawSources.map(function (s) { return (0, from_js_1.from)(s); });
    return _a = {},
        _a[Symbol.asyncIterator] = function () {
            return __asyncGenerator(this, arguments, function _a() {
                var signal, _b, _c, _d, batch, e_6_1, states, startIterator, pending, _e, index, result, returnPromises;
                var _this = this;
                var _f, e_6, _g, _h;
                var _j, _k, _l;
                return __generator(this, function (_m) {
                    switch (_m.label) {
                        case 0:
                            signal = options === null || options === void 0 ? void 0 : options.signal;
                            // Check for abort
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_j = signal.reason) !== null && _j !== void 0 ? _j : new DOMException('Aborted', 'AbortError');
                            }
                            if (!(sources.length === 0)) return [3 /*break*/, 2];
                            return [4 /*yield*/, __await(void 0)];
                        case 1: return [2 /*return*/, _m.sent()];
                        case 2:
                            if (!(sources.length === 1)) return [3 /*break*/, 18];
                            _m.label = 3;
                        case 3:
                            _m.trys.push([3, 10, 11, 16]);
                            _b = true, _c = __asyncValues(sources[0]);
                            _m.label = 4;
                        case 4: return [4 /*yield*/, __await(_c.next())];
                        case 5:
                            if (!(_d = _m.sent(), _f = _d.done, !_f)) return [3 /*break*/, 9];
                            _h = _d.value;
                            _b = false;
                            batch = _h;
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_k = signal.reason) !== null && _k !== void 0 ? _k : new DOMException('Aborted', 'AbortError');
                            }
                            return [4 /*yield*/, __await(batch)];
                        case 6: return [4 /*yield*/, _m.sent()];
                        case 7:
                            _m.sent();
                            _m.label = 8;
                        case 8:
                            _b = true;
                            return [3 /*break*/, 4];
                        case 9: return [3 /*break*/, 16];
                        case 10:
                            e_6_1 = _m.sent();
                            e_6 = { error: e_6_1 };
                            return [3 /*break*/, 16];
                        case 11:
                            _m.trys.push([11, , 14, 15]);
                            if (!(!_b && !_f && (_g = _c.return))) return [3 /*break*/, 13];
                            return [4 /*yield*/, __await(_g.call(_c))];
                        case 12:
                            _m.sent();
                            _m.label = 13;
                        case 13: return [3 /*break*/, 15];
                        case 14:
                            if (e_6) throw e_6.error;
                            return [7 /*endfinally*/];
                        case 15: return [7 /*endfinally*/];
                        case 16: return [4 /*yield*/, __await(void 0)];
                        case 17: return [2 /*return*/, _m.sent()];
                        case 18:
                            states = sources.map(function (source) { return ({
                                iterator: source[Symbol.asyncIterator](),
                                done: false,
                                pending: null,
                            }); });
                            startIterator = function (state, index) {
                                if (!state.done && !state.pending) {
                                    state.pending = state.iterator.next().then(function (result) { return ({ index: index, result: result }); });
                                }
                            };
                            // Start all
                            states.forEach(startIterator);
                            _m.label = 19;
                        case 19:
                            _m.trys.push([19, , 27, 29]);
                            _m.label = 20;
                        case 20:
                            if (!true) return [3 /*break*/, 26];
                            // Check for abort
                            if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                                throw (_l = signal.reason) !== null && _l !== void 0 ? _l : new DOMException('Aborted', 'AbortError');
                            }
                            pending = states
                                .map(function (state, _index) { return (state.pending ? state.pending : null); })
                                .filter(function (p) { return p !== null; });
                            if (pending.length === 0) {
                                // All done
                                return [3 /*break*/, 26];
                            }
                            return [4 /*yield*/, __await(Promise.race(pending))];
                        case 21:
                            _e = _m.sent(), index = _e.index, result = _e.result;
                            // Clear the pending promise
                            states[index].pending = null;
                            if (!result.done) return [3 /*break*/, 22];
                            states[index].done = true;
                            return [3 /*break*/, 25];
                        case 22: return [4 /*yield*/, __await(result.value)];
                        case 23: return [4 /*yield*/, _m.sent()];
                        case 24:
                            _m.sent();
                            // Start next iteration for this source
                            startIterator(states[index], index);
                            _m.label = 25;
                        case 25: return [3 /*break*/, 20];
                        case 26: return [3 /*break*/, 29];
                        case 27:
                            returnPromises = states.map(function (state) { return __awaiter(_this, void 0, void 0, function () {
                                var _b;
                                return __generator(this, function (_c) {
                                    switch (_c.label) {
                                        case 0:
                                            if (!(!state.done && state.iterator.return)) return [3 /*break*/, 4];
                                            _c.label = 1;
                                        case 1:
                                            _c.trys.push([1, 3, , 4]);
                                            return [4 /*yield*/, state.iterator.return()];
                                        case 2:
                                            _c.sent();
                                            return [3 /*break*/, 4];
                                        case 3:
                                            _b = _c.sent();
                                            return [3 /*break*/, 4];
                                        case 4: return [2 /*return*/];
                                    }
                                });
                            }); });
                            return [4 /*yield*/, __await(Promise.all(returnPromises))];
                        case 28:
                            _m.sent();
                            return [7 /*endfinally*/];
                        case 29: return [2 /*return*/];
                    }
                });
            });
        },
        _a;
}

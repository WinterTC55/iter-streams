"use strict";
/**
 * Pull Pipeline - pull(), pullSync(), pipeTo(), pipeToSync()
 *
 * Pull-through pipelines with transforms. Data flows on-demand from source
 * through transforms to consumer.
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
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __asyncDelegator = (this && this.__asyncDelegator) || function (o) {
    var i, p;
    return i = {}, verb("next"), verb("throw", function (e) { throw e; }), verb("return"), i[Symbol.iterator] = function () { return this; }, i;
    function verb(n, f) { i[n] = o[n] ? function (v) { return (p = !p) ? { value: __await(o[n](v)), done: false } : f ? f(v) : v; } : f; }
};
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
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var __values = (this && this.__values) || function(o) {
    var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
    if (m) return m.call(o);
    if (o && typeof o.length === "number") return {
        next: function () {
            if (o && i >= o.length) o = void 0;
            return { value: o && o[i++], done: !o };
        }
    };
    throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.pullSync = pullSync;
exports.pull = pull;
exports.pipeToSync = pipeToSync;
exports.pipeTo = pipeTo;
var from_js_1 = require("./from.js");
var utils_js_1 = require("./utils.js");
// Shared TextEncoder instance for string conversion
var encoder = new TextEncoder();
// =============================================================================
// Type Guards and Helpers
// =============================================================================
/**
 * Check if a value is a TransformObject (has transform property).
 */
function isTransformObject(value) {
    return (value !== null &&
        typeof value === 'object' &&
        'transform' in value &&
        typeof value.transform === 'function');
}
/**
 * Check if a value is a SyncTransformObject.
 */
function isSyncTransformObject(value) {
    return isTransformObject(value);
}
/**
 * Check if a value is a Writer (has write method).
 */
function isWriter(value) {
    return (value !== null &&
        typeof value === 'object' &&
        'write' in value &&
        typeof value.write === 'function');
}
/**
 * Parse variadic arguments for pipeTo/pipeToSync.
 * Returns { transforms, writer, options }
 */
function parsePipeToArgs(args) {
    if (args.length === 0) {
        throw new TypeError('pipeTo requires a writer argument');
    }
    var options;
    var writerIndex = args.length - 1;
    // Check if last arg is options
    var last = args[args.length - 1];
    if ((0, utils_js_1.isPullOptions)(last) && !isWriter(last)) {
        options = last;
        writerIndex = args.length - 2;
    }
    if (writerIndex < 0) {
        throw new TypeError('pipeTo requires a writer argument');
    }
    var writer = args[writerIndex];
    if (!isWriter(writer)) {
        throw new TypeError('pipeTo requires a writer argument with a write method');
    }
    return {
        transforms: args.slice(0, writerIndex),
        writer: writer,
        options: options,
    };
}
// =============================================================================
// Transform Output Flattening
// =============================================================================
/**
 * Flatten transform yield to Uint8Array chunks (sync).
 */
function flattenTransformYieldSync(value) {
    var _i, value_1, item;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                if (!(value instanceof Uint8Array)) return [3 /*break*/, 2];
                return [4 /*yield*/, value];
            case 1:
                _a.sent();
                return [2 /*return*/];
            case 2:
                if (!(typeof value === 'string')) return [3 /*break*/, 4];
                return [4 /*yield*/, encoder.encode(value)];
            case 3:
                _a.sent();
                return [2 /*return*/];
            case 4:
                if (!(0, from_js_1.isSyncIterable)(value)) return [3 /*break*/, 9];
                _i = 0, value_1 = value;
                _a.label = 5;
            case 5:
                if (!(_i < value_1.length)) return [3 /*break*/, 8];
                item = value_1[_i];
                return [5 /*yield**/, __values(flattenTransformYieldSync(item))];
            case 6:
                _a.sent();
                _a.label = 7;
            case 7:
                _i++;
                return [3 /*break*/, 5];
            case 8: return [2 /*return*/];
            case 9: throw new TypeError("Invalid transform yield type: ".concat(typeof value));
        }
    });
}
/**
 * Flatten transform yield to Uint8Array chunks (async).
 */
function flattenTransformYieldAsync(value) {
    return __asyncGenerator(this, arguments, function flattenTransformYieldAsync_1() {
        var _a, value_2, value_2_1, item, e_1_1, _i, value_3, item;
        var _b, e_1, _c, _d;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    if (!(value instanceof Uint8Array)) return [3 /*break*/, 4];
                    return [4 /*yield*/, __await(value)];
                case 1: return [4 /*yield*/, _e.sent()];
                case 2:
                    _e.sent();
                    return [4 /*yield*/, __await(void 0)];
                case 3: return [2 /*return*/, _e.sent()];
                case 4:
                    if (!(typeof value === 'string')) return [3 /*break*/, 8];
                    return [4 /*yield*/, __await(encoder.encode(value))];
                case 5: return [4 /*yield*/, _e.sent()];
                case 6:
                    _e.sent();
                    return [4 /*yield*/, __await(void 0)];
                case 7: return [2 /*return*/, _e.sent()];
                case 8:
                    if (!(0, from_js_1.isAsyncIterable)(value)) return [3 /*break*/, 24];
                    _e.label = 9;
                case 9:
                    _e.trys.push([9, 16, 17, 22]);
                    _a = true, value_2 = __asyncValues(value);
                    _e.label = 10;
                case 10: return [4 /*yield*/, __await(value_2.next())];
                case 11:
                    if (!(value_2_1 = _e.sent(), _b = value_2_1.done, !_b)) return [3 /*break*/, 15];
                    _d = value_2_1.value;
                    _a = false;
                    item = _d;
                    return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(flattenTransformYieldAsync(item))))];
                case 12: return [4 /*yield*/, __await.apply(void 0, [_e.sent()])];
                case 13:
                    _e.sent();
                    _e.label = 14;
                case 14:
                    _a = true;
                    return [3 /*break*/, 10];
                case 15: return [3 /*break*/, 22];
                case 16:
                    e_1_1 = _e.sent();
                    e_1 = { error: e_1_1 };
                    return [3 /*break*/, 22];
                case 17:
                    _e.trys.push([17, , 20, 21]);
                    if (!(!_a && !_b && (_c = value_2.return))) return [3 /*break*/, 19];
                    return [4 /*yield*/, __await(_c.call(value_2))];
                case 18:
                    _e.sent();
                    _e.label = 19;
                case 19: return [3 /*break*/, 21];
                case 20:
                    if (e_1) throw e_1.error;
                    return [7 /*endfinally*/];
                case 21: return [7 /*endfinally*/];
                case 22: return [4 /*yield*/, __await(void 0)];
                case 23: return [2 /*return*/, _e.sent()];
                case 24:
                    if (!(0, from_js_1.isSyncIterable)(value)) return [3 /*break*/, 31];
                    _i = 0, value_3 = value;
                    _e.label = 25;
                case 25:
                    if (!(_i < value_3.length)) return [3 /*break*/, 29];
                    item = value_3[_i];
                    return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(flattenTransformYieldAsync(item))))];
                case 26: return [4 /*yield*/, __await.apply(void 0, [_e.sent()])];
                case 27:
                    _e.sent();
                    _e.label = 28;
                case 28:
                    _i++;
                    return [3 /*break*/, 25];
                case 29: return [4 /*yield*/, __await(void 0)];
                case 30: return [2 /*return*/, _e.sent()];
                case 31: throw new TypeError("Invalid transform yield type: ".concat(typeof value));
            }
        });
    });
}
/**
 * Process transform result (sync).
 */
function processTransformResultSync(result) {
    var batch, _i, result_1, item, _a, _b, chunk;
    return __generator(this, function (_c) {
        switch (_c.label) {
            case 0:
                if (result === null) {
                    return [2 /*return*/];
                }
                if (!(Array.isArray(result) && result.length > 0 && result[0] instanceof Uint8Array)) return [3 /*break*/, 3];
                if (!(result.length > 0)) return [3 /*break*/, 2];
                return [4 /*yield*/, result];
            case 1:
                _c.sent();
                _c.label = 2;
            case 2: return [2 /*return*/];
            case 3:
                if (!(0, from_js_1.isSyncIterable)(result)) return [3 /*break*/, 6];
                batch = [];
                for (_i = 0, result_1 = result; _i < result_1.length; _i++) {
                    item = result_1[_i];
                    for (_a = 0, _b = flattenTransformYieldSync(item); _a < _b.length; _a++) {
                        chunk = _b[_a];
                        batch.push(chunk);
                    }
                }
                if (!(batch.length > 0)) return [3 /*break*/, 5];
                return [4 /*yield*/, batch];
            case 4:
                _c.sent();
                _c.label = 5;
            case 5: return [2 /*return*/];
            case 6: throw new TypeError("Invalid transform result type");
        }
    });
}
/**
 * Process transform result (async).
 */
function processTransformResultAsync(result) {
    return __asyncGenerator(this, arguments, function processTransformResultAsync_1() {
        var resolved, batch, _a, result_2, result_2_1, item, _b, _c, _d, chunk, e_2_1, e_3_1, batch, _i, result_3, item, _e, _f, _g, chunk, e_4_1;
        var _h, e_3, _j, _k, _l, e_2, _m, _o, _p, e_4, _q, _r;
        return __generator(this, function (_s) {
            switch (_s.label) {
                case 0:
                    if (!(result instanceof Promise)) return [3 /*break*/, 5];
                    return [4 /*yield*/, __await(result)];
                case 1:
                    resolved = _s.sent();
                    return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(processTransformResultAsync(resolved))))];
                case 2: return [4 /*yield*/, __await.apply(void 0, [_s.sent()])];
                case 3:
                    _s.sent();
                    return [4 /*yield*/, __await(void 0)];
                case 4: return [2 /*return*/, _s.sent()];
                case 5:
                    if (!(result === null)) return [3 /*break*/, 7];
                    return [4 /*yield*/, __await(void 0)];
                case 6: return [2 /*return*/, _s.sent()];
                case 7:
                    if (!(Array.isArray(result) && (result.length === 0 || result[0] instanceof Uint8Array))) return [3 /*break*/, 12];
                    if (!(result.length > 0)) return [3 /*break*/, 10];
                    return [4 /*yield*/, __await(result)];
                case 8: return [4 /*yield*/, _s.sent()];
                case 9:
                    _s.sent();
                    _s.label = 10;
                case 10: return [4 /*yield*/, __await(void 0)];
                case 11: return [2 /*return*/, _s.sent()];
                case 12:
                    if (!(0, from_js_1.isAsyncIterable)(result)) return [3 /*break*/, 40];
                    batch = [];
                    _s.label = 13;
                case 13:
                    _s.trys.push([13, 29, 30, 35]);
                    _a = true, result_2 = __asyncValues(result);
                    _s.label = 14;
                case 14: return [4 /*yield*/, __await(result_2.next())];
                case 15:
                    if (!(result_2_1 = _s.sent(), _h = result_2_1.done, !_h)) return [3 /*break*/, 28];
                    _k = result_2_1.value;
                    _a = false;
                    item = _k;
                    // Fast path: item is already Uint8Array
                    if (item instanceof Uint8Array) {
                        batch.push(item);
                        return [3 /*break*/, 27];
                    }
                    _s.label = 16;
                case 16:
                    _s.trys.push([16, 21, 22, 27]);
                    _b = true, _c = (e_2 = void 0, __asyncValues(flattenTransformYieldAsync(item)));
                    _s.label = 17;
                case 17: return [4 /*yield*/, __await(_c.next())];
                case 18:
                    if (!(_d = _s.sent(), _l = _d.done, !_l)) return [3 /*break*/, 20];
                    _o = _d.value;
                    _b = false;
                    chunk = _o;
                    batch.push(chunk);
                    _s.label = 19;
                case 19:
                    _b = true;
                    return [3 /*break*/, 17];
                case 20: return [3 /*break*/, 27];
                case 21:
                    e_2_1 = _s.sent();
                    e_2 = { error: e_2_1 };
                    return [3 /*break*/, 27];
                case 22:
                    _s.trys.push([22, , 25, 26]);
                    if (!(!_b && !_l && (_m = _c.return))) return [3 /*break*/, 24];
                    return [4 /*yield*/, __await(_m.call(_c))];
                case 23:
                    _s.sent();
                    _s.label = 24;
                case 24: return [3 /*break*/, 26];
                case 25:
                    if (e_2) throw e_2.error;
                    return [7 /*endfinally*/];
                case 26: return [7 /*endfinally*/];
                case 27:
                    _a = true;
                    return [3 /*break*/, 14];
                case 28: return [3 /*break*/, 35];
                case 29:
                    e_3_1 = _s.sent();
                    e_3 = { error: e_3_1 };
                    return [3 /*break*/, 35];
                case 30:
                    _s.trys.push([30, , 33, 34]);
                    if (!(!_a && !_h && (_j = result_2.return))) return [3 /*break*/, 32];
                    return [4 /*yield*/, __await(_j.call(result_2))];
                case 31:
                    _s.sent();
                    _s.label = 32;
                case 32: return [3 /*break*/, 34];
                case 33:
                    if (e_3) throw e_3.error;
                    return [7 /*endfinally*/];
                case 34: return [7 /*endfinally*/];
                case 35:
                    if (!(batch.length > 0)) return [3 /*break*/, 38];
                    return [4 /*yield*/, __await(batch)];
                case 36: return [4 /*yield*/, _s.sent()];
                case 37:
                    _s.sent();
                    _s.label = 38;
                case 38: return [4 /*yield*/, __await(void 0)];
                case 39: return [2 /*return*/, _s.sent()];
                case 40:
                    if (!(0, from_js_1.isSyncIterable)(result)) return [3 /*break*/, 59];
                    batch = [];
                    _i = 0, result_3 = result;
                    _s.label = 41;
                case 41:
                    if (!(_i < result_3.length)) return [3 /*break*/, 54];
                    item = result_3[_i];
                    // Fast path: item is already Uint8Array
                    if (item instanceof Uint8Array) {
                        batch.push(item);
                        return [3 /*break*/, 53];
                    }
                    _s.label = 42;
                case 42:
                    _s.trys.push([42, 47, 48, 53]);
                    _e = true, _f = (e_4 = void 0, __asyncValues(flattenTransformYieldAsync(item)));
                    _s.label = 43;
                case 43: return [4 /*yield*/, __await(_f.next())];
                case 44:
                    if (!(_g = _s.sent(), _p = _g.done, !_p)) return [3 /*break*/, 46];
                    _r = _g.value;
                    _e = false;
                    chunk = _r;
                    batch.push(chunk);
                    _s.label = 45;
                case 45:
                    _e = true;
                    return [3 /*break*/, 43];
                case 46: return [3 /*break*/, 53];
                case 47:
                    e_4_1 = _s.sent();
                    e_4 = { error: e_4_1 };
                    return [3 /*break*/, 53];
                case 48:
                    _s.trys.push([48, , 51, 52]);
                    if (!(!_e && !_p && (_q = _f.return))) return [3 /*break*/, 50];
                    return [4 /*yield*/, __await(_q.call(_f))];
                case 49:
                    _s.sent();
                    _s.label = 50;
                case 50: return [3 /*break*/, 52];
                case 51:
                    if (e_4) throw e_4.error;
                    return [7 /*endfinally*/];
                case 52: return [7 /*endfinally*/];
                case 53:
                    _i++;
                    return [3 /*break*/, 41];
                case 54:
                    if (!(batch.length > 0)) return [3 /*break*/, 57];
                    return [4 /*yield*/, __await(batch)];
                case 55: return [4 /*yield*/, _s.sent()];
                case 56:
                    _s.sent();
                    _s.label = 57;
                case 57: return [4 /*yield*/, __await(void 0)];
                case 58: return [2 /*return*/, _s.sent()];
                case 59: throw new TypeError("Invalid transform result type");
            }
        });
    });
}
// =============================================================================
// Sync Pipeline Implementation
// =============================================================================
/**
 * Apply a single stateless sync transform to a source.
 */
function applyStatelessSyncTransform(source, transform) {
    var _i, source_1, chunks, result;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                _i = 0, source_1 = source;
                _a.label = 1;
            case 1:
                if (!(_i < source_1.length)) return [3 /*break*/, 4];
                chunks = source_1[_i];
                result = transform(chunks);
                return [5 /*yield**/, __values(processTransformResultSync(result))];
            case 2:
                _a.sent();
                _a.label = 3;
            case 3:
                _i++;
                return [3 /*break*/, 1];
            case 4: return [2 /*return*/];
        }
    });
}
/**
 * Apply a single stateful sync transform to a source.
 */
function applyStatefulSyncTransform(source, transform) {
    var output, _i, output_1, item, batch, _a, _b, chunk;
    return __generator(this, function (_c) {
        switch (_c.label) {
            case 0:
                output = transform(source);
                _i = 0, output_1 = output;
                _c.label = 1;
            case 1:
                if (!(_i < output_1.length)) return [3 /*break*/, 4];
                item = output_1[_i];
                batch = [];
                for (_a = 0, _b = flattenTransformYieldSync(item); _a < _b.length; _a++) {
                    chunk = _b[_a];
                    batch.push(chunk);
                }
                if (!(batch.length > 0)) return [3 /*break*/, 3];
                return [4 /*yield*/, batch];
            case 2:
                _c.sent();
                _c.label = 3;
            case 3:
                _i++;
                return [3 /*break*/, 1];
            case 4: return [2 /*return*/];
        }
    });
}
/**
 * Wrap sync source to add null flush signal at end.
 */
function withFlushSignalSync(source) {
    var _i, source_2, batch;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                _i = 0, source_2 = source;
                _a.label = 1;
            case 1:
                if (!(_i < source_2.length)) return [3 /*break*/, 4];
                batch = source_2[_i];
                return [4 /*yield*/, batch];
            case 2:
                _a.sent();
                _a.label = 3;
            case 3:
                _i++;
                return [3 /*break*/, 1];
            case 4: return [4 /*yield*/, null];
            case 5:
                _a.sent(); // Flush signal
                return [2 /*return*/];
        }
    });
}
/**
 * Create a sync pipeline from source through transforms.
 */
function createSyncPipeline(source, transforms) {
    var current, _i, transforms_1, transform, _a, current_1, batch;
    return __generator(this, function (_b) {
        switch (_b.label) {
            case 0:
                current = withFlushSignalSync((0, from_js_1.normalizeSyncSource)(source));
                // Apply transforms
                // Object = stateful, function = stateless
                for (_i = 0, transforms_1 = transforms; _i < transforms_1.length; _i++) {
                    transform = transforms_1[_i];
                    if (isSyncTransformObject(transform)) {
                        current = applyStatefulSyncTransform(current, transform.transform);
                    }
                    else {
                        current = applyStatelessSyncTransform(current, transform);
                    }
                }
                _a = 0, current_1 = current;
                _b.label = 1;
            case 1:
                if (!(_a < current_1.length)) return [3 /*break*/, 4];
                batch = current_1[_a];
                if (!(batch !== null)) return [3 /*break*/, 3];
                return [4 /*yield*/, batch];
            case 2:
                _b.sent();
                _b.label = 3;
            case 3:
                _a++;
                return [3 /*break*/, 1];
            case 4: return [2 /*return*/];
        }
    });
}
// =============================================================================
// Async Pipeline Implementation
// =============================================================================
/**
 * Check if result is already a Uint8Array[] (fast path).
 */
function isUint8ArrayBatch(result) {
    if (!Array.isArray(result))
        return false;
    if (result.length === 0)
        return true;
    return result[0] instanceof Uint8Array;
}
/**
 * Apply a single stateless async transform to a source.
 */
function applyStatelessAsyncTransform(source, transform, options) {
    return __asyncGenerator(this, arguments, function applyStatelessAsyncTransform_1() {
        var _a, source_3, source_3_1, chunks, result, resolved, batch, _i, _b, item, _c, _d, _e, chunk, e_5_1, e_6_1;
        var _f, e_6, _g, _h, _j, e_5, _k, _l;
        return __generator(this, function (_m) {
            switch (_m.label) {
                case 0:
                    _m.trys.push([0, 39, 40, 45]);
                    _a = true, source_3 = __asyncValues(source);
                    _m.label = 1;
                case 1: return [4 /*yield*/, __await(source_3.next())];
                case 2:
                    if (!(source_3_1 = _m.sent(), _f = source_3_1.done, !_f)) return [3 /*break*/, 38];
                    _h = source_3_1.value;
                    _a = false;
                    chunks = _h;
                    result = transform(chunks, options);
                    // Fast path: result is already Uint8Array[] (common case)
                    if (result === null)
                        return [3 /*break*/, 37];
                    if (!isUint8ArrayBatch(result)) return [3 /*break*/, 6];
                    if (!(result.length > 0)) return [3 /*break*/, 5];
                    return [4 /*yield*/, __await(result)];
                case 3: return [4 /*yield*/, _m.sent()];
                case 4:
                    _m.sent();
                    _m.label = 5;
                case 5: return [3 /*break*/, 37];
                case 6:
                    if (!(result instanceof Promise)) return [3 /*break*/, 14];
                    return [4 /*yield*/, __await(result)];
                case 7:
                    resolved = _m.sent();
                    if (resolved === null)
                        return [3 /*break*/, 37];
                    if (!isUint8ArrayBatch(resolved)) return [3 /*break*/, 11];
                    if (!(resolved.length > 0)) return [3 /*break*/, 10];
                    return [4 /*yield*/, __await(resolved)];
                case 8: return [4 /*yield*/, _m.sent()];
                case 9:
                    _m.sent();
                    _m.label = 10;
                case 10: return [3 /*break*/, 37];
                case 11: 
                // Fall through to slow path
                return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(processTransformResultAsync(resolved))))];
                case 12: 
                // Fall through to slow path
                return [4 /*yield*/, __await.apply(void 0, [_m.sent()])];
                case 13:
                    // Fall through to slow path
                    _m.sent();
                    return [3 /*break*/, 37];
                case 14:
                    if (!((0, from_js_1.isSyncIterable)(result) && !(0, from_js_1.isAsyncIterable)(result))) return [3 /*break*/, 34];
                    batch = [];
                    _i = 0, _b = result;
                    _m.label = 15;
                case 15:
                    if (!(_i < _b.length)) return [3 /*break*/, 30];
                    item = _b[_i];
                    if (!isUint8ArrayBatch(item)) return [3 /*break*/, 16];
                    batch.push.apply(batch, item);
                    return [3 /*break*/, 29];
                case 16:
                    if (!(item instanceof Uint8Array)) return [3 /*break*/, 17];
                    // Single Uint8Array
                    batch.push(item);
                    return [3 /*break*/, 29];
                case 17:
                    if (!(item !== null && item !== undefined)) return [3 /*break*/, 29];
                    _m.label = 18;
                case 18:
                    _m.trys.push([18, 23, 24, 29]);
                    _c = true, _d = (e_5 = void 0, __asyncValues(flattenTransformYieldAsync(item)));
                    _m.label = 19;
                case 19: return [4 /*yield*/, __await(_d.next())];
                case 20:
                    if (!(_e = _m.sent(), _j = _e.done, !_j)) return [3 /*break*/, 22];
                    _l = _e.value;
                    _c = false;
                    chunk = _l;
                    batch.push(chunk);
                    _m.label = 21;
                case 21:
                    _c = true;
                    return [3 /*break*/, 19];
                case 22: return [3 /*break*/, 29];
                case 23:
                    e_5_1 = _m.sent();
                    e_5 = { error: e_5_1 };
                    return [3 /*break*/, 29];
                case 24:
                    _m.trys.push([24, , 27, 28]);
                    if (!(!_c && !_j && (_k = _d.return))) return [3 /*break*/, 26];
                    return [4 /*yield*/, __await(_k.call(_d))];
                case 25:
                    _m.sent();
                    _m.label = 26;
                case 26: return [3 /*break*/, 28];
                case 27:
                    if (e_5) throw e_5.error;
                    return [7 /*endfinally*/];
                case 28: return [7 /*endfinally*/];
                case 29:
                    _i++;
                    return [3 /*break*/, 15];
                case 30:
                    if (!(batch.length > 0)) return [3 /*break*/, 33];
                    return [4 /*yield*/, __await(batch)];
                case 31: return [4 /*yield*/, _m.sent()];
                case 32:
                    _m.sent();
                    _m.label = 33;
                case 33: return [3 /*break*/, 37];
                case 34: 
                // Slow path for other types (async iterables, complex nested structures)
                return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(processTransformResultAsync(result))))];
                case 35: 
                // Slow path for other types (async iterables, complex nested structures)
                return [4 /*yield*/, __await.apply(void 0, [_m.sent()])];
                case 36:
                    // Slow path for other types (async iterables, complex nested structures)
                    _m.sent();
                    _m.label = 37;
                case 37:
                    _a = true;
                    return [3 /*break*/, 1];
                case 38: return [3 /*break*/, 45];
                case 39:
                    e_6_1 = _m.sent();
                    e_6 = { error: e_6_1 };
                    return [3 /*break*/, 45];
                case 40:
                    _m.trys.push([40, , 43, 44]);
                    if (!(!_a && !_f && (_g = source_3.return))) return [3 /*break*/, 42];
                    return [4 /*yield*/, __await(_g.call(source_3))];
                case 41:
                    _m.sent();
                    _m.label = 42;
                case 42: return [3 /*break*/, 44];
                case 43:
                    if (e_6) throw e_6.error;
                    return [7 /*endfinally*/];
                case 44: return [7 /*endfinally*/];
                case 45: return [2 /*return*/];
            }
        });
    });
}
/**
 * Apply a single stateful async transform to a source.
 */
function applyStatefulAsyncTransform(source, transform, options) {
    return __asyncGenerator(this, arguments, function applyStatefulAsyncTransform_1() {
        var output, _a, output_2, output_2_1, item, batch, _b, _c, _d, chunk, e_7_1, e_8_1;
        var _e, e_8, _f, _g, _h, e_7, _j, _k;
        return __generator(this, function (_l) {
            switch (_l.label) {
                case 0:
                    output = transform(source, options);
                    _l.label = 1;
                case 1:
                    _l.trys.push([1, 20, 21, 26]);
                    _a = true, output_2 = __asyncValues(output);
                    _l.label = 2;
                case 2: return [4 /*yield*/, __await(output_2.next())];
                case 3:
                    if (!(output_2_1 = _l.sent(), _e = output_2_1.done, !_e)) return [3 /*break*/, 19];
                    _g = output_2_1.value;
                    _a = false;
                    item = _g;
                    batch = [];
                    _l.label = 4;
                case 4:
                    _l.trys.push([4, 9, 10, 15]);
                    _b = true, _c = (e_7 = void 0, __asyncValues(flattenTransformYieldAsync(item)));
                    _l.label = 5;
                case 5: return [4 /*yield*/, __await(_c.next())];
                case 6:
                    if (!(_d = _l.sent(), _h = _d.done, !_h)) return [3 /*break*/, 8];
                    _k = _d.value;
                    _b = false;
                    chunk = _k;
                    batch.push(chunk);
                    _l.label = 7;
                case 7:
                    _b = true;
                    return [3 /*break*/, 5];
                case 8: return [3 /*break*/, 15];
                case 9:
                    e_7_1 = _l.sent();
                    e_7 = { error: e_7_1 };
                    return [3 /*break*/, 15];
                case 10:
                    _l.trys.push([10, , 13, 14]);
                    if (!(!_b && !_h && (_j = _c.return))) return [3 /*break*/, 12];
                    return [4 /*yield*/, __await(_j.call(_c))];
                case 11:
                    _l.sent();
                    _l.label = 12;
                case 12: return [3 /*break*/, 14];
                case 13:
                    if (e_7) throw e_7.error;
                    return [7 /*endfinally*/];
                case 14: return [7 /*endfinally*/];
                case 15:
                    if (!(batch.length > 0)) return [3 /*break*/, 18];
                    return [4 /*yield*/, __await(batch)];
                case 16: return [4 /*yield*/, _l.sent()];
                case 17:
                    _l.sent();
                    _l.label = 18;
                case 18:
                    _a = true;
                    return [3 /*break*/, 2];
                case 19: return [3 /*break*/, 26];
                case 20:
                    e_8_1 = _l.sent();
                    e_8 = { error: e_8_1 };
                    return [3 /*break*/, 26];
                case 21:
                    _l.trys.push([21, , 24, 25]);
                    if (!(!_a && !_e && (_f = output_2.return))) return [3 /*break*/, 23];
                    return [4 /*yield*/, __await(_f.call(output_2))];
                case 22:
                    _l.sent();
                    _l.label = 23;
                case 23: return [3 /*break*/, 25];
                case 24:
                    if (e_8) throw e_8.error;
                    return [7 /*endfinally*/];
                case 25: return [7 /*endfinally*/];
                case 26: return [2 /*return*/];
            }
        });
    });
}
/**
 * Wrap async source to add null flush signal at end.
 */
function withFlushSignalAsync(source) {
    return __asyncGenerator(this, arguments, function withFlushSignalAsync_1() {
        var _a, source_4, source_4_1, batch, e_9_1;
        var _b, e_9, _c, _d;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    _e.trys.push([0, 7, 8, 13]);
                    _a = true, source_4 = __asyncValues(source);
                    _e.label = 1;
                case 1: return [4 /*yield*/, __await(source_4.next())];
                case 2:
                    if (!(source_4_1 = _e.sent(), _b = source_4_1.done, !_b)) return [3 /*break*/, 6];
                    _d = source_4_1.value;
                    _a = false;
                    batch = _d;
                    return [4 /*yield*/, __await(batch)];
                case 3: return [4 /*yield*/, _e.sent()];
                case 4:
                    _e.sent();
                    _e.label = 5;
                case 5:
                    _a = true;
                    return [3 /*break*/, 1];
                case 6: return [3 /*break*/, 13];
                case 7:
                    e_9_1 = _e.sent();
                    e_9 = { error: e_9_1 };
                    return [3 /*break*/, 13];
                case 8:
                    _e.trys.push([8, , 11, 12]);
                    if (!(!_a && !_b && (_c = source_4.return))) return [3 /*break*/, 10];
                    return [4 /*yield*/, __await(_c.call(source_4))];
                case 9:
                    _e.sent();
                    _e.label = 10;
                case 10: return [3 /*break*/, 12];
                case 11:
                    if (e_9) throw e_9.error;
                    return [7 /*endfinally*/];
                case 12: return [7 /*endfinally*/];
                case 13: return [4 /*yield*/, __await(null)];
                case 14: return [4 /*yield*/, _e.sent()];
                case 15:
                    _e.sent(); // Flush signal
                    return [2 /*return*/];
            }
        });
    });
}
/**
 * Convert sync iterable to async iterable.
 */
function syncToAsync(source) {
    return __asyncGenerator(this, arguments, function syncToAsync_1() {
        var _i, source_5, item;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _i = 0, source_5 = source;
                    _a.label = 1;
                case 1:
                    if (!(_i < source_5.length)) return [3 /*break*/, 5];
                    item = source_5[_i];
                    return [4 /*yield*/, __await(item)];
                case 2: return [4 /*yield*/, _a.sent()];
                case 3:
                    _a.sent();
                    _a.label = 4;
                case 4:
                    _i++;
                    return [3 /*break*/, 1];
                case 5: return [2 /*return*/];
            }
        });
    });
}
/**
 * Create an async pipeline from source through transforms.
 */
function createAsyncPipeline(source, transforms, signal) {
    return __asyncGenerator(this, arguments, function createAsyncPipeline_1() {
        var normalized, _a, normalized_1, normalized_1_1, batch, e_10_1, controller, abortHandler, current, _i, transforms_2, transform, options, completed, _b, current_2, current_2_1, batch, e_11_1, error_1;
        var _c, e_10, _d, _e, _f, e_11, _g, _h;
        var _j, _k, _l;
        return __generator(this, function (_m) {
            switch (_m.label) {
                case 0:
                    // Check for abort
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_j = signal.reason) !== null && _j !== void 0 ? _j : new DOMException('Aborted', 'AbortError');
                    }
                    if ((0, from_js_1.isAsyncIterable)(source)) {
                        normalized = (0, from_js_1.normalizeAsyncSource)(source);
                    }
                    else if ((0, from_js_1.isSyncIterable)(source)) {
                        normalized = syncToAsync((0, from_js_1.normalizeSyncSource)(source));
                    }
                    else {
                        throw new TypeError('Source must be iterable');
                    }
                    if (!(transforms.length === 0)) return [3 /*break*/, 16];
                    _m.label = 1;
                case 1:
                    _m.trys.push([1, 8, 9, 14]);
                    _a = true, normalized_1 = __asyncValues(normalized);
                    _m.label = 2;
                case 2: return [4 /*yield*/, __await(normalized_1.next())];
                case 3:
                    if (!(normalized_1_1 = _m.sent(), _c = normalized_1_1.done, !_c)) return [3 /*break*/, 7];
                    _e = normalized_1_1.value;
                    _a = false;
                    batch = _e;
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_k = signal.reason) !== null && _k !== void 0 ? _k : new DOMException('Aborted', 'AbortError');
                    }
                    return [4 /*yield*/, __await(batch)];
                case 4: return [4 /*yield*/, _m.sent()];
                case 5:
                    _m.sent();
                    _m.label = 6;
                case 6:
                    _a = true;
                    return [3 /*break*/, 2];
                case 7: return [3 /*break*/, 14];
                case 8:
                    e_10_1 = _m.sent();
                    e_10 = { error: e_10_1 };
                    return [3 /*break*/, 14];
                case 9:
                    _m.trys.push([9, , 12, 13]);
                    if (!(!_a && !_c && (_d = normalized_1.return))) return [3 /*break*/, 11];
                    return [4 /*yield*/, __await(_d.call(normalized_1))];
                case 10:
                    _m.sent();
                    _m.label = 11;
                case 11: return [3 /*break*/, 13];
                case 12:
                    if (e_10) throw e_10.error;
                    return [7 /*endfinally*/];
                case 13: return [7 /*endfinally*/];
                case 14: return [4 /*yield*/, __await(void 0)];
                case 15: return [2 /*return*/, _m.sent()];
                case 16:
                    controller = new AbortController();
                    if (signal) {
                        abortHandler = function () {
                            var _a;
                            try {
                                controller.abort((_a = signal.reason) !== null && _a !== void 0 ? _a : new DOMException('Aborted', 'AbortError'));
                            }
                            catch ( /* transform signal listeners may throw — suppress */_b) { /* transform signal listeners may throw — suppress */ }
                        };
                        signal.addEventListener('abort', abortHandler);
                    }
                    current = withFlushSignalAsync(normalized);
                    // Apply transforms — each gets its own options object
                    // Object = stateful, function = stateless
                    for (_i = 0, transforms_2 = transforms; _i < transforms_2.length; _i++) {
                        transform = transforms_2[_i];
                        options = { signal: controller.signal };
                        if (isTransformObject(transform)) {
                            current = applyStatefulAsyncTransform(current, transform.transform, options);
                        }
                        else {
                            current = applyStatelessAsyncTransform(current, transform, options);
                        }
                    }
                    completed = false;
                    _m.label = 17;
                case 17:
                    _m.trys.push([17, 32, 33, 34]);
                    _m.label = 18;
                case 18:
                    _m.trys.push([18, 25, 26, 31]);
                    _b = true, current_2 = __asyncValues(current);
                    _m.label = 19;
                case 19: return [4 /*yield*/, __await(current_2.next())];
                case 20:
                    if (!(current_2_1 = _m.sent(), _f = current_2_1.done, !_f)) return [3 /*break*/, 24];
                    _h = current_2_1.value;
                    _b = false;
                    batch = _h;
                    // Check for abort on each iteration
                    if (controller.signal.aborted) {
                        throw (_l = controller.signal.reason) !== null && _l !== void 0 ? _l : new DOMException('Aborted', 'AbortError');
                    }
                    if (!(batch !== null)) return [3 /*break*/, 23];
                    return [4 /*yield*/, __await(batch)];
                case 21: return [4 /*yield*/, _m.sent()];
                case 22:
                    _m.sent();
                    _m.label = 23;
                case 23:
                    _b = true;
                    return [3 /*break*/, 19];
                case 24: return [3 /*break*/, 31];
                case 25:
                    e_11_1 = _m.sent();
                    e_11 = { error: e_11_1 };
                    return [3 /*break*/, 31];
                case 26:
                    _m.trys.push([26, , 29, 30]);
                    if (!(!_b && !_f && (_g = current_2.return))) return [3 /*break*/, 28];
                    return [4 /*yield*/, __await(_g.call(current_2))];
                case 27:
                    _m.sent();
                    _m.label = 28;
                case 28: return [3 /*break*/, 30];
                case 29:
                    if (e_11) throw e_11.error;
                    return [7 /*endfinally*/];
                case 30: return [7 /*endfinally*/];
                case 31:
                    completed = true;
                    return [3 /*break*/, 34];
                case 32:
                    error_1 = _m.sent();
                    if (!controller.signal.aborted) {
                        try {
                            controller.abort(error_1 instanceof Error ? error_1 : new Error(String(error_1)));
                        }
                        catch ( /* transform signal listeners may throw — suppress */_o) { /* transform signal listeners may throw — suppress */ }
                    }
                    throw error_1;
                case 33:
                    if (!completed && !controller.signal.aborted) {
                        try {
                            controller.abort(new DOMException('Aborted', 'AbortError'));
                        }
                        catch ( /* transform signal listeners may throw — suppress */_p) { /* transform signal listeners may throw — suppress */ }
                    }
                    // Clean up user signal listener to prevent holding controller alive
                    if (signal && abortHandler) {
                        signal.removeEventListener('abort', abortHandler);
                    }
                    return [7 /*endfinally*/];
                case 34: return [2 /*return*/];
            }
        });
    });
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
function pullSync(source) {
    var _a;
    var transforms = [];
    for (var _i = 1; _i < arguments.length; _i++) {
        transforms[_i - 1] = arguments[_i];
    }
    return _a = {},
        _a[Symbol.iterator] = function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [5 /*yield**/, __values(createSyncPipeline(source, transforms))];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        },
        _a;
}
/**
 * Create an async pull-through pipeline with transforms.
 *
 * @param source - The streamable source (sync or async)
 * @param args - Variadic transforms, with optional PullOptions as last argument
 * @returns An async iterable yielding Uint8Array[] batches
 */
function pull(source) {
    var _a;
    var args = [];
    for (var _i = 1; _i < arguments.length; _i++) {
        args[_i - 1] = arguments[_i];
    }
    var _b = (0, utils_js_1.parsePullArgs)(args), transforms = _b.transforms, options = _b.options;
    return _a = {},
        _a[Symbol.asyncIterator] = function () {
            return __asyncGenerator(this, arguments, function _a() {
                return __generator(this, function (_b) {
                    switch (_b.label) {
                        case 0: return [5 /*yield**/, __values(__asyncDelegator(__asyncValues(createAsyncPipeline(source, transforms, options === null || options === void 0 ? void 0 : options.signal))))];
                        case 1: return [4 /*yield*/, __await.apply(void 0, [_b.sent()])];
                        case 2:
                            _b.sent();
                            return [2 /*return*/];
                    }
                });
            });
        },
        _a;
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
function pipeToSync(source) {
    var _a;
    var args = [];
    for (var _i = 1; _i < arguments.length; _i++) {
        args[_i - 1] = arguments[_i];
    }
    var _b = parsePipeToArgs(args), transforms = _b.transforms, writer = _b.writer, options = _b.options;
    // Handle transform-writer: if writer has transform, apply it as last transform
    var finalTransforms = __spreadArray([], transforms, true);
    if (isTransformObject(writer)) {
        finalTransforms.push(writer);
    }
    // Create pipeline
    var pipeline = finalTransforms.length > 0
        ? createSyncPipeline((_a = {}, _a[Symbol.iterator] = function () { return source[Symbol.iterator](); }, _a), finalTransforms)
        : source;
    var totalBytes = 0;
    try {
        for (var _c = 0, pipeline_1 = pipeline; _c < pipeline_1.length; _c++) {
            var batch = pipeline_1[_c];
            if ('writev' in writer && typeof writer.writev === 'function') {
                if (!writer.writev(batch)) {
                    throw new Error('Sync writev failed');
                }
                for (var _d = 0, batch_1 = batch; _d < batch_1.length; _d++) {
                    var chunk = batch_1[_d];
                    totalBytes += chunk.byteLength;
                }
            }
            else {
                for (var _e = 0, batch_2 = batch; _e < batch_2.length; _e++) {
                    var chunk = batch_2[_e];
                    if (!writer.write(chunk)) {
                        throw new Error('Sync write failed');
                    }
                    totalBytes += chunk.byteLength;
                }
            }
        }
        if (!(options === null || options === void 0 ? void 0 : options.preventClose)) {
            writer.end();
        }
    }
    catch (error) {
        if (!(options === null || options === void 0 ? void 0 : options.preventFail)) {
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
function pipeTo(source) {
    var args = [];
    for (var _i = 1; _i < arguments.length; _i++) {
        args[_i - 1] = arguments[_i];
    }
    return __awaiter(this, void 0, void 0, function () {
        var _a, transforms, writer, options, finalTransforms, signal, totalBytes, hasWritev, writeBatch, batch, e_12_1, _b, _c, batch, streamableSource, pipeline, _d, pipeline_2, pipeline_2_1, batch, e_13_1, error_2;
        var _e, _f;
        var _this = this;
        var _g, source_6, source_6_1;
        var _h, e_12, _j, _k, _l, e_13, _m, _o;
        var _p, _q, _r, _s;
        return __generator(this, function (_t) {
            switch (_t.label) {
                case 0:
                    _a = parsePipeToArgs(args), transforms = _a.transforms, writer = _a.writer, options = _a.options;
                    finalTransforms = __spreadArray([], transforms, true);
                    if (isTransformObject(writer)) {
                        finalTransforms.push(writer);
                    }
                    signal = options === null || options === void 0 ? void 0 : options.signal;
                    // Check for abort
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_p = signal.reason) !== null && _p !== void 0 ? _p : new DOMException('Aborted', 'AbortError');
                    }
                    totalBytes = 0;
                    hasWritev = typeof writer.writev === 'function';
                    writeBatch = function (batch) { return __awaiter(_this, void 0, void 0, function () {
                        var _i, batch_3, chunk, promises, _a, batch_4, chunk, result;
                        return __generator(this, function (_b) {
                            switch (_b.label) {
                                case 0:
                                    if (!(hasWritev && batch.length > 1)) return [3 /*break*/, 2];
                                    return [4 /*yield*/, writer.writev(batch, signal ? { signal: signal } : undefined)];
                                case 1:
                                    _b.sent();
                                    for (_i = 0, batch_3 = batch; _i < batch_3.length; _i++) {
                                        chunk = batch_3[_i];
                                        totalBytes += chunk.byteLength;
                                    }
                                    return [3 /*break*/, 4];
                                case 2:
                                    promises = [];
                                    for (_a = 0, batch_4 = batch; _a < batch_4.length; _a++) {
                                        chunk = batch_4[_a];
                                        result = writer.write(chunk, signal ? { signal: signal } : undefined);
                                        if (result !== undefined) {
                                            promises.push(result);
                                        }
                                        totalBytes += chunk.byteLength;
                                    }
                                    if (!(promises.length > 0)) return [3 /*break*/, 4];
                                    return [4 /*yield*/, Promise.all(promises)];
                                case 3:
                                    _b.sent();
                                    _b.label = 4;
                                case 4: return [2 /*return*/];
                            }
                        });
                    }); };
                    _t.label = 1;
                case 1:
                    _t.trys.push([1, 36, , 39]);
                    if (!(finalTransforms.length === 0)) return [3 /*break*/, 20];
                    if (!(0, from_js_1.isAsyncIterable)(source)) return [3 /*break*/, 15];
                    _t.label = 2;
                case 2:
                    _t.trys.push([2, 8, 9, 14]);
                    _g = true, source_6 = __asyncValues(source);
                    _t.label = 3;
                case 3: return [4 /*yield*/, source_6.next()];
                case 4:
                    if (!(source_6_1 = _t.sent(), _h = source_6_1.done, !_h)) return [3 /*break*/, 7];
                    _k = source_6_1.value;
                    _g = false;
                    batch = _k;
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_q = signal.reason) !== null && _q !== void 0 ? _q : new DOMException('Aborted', 'AbortError');
                    }
                    return [4 /*yield*/, writeBatch(batch)];
                case 5:
                    _t.sent();
                    _t.label = 6;
                case 6:
                    _g = true;
                    return [3 /*break*/, 3];
                case 7: return [3 /*break*/, 14];
                case 8:
                    e_12_1 = _t.sent();
                    e_12 = { error: e_12_1 };
                    return [3 /*break*/, 14];
                case 9:
                    _t.trys.push([9, , 12, 13]);
                    if (!(!_g && !_h && (_j = source_6.return))) return [3 /*break*/, 11];
                    return [4 /*yield*/, _j.call(source_6)];
                case 10:
                    _t.sent();
                    _t.label = 11;
                case 11: return [3 /*break*/, 13];
                case 12:
                    if (e_12) throw e_12.error;
                    return [7 /*endfinally*/];
                case 13: return [7 /*endfinally*/];
                case 14: return [3 /*break*/, 19];
                case 15:
                    _b = 0, _c = source;
                    _t.label = 16;
                case 16:
                    if (!(_b < _c.length)) return [3 /*break*/, 19];
                    batch = _c[_b];
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_r = signal.reason) !== null && _r !== void 0 ? _r : new DOMException('Aborted', 'AbortError');
                    }
                    return [4 /*yield*/, writeBatch(batch)];
                case 17:
                    _t.sent();
                    _t.label = 18;
                case 18:
                    _b++;
                    return [3 /*break*/, 16];
                case 19: return [3 /*break*/, 33];
                case 20:
                    streamableSource = (0, from_js_1.isAsyncIterable)(source)
                        ? (_e = {}, _e[Symbol.asyncIterator] = function () { return source[Symbol.asyncIterator](); }, _e) : (_f = {}, _f[Symbol.iterator] = function () { return source[Symbol.iterator](); }, _f);
                    pipeline = createAsyncPipeline(streamableSource, finalTransforms, signal);
                    _t.label = 21;
                case 21:
                    _t.trys.push([21, 27, 28, 33]);
                    _d = true, pipeline_2 = __asyncValues(pipeline);
                    _t.label = 22;
                case 22: return [4 /*yield*/, pipeline_2.next()];
                case 23:
                    if (!(pipeline_2_1 = _t.sent(), _l = pipeline_2_1.done, !_l)) return [3 /*break*/, 26];
                    _o = pipeline_2_1.value;
                    _d = false;
                    batch = _o;
                    if (signal === null || signal === void 0 ? void 0 : signal.aborted) {
                        throw (_s = signal.reason) !== null && _s !== void 0 ? _s : new DOMException('Aborted', 'AbortError');
                    }
                    return [4 /*yield*/, writeBatch(batch)];
                case 24:
                    _t.sent();
                    _t.label = 25;
                case 25:
                    _d = true;
                    return [3 /*break*/, 22];
                case 26: return [3 /*break*/, 33];
                case 27:
                    e_13_1 = _t.sent();
                    e_13 = { error: e_13_1 };
                    return [3 /*break*/, 33];
                case 28:
                    _t.trys.push([28, , 31, 32]);
                    if (!(!_d && !_l && (_m = pipeline_2.return))) return [3 /*break*/, 30];
                    return [4 /*yield*/, _m.call(pipeline_2)];
                case 29:
                    _t.sent();
                    _t.label = 30;
                case 30: return [3 /*break*/, 32];
                case 31:
                    if (e_13) throw e_13.error;
                    return [7 /*endfinally*/];
                case 32: return [7 /*endfinally*/];
                case 33:
                    if (!!(options === null || options === void 0 ? void 0 : options.preventClose)) return [3 /*break*/, 35];
                    return [4 /*yield*/, writer.end(signal ? { signal: signal } : undefined)];
                case 34:
                    _t.sent();
                    _t.label = 35;
                case 35: return [3 /*break*/, 39];
                case 36:
                    error_2 = _t.sent();
                    if (!!(options === null || options === void 0 ? void 0 : options.preventFail)) return [3 /*break*/, 38];
                    return [4 /*yield*/, writer.fail(error_2 instanceof Error ? error_2 : new Error(String(error_2)))];
                case 37:
                    _t.sent();
                    _t.label = 38;
                case 38: throw error_2;
                case 39: return [2 /*return*/, totalBytes];
            }
        });
    });
}

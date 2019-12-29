
'use strict';

const _ = require('./lodash');
const stream = require('readable-stream');

function wait(delay) {
  return new Promise((resolve) => {
    setTimeout(resolve, delay);
  });
}

module.exports.wait = wait;

function streamToArray(readStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readStream
      .on('data', (chunk) => { chunks.push(chunk); })
      .on('end', () => { resolve(chunks); })
      .on('error', (err) => { reject(err); });
  });
}

module.exports.streamToArray = streamToArray;

function streamToString(readStream) {
  return new Promise((resolve, reject) => {
    let buffer = '';
    readStream
      .on('data', (chunk) => { buffer += chunk.toString(); })
      .on('end', () => { resolve(buffer); })
      .on('error', (err) => { reject(err); });
  });
}

module.exports.streamToString = streamToString;

function isReadableStream(obj) {
  return _.isObject(obj)
    && _.isFunction(obj.on)
    && _.isFunction(obj.read);
}

module.exports.isReadableStream = isReadableStream;

function isPromise(obj) {
  return _.isObject(obj)
    && _.isFunction(obj.then);
}

module.exports.isPromise = isPromise;

function isAbstractLevelDownClass(obj) {
  return _.isFunction(obj)
    && _.isFunction(obj.prototype.batch)
    && _.isFunction(obj.prototype.iterator);
}

module.exports.isAbstractLevelDownClass = isAbstractLevelDownClass;

function isAbstractLevelDOWNInstance(obj) {
  return _.isObject(obj)
    && _.isFunction(obj.put)
    && _.isFunction(obj.del)
    && _.isFunction(obj.batch);
}

module.exports.isAbstractLevelDOWNInstance = isAbstractLevelDOWNInstance;

function createArrayStream(arr) {
  let i = 0;
  const l = arr.length;
  return new stream.Readable({
    objectMode: true,
    read() {
      this.push(i < l ? arr[i++] : null);
    }
  });
}

module.exports.createArrayStream = createArrayStream;

function resolveOnEvent(emitter, event, rejectOnError) {
  return new Promise((resolve, reject) => {
    emitter.on(event, resolve);
    if (rejectOnError) {
      emitter.on('error', reject);
    }
  });
}

module.exports.resolveOnEvent = resolveOnEvent;
module.exports.waitForEvent = resolveOnEvent;

class IteratorStream extends stream.Readable {
  constructor(iterator) {
    super({ objectMode: true });
    const is = this;
    this._reading = false;
    this._iterator = iterator;
    this._iterator.on('end', () => {
      is.push(null);
    });
  }
  _read() {
    const is = this;
    is._startReading();
  }
  _startReading() {
    const is = this;
    if (is._reading) return;
    is._reading = true;
    is._iterator.on('data', (quad) => {
      if (!is.push(quad)) {
        is._stopReading();
      }
    });
  }
  _stopReading() {
    const is = this;
    is._iterator.removeAllListeners('data');
    is._reading = false;
  }
}

function createIteratorStream(iterator) {
  return new IteratorStream(iterator);
}

module.exports.createIteratorStream = createIteratorStream;

function wrapError(err, message) {
  const wrapperError = new Error(message);
  wrapperError.stack += '\nCaused by:' + err.stack;
  return wrapperError;
}

module.exports.wrapError = wrapError;

function defineReadOnlyProperty(obj, key, value) {
  Object.defineProperty(obj, key, {
    value,
    writable: false,
    enumerable: true,
    configurable: true
  });
}

module.exports.defineReadOnlyProperty = defineReadOnlyProperty;

function mapToObj(map) {
  const obj = {};
  for (const prop of map) {
    obj[prop[0]] = prop[1];
  }
  return obj;
}

module.exports.mapToObj = mapToObj;

function createMapToObjTransformStream() {
  return new stream.Transform({
    objectMode: true,
    transform(map, enc, cb) {
      this.push(mapToObj(map));
      cb();
    }
  })
}

module.exports.createMapToObjTransformStream = createMapToObjTransformStream;

function noop() {}

module.exports.noop = noop;

function hasAllTerms(coll, contextKey) {
  if (typeof(coll) !== 'object') throw new Error('not an object');
  const found = {};
  const terms = Array.isArray(coll) ? coll : Object.keys(coll);
  if (terms.length !== 4) {
    return false;
  }
  for (let t = 0; t < terms.length; t += 1) {
    switch (terms[t]) {
      case 'subject':
      case 'predicate':
      case 'object':
      case contextKey:
        if (found[terms[t]]) {
          return false;
        }
        found[terms[t]] = true;
        break;
      default:
        return false;
    }
  }
  return true;
}

module.exports.hasAllTerms = hasAllTerms;

function genDefaultIndexes(contextKey) {
  return [
    ['subject', 'predicate', 'object', contextKey],
    ['object', contextKey, 'subject', 'predicate'],
    [contextKey, 'subject', 'predicate', 'object'],
    ['object', 'subject', 'predicate', contextKey],
    ['predicate', 'object', contextKey, 'subject'],
    [contextKey, 'predicate', 'object', 'subject'],
  ];
}

module.exports.genDefaultIndexes = genDefaultIndexes;

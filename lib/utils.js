
'use strict';

const _ = require('lodash');
const n3u = require('n3').Util;
const stream = require('stream');
const factory = require('rdf-data-model');

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

function isLevel(obj) {
  return _.isObject(obj)
    && _.isFunction(obj.put)
    && _.isFunction(obj.del)
    && _.isFunction(obj.batch);
}

module.exports.isLevel = isLevel;

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

// function n3TermToRdfjsTerm(term) {
//   let materialized;
//   if (term === DEFAULT_GRAPH) {
//     materialized = factory.defaultGraph();
//   } else if (n3u.isLiteral(term)) {
//     const value = n3u.getLiteralValue(term);
//     const datatype = n3u.getLiteralType(term);
//     const language = n3u.getLiteralLanguage(term);
//     materialized = factory.literal(value, language || (datatype && factory.namedNode(datatype)) || null);
//   } else if (n3u.isBlank(term)) {
//     materialized = factory.blankNode(term.slice(2));
//   } else if (n3u.isIRI(term)) {
//     materialized = factory.namedNode(term);
//   } else {
//     throw new Error(`Bad term "${term}", cannot export`);
//   }
//   return materialized;
// }
//
// function n3QuadToRdfjsQuad(quad) {
//   return factory.quad(
//     materializeN3Term(quad.subject),
//     materializeN3Term(quad.predicate),
//     materializeN3Term(quad.object),
//     materializeN3Term(quad.graph)
//   );
// }
//
// class N3ToRdfjsStream extends stream.Transform {
//   constructor(opts) {
//     super({ objectMode: true });
//   }
//   _transform(quad, enc, done) {
//     this.push(n3QuadToRdfjsQuad(quad));
//     done();
//   }
// }
//
// module.exports.N3ToRdfjsStream = N3ToRdfjsStream;
//
// function rdfjsTermToN3Term(term) {
//   switch(term.termType) {
//     case 'DefaultGraph':
//       return '';
//     case 'BlankNode':
//     case 'NamedNode':
//       return term.value;
//     case 'Literal':
//       return n3u.createLiteral(term.value, (term.datatype && term.datatype.value) || term.language);
//     default:
//       throw new Error('Unsupported term type');
//   }
// }
//
// function rdfjsQuadToN3Quad(quad) {
//   return {
//     subject: rdfjsTermToN3Term(quad.subject),
//     predicate: rdfjsTermToN3Term(quad.predicate),
//     object: rdfjsTermToN3Term(quad.object),
//     graph: rdfjsTermToN3Term(quad.graph),
//   }
// }
//
// class RdfjsToN3Stream extends stream.Transform {
//   constructor(opts) {
//     super({ objectMode: true });
//   }
//   _transform(quad, enc, done) {
//     this.push(rdfjsQuadToN3Quad(quad));
//     done();
//   }
// }
//
// module.exports.RdfjsToN3Stream = RdfjsToN3Stream;



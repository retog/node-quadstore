
'use strict';

const _ = require('./lodash');
const utils = require('./utils');
const nanoid = require('nanoid');
const stream = require('stream');
const assert = require('assert');
const EventEmitter = require('events').EventEmitter;
const levelup = require('levelup');
const encode = require('encoding-down');
const AsyncIterator = require('asynciterator');

/**
 *
 */
class QuadStore extends EventEmitter {

  constructor(abstractLevelDOWN, opts) {
    super();
    if (_.isNil(opts)) opts = {};
    if (!utils.isAbstractLevelDOWNInstance(abstractLevelDOWN)) {
      throw new Error(`The abstractLevelDOWN parameter is not an instance of AbstractLevelDOWN.`);
    }
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    this._abstractLevelDOWN = abstractLevelDOWN;
    this._db = levelup(encode(abstractLevelDOWN, {valueEncoding: 'json'}));
    this._contextKey = opts.contextKey || 'graph';
    this._defaultContextValue = opts.defaultContextValue || '_DEFAULT_CONTEXT_';
    this._indexes = [];
    this._id = nanoid();
    utils.defineReadOnlyProperty(this, 'id', this._id);
    utils.defineReadOnlyProperty(this, 'contextKey', this._contextKey);
    utils.defineReadOnlyProperty(this, 'defaultContextValue', this._defaultContextValue);
    utils.defineReadOnlyProperty(this, 'boundary', opts.boundary || '\uDBFF\uDFFF');
    utils.defineReadOnlyProperty(this, 'separator', opts.separator || '\u0000\u0000');

    const indexes = [
      {name: 'SPOG', terms: ['subject', 'predicate', 'object', this._contextKey]},
      {name: 'POGS', terms: ['predicate', 'object', this._contextKey, 'subject']},
      {name: 'OGSP', terms: ['object', this._contextKey, 'subject', 'predicate']},
      {name: 'GSPO', terms: [this._contextKey, 'subject', 'predicate', 'object']},
      {name: 'GPSO', terms: [this._contextKey, 'predicate', 'subject', 'object']},
      {name: 'OSPG', terms: ['object', 'subject', 'predicate', this._contextKey]},

      // {name: 'SPOG', terms: ['subject', 'predicate', 'object', this._contextKey]},
      // {name: 'SOPG', terms: ['subject', 'object', 'predicate', this._contextKey]},
      // {name: 'PSOG', terms: ['predicate', 'subject', 'object', this._contextKey]},
      // {name: 'POSG', terms: ['predicate', 'object', 'subject', this._contextKey]},
      // {name: 'OSPG', terms: ['object', 'subject', 'predicate', this._contextKey]},
      // {name: 'OPSG', terms: ['object', 'predicate', 'subject', this._contextKey]},

      // {name: 'GSPO', terms: [this._contextKey, 'subject', 'predicate', 'object']},
      // {name: 'GSOP', terms: [this._contextKey, 'subject', 'object', 'predicate']},
      // {name: 'GPSO', terms: [this._contextKey, 'predicate', 'subject', 'object']},
      // {name: 'GPOS', terms: [this._contextKey, 'predicate', 'object', 'subject']},
      // {name: 'GOSP', terms: [this._contextKey, 'object', 'subject', 'predicate']},
      // {name: 'GOPS', terms: [this._contextKey, 'object', 'predicate', 'subject']},
    ];

    for (const index of indexes) {
      const keyGen = this._createIndexKeyGen(index.terms);
      const scoreGen = this._createIndexScoreGen(index.terms);
      const valueGen = this._createIndexValueGen(index.terms);
      const queryGen = this._createIndexQueryGen(index.terms);
      this.registerIndex(index.name, index.terms, keyGen, valueGen, scoreGen, queryGen);
    }

    setImmediate(() => { this._initialize(); });
  }

  _initialize() {
    this.emit('ready');
  }

  toString() {
    return this.toJSON();
  }

  toJSON() {
    return `[object ${this.constructor.name}::${this._id}]`;
  }

  //
  // CUSTOM INDEXES
  //

  _getIndex(name) {
    return _.find(this._indexes, index => index.name === name);
  }

  _setIndex(name, terms, keyGen, valueGen, scoreGen, queryGen) {
    this._indexes.push({name, terms, keyGen, valueGen, scoreGen, queryGen});
  }

  _createIndexKeyGen(indexTerms) {
    return ((quad) => {
      return indexTerms.map(indexTerm => quad[indexTerm]).join(this.separator)
        + this.separator;
    });
  }

  _createIndexValueGen(indexTerms) {
    return ((quad) => {
      if (!quad[this._contextKey]) {
        quad = {
          ...quad,
          [this._contextKey]: this._defaultContextValue
        };
      }
      return quad;
    });
  }

  _createIndexScoreGen(indexTerms) {
    return ((matchTerms) => {
      const termKeys = Object.keys(matchTerms);
      if (termKeys.length === 0) {
        return 1;
      }
      const score = indexTerms.reduce((score, term, pos) => {
        return matchTerms[term] && score >= pos
          ? score + 1
          : score;
      }, 0);
      return score / termKeys.length;
    });
  }

  _createIndexQueryGen(indexTerms) {
    return ((matchTerms) => {
      const pattern = indexTerms.reduce((parts, term, pos) => {
        if (matchTerms[term] && parts.length >= pos) {
          parts.push(matchTerms[term]);
        }
        return parts;
      }, []).join(this.separator);
      return {
        gte: (pattern.length > 0 ? pattern + this.separator : ''),
        lte: (pattern.length > 0 ? pattern + this.separator : '') + this.boundary
      };
    });
  }

  registerIndex(name, terms, keyGen, valueGen, scoreGen, queryGen) {
    assert(_.isString(name), 'Invalid index name (not a string).');
    assert(_.isArray(terms), 'Invalid index terms (not an array).');
    assert(_.isFunction(keyGen), 'Invalid key generator (not a function).');
    assert(_.isFunction(valueGen), 'Invalid value generator (not a function).');
    assert(_.isFunction(scoreGen), 'Invalid score generator (not a function).');
    assert(_.isFunction(queryGen), 'Invalid query generator (not a function).');
    assert(_.isNil(this._getIndex(name)), 'Invalid index name (duplicate name).');
    this._setIndex(name, terms, keyGen, valueGen, scoreGen, queryGen);
    return this;
  }

  getByIndex(name, opts, cb) {
    const store = this;
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(_.isString(name), 'The "name" argument is not a string.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    const quads = [];
    function _getByIndex(resolve, reject) {
      store.getByIndexStream(name, opts)
        .on('data', (quad) => { quads.push(quad); })
        .on('end', () => { resolve(quads); })
        .on('error', (err) => { reject(err); });
    }
    if (!_.isFunction(cb)) {
      return new Promise(_getByIndex);
    }
    _getByIndex(cb.bind(null, null), cb);
  }

  getByIndexStream(name, opts) {
    if (_.isNil(opts)) opts = {};
    assert(_.isString(name), 'The "name" argument is not a string.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    const levelOpts = {};
    if (_.isString(opts.gte)) levelOpts.gte = name + this.separator + opts.gte;
    if (_.isString(opts.lte)) levelOpts.lte = name + this.separator + opts.lte;
    if (_.isString(opts.gt)) levelOpts.gt = name + this.separator + opts.gt;
    if (_.isString(opts.lt)) levelOpts.lt = name + this.separator + opts.lt;
    if (_.isNumber(opts.limit)) levelOpts.limit = opts.limit;
    if (_.isBoolean(opts.reverse)) levelOpts.reverse = opts.reverse;
    const quadStream = this._db.createValueStream(levelOpts);
    if (opts.offset) {
      if (levelOpts.limit) {
        levelOpts.limit += opts.offset;
      }
      quadStream.pipe(this._createOffsetStream(opts.offset));
    }
    return quadStream;
  }

  put(quads, opts, cb) {
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(_.isObject(quads), 'The "quads" argument is not an object.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    const maybePromise = this._delput([], quads, opts, cb);
    if (utils.isPromise(maybePromise)) return maybePromise;
  }

  del(matchTermsOrOldQuads, opts, cb) {
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(_.isObject(matchTermsOrOldQuads), 'The "matchTermsOrOldQuads" argument is not an object.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    const maybePromise = (Array.isArray(matchTermsOrOldQuads) || this._isQuad(matchTermsOrOldQuads))
      ? this._delput(matchTermsOrOldQuads, [], opts, cb)
      : this._getdelput(matchTermsOrOldQuads, [], opts, cb);
    if (utils.isPromise(maybePromise)) return maybePromise;
  }

  /**
   * Returns all quads matching the provided terms.
   * @param matchTerms
   * @param cb
   */
  get(matchTerms, opts, cb) {
    const store = this;
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isFunction(matchTerms)) {
      cb = matchTerms;
      opts = {};
      matchTerms = {};
    }
    if (_.isNil(opts)) opts = {};
    if (_.isNil(matchTerms)) matchTerms = {};
    assert(_.isObject(matchTerms), 'The "matchTerms" argument is not an object.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    const quads = [];
    function _get(resolve, reject) {
      store.getStream(matchTerms, opts)
        .on('data', (quad) => { quads.push(quad); })
        .on('end', () => { resolve(quads); })
        .on('error', (err) => { reject(err); });
    }
    if (!_.isFunction(cb)) {
      return new Promise(_get);
    }
    _get(cb.bind(null, null), cb);
  }

  patch(matchTermsOrOldQuads, newQuads, opts, cb) {
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isFunction(newQuads)) {
      cb = newQuads;
      opts = {};
      newQuads = [];
    }
    if (_.isNil(opts)) opts = {};
    assert(_.isObject(matchTermsOrOldQuads), 'Invalid type of "matchTermsOrOldQuads" argument.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    const maybePromise = (Array.isArray(matchTermsOrOldQuads) || this._isQuad(matchTermsOrOldQuads))
      ? this._delput(matchTermsOrOldQuads, newQuads, opts, cb)
      : this._getdelput(matchTermsOrOldQuads, newQuads, opts, cb);
    if (utils.isPromise(maybePromise)) return maybePromise;
  }

  //
  // STREAMS
  //

  getStream(matchTerms, opts) {
    if (_.isNil(matchTerms)) matchTerms = {};
    if (_.isNil(opts)) opts = {};
    assert(_.isObject(matchTerms), 'The "matchTerms" argument is not a function..');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    let bestScore = 0;
    let bestIndex = null;
    for (const index of this._indexes) {
      const score = index.scoreGen(matchTerms);
      if (score >= bestScore) {
        bestScore = score;
        bestIndex = index;
      }
    }
    if (bestScore === 0) {
      throw new Error(`No index with score > 0`);
    } else {
      // console.log(`SELECTED INDEX ${bestIndex.name}`)
    }
    const baseQuery = bestIndex.queryGen(matchTerms);
    const query = {
      lte: bestIndex.name + this.separator + baseQuery.lte,
      gte: bestIndex.name + this.separator + baseQuery.gte,
      limit: opts.limit
    };
    if (opts.offset && query.limit) {
      query.limit += opts.offset;
    }
    console.log(`SELECTED INDEX ${bestIndex.name}, QUERY`, query);
    const valueStream = this._db.createValueStream(query);
    return opts.offset
      ? valueStream.pipe(this._createOffsetStream(opts.offset))
      : valueStream;
  }

  getApproximateCount(matchTerms, opts, cb) {
    const store = this;
    if (_.isNil(matchTerms)) matchTerms = {};
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(_.isObject(matchTerms), 'The "matchTerms" argument is not a function..');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    function _getApproximateSize(resolve, reject) {
      const pattern = store._queryToPattern(matchTerms, {
        separator: store.separator,
        contextKey: store._contextKey,
      });
      if (_.isFunction(store._abstractLevelDOWN.approximateSize)) {
        store._abstractLevelDOWN.approximateSize(pattern, pattern + store.boundary, (err, size) => {
          let approximateSize = Math.round(size / 128);
          err ? reject(err) : resolve(approximateSize);
        });
      } else {
        resolve(0);
      }
    }
    if (!_.isFunction(cb)) {
      return new Promise(_getApproximateSize);
    }
    _getApproximateSize(cb.bind(null, null), cb);
  }

  putStream(source, opts, cb) {
    const store = this;
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(utils.isReadableStream(source), 'The "source" argument is not a readable stream.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    function _putStream(resolve, reject) {
      AsyncIterator.wrap(source).transform((quad, cb) => {
        store._delput([], [quad], opts, cb);
      })
        .on('data', utils.noop)
        .on('end', resolve)
        .on('error', reject);
    }
    if (!_.isFunction(cb)) {
      return new Promise(_putStream);
    }
    _putStream(cb.bind(null, null), cb);
  }

  delStream(source, opts, cb) {
    const store = this;
    if (_.isFunction(opts)) {
      cb = opts;
      opts = {};
    }
    if (_.isNil(opts)) opts = {};
    assert(utils.isReadableStream(source), 'The "source" argument is not a readable stream.');
    assert(_.isObject(opts), 'The "opts" argument is not an object.');
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    function _delStream(resolve, reject) {
      AsyncIterator.wrap(source).transform((quad, cb) => {
        store._delput([quad], [], opts, cb);
      })
        .on('data', utils.noop)
        .on('end', resolve)
        .on('error', reject);
    }
    if (!_.isFunction(cb)) {
      return new Promise(_delStream);
    }
    _delStream(cb.bind(null, null), cb);
  }

  close(cb) {
    const store = this;
    assert(_.isNil(cb) || _.isFunction(cb), 'The "cb" argument is not a function.');
    function _close(resolve, reject) {
      store._db.close((err) => {
        err ? reject(err) : resolve();
      });
    }
    if (!_.isFunction(cb)) {
      return new Promise(_close);
    }
    _close(cb.bind(null, null), cb);
  }

  _isQuad(obj) {
    return _.isString(obj.subject)
      && _.isString(obj.predicate)
      && _.isString(obj.object)
      && _.isString(obj[this._contextKey]);
  }

  _delput(oldQuads, newQuads, opts, cb) {
    const store = this;
    if (!Array.isArray(oldQuads)) oldQuads = [oldQuads];
    if (!Array.isArray(newQuads)) newQuads = [newQuads];
    const batch = [].concat(
      _.flatMap(oldQuads, store._createQuadToBatchIteratee({
        type: 'del',
        separator: store.separator,
        contextKey: store._contextKey,
      })),
      _.flatMap(newQuads, store._createQuadToBatchIteratee({
        type: 'put',
        separator: store.separator,
        contextKey: store._contextKey,
      }))
    );
    function __delput(resolve, reject) {
      store._db.batch(batch, opts, (err) => {
        if (err) reject(err); else resolve();
      });
    }
    if (!_.isFunction(cb)) {
      return new Promise(__delput);
    }
    __delput(cb.bind(null, null), cb);
  }

  _getdelput(matchTerms, newQuads, opts, cb) {
    const store = this;
    function __getdelput(resolve, reject) {
      store.get(matchTerms, opts, (matchErr, oldQuads) => {
        if (matchErr) { reject(matchErr); return; }
        store._delput(oldQuads, newQuads, opts, (delputErr) => {
          if (delputErr) reject(delputErr); else resolve();
        });
      });
    }
    if (!_.isFunction(cb)) {
      return new Promise(__getdelput);
    }
    __getdelput(cb.bind(null, null), cb);
  }

  /**
   * Transforms a quad into a batch of either put or del
   * operations, one per each of the six indexes.
   * @param quad
   * @param opts
   * @returns {}
   */
  _quadToBatch(quad, opts) {
    const type = opts.type;
    const indexes = this._indexes;
    const operations = [];
    for (let i = 0, index; i < indexes.length; i += 1) {
      index = indexes[i];
      operations.push({
        type,
        key: index.name + this.separator + index.keyGen(quad),
        value: index.valueGen(quad)
      });
    }
    console.log('OPERATIONS', operations.map(o => o.key));
    return operations;
  }

  /**
   * Helper function - curries quadToBatch().
   * @param opts
   * @returns {batchifier}
   */
  _createQuadToBatchIteratee(opts) {
    const store = this;
    return function quadToBatchIteratee(quad) {
      return store._quadToBatch(quad, opts);
    };
  }

  // /**
  //  * Transforms a query into a matching pattern targeting
  //  * the appropriate index.
  //  * @param query
  //  * @returns {*}
  //  */
  // _queryToPattern(query, opts) {
  //   const separator = opts.separator;
  //   const contextKey = opts.contextKey;
  //   let pattern;
  //   if (query.subject) {
  //     if (query.predicate) {
  //       if (query.object) {
  //         if (query[contextKey]) {
  //           pattern = 'SPOG' + separator + query.subject + separator + query.predicate + separator + query.object + separator + query[contextKey];
  //         } else {
  //           pattern = 'SPOG' + separator + query.subject + separator + query.predicate + separator + query.object + separator;
  //         }
  //       } else if (query[contextKey]) {
  //         pattern = 'GSP' + separator + query[contextKey] + separator + query.subject + separator + query.predicate + separator;
  //       } else {
  //         pattern = 'SPOG' + separator + query.subject + separator + query.predicate + separator;
  //       }
  //     } else if (query.object) {
  //       if (query[contextKey]) {
  //         pattern = 'OGS' + separator + query.object + separator + query[contextKey] + separator + query.subject + separator;
  //       } else {
  //         pattern = 'OS' + separator + query.object + separator + query.subject + separator;
  //       }
  //     } else if (query[contextKey]) {
  //       pattern = 'GSP' + separator + query[contextKey] + separator + query.subject + separator;
  //     } else {
  //       pattern = 'SPOG' + separator + query.subject + separator;
  //     }
  //   } else if (query.predicate) {
  //     if (query.object) {
  //       if (query[contextKey]) {
  //         pattern = 'POG' + separator + query.predicate + separator + query.object + separator + query[contextKey] + separator;
  //       } else {
  //         pattern = 'POG' + separator + query.predicate + separator + query.object + separator;
  //       }
  //     } else if (query[contextKey]) {
  //       pattern = 'GP' + separator + query[contextKey] + separator + query.predicate + separator;
  //     } else {
  //       pattern = 'POG' + separator + query.predicate + separator;
  //     }
  //   } else if (query.object) {
  //     if (query[contextKey]) {
  //       pattern = 'OGS' + separator + query.object + separator + query[contextKey] + separator;
  //     } else {
  //       pattern = 'OGS' + separator + query.object + separator;
  //     }
  //   } else if (query[contextKey]) {
  //     pattern = 'GSP' + separator + query[contextKey] + separator;
  //   } else {
  //     pattern = 'SPOG' + separator;
  //   }
  //   return pattern;
  // }

  _createQuadComparator(termNamesA, termNamesB) {
    if (!termNamesA) termNamesA = ['subject', 'predicate', 'object', this._contextKey];
    if (!termNamesB) termNamesB = termNamesA.slice();
    if (termNamesA.length !== termNamesB.length) throw new Error('Different lengths');
    return function comparator(quadA, quadB) {
      for (let i = 0; i <= termNamesA.length; i += 1) {
        if (i === termNamesA.length) return 0;
        else if (quadA[termNamesA[i]] < quadB[termNamesB[i]]) return -1;
        else if (quadA[termNamesA[i]] > quadB[termNamesB[i]]) return 1;
      }
    };
  }

  /*
   * Offset stream
   */

  _createOffsetStream(skipQty) {
    let missing = skipQty;
    function pushTransform(quad, enc, cb) {
      this.push(quad);
      cb();
    }
    function skipTransform(quad, enc, cb) {
      if (missing === 0) {
        this._transform = pushTransform;
        pushTransform.call(this, quad, enc, cb);
      } else {
        missing -= 1;
        cb();
      }
    }
    return new stream.Transform({
      objectMode: true,
      transform: skipTransform
    });
  }

}

module.exports = QuadStore;

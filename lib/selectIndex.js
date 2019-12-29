
const stream = require('stream');
const multistream = require('multistream');


class FilterStream extends stream.Transform {
  constructor(fn) {
    super({objectMode: true});
    this._transform = (quad, enc, done) => {
      if (fn(quad)) {
        this.push(quad);
      }
      done();
    };
  }
}


class LimitStream extends stream.Transform {
  constructor(limit) {
    super({objectMode: true});
    this._transform = (quad, enc, done) => {
      this.push(quad);
      if (--limit < 1) {
        this.push(null);
      }
      done();
    };
  }
}

class OffsetStream extends stream.Transform {
  constructor(offset) {
    super({objectMode: true});
    this._transform = (quad, enc, done) => {
      if (offset > 0) {
        offset -= 1;
      } else {
        this.push(quad);
      }
      done();
    };
  }
}


const OPERATION = {
  RANGE_MATCH: 'RANGE_MATCH',
  VALUE_MATCH: 'VALUE_MATCH',
};

const TYPE = {
  MEMORY: 'MEMORY',
  INDEX: 'INDEX',
};


const ensureArray = (a) => {
  return Array.isArray(a) ? a : [a];
};

const last = (arr) => {
  return arr[arr.length - 1];
};

const omit = (o, p) => {
  const c = { ...o };
  delete c[p];
  return c;
};

const isNil = (v) => {
  return v === null || v === undefined;
};

const isBoundRange = (r) => {
  return (!isNil(r.lt) || !isNil(r.lte)) && (!isNil(r.gt) || !isNil(r.gte));
};

const emptyPath = {
  lt: [],
  gt: [],
  lte: false,
  gte: false,
  flt: [],
};

const copyPath = (path) => {
  return {
    lt: [...path.lt],
    gt: [...path.gt],
    lte: path.lte,
    gte: path.gte,
    flt: [...path.flt],
  };
};

const canAddIndex = (path) => {
  if (path.flt.length > 0) {
    return false;
  }
  if (path.lte !== path.gte) {
    return false;
  }
  if (path.lt.length !== path.gt.length) {
    return false;
  }
  for (let i = 0; i < path.lt.length; i += 1) {
    if (path.lt[i] !== path.gt[i]) {
      return false;
    }
  }
  return true;
};


const compilePath = (path) => {
  return {
    [path.lte ? 'lte' : 'lt']: path.lt,
    [path.gte ? 'gte' : 'gt']: path.gt,
    score: 100 - path.flt.length,
    filter: path.flt.length > 0 ? eval(`(quad) => (${path.flt.join(' && ')})`) : null,
  };
};



const populatePathsWithInMemorySteps = (query, path) => {
  const terms = Object.keys(query);
  if (terms.length < 1) {
    return [compilePath(path)];
  }
  const term = terms[0];
  const matches = ensureArray(query[term]);
  return matches.reduce((acc, match) => {
    switch (typeof(match)) {
      case 'string':
      case 'number':
      case 'boolean':
        path = copyPath(path);
        path.flt.push(`quad['${term}'] === '${match}'`);
        return [
          ...acc,
          ...populatePathsWithInMemorySteps(omit(query, term), path),
        ];
      case 'object':
        path = copyPath(path);
        if (match.lt) path.flt.push(`quad['${term}'] < '${match.lt}'`);
        if (match.lte) path.flt.push(`quad['${term}'] <= '${match.lte}'`);
        if (match.gt) path.flt.push(`quad['${term}'] > '${match.gt}'`);
        if (match.gte) path.flt.push(`quad['${term}'] >= '${match.gte}'`);
        return [
          ...acc,
          ...populatePathsWithInMemorySteps(omit(query, term), path),
        ];
      default:
        throw new Error('unsupported');
    }
  }, []);
};





const populatePaths = (query, path, terms) => {

  // If we've run out of query terms (i.e. the query is empty) we can quit
  // our recursive loop as we can satisfy the query in its entirety.
  if (Object.keys(query).length < 1) {
    return [compilePath(path)];
  }
  // If we've run out of index terms (i.e. the terms array is empty) we must
  // satisfy the remaining query terms in-memory.
  if (terms.length < 1) {
    return populatePathsWithInMemorySteps(query, path);
  }

  const term = terms[0];
  const matches = query.hasOwnProperty(term) ? ensureArray(query[term]) : null;

  // If we still have both index terms and query terms but the query has no
  // matches for the current index term, this index is not suitable for the
  // remaining part of the query.
  if (!matches) {
    return populatePathsWithInMemorySteps(query, path);
  }

  return matches.reduce((acc, match) => {
    switch (typeof(match)) {
      case 'string':
      case 'number':
      case 'boolean':
        if (canAddIndex(path)) {
          path = copyPath(path);
          path.lt.push(match);
          path.lte = true;
          path.gt.push(match);
          path.gte = true;
          return [...acc, ...populatePaths(omit(query, term), path, terms.slice(1))];
        }
        return populatePathsWithInMemorySteps(omit(query, term), path);
      case 'object':
        if (canAddIndex(path)) {
          path = copyPath(path);
          if (match.lte) {
            path.lt.push(match.lte);
            path.lte = true;
          } else if (match.lt) {
            path.lt.push(match.lt);
            path.lte = false;
          }
          if (match.gte) {
            path.gt.push(match.gte);
            path.gte = true;
          } else if (match.gt) {
            path.gt.push(match.gt);
            path.gte = false;
          }
          return [...acc, ...populatePaths(omit(query, term), path, terms.slice(1))];
        }
        return populatePathsWithInMemorySteps(omit(query, term), path);
      default:
        throw new Error('unsupported')
    }
  }, []);
};

const generateStrategy = (query, index) => {
  const paths = populatePaths(query, emptyPath, index.terms);
  return { index, paths, score: Math.min(...paths.map(p => p.score))};
};

const generateStrategies = (query, indexes) => {
  return indexes.map(index => generateStrategy(query, index));
};


const pickBestStrategy = (strategies) => {
  let best = strategies[0];
  for (let i = 1; i < strategies.length; i += 1) {
    if (strategies[i].score > best.score) {
      best = strategies[i];
    }
  }
  return best;
};

module.exports.pickBestStrategy = pickBestStrategy;

module.exports.createQueryStrategies = generateStrategies;



const executeStrategy = (strategy, opts, store) => {
  const streams = strategy.paths.map((path) => {
    return () => {
      const opts = {};
      if (path.lt && path.lt.length > 0) {
        opts.lt = strategy.index.name + store.separator + path.lt.join(store.separator) + store.separator;
      } else if (path.lte && path.lte.length > 0) {
        opts.lte = strategy.index.name + store.separator + path.lte.join(store.separator) + store.separator + store.boundary;
      } else {
        opts.lte = strategy.index.name + store.separator + store.boundary;
      }
      if (path.gt && path.gt.length > 0) {
        opts.gt = strategy.index.name + store.separator + path.gt.join(store.separator) + store.separator;
      } else if (path.gte && path.gte.length > 0) {
        opts.gte = strategy.index.name + store.separator + path.gte.join(store.separator) + store.separator;
      } else {
        opts.gte = strategy.index.name + store.separator;
      }
      let s = store._db.createValueStream(opts);
      if (path.filter) {
        s = s.pipe(new FilterStream(path.filter));
      }
      return s;
    };
  });

  let multi = multistream.obj(streams);

  if (opts.offset) {
    multi = multi.pipe(new OffsetStream(opts.offset));
  }

  if (opts.limit) {
    multi = multi.pipe(new LimitStream(opts.limit));
  }

  return multi;
};

module.exports.executeStrategy = executeStrategy;


const executeQuery = (query, opts, indexes, store) => {
  const strategies = generateStrategies(query, indexes);
  const strategy = pickBestStrategy(strategies);
  return executeStrategy(strategy, opts, store);
};

module.exports.executeQuery = executeQuery;

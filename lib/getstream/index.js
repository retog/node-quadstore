
const utils = require('../utils');
const multistream = require('multistream');
const AsyncIterator = require('asynciterator');

const ensureArray = (a) => {
  return Array.isArray(a) ? a : [a];
};

const omit = (o, p) => {
  const c = { ...o };
  delete c[p];
  return c;
};

const emptyPath = {
  lt: [],
  gt: [],
  lte: false,
  gte: false,
  filter: [],
};

const copyPath = (path) => {
  return {
    lt: [...path.lt],
    gt: [...path.gt],
    lte: path.lte,
    gte: path.gte,
    filter: [...path.filter],
  };
};

const canAddToIndexConditions = (path) => {
  if (path.filter.length > 0) {
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
    score: 100 - path.filter.length,
    filter: path.filter.length > 0
      ? eval(`(quad) => (${path.filter.join(' && ')})`)
      : null,
  };
};

const generateFilterOnlyPaths = (query, path, store) => {
  const terms = Object.keys(query);
  if (terms.length < 1) {
    return [compilePath(path)];
  }
  const term = terms[0];
  const matches = ensureArray(query[term]);
  return matches.reduce((acc, match) => {
    const matchPath = copyPath(path);
    switch (typeof(match)) {
      case 'string':
      case 'number':
      case 'boolean':
        matchPath.filter.push(`quad['${term}'] === '${match}'`);
        return [
          ...acc,
          ...generateFilterOnlyPaths(omit(query, term), matchPath, store),
        ];
      case 'object':
        if (match.lt) matchPath.filter.push(`quad['${term}'] < '${match.lt}'`);
        if (match.lte) matchPath.filter.push(`quad['${term}'] <= '${match.lte}'`);
        if (match.gt) matchPath.filter.push(`quad['${term}'] > '${match.gt}${store.boundary}'`);
        if (match.gte) matchPath.filter.push(`quad['${term}'] >= '${match.gte}'`);
        return [
          ...acc,
          ...generateFilterOnlyPaths(omit(query, term), matchPath, store),
        ];
      default:
        throw new Error('unsupported');
    }
  }, []);
};





const generatePaths = (query, path, terms, store) => {

  // If we've run out of query terms (i.e. the query is empty) we can quit
  // our recursive loop as we can satisfy the query in its entirety.
  if (Object.keys(query).length < 1) {
    return [compilePath(path)];
  }
  // If we've run out of index terms (i.e. the terms array is empty) we must
  // satisfy the remaining query terms in-memory.
  if (terms.length < 1) {
    return generateFilterOnlyPaths(query, path, store);
  }

  const term = terms[0];
  const matches = query.hasOwnProperty(term) ? ensureArray(query[term]) : null;

  // If we still have both index terms and query terms but the query has no
  // matches for the current index term, this index is not suitable for the
  // remaining part of the query.
  if (!matches) {
    return generateFilterOnlyPaths(query, path, store);
  }

  return matches.reduce((acc, match) => {
    const matchPath = copyPath(path);
    switch (typeof(match)) {
      case 'string':
      case 'number':
      case 'boolean':
        if (canAddToIndexConditions(matchPath)) {
          matchPath.lt.push(match);
          matchPath.lte = true;
          matchPath.gt.push(match);
          matchPath.gte = true;
          return [...acc, ...generatePaths(omit(query, term), matchPath, terms.slice(1), store)];
        }
        return generateFilterOnlyPaths(query, matchPath, store);
      case 'object':
        if (canAddToIndexConditions(matchPath)) {
          if (match.lte) {
            matchPath.lt.push(match.lte);
            matchPath.lte = true;
          } else if (match.lt) {
            matchPath.lt.push(match.lt);
            matchPath.lte = false;
          }
          if (match.gte) {
            matchPath.gt.push(match.gte);
            matchPath.gte = true;
          } else if (match.gt) {
            matchPath.gt.push(match.gt);
            matchPath.gte = false;
          }
          return [...acc, ...generatePaths(omit(query, term), matchPath, terms.slice(1), store)];
        }
        return generateFilterOnlyPaths(query, matchPath, store);
      default:
        throw new Error('unsupported')
    }
  }, []);
};

const generateStrategy = (query, index, store) => {
  const paths = generatePaths(query, emptyPath, index.terms, store);
  return { index, paths, score: Math.min(...paths.map(p => p.score))};
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

const generateStreamFactoryForPath = (store, strategy, path) => {
  return () => {
    const opts = {};
    if (path.lt && path.lt.length > 0) {
      opts.lt = strategy.index.name + store.separator + path.lt.join(store.separator) + store.separator;
    } else if (path.lte && path.lte.length > 0) {
      opts.lte = strategy.index.name + store.separator + path.lte.join(store.separator) + store.separator + store.boundary;
    } else {
      opts.lt = strategy.index.name + store.separator + store.boundary;
    }
    if (path.gt && path.gt.length > 0) {
      opts.gt = strategy.index.name + store.separator + path.gt.join(store.separator) + store.boundary;
    } else if (path.gte && path.gte.length > 0) {
      opts.gte = strategy.index.name + store.separator + path.gte.join(store.separator) + store.separator;
    } else {
      opts.gt = strategy.index.name + store.separator;
    }
    let valueIterator = AsyncIterator.wrap(store._db.createValueStream(opts));
    if (path.filter) {
      valueIterator = valueIterator.filter(path.filter);
    }
    return valueIterator;
  };
};

const executeStrategy = (strategy, opts, store) => {
  const streamFactories = strategy.paths.map(
    (path) => generateStreamFactoryForPath(store, strategy, path),
  );
  let multiIterator = AsyncIterator.wrap(multistream.obj(streamFactories));
  if (opts.offset || opts.limit) {
    if (opts.offset) {
      multiIterator = multiIterator.skip(opts.offset);
    }
    if (opts.limit) {
      multiIterator = multiIterator.take(opts.limit);
    }
  }
  return multiIterator;
};

/**
 *
 * @param matchTerms
 * @param opts
 * @param {QuadStore} store
 * @returns {stream.Readable}
 */
const getStream = (matchTerms, opts, store) => {
  const strategies = store._indexes.map(
    index => generateStrategy(matchTerms, index, store),
  );
  const strategy = pickBestStrategy(strategies);
  return executeStrategy(strategy, opts, store);
};

module.exports = getStream;


const multistream = require('multistream');
const AsyncIterator = require('asynciterator');

const compile = (store, strategy) => {
  return () => {
    const levelOpts = {};
    if (strategy.lt.length > 0) {
      if (strategy.lte) {
        levelOpts.lte = strategy.index.name
          + store.separator
          + strategy.lt.join(store.separator)
          + store.separator
          + store.boundary;
      } else {
        levelOpts.lt = strategy.index.name
          + store.separator
          + strategy.lt.join(store.separator)
          + store.separator;
      }
    } else {
      levelOpts.lt = strategy.index.name
        + store.separator
        + store.boundary;
    }
    if (strategy.gt.length > 0) {
      if (strategy.gte) {
        levelOpts.gte = strategy.index.name
          + store.separator
          + strategy.gt.join(store.separator)
          + store.separator;
      } else {
        levelOpts.gt = strategy.index.name
          + store.separator
          + strategy.gt.join(store.separator)
          + store.boundary;
      }
    } else {
      levelOpts.gt = strategy.index.name
        + store.separator;
    }
    const filterFn = strategy.filter.length > 0
      ? eval(`(quad) => (${strategy.filter.join(' && ')})`)
      : null;

    let valueIterator = AsyncIterator.wrap(store._db.createValueStream(levelOpts));
    if (filterFn) {
      valueIterator = valueIterator.filter(filterFn);
    }
    return valueIterator;
  };
};

const execute = (strategies, opts, store) => {
  const streamFactories = strategies.map((strategy) => {
    return compile(store, strategy);
  });
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

module.exports.execute = execute;

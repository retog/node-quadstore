
const omit = (o, p) => {
  const c = { ...o };
  delete c[p];
  return c;
};

const last = (a) => {
  return a[a.length - 1];
};

const addFilterMatch = (strategy, term, valueOrRange, store) => {
  switch (typeof(valueOrRange)) {
    case 'string':
    case 'number':
    case 'boolean':
      strategy.filter.push(`quad['${term}'] === '${valueOrRange}'`);
      break;
    case 'object':
      if (valueOrRange.lt) {
        strategy.filter.push(`quad['${term}'] < '${valueOrRange.lt}'`);
      }
      if (valueOrRange.lte) {
        strategy.filter.push(`quad['${term}'] <= '${valueOrRange.lte}'`);
      }
      if (valueOrRange.gt) {
        strategy.filter.push(`quad['${term}'] > '${valueOrRange.gt}${store.boundary}'`);
      }
      if (valueOrRange.gte) {
        strategy.filter.push(`quad['${term}'] >= '${valueOrRange.gte}'`);
      }
      break;
    default:
      throw new Error('unsupported');
  }
  strategy.score -= 10;
};

const addIndexMatch = (strategy, term, valueOrRange, store) => {
  switch (typeof(valueOrRange)) {
    case 'string':
    case 'number':
    case 'boolean':
      strategy.lt.push(valueOrRange);
      strategy.lte = true;
      strategy.gt.push(valueOrRange);
      strategy.gte = true;
      break;
    case 'object':
      if (valueOrRange.lte) {
        strategy.lt.push(valueOrRange.lte);
        strategy.lte = true;
      } else if (valueOrRange.lt) {
        strategy.lt.push(valueOrRange.lt);
        strategy.lte = false;
      }
      if (valueOrRange.gte) {
        strategy.gt.push(valueOrRange.gte);
        strategy.gte = true;
      } else if (valueOrRange.gt) {
        strategy.gt.push(valueOrRange.gt);
        strategy.gte = false;
      }
      break;
    default:
      throw new Error('unsupported');
  }
};

const populateWithFilters = (subQuery, indexTerms, strategy, store) => {
  for (let term in subQuery) {
    if (subQuery.hasOwnProperty(term)) {
      addFilterMatch(strategy, term, subQuery[term], store);
    }
  }
};

const canAddIndexMatch = (strategy) => {
  if (strategy.filter.length > 0) {
    return false;
  }
  if (strategy.lte !== strategy.gte) {
    return false;
  }
  if (strategy.lt.length !== strategy.gt.length) {
    return false;
  }
  if (last(strategy.lt) !== last(strategy.gt)) {
    return false;
  }
  return true;
};

const populate = (subQuery, indexTerms, strategy, store) => {
  if (Object.keys(subQuery).length < 1) {
    return;
  }
  if (indexTerms.length < 1) {
    populateWithFilters(subQuery, indexTerms, strategy, store);
    return;
  }
  const term = indexTerms[0];
  const valueOrRange = subQuery.hasOwnProperty(term) ? subQuery[term] : null;
  if (!valueOrRange) {
    populateWithFilters(subQuery, indexTerms, strategy, store);
    return;
  }
  if (!canAddIndexMatch(strategy)) {
    populateWithFilters(subQuery, indexTerms, strategy, store);
    return;
  }
  addIndexMatch(strategy, term, valueOrRange, store);
  populate(omit(subQuery, term), indexTerms.slice(1), strategy, store);
};

const generateForIndex = (subQuery, index, store) => {
  const strategy = {
    index,
    subQuery,
    lt: [],
    gte: false,
    gt: [],
    lte: false,
    filter: [],
    score: 100,
  };
  populate(subQuery, index.terms, strategy, store);
  return strategy;
};

const selectBest = (strategies) => {
  let best = strategies[0];
  for (let i = 1, strategy; i < strategies.length; i += 1) {
    strategy = strategies[i];
    if (strategy.score > best.score) {
      best = strategy;
    }
  }
  return best;
};

const generate = (subQuery, store) => {
  const strategies = store._indexes.map((index) => {
    return generateForIndex(subQuery, index, store);
  });
  return selectBest(strategies);
};

module.exports.generate = generate;

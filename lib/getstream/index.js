
const { divide } = require('./query-divide');
const { execute } = require('./strategy-execute');
const { generate } = require('./strategy-generate');

const getStream = (query, opts, store) => {
  const subQueries = divide(query);
  const strategies = subQueries.map(subQuery => generate(subQuery, store));
  return execute(strategies, opts, store);
};

module.exports = getStream;

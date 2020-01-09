
const helper = (query, terms, subQuery) => {
  if (terms.length < 1) {
    return [subQuery];
  }
  const term = terms[0];
  const match = query[term];
  if (!Array.isArray(query[term])) {
    return helper(
      query,
      terms.slice(1),
      { ...subQuery, [term]: match },
    );
  }
  return match.reduce((acc, valueOrRange) => {
    return [
      ...acc,
      ...helper(
        query,
        terms.slice(1),
        { ...subQuery, [term]: valueOrRange },
      ),
    ];
  }, []);
};

const divide = (query) => {
  return helper(query, Object.keys(query), {});
};

module.exports.divide = divide;

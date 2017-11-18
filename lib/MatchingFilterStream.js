
'use strict';

const stream = require('stream');

class TermMatchingFilterStream extends stream.Transform {

  constructor(terms, opts) {
    super({ objectMode: true });
    this._terms = terms;
    this._termNames = Object.keys(terms);
  }

  _transform(quad, enc, done) {
    const terms = this._terms;
    const termNames = this._termNames;
    for (let i = 0; i < termNames.length; i++) {
      if (quad[termNames[i]] !== terms[termNames[i]]) {
        done();
        return;
      }
    }
    this.push(quad);
    done();
  }

}

module.exports = TermMatchingFilterStream;

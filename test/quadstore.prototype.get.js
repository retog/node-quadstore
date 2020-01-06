
'use strict';

const _ = require('../lib/utils/lodash');
const should = require('should');

module.exports = () => {

  describe('QuadStore.prototype.get()', () => {

    beforeEach(async function () {
      await this.store.put([
        { subject: 's', predicate: 'p', object: 'o', graph: 'c' },
        { subject: 's', predicate: 'p2', object: 'o2', graph: 'c2' },
        { subject: 's2', predicate: 'p', object: 'o', graph: 'c' },
        { subject: 's2', predicate: 'p', object: 'o2', graph: 'c' },
        { subject: 's2', predicate: 'p2', object: 'o2', graph: 'c2' },
      ]);
    });

    describe('Match by value', () => {

      it('should match quads by subject', async function () {
        const quads = await this.store.get({ subject: 's' });
        should(quads).have.length(2);
      });

      it('should match quads by predicate', async function () {
        const quads = await this.store.get({ predicate: 'p' });
        should(quads).have.length(3);
      });

      it('should match quads by object', async function () {
        const quads = await this.store.get({ object: 'o' });
        should(quads).have.length(2);
      });

      it('should match quads by context', async function () {
        const quads = await this.store.get({ graph: 'c' });
        should(quads).have.length(3);
      });

      it('should match quads by subject and predicate', async function () {
        const quads = await this.store.get({ subject: 's2', predicate: 'p' });
        should(quads).have.length(2);
      });

      it('should match quads by subject and object', async function () {
        const quads = await this.store.get({ subject: 's2', object: 'o' });
        should(quads).have.length(1);
      });

      it('should match quads by subject and context', async function () {
        const quads = await this.store.get({ subject: 's2', graph: 'c' });
        should(quads).have.length(2);
      });

      it('should match quads by predicate and object', async function () {
        const quads = await this.store.get({ predicate: 'p', object: 'o' });
        should(quads).have.length(2);
      });

      it('should match quads by predicate and context', async function () {
        const quads = await this.store.get({ predicate: 'p', graph: 'c' });
        should(quads).have.length(3);
      });

      it('should match quads by object and context', async function () {
        const quads = await this.store.get({ object: 'o2', graph: 'c2' });
        should(quads).have.length(2);
      });

      it('should match quads by subject, predicate and object', async function () {
        const quads = await this.store.get({ subject: 's', predicate: 'p2', object: 'o2' });
        should(quads).have.length(1);
      });

      it('should match quads by subject, predicate and context', async function () {
        const quads = await this.store.get({ subject: 's', predicate: 'p2', graph: 'c2' });
        should(quads).have.length(1);
      });

      it('should match quads by subject, object and context', async function () {
        const quads = await this.store.get({ subject: 's', object: 'o2', graph: 'c2' });
        should(quads).have.length(1);
      });

      it('should match quads by predicate, object and context', async function () {
        const quads = await this.store.get({ predicate: 'p2', object: 'o2', graph: 'c2' });
        should(quads).have.length(2);
      });

      it('should match quads by subject, predicate, object and context', async function () {
        const quads = await this.store.get({ subject: 's2', predicate: 'p2', object: 'o2', graph: 'c2' });
        should(quads).have.length(1);
      });

    });

    describe('Match by multiple values', () => {

      it('should match quads by subject [VAL, VAL] and object [VAL]', async function () {
        const quads = await this.store.get({
          object: ['o2'],
          subject: ['s', 's2'],
        });
        should(quads).have.length(3);
      });

      it('should match quads by subject [VAL, VAL] and predicate [VAL, VAL]', async function () {
        const quads = await this.store.get({
          subject: ['s', 's2'],
          predicate: ['p', 'p2' ],
        });
        should(quads).have.length(5);
      });

    });

    describe('Match by range', () => {

      it('should match quads by subject [GTE]', async function () {
        const quads = await this.store.get({ subject: { gte: 's'} });
        should(quads).have.length(5);
      });

      it('should match quads by subject [LTE]', async function () {
        const quads = await this.store.get({ subject: { lte: 's2'} });
        should(quads).have.length(5);
      });

      it('should match quads by subject [VAL] and predicate [GTE]', async function () {
        const quads = await this.store.get({
          subject: 's2',
          predicate: { gte: 'p2' },
        });
        should(quads).have.length(1);
      });

      it('should match quads by subject [LTE] and predicate [GTE]', async function () {
        const quads = await this.store.get({
          subject: { lte: 's2' },
          predicate: { gte: 'p2' },
        });
        should(quads).have.length(2);
      });

      it('should match quads by subject [GTE] and predicate [VAL, VAL]', async function () {
        const quads = await this.store.get({
          subject: { gte: 's' },
          predicate: ['p', 'p2' ],
        });
        should(quads).have.length(5);
      });

    });

  });

};


'use strict';

const _ = require('lodash');
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

  describe('QuadStore.prototype.get() using ranges on the subject term', () => {

    beforeEach(async function () {

      this.quads = [
        { subject: 's0', predicate: 'p0', object: 'o0', graph: 'c0' },
        { subject: 's1', predicate: 'p1', object: 'o0', graph: 'c1' },
        { subject: 's2', predicate: 'p1', object: 'o0', graph: 'c1' },
        { subject: 's3', predicate: 'p2', object: 'o1', graph: 'c2' },
        { subject: 's4', predicate: 'p2', object: 'o1', graph: 'c3' },
        { subject: 's5', predicate: 'p3', object: 'o1', graph: 'c3' },
        { subject: 's7', predicate: 'p3', object: 'o2', graph: 'c3' },
      ];

      await this.store.put(this.quads);
    });

    it('should filter quads by subject', async function () {
      const quads = await this.store.get({
        subject: [
          { test: 'gte', comparate: 's0' },
          { test: 'lte', comparate: 's3' }
        ]
      });
      should(quads).deepEqual(this.quads.slice(0, 4));
    });

    it('should filter quads by subject and match by predicate', async function () {
      const quads = await this.store.get({
        subject: [
          { test: 'gte', comparate: 's0' },
          { test: 'lte', comparate: 's4' }
        ],
        predicate: 'p1'
      });
      should(quads).deepEqual(this.quads.slice(1, 3));
    });

    it('should filter quads by subject and match by object', async function () {
      const quads = await this.store.get({
        subject: [
          { test: 'gte', comparate: 's3' },
          { test: 'lte', comparate: 's7' }
        ],
        object: 'o1'
      });
      should(quads).deepEqual(this.quads.slice(3, 6));
    });

  });

  describe('QuadStore.prototype.get() using ranges on the object term', () => {

    beforeEach(async function () {

      this.quads = [
        { subject: 's0', predicate: 'p0', object: 'o0', graph: 'c0' },
        { subject: 's0', predicate: 'p1', object: 'o1', graph: 'c1' },
        { subject: 's0', predicate: 'p1', object: 'o2', graph: 'c1' },
        { subject: 's1', predicate: 'p2', object: 'o3', graph: 'c2' },
        { subject: 's1', predicate: 'p2', object: 'o4', graph: 'c3' },
        { subject: 's1', predicate: 'p3', object: 'o5', graph: 'c3' },
        { subject: 's2', predicate: 'p3', object: 'o6', graph: 'c3' },
      ];

      await this.store.put(this.quads);
    });

    it('should filter quads by object', async function () {
      const quads = await this.store.get({
        object: [
          { test: 'gte', comparate: 'o0' },
          { test: 'lte', comparate: 'o3' }
        ]
      });
      should(quads).deepEqual(this.quads.slice(0, 4));
    });

    it('should filter quads by object and match by predicate and graph', async function () {
      const quads = await this.store.get({
        predicate: 'p2',
        object: [
          { test: 'gte', comparate: 'o0' },
          { test: 'lte', comparate: 'o5' }
        ],
        graph: 'c2'
      });
      should(quads).deepEqual(this.quads.slice(3, 4));
    });

  });

};

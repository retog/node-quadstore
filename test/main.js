
'use strict';

const os = require('os');
const fs = require('fs-extra');
const util = require('util');
const path = require('path');
const utils = require('../lib/utils');
const memdown = require('memdown');
const leveldown = require('leveldown');
const rdfSuite = require('./rdf');
const rdfStoreSuite = require('./rdfstore');
const quadStoreSuite = require('./quadstore');

const remove = util.promisify(fs.remove);

rdfSuite();

describe('MemDOWN backend, standard indexes', () => {

  beforeEach(async function () {
    this.db = memdown();
    this.indexes = null;
  });

  quadStoreSuite();
  rdfStoreSuite();

});

describe('MemDOWN backend, single SPOG index', () => {

  beforeEach(async function () {
    this.db = memdown();
    this.indexes = [
      ['subject', 'predicate', 'object', 'graph'],
    ];
  });

  quadStoreSuite();
  rdfStoreSuite();

});

describe('MemDOWN backend, single GOPS index', () => {

  beforeEach(async function () {
    this.db = memdown();
    this.indexes = [
      ['graph', 'object', 'predicate', 'subject'],
    ];
  });

  quadStoreSuite();
  rdfStoreSuite();

});

describe('LevelDOWN backend, standard indexes', () => {

  beforeEach(async function () {
    this.location = path.join(os.tmpdir(), 'node-quadstore-' + utils.nanoid());
    this.db = leveldown(this.location);
    this.indexes = null;
  });

  afterEach(async function () {
    await remove(this.location);
  });

  quadStoreSuite();
  rdfStoreSuite();

});

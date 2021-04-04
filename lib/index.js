'use strict'

/**
 * Module dependencies.
 */

import { MongoClient } from 'mongodb'
import uri from 'mongodb-uri'
import thunky from 'thunky'
import zlib from 'zlib'

/**
 * Module constants.
 */

const noop = () => {}

const createIndex =(collection, cb) => {
  collection.createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 }, (err) => {
    cb(err, collection);
  })
};

const OPTIONS_LIST = [
  'port',
  'host',
  'username',
  'password',
  'database',
  'collection',
  'compression',
  'engine',
  'Promise',
  'delimiter',
  'prefix',
  'ttl',
  'count',
  'hosts'
]

function hasFunction(funcName, ...args) {
  return args.filter((arg) => 'object' === typeof arg && 'function' === typeof arg[funcName]).shift();
}

export default class MongoStore {
  /**
   * MongoStore constructor.
   *
   * @param {Object} options
   * @api public
   */

  constructor(arg1, opts = {}) {
    let store = this;
    let options = {
      collection: 'cacheman',
      db: 'test',
      compression: false,
      ...opts,
    };
    if ('string' === typeof arg1) {
      this.url = arg1;
    } else if ('object' === typeof arg1) {
      const collection = hasFunction('removeMany', arg1, options.collection);
      const db = hasFunction('collection', arg1, arg1.client, arg1.database, options.database, options.client);
      const client = hasFunction('db', arg1, arg1.client, options.client);

      if (collection) {
        this.setCollection(collection);
      } else if (db) {
        this.setCollection(db.collection(options.collection));
      } else if (client) {
        this.setCollection(client.db(options.database || options.db).collection(options.collection));
      } else {
        options = { ...options, ...arg1 };
      }
    }
    this.compression = options.compression; 
    this.ready = thunky((cb) => {
      if (!this.collection) {
        const mongoOptions = OPTIONS_LIST.reduce((opt, key) => {
          delete opt[key]
          return opt
        }, Object.assign({ useUnifiedTopology: true }, options))

        MongoClient.connect(this.url, { useUnifiedTopology: true }, (err, client) => {
          if (err) return cb(err)
          this.setCollection(client.db(options.database).collection(options.collection));
          createIndex(this.collection, cb);
        })
      } else {
        if (this.client) return createIndex(this.collection, cb);
        cb(new Error('Invalid mongo connection.'))
      }
    })
  }

  setCollection(col) {
    this.client = col.s.db.topology;
    this.collection = col;
    return this;
  }

  /**
   * Get an entry.
   *
   * @param {String} key
   * @param {Function} fn
   * @api public
   */

  get(key, fn = noop) {
    this.ready((err, col) => {
      if (err) return fn(err)
      col.findOne({ key: key }, (err, data) => {
        if (err) return fn(err)
        if (!data) return fn(null, null)
        //Mongo's TTL might have a delay, to fully respect the TTL, it is best to validate it in get.
        if (data.expireAt.getTime() < Date.now()) {
          this.del(key)
          return fn(null, null)
        }
        try {
          if (data.compressed) return decompress(data.value, fn)
          fn(null, data.value)
        } catch (err) {
          fn(err)
        }
      })
    })
  }

  /**
   * Set an entry.
   *
   * @param {String} key
   * @param {Mixed} val
   * @param {Number} ttl
   * @param {Function} fn
   * @api public
   */

  set(key, val, ttl, fn = noop) {
    if ('function' === typeof ttl) {
      fn = ttl
      ttl = null
    }

    let data
    let store = this
    let query = { key: key }
    let options = { upsert: true, safe: true }

    try {
      data = {
        key: key,
        value: val,
        expireAt: new Date(Date.now() + (ttl || 60) * 1000)
      }
    } catch (err) {
      return fn(err)
    }

    this.ready((err, col) => {
      if (err) return fn(err)
      if (!this.compression) {
        update(data)
      } else {
        compress(data, function compressData(err, data) {
          if (err) return fn(err)
          update(data)
        })
      }
      function update(data) {
        col.updateOne(query, { $set: data }, options, (err, data) => {
          if (err) return fn(err)
          if (!data) return fn(null, null)
          fn(null, val)
        })
      }
    })
  }

  /**
   * Delete an entry.
   *
   * @param {String} key
   * @param {Function} fn
   * @api public
   */

  del(key, fn = noop) {
    this.ready((err, col) => {
      if (err) return fn(err)
      col.removeOne({ key: key }, { safe: true }, fn)
    })
  }

  /**
   * Clear all entries for this bucket.
   *
   * @param {Function} fn
   * @api public
   */

  clear(fn = noop) {
    this.ready((err, col) => {
      if (err) return fn(err)
      col.removeMany({}, { safe: true }, fn)
    })
  }

  /**
   * Close the cache client session.
   * @api public
   */

  close(fn = noop) {
    if (this.collection) {
      this.client.close((err, res) => {
        if (err) fn(err);
        else {
          delete this.collection;
          delete this.client;
          fn(null);
        }
      });
    } else fn(null);
  }
}

/**
 * Non-exported Helpers
 */

/**
 * Compress data value.
 *
 * @param {Object} data
 * @param {Function} fn
 * @api public
 */

function compress(data, fn) {
  // Data is not of a "compressable" type (currently only Buffer)
  if (!Buffer.isBuffer(data.value)) return fn(null, data)

  zlib.gzip(data.value, (err, val) => {
    // If compression was successful, then use the compressed data.
    // Otherwise, save the original data.
    if (!err) {
      data.value = val
      data.compressed = true
    }
    fn(err, data)
  })
}

/**
 * Decompress data value.
 *
 * @param {Object} value
 * @param {Function} fn
 * @api public
 */

function decompress(value, fn) {
  let v = value.buffer && Buffer.isBuffer(value.buffer) ? value.buffer : value
  zlib.gunzip(v, fn)
}

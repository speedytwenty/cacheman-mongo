'use strict';

/**
 * Module dependencies.
 */

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; };

var _mongodb = require('mongodb');

var _mongodbUri = require('mongodb-uri');

var _mongodbUri2 = _interopRequireDefault(_mongodbUri);

var _thunky = require('thunky');

var _thunky2 = _interopRequireDefault(_thunky);

var _zlib = require('zlib');

var _zlib2 = _interopRequireDefault(_zlib);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * Module constants.
 */

var noop = function noop() {};

var createIndex = function createIndex(collection, cb) {
  collection.createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 }, function (err) {
    cb(err, collection);
  });
};

var OPTIONS_LIST = ['port', 'host', 'username', 'password', 'database', 'collection', 'compression', 'engine', 'Promise', 'delimiter', 'prefix', 'ttl', 'count', 'hosts'];

function hasFunction(funcName) {
  for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
    args[_key - 1] = arguments[_key];
  }

  return args.filter(function (arg) {
    return 'object' === (typeof arg === 'undefined' ? 'undefined' : _typeof(arg)) && 'function' === typeof arg[funcName];
  }).shift();
}

var MongoStore = function () {
  /**
   * MongoStore constructor.
   *
   * @param {Object} options
   * @api public
   */

  function MongoStore(arg1) {
    var _this = this;

    var opts = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

    _classCallCheck(this, MongoStore);

    var store = this;
    var options = _extends({
      collection: 'cacheman',
      db: 'test',
      compression: false
    }, opts);
    if ('string' === typeof arg1) {
      this.url = arg1;
    } else if ('object' === (typeof arg1 === 'undefined' ? 'undefined' : _typeof(arg1))) {
      var collection = hasFunction('removeMany', arg1, options.collection);
      var db = hasFunction('collection', arg1, arg1.client, arg1.database, options.database, options.client);
      var client = hasFunction('db', arg1, arg1.client, options.client);

      if (collection) {
        this.setCollection(collection);
      } else if (db) {
        this.setCollection(db.collection(options.collection));
      } else if (client) {
        this.setCollection(client.db(options.database || options.db).collection(options.collection));
      } else {
        options = _extends({}, options, arg1);
      }
    }
    this.compression = options.compression;
    this.ready = (0, _thunky2.default)(function (cb) {
      if (!_this.collection) {
        var mongoOptions = OPTIONS_LIST.reduce(function (opt, key) {
          delete opt[key];
          return opt;
        }, Object.assign({ useUnifiedTopology: true }, options));

        _mongodb.MongoClient.connect(_this.url, { useUnifiedTopology: true }, function (err, client) {
          if (err) return cb(err);
          _this.setCollection(client.db(options.database).collection(options.collection));
          createIndex(_this.collection, cb);
        });
      } else {
        if (_this.client) return createIndex(_this.collection, cb);
        cb(new Error('Invalid mongo connection.'));
      }
    });
  }

  _createClass(MongoStore, [{
    key: 'setCollection',
    value: function setCollection(col) {
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

  }, {
    key: 'get',
    value: function get(key) {
      var _this2 = this;

      var fn = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : noop;

      this.ready(function (err, col) {
        if (err) return fn(err);
        col.findOne({ key: key }, function (err, data) {
          if (err) return fn(err);
          if (!data) return fn(null, null);
          //Mongo's TTL might have a delay, to fully respect the TTL, it is best to validate it in get.
          if (data.expireAt.getTime() < Date.now()) {
            _this2.del(key);
            return fn(null, null);
          }
          try {
            if (data.compressed) return decompress(data.value, fn);
            fn(null, data.value);
          } catch (err) {
            fn(err);
          }
        });
      });
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

  }, {
    key: 'set',
    value: function set(key, val, ttl) {
      var _this3 = this;

      var fn = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : noop;

      if ('function' === typeof ttl) {
        fn = ttl;
        ttl = null;
      }

      var data = void 0;
      var store = this;
      var query = { key: key };
      var options = { upsert: true, safe: true };

      try {
        data = {
          key: key,
          value: val,
          expireAt: new Date(Date.now() + (ttl || 60) * 1000)
        };
      } catch (err) {
        return fn(err);
      }

      this.ready(function (err, col) {
        if (err) return fn(err);
        if (!_this3.compression) {
          update(data);
        } else {
          compress(data, function compressData(err, data) {
            if (err) return fn(err);
            update(data);
          });
        }
        function update(data) {
          col.updateOne(query, { $set: data }, options, function (err, data) {
            if (err) return fn(err);
            if (!data) return fn(null, null);
            fn(null, val);
          });
        }
      });
    }

    /**
     * Delete an entry.
     *
     * @param {String} key
     * @param {Function} fn
     * @api public
     */

  }, {
    key: 'del',
    value: function del(key) {
      var fn = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : noop;

      this.ready(function (err, col) {
        if (err) return fn(err);
        col.removeOne({ key: key }, { safe: true }, fn);
      });
    }

    /**
     * Clear all entries for this bucket.
     *
     * @param {Function} fn
     * @api public
     */

  }, {
    key: 'clear',
    value: function clear() {
      var fn = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : noop;

      this.ready(function (err, col) {
        if (err) return fn(err);
        col.removeMany({}, { safe: true }, fn);
      });
    }

    /**
     * Close the cache client session.
     * @api public
     */

  }, {
    key: 'close',
    value: function close() {
      var _this4 = this;

      var fn = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : noop;

      if (this.collection) {
        this.client.close(function (err, res) {
          if (err) fn(err);else {
            delete _this4.collection;
            delete _this4.client;
            fn(null);
          }
        });
      } else fn(null);
    }
  }]);

  return MongoStore;
}();

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

exports.default = MongoStore;
function compress(data, fn) {
  // Data is not of a "compressable" type (currently only Buffer)
  if (!Buffer.isBuffer(data.value)) return fn(null, data);

  _zlib2.default.gzip(data.value, function (err, val) {
    // If compression was successful, then use the compressed data.
    // Otherwise, save the original data.
    if (!err) {
      data.value = val;
      data.compressed = true;
    }
    fn(err, data);
  });
}

/**
 * Decompress data value.
 *
 * @param {Object} value
 * @param {Function} fn
 * @api public
 */

function decompress(value, fn) {
  var v = value.buffer && Buffer.isBuffer(value.buffer) ? value.buffer : value;
  _zlib2.default.gunzip(v, fn);
}
module.exports = exports['default'];
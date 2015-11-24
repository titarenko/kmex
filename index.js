var Promise = require('bluebird');
var mongodb = Promise.promisifyAll(require('mongodb'));
var _ = require('lodash');

module.exports = kmex;

function kmex (uri) {
	getApi.close = close;
	return getApi;

	function getApi (name) {
		var context = {};

		var api = {
			insert: insert.bind(context, name),

			select: select.bind(context, name),
			where: where.bind(context, name),
			orderBy: orderBy.bind(context, name),
			limit: limit.bind(context, name),
			then: then.bind(context, name),
			first: first.bind(context, name)
		};

		return api;

		function insert (name, itemOrItems) {
			return getCollection(name).then(function (collection) {
				return _.isArray(itemOrItems)
					? collection.insertManyAsync(itemOrItems)
					: collection.insertAsync(itemOrItems);
			});
		}

		function select (name, fields) {
			this.project = fields;
			return api;
		}

		function where (name, query) {
			this.find = find;
			return api;
		} 

		function orderBy (name, field, direction) {
			this.sort = _.zipObject([field], [direction == 'desc' ? -1 : 1]);
			return api;
		}

		function limit (name, limit) {
			this.limit = limit;
			return api;
		}

		function then (name, fn) {
			return getCollection(name).then(function (collection) {
				var cursor = collection.find(this.find || this.projection && {}, this.projection);
				if (this.sort) {
					cursor = cursor.sort(this.sort);
				}
				if (this.limit) {
					cursor = cursor.limit(this.limit);
				}
				return this.first 
					? cursor.toArrayAsync().then(function (rows) { return rows[0]; })
					: cursor.toArrayAsync();
			}.bind(this)).then(fn);
		}

		function first (name) {
			this.limit = 1;
			this.first = true;
			return api;
		}
	}

	function getCollection (name) {
		return getConnection().then(function (connection) {
			return connection.collection(name);
		});
	}

	function close () {
		return getConnection().then(function (connection) {
			return connection.close();
		});
	}

	var connectionPromise;
	function getConnection () {
		return connectionPromise || (
			connectionPromise = mongodb.MongoClient.connectAsync(uri)
		);
	}
}

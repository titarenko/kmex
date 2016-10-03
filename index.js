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
			update: update.bind(context, name),
			orInsert: orInsert.bind(context, name),

			del: del.bind(context, name),
			remove: del.bind(context, name),

			select: select.bind(context, name),
			where: where.bind(context, name),
			orderBy: orderBy.bind(context, name),
			limit: limit.bind(context, name),
			first: first.bind(context, name),

			aggregate: aggregate.bind(context, name),

			then: then.bind(context, name)
		};

		[
			'bind', 'catch', 'finally', 'asCallback', 
			'spread', 'map', 'reduce', 'tap', 'thenReturn',
			'return', 'yield', 'ensure', 'reflect'
		].forEach(function (method) {
			api[method] = function () {
				var then = this.then();
				then = then[method].apply(then, arguments);
				return then;
			};
		});

		return api;

		function insert (name, itemOrItems) {
			this.insert = itemOrItems;
			return api;
		}

		function update (name, item, options) {
			this.update = { $set: item };
			this.updateOptions = _.defaults(options || {}, { multi: true });
			return api;
		}

		function del () {
			this.del = true;
		}

		function orInsert (name, item) {
			var setOnInsert = _.omit(item, _.keys(this.update.$set));
			if (!_.isEmpty(setOnInsert)) {
				this.update['$setOnInsert'] = setOnInsert;
			}
			this.updateOptions.upsert = true;
			return api;
		}

		function select (name) {
			this.projection = Array.prototype.slice.call(arguments, 1);
			return api;
		}

		function where (name, query) {
			this.find = { };
			for (var i in query) {
				if (query.hasOwnProperty(i)) {
					this.find[i == 'id' ? '_id' : i] = i == 'id' || i == '_id'
						? mongodb.ObjectID(query[i])
						: query[i];
				}
			}
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

		function first (name) {
			this.limit = 1;
			this.first = true;
			return api;
		}

		function aggregate (name) {
			var pipeline = _.slice(arguments, 1);
			this.aggregate = pipeline;
			return api;
		}

		function then (name, resolve, reject) {
			return getCollection(name).then(function (collection) {
				if (this.insert) {
					return doInsert.call(this, collection);
				} else if (this.update) {
					return doUpdate.call(this, collection);
				} else if (this.del) {
					return doDelete.call(this, collection);
				} else if (this.aggregate) {
					return doAggregate.call(this, collection);
				} else {
					return doSelect.call(this, collection);
				}
			}.bind(this)).then(resolve, reject);
		}

		function doInsert (collection) {
			return _.isArray(this.insert)
					? collection.insertManyAsync(this.insert)
					: collection.insertAsync(this.insert);
		}

		function doUpdate (collection) {
			return collection.updateAsync(this.find, this.update, this.updateOptions);
		}

		function doDelete (collection) {
			return collection.removeAsync(this.find);
		}

		function doAggregate (collection) {
			return collection.aggregateAsync(this.aggregate, { allowDiskUse: true });
		}

		function doSelect (collection) {
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
		}
	}

	function getCollection (name) {
		return getConnection().then(function (connection) {
			return connection.collection(name);
		});
	}

	var connectionPromise;
	function getConnection () {
		return connectionPromise || (
			connectionPromise = mongodb.MongoClient.connectAsync(uri)
		);
	}

	function close () {
		return getConnection().then(function (connection) {
			return connection.close();
		}).finally(function () {
			connectionPromise = null
		});
	}
}


var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-ubb]';

(function(Exporter) {

	Exporter.setup = function(config, callback) {
		Exporter.log('setup');

		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || '',
			user: config.dbuser || config.user || '',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database || ''
		};

		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');

		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();
		Exporter.log('########  Mysql connected    #######');
		callback(null, Exporter.config());
	};
	Exporter.getUsers = function(callback) {
		return Exporter.getPaginatedUsers(0, -1, callback);
	};
	Exporter.getPaginatedUsers = function(start, limit, callback) {
        Exporter.log('Prepare get Users');
		callback = !_.isFunction(callback) ? noop : callback;
        Exporter.log('PREPARE DB statement');
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query = 'SELECT '
		               + 'core_user.id as _uid, '
                               + 'core_user.id as _username, '
                               + 'core_user.id as _alternativeUsername, '
                               + 'core_user.email as _registrationEmail, '
                               + 'UNIX_TIMESTAMP(core_user.reg_time) as _joindate, '
//		               + 'MIN(UNIX_TIMESTAMP(core_user_session.core_user_id)) as _lastonline, '
                               + 'core_user.deleted as _banned, '
                               + 'core_user.email as _email, '
                               + 'core_user.signature as _signature, '
                               + 'core_user.www as _website, '
                               + 'core_user.avatar as _picture, '
                               + 'core_user.id as _badge, '
                               + 'core_user.birthdate as _birthday '
                               + 'FROM ' + 'core_user '
//		               + 'LEFT JOIN core_user_session ON core_user_session.core_user_id=core_user.id '
//		               + 'GROUP BY core_user.id '

	         	       + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ', ' + limit : '');


		Exporter.log('QUERY: ' + query);
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}
		var test = "line 67 check"
		Exporter.warn(test);
		Exporter.log("User query: " + query);

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

	        Exporter.log("User Rows: " + rows.length);

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						// from unix timestamp (s) to JS timestamp (ms)
						row._joindate = ((row._joindate || 0) * 1000) || startms;

						// lower case the email for consistency
						row._email = (row._email || '').toLowerCase();

						// I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
						row._picture = Exporter.validateUrl(row._picture);
						row._website = Exporter.validateUrl(row._website);

						row._banned = row._banned ? 1 : 0;

						if (row._gid) {
							row._groups = [row._gid];
						}
						delete row._gid;


						map[row._uid] = row;
					});

					callback(null, map);
				});
	};
	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
                Exporter.log('Limit: ' + limit);
		Exporter.log('Start: ' + start);
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		// board = category ~ 30
		var query = 'SELECT '
				+ 'forum_board.id as _cid, '
				+ 'forum_board.name as _name, '
				+ 'forum_board.description as _description '
				+ 'FROM ' + 'forum_board '

				+ (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ', ' + limit : '');
		Exporter.log('QUERY: ' + query);
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}
		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						Exporter.error(query);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						row._name = row._name || 'Untitled Category '
						row._description = row._description || 'No decsciption available';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;

						map[row._cid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};
	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var err;
		var prefix = Exporter.config('prefix');
		// topic = thread
		var startms = +new Date();
		var query =     
				'SELECT '
		             +  'forum_thread.id as _tid, '
                             +  'forum_thread.title as _title, '
                             +  'forum_board_forum_thread.forum_board_id as _cid, '
                             +  'forum_thread.start_user_id as _uid, '
                             +  'forum_post.message as _content, '
                             +  'forum_thread.post_count as _viewcount, '
                             +  'UNIX_TIMESTAMP(forum_thread.start_time) as _timestamp, '
                             +  'forum_thread.sticky as _pinned, '
                             +  'UNIX_TIMESTAMP(forum_post.lastedit_time) as _edited, '
                             +  'forum_post.ip_address as _ip '
                             +  'FROM forum_thread '
                             +  'JOIN forum_board_forum_thread ON forum_thread.id=forum_board_forum_thread.forum_thread_id '
                             +  'JOIN forum_post ON forum_thread.start_post_id=forum_post.id '
		

		             + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ', ' + limit : '');
	
		Exporter.log('QUERY: ' + query);
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			Exporter.error(query);
			return callback(err);
		}

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};

					rows.forEach(function(row) {
						row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						row._edited = row._edited ? row._edited * 1000 : row._edited;

						map[row._tid] = row;
					});

					callback(null, map);
				});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};
	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query =
				'SELECT '
                              + 'forum_post.id as _pid, '
                              + 'forum_post.post_user_id as _uid, '
                              + 'forum_post_forum_thread.forum_thread_id as _tid, '
                              + 'forum_post.message as _content, '
                              + 'forum_post.parent_id as _toPid, '
		              + 'UNIX_TIMESTAMP(forum_post.post_time) as _timestamp, '
                              + 'UNIX_TIMESTAMP(forum_post.lastedit_time) as _edited, '
                              + 'forum_post.ip_address as _ip '
                              + 'FROM forum_post '
                              + 'JOIN forum_post_forum_thread ON forum_post_forum_thread.forum_post_id=forum_post.id '
					// this post cannot be a its topic's main post, it MUST be a reply-post
					// see https://github.com/akhoury/nodebb-plugin-import#important-note-on-topics-and-posts
		            //  + 'WHERE parent_id is NOT NULL '
		           // might be a problem due to the reference of the parent ...
		              + 'WHERE forum_post.deleted=0 '
		              // unelegante Lösung ...
		              + 'AND forum_post_forum_thread.forum_post_id NOT IN (SELECT start_post_id from forum_thread) '
		              // für das kleinere Set nur 2020er Posts
                              + 'AND UNIX_TIMESTAMP(forum_post.post_time) > 1577836800 '   

			      + (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ', ' + limit : '');

		Exporter.log('QUERY: ' + query);
		if (!Exporter.connection) {
			err = {error: 'MySQL connection is not setup. Run setup(config) first'};
			Exporter.error(err.error);
			return callback(err);
		}
		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(async function test(row) {
						//TODO PostId
						var _tempstring = row._content;
						var _begin = _tempstring.indexOf("[quote=");
						var _parent = 0;
						while (_begin >= 0) {
							_tempstring = _tempstring.substring(_begin + 7, _tempstring._end);
							var _end = _tempstring.indexOf("]");

							if(_end > 7 || _end < 0){
								_end = 7;
							}
							_parent = _tempstring.substring(0, _end);
				
							if(isNaN(_parent) || _parent == ''){
								Exporter.warn(row._pid);
								Exporter.warn(_parent);
							}
							else
							{
								// has to wait for query results
								const test = await getUser(_parent);
								row._content = row._content.replace(_parent, test.post_user_id + ";" + test.id);
								//Exporter.warn(_parent);
								//Exporter.warn(row._content);
							}

							_begin = _tempstring.indexOf("[quote=");
						}
						row._content = row._content || '';

						row._timestamp = ((row._timestamp || 0) * 1000) || startms;
						row._edited = row._edited ? row._edited * 1000 : row._edited;

						map[row._pid] = row;
					
					
					});

					Exporter.warn("Done");

					callback(null, map);
				});


	};

	function getUser(_parent){
		return new Promise(function(resolve, reject){
			var tempquery = "select post_user_id, id from forum_post where id=" +  _parent;

			Exporter.connection.query(tempquery,
				function(err, result) {
					if (err) {
						Exporter.error(err);
						return reject(err); 
					}

				resolve(result[0]);

			});	


		});
	}

	// todo: possible memory issues
	/*
	function getConversations (callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		var query = 'SELECT * from pm_message';
				//+ 'PRIVATE_MESSAGE_USERS.TOPIC_ID as _cvid, '
				//+ 'PRIVATE_MESSAGE_USERS.USER_ID as _uid1, '
				//+ 'PRIVATE_MESSAGE_POSTS.USER_ID as _uid2 '
				//+ 'FROM ' + 'PRIVATE_MESSAGE_USERS '
				//+ 'JOIN ' + 'PRIVATE_MESSAGE_POSTS '
				//+ 'ON ' + 'PRIVATE_MESSAGE_POSTS.TOPIC_ID = ' + 'PRIVATE_MESSAGE_USERS.TOPIC_ID '
				//+ 'AND ' + 'PRIVATE_MESSAGE_POSTS.USER_ID != ' + 'PRIVATE_MESSAGE_USERS.USER_ID '

		var parse = function(v) { return parseInt(v, 10); };

		Exporter.connection.query(query,
				function(err, rows) {
					if (err) {
						Exporter.error(err);
						Exporter.error(query);
						return callback(err);
					}

					//normalize here
					var map = {};
					rows.forEach(function(row) {
						if (!row._uid1 || !row._uid2) {
							return;
						}
						row._uids = {};
						row._uids[row._uid1] = row._uid2;
						row._uids[row._uid2] = row._uid1

						map[row._cvid] = row;
					});

					callback(null, map);
				});

	}
        */
        /*
	Exporter.getMessages = function(callback) {
		return Exporter.getPaginatedMessages(0, -1, callback);
	};
	Exporter.getPaginatedMessages = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;

		if (!Exporter.connection) {
			Exporter.setup(Exporter.config());
		}

		var prefix = Exporter.config('prefix');
		var startms = +new Date();

		var query = 'SELECT * from pm_message';
				+ 'PRIVATE_MESSAGE_POSTS.POST_ID as _mid, '
				+ 'PRIVATE_MESSAGE_POSTS.POST_BODY as _content, '
				+ 'PRIVATE_MESSAGE_POSTS.USER_ID as _fromuid, '
				+ 'PRIVATE_MESSAGE_POSTS.TOPIC_ID as _cvid, '
				+ 'PRIVATE_MESSAGE_POSTS.POST_TIME as _timestamp '

				+ 'FROM ' + 'PRIVATE_MESSAGE_POSTS '

				+ (start >= 0 && limit >= 0 ? ' LIMIT ' + start + ', ' + limit : '');

				+ (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
			
		getConversations(function(err, conversations) {
			if (err) {
				return callback(err);
			}

			Exporter.connection.query(query,
					function(err, rows) {
						if (err) {
							Exporter.error(err);
							Exporter.error(query);
							return callback(err);
						}

						//normalize here
						var map = {};
						rows.forEach(function(row) {

							var conversation = conversations[row._cvid];
							if (!conversation) {
								return;
							}

							row._touid = conversation._uids[row._fromuid];
							if (!row._touid) {
								return;
							}

							row._content = row._content || '';
							row._timestamp = ((row._timestamp || 0) * 1000) || startms;

							delete row._cvid;

							map[row._mid] = row;
						});

						callback(null, map);
					});
		});
	};
        */
	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();

		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			/*
			function(next) {
				Exporter.getUsers(next);
			},
			function(next) {
				Exporter.getGroups(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			*/
			function(next) {
				Exporter.getPosts(next);
			}
			/*,
			function(next) {
				Exporter.getMessages(next);
			},
			function(next) {
				Exporter.teardown(next);
			}
			*/
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {

				Exporter.getPaginatedUsers(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};

})(module.exports);

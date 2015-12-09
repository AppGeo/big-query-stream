'use strict';
var Promise = require('bluebird');
var inherits = require('inherits');
var Writable = require('readable-stream').Writable;
var fs = require('fs');
var readFile = Promise.promisify(fs.readFile, fs);
var Deque = require('double-ended-queue');
var auth = Promise.promisify(require('google-auth2-service-account').auth);
var scope = 'https://www.googleapis.com/auth/bigquery';
var rawRequest = Promise.promisify(require('request'));
var noms = require('noms').obj;
var uuid = require('node-uuid');
var debug = require('debug')('big-query')
module.exports = BigQuery;
inherits(BigQuery, Writable);
function BigQuery(key, email, project, dataset, table) {
  Writable.call(this, {objectMode: true});
  if (Buffer.isBuffer(key)) {
    this.key = Promise.resolve(key);
  } else {
    this.key = readFile(key);
  }
  this.iss = email;
  this.project = project;
  this.dataset = dataset;
  this.table = table;
  this.datasetcreateurl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/datasets';
  this.dataseturl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/datasets/' + dataset;
  this.createurl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/datasets/' + dataset + '/tables';

  this.checkurl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/datasets/' + dataset + '/tables/' + table;
  this.baseurl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/datasets/' + dataset + '/tables/' + table + '/insertAll';
  this.insertUrl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/jobs';
  this.queryurl = 'https://www.googleapis.com/bigquery/v2/projects/' + project + '/queries';
  this.defaultDataset = {
    projectId: project,
    datasetId: dataset
  };
  this.queue = new Deque();
}
BigQuery.prototype._request = function (opts) {
  return rawRequest(opts).then(function (resp) {
    resp = resp[1];
    if (resp && resp.error) {
      throw resp.error;
    }
    return resp;
  });
};
BigQuery.prototype.request = function (opts) {
  if (this.inProgress) {
    var resolver = Promise.defer();
    this.queue.push({
      resolver: resolver,
      opts: opts
    });
    return resolver.promise;
  }
  return this.createRequest(opts);
};
BigQuery.prototype.createRequest = function (opts) {
  var self = this;
  this.inProgress = true;
  var tries = 1;
  debug('creating request');
  function attemptDownload(){
    return self.auth().then(function (auth) {
      opts.headers = {
        Authorization: 'Bearer ' + auth
      };
      return self._request(opts);
    }).catch(function (err) {
      err = err || new Error('unknown error');
      if (tries++ > 2 || [401, 403].indexOf(err.code) === -1 || self.stopOnError) {
        throw err;
      }
      debug('tries: ' + tries);
      debug(err.stack || err.message || JSON.stringify(err));
      return Promise.delay(500 << tries).then(function () {
        return attemptDownload();
      });
    });
  }
  return attemptDownload().finally(function () {
    if (self.queue.length) {
      var next = self.queue.shift();
      next.resolver.resolve(self.createRequest(next.opts));
      return;
    }
    self.inProgress = false;
  });
};
BigQuery.prototype.auth = function () {
  if (this.token) {
    return Promise.resolve(this.token);
  }
  var self = this;
  return this.key.then(function (key) {
    return auth(key, {
      iss: self.iss,
      scope: scope
    }).then(function (token) {
      if (self.timeout) {
        clearTimeout(self.timeout);
        self.timeout = null;
      }
      self.token = token;
      self.timeout = setTimeout(function () {
        self.token = null;
        self.timeout = null;
      }, 5 * 60 * 1000);
      self.timeout.unref();
      return token;
    });
  });
};
BigQuery.prototype.post = function (url, body) {
  var self = this;
  var opts = {
    url: url,
    body: body,
    json: true,
    method: 'POST'
  };
  return self.request(opts);
};
BigQuery.prototype.get = function (url, body) {
  var self = this;
  var opts = {
    url: url,
    qs: body,
    json: true
  };
  return self.request(opts);
};
BigQuery.prototype._write = function (data, _, next) {
  if (!Array.isArray(data)) {
    data = [data];
  }
  return this.insert(data.map(function (row) {
      return {
        insertId: uuid.v4(),
        json: row
      };
    })
  ).then(function () {
    next();
  }, next);
};
BigQuery.prototype.insert = function (data) {
  var self = this;
  return this.post(this.baseurl, {
    kind: 'bigquery#tableDataInsertAllRequest',
    rows: data
  }).then(function (resp) {
    if (!resp.insertErrors || !resp.insertErrors.length) {
      return false;
    }
    return self.insert(resp.insertErrors.map(function (error, i) {
      return data[error.index || i];
    }));
  });
};
function fixRows(schema, rows) {
  return rows.map(function (row) {
    var out = {};
    row.f.forEach(function (value, i) {
      var val = value.v;
      if (schema.fields[i].type === 'TIMESTAMP') {
        val = new Date(parseFloat(val) * 1000);
      }
      out[schema.fields[i].name] = val;
    });
    return out;
  });
}
BigQuery.prototype.maybeCreateTable = function (schema) {
  var self = this;
  return this.maybeCreateDataset().then(function () {
    return self.checkTable();
  }).catch(function (e) {
    if (e.code === 404) {
      return self.createTable(schema);
    }
    throw e;
  });
};

BigQuery.prototype.createTable = function (schema) {
  var data = {
    schema: {
      fields: []
    },
    tableReference: {
      datasetId: this.dataset,
      projectId: this.project,
      tableId: this.table
    }
  };
  Object.keys(schema).forEach(function (key) {
    data.schema.fields.push({
      name: key,
      type: schema[key]
    });
  });
  return this.post(this.createurl, data);
};
BigQuery.prototype.checkTable = function () {
  return this.get(this.checkurl);
};
BigQuery.prototype.maybeCreateDataset = function () {
  var self = this;
  return this.get(this.dataseturl).catch(function (e){
    if (e.code === 404) {
      return self.post(self.datasetcreateurl, {
        datasetReference: {
          projectId: self.project,
          datasetId: self.dataset
        }
      });
    }
    throw e;
  });
};
BigQuery.prototype.query = function (query) {
  var pageToken, queryUrl;
  var maxResults = 100;
  var initialBody = {
      defaultDataset: this.defaultDataset,
      query: query,
      useQueryCache: true,
      timeoutMs: 110000,
      maxResults: maxResults
  };
  var self = this;
  return noms(function (next) {
    var stream = this;
    var promise;
    if (!queryUrl) {
      promise = self.post(self.queryurl, initialBody).then(function (resp) {
        queryUrl = self.queryurl + '/' + resp.jobReference.jobId;
        return resp;
      });
    } else {
      promise = self.get(queryUrl, {
        maxResults: maxResults,
        pageToken: pageToken
      });
    }
    promise.then(function (resp) {
      pageToken = resp.pageToken;
      if (!resp.rows) {
        return stream.push(null);
      }
      fixRows(resp.schema, resp.rows).forEach(function (row) {
        stream.push(row);
      });
      if (!pageToken) {
        stream.push(null);
      }
      next();
    }).catch(next);
  });
};

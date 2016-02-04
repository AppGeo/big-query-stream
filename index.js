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
var EE = require('events');
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
BigQuery.prototype.fixRows = function fixRows(schema, rows) {
  return rows.map(function (row) {
    var out = {};
    row.f.forEach(function (value, i) {
      var val = value.v;
      if (schema[i].type === 'TIMESTAMP') {
        val = new Date(parseFloat(val) * 1000);
      }
      out[schema[i].name] = val;
    });
    return out;
  });
}
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
  })).then(function () {
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
BigQuery.prototype.pagedQuery = function (query, opts) {
  var out = {};
  var ee = new EE();
  ee.once('jobinfo', function (d) {
    out.job = d;
  }).once('tablemeta', function (d) {
    out.tablemeta = d;
  }).once('tableinfo', function (d) {
    out.tableinfo = d;
  });
  if (typeof opts === 'string') {
    opts = {
      jobid: opts
    };
  }
  opts = opts || {};
  opts.dataset = opts.dataset || this.dataset;
  opts.defaultDataset = this.defaultDataset;
  if (typeof opts.maxResults !== 'number') {
    opts.maxResults = 20;
  }
  var queryObj = new Query(this, query, opts, ee);
  return queryObj.getRequest().then(rows => {
    out.rows = rows;
    out.pageToken = queryObj.pageToken;
    if (queryObj.totalRows) {
      out.totalRows = queryObj.totalRows;
    }
    return out;
  });
}
BigQuery.prototype.query = function (query, opts) {
  var pageToken;
  if (typeof opts === 'string') {
    opts = {
      jobid: opts
    };
  }
  opts = opts || {};
  opts.dataset = opts.dataset || this.dataset;
  opts.defaultDataset = this.defaultDataset;

  var queryObj = new Query(this, query, opts);

  var out = noms(function (next) {

    queryObj.getRequest().then(rows => {
      if (!rows) {
        this.push(null);
        return next();
      }
      rows.forEach(row => {
        this.push(row);
      });
      next();
    }).catch(next);
  });
  queryObj.out = out;
  return out;
};
var DONE = {};
function Query(bq, query, opts, ee) {
  this.out = ee || new EE();
  this.bq = bq;
  this.jobId = opts.jobid;
  this.destTableRaw = opts.table;
  this.noCreate = opts.noCreate;
  this.destDataset = opts.dataSet;
  this.maxResults = opts.maxResults;
  this.initialBody = {
    configuration: {
      query: {
        defaultDataset: opts.defaultDataset,
        query: query
      }
    }
  };
  this.queryUrl = this.progressUrl = this.totalRows = this.schema = null;
  this.time = 0;
  this.pageToken = null;
}
Query.prototype.dealWithTable = function dealWithTable(destTable) {
  var bq = this.bq;
  var metaUrl = bq.datasetcreateurl + '/' + destTable.datasetId + '/tables/' + destTable.tableId;
  this.queryUrl = metaUrl + '/data';
  var getOpts;
  if (this.maxResults) {
    getOpts = getOpts || {};
    getOpts.maxResults = this.maxResults;
  }
  if (this.pageToken) {
    getOpts = getOpts || {};
    getOpts.pageToken = this.pageToken;
  }
  return Promise.all([
    bq.get(metaUrl),
    getOpts ? bq.get(this.queryUrl, getOpts) : bq.get(this.queryUrl)
  ]).then(resp => {
    this.out.emit('tablemeta', resp[0]);
    this.schema = resp[0].schema.fields;
    return this.handleOutput(resp[1]);
  }, e => {
    if (this.jobId) {
      debug('table nolonger valid');
      this.jobId = null;
      this.queryUrl = null;
      return this.getRequest();
    } if (this.destTableRaw && !this.noCreate) {
      debug(`can not find perminent table ${destTableRaw}, creating one`);
      this.initialBody.configuration.query.destinationTable = destTable;
      this.destTableRaw = null;
      this.queryUrl = null;
      return this.getRequest();
    } else {
      throw e;
    }
  })
}
Query.prototype.pollTable = function pollTable() {
  var bq = this.bq;
  debug('polling try #' + (this.time + 1));
  return bq.get(this.progressUrl).then(resp=> {
    this.time++;
    if (resp.status.state === 'DONE') {
      debug('got finished job');
      var destTable = resp.configuration.query.destinationTable;
      this.out.emit('jobinfo', resp);
      this.out.emit('tableinfo', destTable);
      if (resp.status.errorResult) {
        return null;
      }
      return this.dealWithTable(destTable);
    } else {
      if (this.time < 4) {
        return Promise.delay(50).then(r=>this.pollTable(r));
      }
      if (this.time < 8) {
        return Promise.delay(200).then(r=>this.pollTable(r));
      }
      return Promise.delay(2000).then(r=>this.pollTable(r));
    }
  }, e=> {
    if (this.jobId) {
      debug('job id nolonger valid');
      this.jobId = null;
      return this.getRequest();
    } else {
      throw e;
    }
  }
);
}
Query.prototype.getRequest = function getRequest(pagetoken) {
  pagetoken = pagetoken || this.pageToken;
  if (pagetoken === DONE) {
    return Promise.resolve(null);
  }
  var bq = this.bq;
  if (!this.queryUrl) {
    if (this.jobId) {
      this.progressUrl = bq.insertUrl + '/' + this.jobId;
      return this.pollTable();
    } else if (this.destTableRaw) {
      return this.dealWithTable({
        projectId: bq.project,
        datasetId: this.destDataset,
        tableId: this.destTableRaw
      });
    } else {
      debug('inserting query');
      return bq.post(bq.insertUrl, this.initialBody).then(resp=> {
        this.out.emit('jobid', resp.jobReference.jobId)
        this.progressUrl = bq.insertUrl + '/' + resp.jobReference.jobId;
        return this.pollTable();
      });
    }
  } else {
    let opts =  {
      pageToken: pagetoken
    };
    if (this.maxResults) {
      opts.maxResults = this.maxResults;
    }
    return bq.get(this.queryUrl,opts).then(resp=> {
      return this.handleOutput(resp);
    });
  }
}

Query.prototype.handleOutput = function handleOutput(output) {
  if (output.pageToken) {
    this.pageToken = output.pageToken;
  } else {
    this.pageToken = DONE;
  }
  if (output.totalRows) {
    this.totalRows = output.totalRows;
  }
  if (!output.rows) {
    this.out.emit('bqdone');
    return null;
  }
  return this.fixRows(output.rows);
}
Query.prototype.fixRows = function (rows) {
  var schema = this.schema;
  return this.bq.fixRows(schema, rows);
}

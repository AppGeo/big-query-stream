big-query-stream
===

stream data into and out of big query


```js
var BigQueryStream = require('big-query-stream');

var bigQuery = new BigQueryStream(key, email, project, dataset, table);
// bigQuery is a writable stream
var query = bigQuery.query('select * from table');
// query is a readable stream

bigQuery.maybeCreateTable(schema).then(success, fail)
// create the table if it doesn't exist, do nothing if it exists

bigQuery.createTable(schema).then(success, fail)
// create the table if it doesn't exist, throw error if it exists
```

query takes an optional second parameter which is a job id for a query, it will use this to load cached results if possible, if it can't find the job or can't find the table the job refers to it runs the job like normal.

the query emits a **jobid** event when it creates a job which you can use in order to take advantage of cached queries.

schemes for table creation should be objects where they keys represent the column name and the value is the type.

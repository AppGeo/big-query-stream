big-query-stream
===

stream data into and out of big query


```js
var BigQueryStream = require('big-query-stream');

var bigQuery = new BigQueryStream(key, email, project, dataset, table);
// bigQuery is a writable stream
var query = bigQuery.query('select * from table');
// query is a readable stream
```
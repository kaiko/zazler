
# Zazler

Stop writing customized APIs! Using SQL data structure and your declared rules, Zazler allows you to create APIs on the fly.

Zazler is express middleware that turns MySQL/PostgreSQL or SQLite into RESTful API using your declarations for what to expose.  No object-relational mapping required.

## When to use

  * When you need an API for mobile apps and any other platform (SQL data is converted into JSON)
  * When you want to share live data (html, xml, csv) - importing into Excel, for example.

## Fast set-up, no coding

Zazler learns database schema and already includes many types of data output such as json, xml, csv, html, and more.

All you have to do is tell Zazler which tables/fields can be accessed. You do this by creating declarative rules, not by programming or writing SQL queries.

# Getting started

## "Hello world"

Here’s how you would make an API from the table `hello` using Zazler:

```javascript
let express = require('express')();
require('zazler')("mysql://root:pass@127.0.0.1/dbname", { read: "hello" } )
.then(sqlApi => {
  express.use('/my/', sqlApi.expressRequest);
  express.listen(80);
});
```

  * `hello` content as json: `http://localhost/my/hello.json`
  * To query rows by id: `http://localhost/my/hello.json?where=id=1`

## SQL Connections

  - `psql://user:pass@host:port/db`
  - `mysql://user:pass@host:port/db`
  - `sqilte:///tmp/db.file`

In addition to URL-style connections, you can use objects. This method is more dynamic because you can use every connection parameter depending on library. For postgresql, it is library pg and for MySQL it is mysql.

  - `{ type: 'pg', hostname: '127.0.0.1', database: 'foo'}`
  - `{ type: 'my', hostname: '127.0.0.1', database: 'foo'}`

For exact connection parameters, take a look at these libraries: [pg](https://www.npmjs.com/package/pg), [mysql](https://www.npmjs.com/package/mysql) and [sqlite](https://www.npmjs.com/package/sqlite)


## API requests

Requests are `table.format?select=field1,field2&where=field1=1` where select and where

  - `http://127.0.0.1/api/table.json?where=id=10` − query by id (there is shorter version: ?id:=10 (yes, with colon)
  - `http://127.0.0.1/api/table.json?where=like(firstname,$L)&L=J%25` − query by text matching. Notice that the “where” expression is using variable L.
  - `http://127.0.0.1/api/table.json?order=firstname` − for ordering data.
  - `http://127.0.0.1/api/table.json?limit=10` − for limiting output.
  - `http://127.0.0.1/api/table.json?select=firstname,lastname` − to select only some fields.
  - `http://127.0.0.1/api/table.json?select=concat(firstname,$spc,lastname)&spc=%20` – here, the SQL server does more complicated work.
  - `http://127.0.0.1/api/table.json?group=IDparty&select=IDparty,count(*) − to group data, you may want also use the `having` parameter.

## Limit database access

```javascript
    let DBApi = require('zazler');
    dbApi = await DBApi("mysql://host/db", {

       // table and column level access limits
       // allows to query all columns from table1
       // and field1 and field2 from table2
       read: "table1 table2(field1 field2)",

       // for table `foo` `id > 100` is always added by this module
       // to where condition (addition to user condition from URL)
       filter: [
          { table: "foo",
            where: "id>100"
          } ]
    });
```

`read` can use wildchars and exclusion: `{read: "* -foo"}` makes the whole database readable except table `foo`.

# What users say

## Tiit Remmel

Our company have been using Zazler for a while and could not imagine backend without Zazler. It's simple to use and configure, but still capable for extremly advanced solutions. I don't see point of using any other backend language - Zazler is just perfect!

# Similar projects

  * [PostgREST](https://postgrest.com/)
  * [HtSQL](http://htsql.org/)
  * [YQL - Yahoo! Query Language](https://www.datatables.org/)



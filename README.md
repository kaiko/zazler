

# Zazler

Zazler builds sophisticated API from SQL structure allowing to make API calls safely.

Supported databases are MySQL, PostgreSQL and SQLite.

# Motivation

Custom API makes development process slow and clumsy. Every change to API may cause problems for existing API users. Worse than that - it takes time for backend developers to implement small changes.

Zazler instead allows to define on backend which data may be accessed and you can customize API call on frontend. And this is safe, no direct SQL access.

## When to use

  * API from mobile apps and any other platform (json, wsdl/soap)
  * Very quickly share LIVE data to somebody (html, xml, csv)

## Fast setup, no coding

Zazler learns database schema and has already all kind of data output as json, xml, csv, html etc.

Only thing you have to do is tell which tablefields and on what condition data is accessible. You do this by declarative rules, not programming or writing SQL queries.

# Getting started

## "Hello wolrd"

```javascript
let srv = require('express')();
require('zazler')("file:///tmp/my.db", { read: '*' } )
.then(api => {
  srv.use('/my/', api.expressRequest);
  srv.listen(80);
});
```

  * `tablename` content as json: `http://localhost/my/tablename.json`
  * Row here id is 1: `http://localhost/my/tablename.json?where=id=1

## Connecting

  - `psql://user:pass@host:port/db`
  - `mysql://user:pass@host:port/db`
  - `sqilte:///tmp/db.file`

Additionally to URL style connection you may use objects. This is more dynamic because you can use every connection parameter depending on library. For postgresql it is library `pg` and for MySQL it is `mysql`.

  - `{ type: 'pg', hostname: '127.0.0.1', database: 'foo'}`
  - `{ type: 'my', hostname: '127.0.0.1', database: 'foo'}`

## API requests

Requests are `table.format?select=field1,field2&where=field1=1` where select and where

 - `http://127.0.0.1/api/table.json?where=id=10` − query by id (there is shorter version: `?id:=10` (yes, with colon)
 - `http://127.0.0.1/api/table.json?where=like(firstname,$L)&L=J%25` − query by text matching. Notice how where expression is using variable L.
 - `http://127.0.0.1/api/table.json?order=firstname` − for ordering data.
 - `http://127.0.0.1/api/table.json?limit=10` − for limiting output.
 - `http://127.0.0.1/api/table.json?select=firstname,lastname` − to select only some fields.
 - `http://127.0.0.1/api/table.json?select=concat(firstname,$spc,lastname)&spc=%20` − here SQL server does already more complicated work.
 - `http://127.0.0.1/api/table.json?group=IDparty&select=IDparty,count(*)` − to group data; you may want also use `having` parameter.

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

`read` can use wildchars and exclusion: `{read: "* -foo"}` makes whole database is readable except table `foo`.

# Similar projects

  * [PostgREST](https://postgrest.com/)
  * [HtSQL](http://htsql.org/)
  * [YQL - Yahoo! Query Language](https://www.datatables.org/)



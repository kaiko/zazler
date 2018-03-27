
I've always been passinate about making development process as fast as possible.

This platform allows you to query SQL data at frontend so easily as possible without writing all tedious SQL queries one by one on server side.

Works with MySQL, PostgreSQL and SQLite. Provides JSON output and convienent way to see result in html.

Here is "hello world" example (assume there is sqlite database):

    let srv = require("express")();
    let api = await require("zazler")("file:///tmp/my.db", { read: "*" });
    srv.use('/my/', api.expressRequest);
    srv.listen(80);

It allows to query like this:

    curl http://localhost/my/tablename.json # `tablename` content as json
    curl http://localhost/my/tablename.json?where=id=1 # row where id=1

## Connecting

Examples to connect to database:

  - postgresql: `psql://user:pass@host:port/db` or { type: 'pg', hostname: '...', database: '...'} (look at module `pg` for all options)
  - mysql: `mysql://user:pass@host:port/db` or { type: 'my', hostname: '...', database: '...'} (look at module `mysql` for all options)
  - sqlite: `file:///tmp/db.file`

## Query data

 - `?where=id=10` − query by id (there is shorter version: `?id:=10` (yes, with colon)
 - `?where=like(firstname,$L)&L=J%25` − query by text matching. Notice how where expression is using variable L.
 - `?order=firstname` − for ordering data.
 - `?limit=10` − for limiting output.
 - `?select=firstname,lastname` − to select only some fields.
 - `?select=concat(firstname,$spc,lastname)&spc=%20` − here SQL server does already more complicated work.
 - `?group=IDparty&select=IDparty,count(*)` − to group data; you may want also use `having` parameter.

## Limit database access

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

`read` can use wildchars and exclusion: `{read: "* -foo"}` makes whole database is readable except table `foo`.



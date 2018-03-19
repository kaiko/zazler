
I've always been passinate about making development process as fast as possible.
This platform is to start using data at frontend so easily as possible without writing all these tedious SQL queries one by one.
Zazler makes API to use SQL power safely and easily. This you can **raise productivity by 30-40%**.

Works with MySQL, PostgreSQL and SQLite. Provides JSON output and convienent way to see result in html.

Here is brief example on server side:

    // sqlite3 /tmp/my.db "create table a(id integer); insert into a values(1);"

    var {App} = require("zazler");
    var app = new App("sqlite:///tmp/my.db", { read: "*" });
    var srv = require('express');
    srv.use('/my/', app.expressRequest);
    srv.listen(80);

It allows to query like this:

    curl http://localhost/db/a.json # gives whole table

## Query data

 - `?where=id=10` − query by id
 - `?where=like(firstname,L)&L=J%25` − query by text matching. Notice how where expression is using variable L.
 - `?order=firstname` − for ordering data.
 - `?limit=10` − for limiting output.
 - `?select=firstname,lastname` − to select only some fields.
 - `?select=concat(firstname,spc,lastname)&spc=%20` − here SQL server does already more complicated work.
 - `?group=IDparty&select=IDparty,count(*)` − to group data; you may want also use `having` parameter.

## Limit database access

    app.db("mysql://host/db", {

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



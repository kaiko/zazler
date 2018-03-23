
let srv = require('express')();
let api = require('zazler') ("file:///tmp/my.db", { read: '*' } );
srv.use('/foo/', api.expressRequest);
srv.listen(3000);


let express = require('express');
let App = require('zazler').App;

let a = new App({file: ':memory:', type: 'lt'}, { read: '*' } );
srv = express();
srv.use('/foo/', a.expressRequest);
srv.listen(3000);

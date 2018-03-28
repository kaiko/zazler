
let srv = require('express')();
require('zazler')("file:///tmp/my.db", { read: '*' } ).then(api => {
  srv.use('/foo/', api.expressRequest);
  srv.listen(3000);
});

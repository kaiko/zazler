let assert = require('assert');
let req = require('request-promise');
let Z = require(process.env.ZAZLER_HOME || 'zazler');
let E = require('express')();
let fs = require('fs');
let util = require('util');

E.use( require('cookie-parser')() );

let pgConn = "psql://postgres@127.0.0.1/alfa";

let doSettle = process.env.TEST_SETTLE ? true : false;
let readFile = util.promisify(fs.readFile);

E.listen(80, () => { 
  Promise.all([
    fnSimpleEqBody('simple-alfa-html', 'alfa.html')
  , fnSimpleEqBody('simple-alfa-json', 'alfa.json')
  , fnSimpleEqBody('simple-alfa-xml' , 'alfa.xml')
  , fnSimpleEqJson('simple-schema'   , '_schema.debug.json', { read: '*' })
  , fnWrite1()
  , fnAuthContent()
  ]).then(() => {
    process.exit(0);
    // E.close();
  })
})

endOfWorld = err => { console.error(err); process.exit(100); }
process.on('uncaughtException', endOfWorld);
process.on('unhandledRejection', endOfWorld);

function mkId(i) {
  return {
    id: i,
    file: 'test/expect/' + i, 
    postFile: 'test/expect/post--' + i, 
    url: 'http://127.0.0.1/' + i + '/',
    use: '/' + i + '/'
  }
}

/////////////

async function fnSimpleEqBody(name, url, conf = { read: '*' }) {
  let id, app, bodyIs, body;
  id = mkId(name);
  app = await Z(pgConn, conf);
  E.use(id.use, app.expressRequest);
  bodyIs = await req(id.url + url);
  if (doSettle) fs.writeFileSync(id.file, bodyIs);
  else {
    try { assert.equal(await readFile(id.file), bodyIs); } catch (e) { console.log(id.id); throw e; }
    console.log('OK - ' + id.id)
  }
}

async function fnSimpleEqJson(name, url, conf = { read: '*' }) {
  let id, app, bodyIs, body;
  id = mkId(name);
  app = await Z(pgConn, conf);
  E.use(id.use, app.expressRequest);
  bodyIs = await req(id.url + url);
  if (doSettle) fs.writeFileSync(id.file, JSON.stringify(JSON.parse(bodyIs), null, 4));
  else {
     try { assert.deepStrictEqual(JSON.parse(bodyIs), JSON.parse(await readFile(id.file))); } catch (e) { console.log(id.id); throw e; }
     console.log('OK - ' + id.id);
  }
}

async function fnAuthContent() {
  let id, app, bodyIs, body;
  id = mkId('authcont');
  app = await Z(pgConn,
    { read: "*"
    , prot: "*"
    , templates: [ "/z/test/templates/" ]
    , auth: {
        table: "users"
      , select: "id,login"
      , where: "login=cookie.u"
      , content: "_empty.doauth"
      }
    });
  E.use(id.use, app.expressRequest);
  // TODO: test if headers are correct
  bodyIs = await req({ url: id.url + 'alfa.debug.json', headers: { Cookie: "u=nosuchuser" }});
  if (doSettle) fs.writeFileSync(id.file, bodyIs);
  else {
    assert.deepStrictEqual(JSON.parse(bodyIs), JSON.parse(await readFile(id.file)));
    console.log('OK - ' + id.id);
  }
}


async function fnWrite1() {
  let id, app, bodyIs, body, postBody, irow;
  id = mkId('write1');
  app = await Z(pgConn,
    { read: "w"
    , write: "w"
    , varsWithout$: true
    , inlineStrings: true
    , "write-rules": [
      { table: "w", on: "new.x=i:new.t=v", action: "insert", vars: { v: "ahaa", AA: "always..." }, set: { c: "AA", b: "new.t" } }
    ]
    });
  // irow = [{ x: 'i', t: "ahaa", b: 'b', c: 'c'  }]; // TODO: with this row problem emerges
  irow = [{ x: 'i', t: "ahaa", b: 'x', c: 'y'  }];

  // app.on('sql', q => console.log(q.sql));
  E.use(id.use, app.expressRequest);
  postBody = await req({ url: id.url + 'w.json', method: 'POST', body: JSON.stringify(irow) });
  bodyIs = await req({ url: id.url + 'w.debug.json' });
  if (doSettle) fs.writeFileSync(id.file, JSON.stringify(JSON.parse(bodyIs), null, 4));
  else {
     assert.deepStrictEqual(JSON.parse(bodyIs), JSON.parse(await readFile(id.file)));
     console.log('OK - ' + id.id);
  }
}


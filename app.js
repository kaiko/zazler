
// node.js dependencies
const VM   = require('vm');
const md5  = require('md5');
const util = require('util');
const path = require('path');
const fs   = require('fs');

// 3rd party dependeincies
const wildcard = require('wildcard');

// local dependeincies
const {DbConn,urlToConnection} = require('./db');
const AccessRule = require('./access').AccessRule;
const { Select, Filters, WriteRule, jsToQVal, expression } = require('./query')
const Opts = require('./opts');

// to test if function is sync or async
const AsyncFunction = (async () => {}).constructor;

const { breakOn, getBody, btoa, trace } = require('./toolbox');

const exceptionRegister = {
// id,  A - isAppFatal, R - isReqFatal, w - isWrapper (if it not app or req fatal, it is more like warning)
  101: [ 'A' , "Auth must have `table` property" ]
, 102: [ 'A' , "auth must have `realm`, `content` or `location` parameter" ]
, 103: [ 'A' , "auth must have `select` option. NB! everything you put into select can be accessed in the web after authentication" ]
, 104: [ 'A' , "Authentication needed but not configured!" ]
, 105: [ 'A' , "Auth needed in auth" ]
, 106: [ 'A' , "Format missing in `auth.context` declaration" ]
, 110: [ ''  , "Unknown event" ]
, 200: [ 'A' , "Use module express-form-data" ]
, 300: [ 'R' , "Template not found" ]
, 302: [ 'R' , "Table missing" ]
, 500: [ 'R' , "Running query on empty database (or didn't you `await` for newTable)" ]
, 501: [ 'Aw', "Engine initialization error" ]
, 502: [ 'Aw', "Engine script error" ]
, 503: [ 'Rw', "Template error" ]
, 504: [ 'R' , "Unknown token" ]
, 505: [ 'R' , "Can't read request body" ]
}

class Exception {
  constructor (no, err, context = {}) {
    this.no = no;
    this.context = context;
    this.err = err;
    let m = exceptionRegister[no];
    this.isAppFatal = m[0].indexOf('A') > -1;
    this.isReqFatal = m[0].indexOf('R') > -1;
    this.isWrapper  = m[0].indexOf('w') > -1;
    this.message    = m[1];
  }
  toString() {
    if (this.isWrapper)
      return this.err.toString();
    else return this.message + ' #no' + this.no;
  }
  toWeb() {
  }
}

/////////////

async function App(conn, conf) {
  
  if (typeof conn === 'string') conn = urlToConnection(conn);

  let app = Object.create(AppPrototype, 
  { dbName     : { value: conf.dbName }
  // , dbSchema   : { value: null } // ment to be pg schema but it is not used
  , type       : { value: conn.type }
  , sqlType    : { value: ({pg: SqlPg, lt: SqlLt, my: SqlMy})[conn.type] }
  , driverName : { value: conn.type   }  // DriverName :: !String

  , lite       : { value: await DbConn({ filename: ':memory:', type: 'lt' }) }
  , conn       : { value: await DbConn(conn) }

  , engines    : { value: {}, writable: true }
  , export     : { value: {}, writable: true }
  , sqlFn      : { value: {}, writable: true }
  , pipes      : { value: {}, writable: true }

  , emptyResult : { value: { data: [], cols: [], types: [], rawTypes: [], rowsTotal: () => 0 } }

  , events   : { value: { onWebRequest: [], onPost: [], onAfterUpload: [],  onSql: [], onError: [] /*, onAfterPost: [], onUpload: [], onEvGet: [] */ } }
  , eventMap : { value: {
      "web-request" : "onWebRequest"
    , "data-post"   : "onPost"
    , "upload-after": "onAfterUpload"
    , "error"       : "onError"
    , "sql"         : "onSql"
  } }
  , logAccess: { value: () => {}, writable: true }

  , block : { value: Promise.resolve(true), writable: true }
  , _schema: { value: null, writable: true }


  })
  app.expressRequest = AppPrototype.expressRequest.bind(app);
  app.format         = AppPrototype.format.bind(app);
  app.runSelect      = AppPrototype.runSelect.bind(app);

  // You can get these from connection but I wanted to have these values available without using any connection

  if (conf.logAccess) fs.open(conf.logAccess, 'a', (err, f) => app.logAccess = m => fs.write(f, m.url + "\n", () =>{} ))
  if (conf.logSql   ) fs.open(conf.logSql   , 'a', (err, f) => app.on('sql'  , e => fs.write(f, '[' + new Date().toString() + '] ' + e.sql + "\n", () => {} )));
  if (conf.logError ) fs.open(conf.logError , 'a', (err, f) => app.on('error', e => fs.write(f, '[' + new Date().toString() + '] ' + e.toString()  +'\n' , () => {} )));

  await app.setConf(conf);
  await app.updateSchema();
  return app;
}

AppPrototype = {
  evSql:   function (e) { this.events.onSql  .forEach(fn => fn(e)); },
  evError: function (e) { this.events.onError.forEach(fn => fn(e)); },

  setConf: async function (conf) {
    this.index = conf.index || '_schema.dashboard';

    this.varsWithout$  = conf.hasOwnProperty('varsWithout$' ) ? !!conf.varsWithout$  : false;
    this.inlineStrings = conf.hasOwnProperty('inlineStrings') ? !!conf.inlineStrings : false;
    
    if (typeof conf.templates === 'string') conf.templates = [conf.templates];
    if (typeof conf.parsers   === 'string') conf.parsers   = [conf.parsers  ];
    this.tmplDirs = [... (conf.templates || []), __dirname + '/templates/'];
    this.prsDirs  = [... (conf.parsers   || []), __dirname + '/parsers/'  ];
    await this.addEngine("mt.html", __dirname + "/engines/microtemplate.js");

    this.read  = new AccessRule(conf.read);
    this.write = new AccessRule(conf.write);
    this.hide  = new AccessRule(conf.hide);
    this.prot  = new AccessRule(conf.prot || conf.protect);

    this.wrules = (conf["write-rules"] || []).map(r => new WriteRule(r, { varsWithout$: this.varsWithout$, inlineStrings: this.inlineStrings } ));

    if (!conf.auth) this.auth = null;
    else {
      let A = conf.auth;
      if (!conf.auth.table) throw new Exception(101, new Error(), {conf: conf.auth});
      this.auth = Select.tearUp(conf.auth.table, null, conf.auth);
      if      (A.location) { this.auth.type = 'location'; this.auth.location = A.location; }
      else if (A.content)  { this.auth.type = 'content';  this.auth.content  = A.content; this.auth.contentVars = conf.auth['content-vars'] || {} }
      else if (A.realm  )  { this.auth.type = 'basic';    this.auth.realm = A.realm; }
      else throw new Exception(102, new Error(), { auth: conf.auth });
      if (!this.auth.Q.select) throw new Exception(103, new Error(), { auth: conf.auth });
    }

    this.filt   = (conf.filter||conf.filters||[]).map(f => Select.tearUp(f.table, null, f).Q );
    this.vars   = conf.vars || [];
    this.consts = conf.const || conf.consts || [];

    this.alias = conf.alias || []; // Alias  :: ![QueryAlias]

    this.metaF = {};
    this.metaT = {};
    Object.map(conf.meta || {}, (v, k) => {
      let [t,f] = breakOn(k, '.');
      if (!f && !this.metaT[t]) this.metaT[t] = {};
      if (f) {
        if (!this.metaF[t])    this.metaF[t]    = {};
        if (!this.metaF[t][f]) this.metaF[t][f] = {};
      }
      if (!f) this.metaT[t]    = v;
      if ( f) this.metaF[t][f] = v;
    });

  },

  meta: function (t, f = null) { return f ? (this.metaF[t]||{})[f] || null : this.metaT[t] || null; },

  query: async function(tableAs, vars, user, pass) {

    let [tq, fmt] = breakOn(tableAs, '.');
    let [table, as] = breakOn(tq, '@');
    let R = { table, as, format: null, user: null, pass: null, req: { user: null, pass: null, pipe: null, format: null, isMain: true, url: null, isPost: false }, vars: vars, meta: {}, cookie: {} };
    let me = this;

    let _auth = null;
    R.meta = {
      cookie: async () => ({})
    , req:    async () => Object.map(R.req, jsToQVal)
    , auth:   async () => {
        if (!_auth) {
            if (!me.auth) throw new Exception(104, new Error());
            let unprotectedSchema = (await this.schema()).map(f => Object.assign({}, f, {prot: false}));
            _auth = (await me.runSelect(me.auth.Q.from.table, null, me.auth.Q,
                  Object.assign({}, R.meta, { auth: async () => { throw new Exception(105, new Error(), { auth: conf.auth })  } }),
                  unprotectedSchema, [], R
                )).data[0] || null;
        }
        if (!_auth) throw new NeedAuth();
        return _auth;
      }
    }
    
    let sqlResult;
    try { sqlResult = await this.runSelect(R.table, R.as, R.vars, R.meta, await this.schema(), this.filt, R.req); }
    catch (someErr) {
      if (someErr instanceof NeedAuth) { throw "Unauthorized"; }
      else { this.evError(someErr); throw someErr; }
    }

    if (!fmt) return sqlResult;

    try { return (await sqlResult.format(fmt)).text(); }
    catch (someErr) {
      if (someErr instanceof NeedAuth) { throw "Unauthorized"; }
      else { this.evError(someErr); throw someErr; }
    }
  },

  // returns { headers: [ {key: val }], text: return content, code: 200 }
  // code can be "unauthorized" (401)
  // if error/exception emerges it is thrown
  request: async function (tableFormat, vars, extra = {}, cookies = {}, post = null, files = null, user = null, pass = null) {
    
    await this.block; // wait till schema is learned at the beginning

    const me = this;
    const sch = await this.schema();

    let table, as, format;

    [table, format] = breakOn(tableFormat || this.index, '.');
    [table, as] = breakOn(table, '@');

    if (!format) throw new Exception(300, new Error(), { template: template, templateDirs: this.tmplDirs });
    //if ( !['_empty', '_schema', '_meta', '_single'].includes(table) && !sch.find(e => e._ === 'table' && e.name === table))
    //  throw new Exception(302, new Error(), { table: table }); // TODO: look for joins and alias

    let R = { table, as, format, user, pass, opts: {}, vars, meta: {}, cookie: {}, req: { ...extra, table, format, user, pass, pipe: null, isMain: true, isPost: !!post } };

    R.opts = Object.assign({}, Opts.def, vars.opts ? Opts.parse(vars.opts) : {});

    if (this.dbName) R.req.dbName = this.dbName;

    // test for pipe
    R.pipe = R.req.pipe = Object.keys(this.pipes).find(p => R.format.substr(-(p.length+1)) === "." + p);
    if (R.pipe) R.format = R.format.substr(0, R.format.length - R.pipe.length - 1); // cut off pipe part (".pdf" for example)

    // TODO: bad solution (DReq must resolve this)
    R.req.format = R.format;
    R.req.opts = R.opts;
    R.req.user = R.user;
    R.req.pass = R.pass;
    R.vars = R.req.vars = vars;

    let _auth = null, _cookie = null, _req = null;
    R.meta = {
      cookie: async () => { if (!_cookie) _cookie = Object.map(cookies||{}, jsToQVal); return _cookie; }
    , req:    async () => Object.map(R.req, jsToQVal)
    , auth:   async () => {
        if (!_auth) {
            if (!me.auth) throw new Exception(104, new Error());
            let unprotectedSchema = (await this.schema()).map(f => Object.assign({}, f, {prot: false}));
            _auth = (await me.runSelect(me.auth.Q.from.table, null, me.auth.Q, Object.assign({}, R.meta, { auth: async () => ({}) }), unprotectedSchema, [], {})).data[0] || null;
        }
        if (!_auth) throw new NeedAuth();
        return _auth;
      }
    }

    let arg = { vars: R.vars, table: R.table, tableAs: R.as
      , cookie: cookies || {}
      , req: R.req
      , query: async function (qTable, qVars) { // FIXME: this is double in format
          let [qt,f] = breakOn(qTable, '.');
          let [t,as] = breakOn(qt, '@');
          let sqlRes = await me.runSelect(t, as, qVars, R.meta, sch, me.filt, arg);
          return f ? (await sqlRes.format(f)).text() : sqlRes;
      }
      , post: (qTable, getVars, input) => me.runPost(qTable, getVars, arg, R.meta, sch, input)
    }
    for (let e = 0; e < this.events.onWebRequest.length; e++) {
      let fn = this.events.onWebRequest[e];
      if (fn instanceof AsyncFunction)
        await fn(arg);
      else 
        fn(arg);
    }

    R.vars = arg.vars;
    R.table  = arg.req.table  || R.table;
    R.format = arg.req.format || R.format;

    let fmtResult = {}, sqlResult;
    try {
        sqlResult = R.req.isPost
           ? await this.runPost  (R.table, R.vars, R.req, R.meta, sch, post, R, R.files)
           : await this.runSelect(R.table, R.as, R.vars, R.meta, sch, this.filt, {...R.req, ...{ cookie: cookies} } );
        fmtResult = await sqlResult.format(R.format);
        // before it was: , R.table, Object.assign({}, R.req.vars, R.vars), Object.assign({}, {...R.req, ...{ cookie: req.cookies}}, { vars: R.vars}), R.meta, await this.schema() );
    } catch (someErr) {
      if (someErr instanceof NeedAuth) {
        fmtResult.unAuthorized = true;
      } else {
        this.evError(someErr);
        fmtResult.error = someErr;
      }
    }

    if (fmtResult.unAuthorized) switch(this.auth.type) {
      case 'basic':    return { status: 401, headers: [ { key: 'WWW-Authenticate', value: 'Basic realm="' + this.auth.realm + '"' } ], body: "Unauthorized" }; break;
      case 'location': return { status: 307, headers: [ { key: 'Location', value: this.auth.location } ], body: '' }; break;
      case 'content': {
        let [t,f] = breakOn(this.auth.content, '.'); // TODO: move it to setConf or somewhere
        if (!f) throw new Exception(106, new Error(), { auth: conf.auth });
        let m  = Object.assign({}, R.meta.req, { isMain: false, vars: Object.assign({}, R.vars, this.auth.contentVars||{}) });
        try {
          sqlResult = await this.runSelect(t, null,
              Object.assign({}, R.vars, this.auth.contentVars || {}),
              Object.assign({}, R.meta, { auth: async () => null }),
              sch, [], // remove filters in this context
              {...R.req, ...{ cookie: cookies } });
          fmtResult = await sqlResult.format(f);
        } catch (someE) {
          if (someE instanceof NeedAuth) { throw new Exception(105, new Error(), { auth: conf.auth }); }
          this.evError(someE);
          fmtResult = { error: someE };
        }
      }
    }

    if (fmtResult.unAuthorized) {
      return { status: 500, headers: [ { key: 'content-type', value: 'text' } ], body: 'Unauthorized' }; // res.status(500).type('text').send('Unauthorized');
    } else if (fmtResult.error) {
      throw fmtResult.error;
    } else {
      return { status: 200
        , headers: fmtResult.headers.concat(fmtResult.contentType ? [{ key: 'content-type', value: fmtResult.contentType}] : [])
        , body: !R.pipe
          ? fmtResult.out 
          : await (this.pipes[R.pipe])(fmtResult.out, t => res.type(t),Object.assign({}, {...R.req, ...{ cookie: cookies}}, { vars: R.vars }))
      }
    }
  },

  expressRequest: async function (req, res, next) {
    req.setEncoding('utf8');

    this.logAccess(req);

    let [_, search] = breakOn(req.originalUrl, '?');
    let vars =
      (search||'').split('&').filter(Boolean)
      .map(a => { let [k,v] = breakOn(a,'='); return { key: decodeURIComponent(k), val: decodeURIComponent(v)}; })
      .reduce((a,c) => Object.assign(a,{[c.key]: c.val}),{});

    let post = null, files = null, user = null, pass = null;
    if (this.auth && this.auth.type === 'basic')
      (([u,p]) => { if (u) { user = u; pass = p; } })( breakOn( btoa( ((req.get('Authorization') || '').match(/^Basic (.*)/) || [])[1] || ''), ':' ) );

    if (req.method === 'POST') {
      if ((req.headers['content-type']||'').toLowerCase().indexOf('multipart/form-data;') === 0) {
        if (!req.body) {
          console.log([ // TODO: this is not console.log
            'Use express-form-data like this: '
           ,"   const app   = require('express')();"
           ,"   const forms = require('express-form-data');"
           ,"   app.use(forms.parse({ autoFiles: true }));"
           ,"   app.use(forms.format());"
           ,"   app.use(forms.stream());"
           ,"   app.use(forms.union());"
          ].join("\n"));
          throw new Exception(200, new Error()); 
        }
        post  = [Object.keys(req.body).filter(k => typeof req.body[k] === 'string').reduce((r, k) => Object.assign(r, {[k]: req.body[k]}), {})];
        files =  Object.keys(req.body).filter(k => typeof req.body[k] === 'object').reduce((r, k) => Object.assign(r, {[k]: req.body[k]}), {});
      }
      else {
        let [table, format] = breakOn(req.path.split('/').pop(), '.');
        post = await inputRows(this.prsDirs, format, await getBody(req));
      }
    }

    let x ;
    try {
      x = await this.request(req.path.split('/').pop(), vars, { url: req.originalUrl }, req.cookies, post, files, user, pass);
    } catch (e) {
      console.error(e.isWrapper ? e.err : e);
      res.write(e.toString());
      res.end();
      next();
      return;
    }

    res.status(x.status);
    (x.headers || []).forEach(({key,value}) => res.append(key, value)); // TODO: some limits?
    res.write(x.body);
    res.end();

    next();
  },

  on: function (ev, fn) {
    if (!this.eventMap[ev]) throw new Exception(110, new Error(), { unknownEvent: ev, allowedEvents: Object.keys(this.eventMap) });
    this.events[this.eventMap[ev]].push(fn);
  },

   // yeah, schema is in 'this' but on auth there is modified schema used 
  runSelect: async function (table, as, vars, meta, sch, filt = [], dreq = {opts:{}}) { // FIXME: when is dreq missing?
    if (!meta) meta = this.meta;

    if (table === '_single') return await this.lite.query('SELECT NULL'); // TODO: run on main database
    if (table === '_empty' ) {
      let r;
      r = Object.assign({}, this.emptyResult, { format: (fmt, extraVars) => this.format(r, fmt, table, as, Object.assign({}, vars, extraVars), dreq, meta, sch) });
      return r;
    }

    let alias = this.alias.find(a => a.name === table);
    if (alias) {
      table = alias.table;
      as    = as || alias.name;
      vars  = Object.assign({}, alias.vars, vars, alias.const);
      dreq.vars = vars;
    }
    if (table === '_schema') await this.schema(); // just to initialize table if not done already

    let cn = table !== '_schema' ? this.conn    : this.lite;
    let tp = table !== '_schema' ? this.sqlType : SqlLt;
    let sc = table !== '_schema' ? sch          : (await this.lite.schema()).map(x => Object.assign(x, {read: true}));

    let vr, V, Q, S;
    V = Select.tearUp(table, as, vr = this.combineVars(table, vars));
    S = Select.create(table, as, V.Q);
    S = filt.reduce((S, f) => S.tableFilter(f.from, f.where, f.order), S);
    Q = await this.fillSelect(S, V.vars, meta, sc, dreq);

    let rsql = Q.sqlSnippet(tp);
    this.evSql({sql: rsql});
    let res = await cn.query( rsql ); 
    if ((dreq.opts||{}).schemaOnly) res.data = []; // with SQLite it fetches one row to know datatypes
    res.format = (fmt, extraVars = {}) => this.format(res, fmt, table, as, Object.assign({}, vars, extraVars), dreq, meta, sch);
    res.explainQuery = filled => (filled ? Q : S).describe();
    res.rowsTotal = async () => {
      if (dreq.opts.schemaOnly) return 0;
      let csql = Q.count().sqlSnippet(tp);
      this.evSql({sql: csql});
      return parseInt((await cn.query(csql)).data[0].count, 10);
    }
    return res;
  },
  runPost: async function (table, initVars, req, meta, sch, irows, dreq, files) {
    let me = this, results = [], affected = [], rules;
    files = files || {};

    let as, alias = this.alias.find(a => a.name === table);
    if (alias) {
      table = alias.table;
      as    = as || alias.name;
      initVars  = Object.assign({}, alias.vars, initVars, alias.const);
    }

    rules = this.wrules.filter(t => t.table === table);

    // TODO: same as in Select (let, sqlFn...) (`fill` for R and CUD?)
    let vars = this.combineVars(table, initVars);

    let arg = Object.assign({}, req,
    { tableAs: as
    , vars: vars
    , post: Object.keys(files).length === 0 ? irows : irows.map(r => Object.assign({}, r, Object.map(files, f => f.path)))
    });
    this.events.onPost.reduce((ev, fn) => fn(ev), arg);
    irows = arg.post; // f.path should be updaded file name in browser
    table  = (arg.req||{}).table || table;
    vars = arg.vars;
    // fmt = arg.req.format; // FIXME: fmt should be set somewhere else

    // find write rule to every input row
    // (irows * rules) where match => QValue (QInsert/QUpdate/QDelete etc)
    let rowRule = [], rowQ = [];
    for (let i = 0; i < irows.length; i++)
    for (let r = 0; r < rules.length; r++) {
      if (rowQ[i] = await rules[r].match(irows[i], sch, vars, me.sqlFn, meta, arg)) {
          rowRule[i] = rules[r];
          break;
       }
    }

    let db = await this.conn.transaction();

    // generate list of functions and execute them later in sync
    let jobLs = rowQ.map((Q,i) => async () => { // do queries and get returning rows
      if (!Q) return;         // remove where are no match (don't filter to have array indexes 1:1 rules to input rows)
      let R = rowRule[i];
      let ret;

      let travNew = f => f.table === 'new' ? jsToQVal(irows[i][f.field] || null) : f;

      let cmd, sqlGen = Q.sqlCommands(this.sqlType), lastAffected = 0;
      do {
        cmd = lastAffected === 0 ? sqlGen.next() : {done: true, value: null};
        if (cmd.value) {
          this.evSql({sql: cmd.value});
          affected.push( lastAffected = await db.exec(cmd.value) );
        }
        // console.log('Affected: ' + lastAffected  +'; cmd: ' + JSON.stringify(cmd));
      } while (cmd.value && !cmd.done);


      if (R.retWhere) {
        let V = Select.tearUp(R.table, null, vars);
        let S = Select.create(R.table, null, V.Q).tableFilter(R.table, R.retWhere);
        S = this.filt.reduce((S, f) => S.tableFilter(f.from, f.where, f.order), S);
        let s = (await this.fillSelect(S, V.vars, meta, sch, dreq)).travField(travNew);
        let sql = s.sqlSnippet(this.sqlType);
        this.evSql({sql: sql})
        results.push(ret = await db.query( sql ));
      }

      await Promise.all((Q.wrFields ? Q.wrFields() : []).map(field => {
        if (!files[field]) return;
        const meta = me.meta(table, field);                             if (!meta) return;
        const [dir,nameExpr] = [meta['upload-dir'], meta['upload-name']]; if (!dir || !nameExpr) return;
        let name = expression(nameExpr).travField(travNew).travField(f => f.table === 'ret' ? jsToQVal((ret.data[0]||{})[f.field] || null) : f).sqlSnippet(SqlJsPseudo);
        let toPath = path.join(dir, name);
        // console.log(dir, name, toPath);
        // util.promisify(fs.rename)(file[field].path, toPath);
        return new Promise((ok) => {
          fs.rename(files[field].path, toPath, () => {
            let evFn = me.events.onAfterUpload || [];
            let arg = Object.assign({}, dreq, { path: toPath });
            let callNr = 0, callIt = () => { if (callNr === evFn.length) ok(); else evFn(() => { callNr++; callIt() }, arg); }
            callIt();
          })
        })
      }));

    });
    for (let j = 0; j < jobLs.length; j++) await jobLs[j]();
    await db.commit();

    let r;
    // as result is list of queryResults but they must have same structure, combine these together
    return  Object.assign({ affected: affected }, 
      r  = results.length
      ? Object.assign({}, results[0], { data: results.reduce((R,r) => R.concat(r.data), []) })
      : this.emptyResult,
      { format: (fmt, extraVars) => this.format(r, fmt, table, as, Object.assign({}, vars, extraVars), dreq, meta, sch) });
  },

  format: async function (queryResult, format, table, tableAs, vars, req, meta, sch) {

    let F = await this.findTemplate(format);
    let me = this, result = {
      out: '',
      headers: [],
      text: () => {
        if (result.error) {
          me.evError(result.error);
          throw new Exception(503, result.error, { format: format, vars: vars, table: table });
        } else return result.out
      }
    }; // this is returned

    let evArg = Object.assign({}, req,
        { req: Object.assign({}, {table, tableAs, format, vars}, req)
        , vars: vars
        , auth: async () => { try { return await meta.auth(); } catch (e) { if (e instanceof NeedAuth) return null; else throw e; } }
        , newDb: this.templNewDb.bind(this)
        , query: async function (qTable, qVars) {
            let [qt,fmt] = breakOn(qTable, '.');
            let [t,as] = breakOn(qt, '@');
            let sqlRes = await me.runSelect(t, as, qVars, meta, sch, me.filt, evArg);
            return fmt ? (await sqlRes.format(fmt)).text() : sqlRes;
        }
        , post: (qTable, getVars, input) => me.runPost(qTable, getVars, evArg, meta, sch, input)
        , opts: o => req.opts[o] || null
        , parseQuery: async function (expr, table, vars) {
            let alias = me.alias.find(a => a.name === table);
            if (alias) {
              table = alias.table;
              // vars = Object.assign({}, alias.vars, vars, alias.const);
            }
            let V = Select.tearUp(table||'.', null, vars = Object.assign({select: expr}, vars));
            let Q = Select.create(table||'.', null, V.Q);
            Q = await me.fillSelect(Q, vars, meta, sch, Object.assign({}, evArg, { url: '' })); // URL is hack to use parseQuery in wsdl, ugly, shame on you, Kaiko :)
            let r = Q.select.ls[0].describe();
            return r;
        }
    })

    return new Promise((ok, bad) => {
      F.script.runInNewContext(Object.assign({}, evArg, F.ctx
        , Object.map(this.export, v => typeof v !== 'function' ? v : function () { return v.apply(null, [evArg].concat(Array.from(arguments))); } )
        , { console: console
        ,   result: Object.assign({}, queryResult, { format: async (f,a) => (await queryResult.format(f,a)).text() }) // here we want just output text, error are thrown
        ,   contentType: (t,c) => result.contentType = t
        ,   header     : (k,v) => result.headers.push({ key: k, value: v})
        ,   __success__: ok
        ,   __failure__: e => { console.error(e); result.error = e.toString(); bad(e); }
        ,   print:   x => { if (x !== undefined && x !== null) result.out += x; }
        ,   printLn: x => { result.out += x; }
        }));
    })
    .then(() => result).catch(e => { this.evError(e); result.error = e; return result; });
  },

  templNewDb: async function () {
    let conf = { read : '*', templates: this.tmplDirs, parsers: this.prsDirs, varsWithout$: this.varsWithout$, inlineStrings: this.inlineStrings };
    let a = await App({type: 'lt'}, conf);
    a.engines = this.engines;
    return Object.create(Object.prototype, {
      newTable: { value: async function (name, rows) {
        let names = [];
        rows.forEach(r => Object.keys(r).forEach(n => { if (!names.includes(n)) names.push(n) }));
        let db = await a.conn.transaction();
        await db.exec('CREATE TABLE ' + name + ' (' + names.join(', ') + ')'); // TODO: escaping and sutff
        let ins = 'INSERT INTO ' + name + '(';
        for (let r = 0; r < rows.length; r++)
          await db.exec(ins + Object.keys(rows[r]).join(',') + ') VALUES (' + Object.keys(rows[r]).map(k=>jsToQVal(rows[r][k]).sqlSnippet(SqlLt)).join(',') + ')');
        await db.commit();
        await a.updateSchema()
      } },
      query: { value: async function (tf, vars = {}) {
        if (!a._schema) await a.schema();
        if (!a._schema) throw new Exception(500, new Error());
        let [table, fmt] = breakOn(tf, '.');
        let [t, as] = breakOn(table, '@');
        const sch = await a.schema();
        let qRes = await a.runSelect(t, as, vars, a.meta, sch, a.filt, {});
        return !fmt ? qRes : (await a.format(qRes, fmt, table, as, vars, { isMain: false, isPost: false, vars: vars, url: tf}, meta, sch )).text();
      } }
    })
  },

  findTemplate: async function (fmt) {
    let ls = [], dirs = this.tmplDirs;
    let rd = util.promisify(fs.readdir);
    let rf = util.promisify(fs.readFile);

    // let's read all files
    for (let i = 0; i < dirs.length; i++) (await rd(dirs[i])).forEach(f => ls.push({ dir: dirs[i], file: f}));

    // now let's try if we find something for some engine
    let engines = Object.keys(this.engines);
    let engineInput = null;
    let engine = Object.keys(this.engines).find(eng => {
      let f = ls.find(({file}) => (fmt + '.' + this.engines[eng].ext) === file);
      if (f) engineInput = f;
      return f;
    })

    if (engine) 
      return {
        script: this.engines[engine].script
      , ctx   : { template: await rf(engineInput.dir + '/' + engineInput.file, 'utf8') }
      };

    // search for pure javascript
    let f = ls.find(({file}) => (fmt + '.js') === file);
    if (f) {
      let script = await rf(f.dir + '/' + f.file, 'utf8');
      return { ctx: {}, script: new VM.Script('(async function () { \n try { ' + script + '\n__success__(); } catch (E) { __failure__(E); } })()') };
    }

    throw new Exception(300, new Error(), { template: fmt, templateDirs: this.tmplDirs }) 
  },

  // vars from conf, user defined vars from url and constants (can't be overwritten)
  combineVars: function (table, vars) {
    let V = {};
    this.vars  .filter(v => wildcard(v.table, table)).forEach( tv => Object.assign(V, tv) ); // NOTE: "table" is ignored (not removed)
    Object.assign(V, vars);
    this.consts.filter(v => wildcard(v.table, table)).forEach( tv => Object.assign(V, tv) );
    delete V.table;
    return V
  },

  // TODO: this must be part of query
  fillSelect: async function (S, vars, meta, schema = null, dreq={opts:{}}) {

    // ?select=x&let.x=1 gives `SELECT 1 AS x`
    S = S.mapSelect(el => (el.token && vars['let.' + el.token]) ? el.toAs(el.token) : el);

    let lets = letReplacer(vars);

    S = S.travFunc (lets.func)
         .travToken(lets.vars)
         .travFunc (custFnReplacer(this.sqlFn, dreq));

    // seenFields is object where key is fieldname and value is object { name, as } where table name and `as` are present
    // seenFields is ordered in a way where first joins are more prioritized
    let tables = S.tables().map((t,i) => Object.assign(t, { prio: -i }));
    let tbIdx = tables.reverse().reduce((m,c) => m.set(c.name, c), new Map()); // may not be unique, keeps tables first in join order (reverse does that)
    // let asIdx = tables.          reduce((a,c) => Object.assign(a, {[c.as  ]: c}), {}); // unique
    let seenFields = schema
        .filter(r => r._ === 'field' && r.read && tbIdx.has(r.table)) // only fields in this query
        .map(r => { let T = tbIdx.get(r.table); return Object.assign({}, r, { as: T.as, prio: T.prio}); }); // simplified object
    let schTables = Set.fromArray(schema.filter(t => t._ === 'table').map(t => t.name));
    let specTables = Set.fromArray(['_empty','_schema','_single','_meta']);

    let missingTables = tables.map(t => t.name).filter(t => !specTables.has(t)).filter(t => !schTables.has(t));
    // if (missingTables.length) throw new Exception(302, new Error(), { table: missingTables.join(",") }); // TODO: alias

    // important to have this here, before sort
    if (!S.select) S = S.setSelect(seenFields.filter(f => !f.hide).map(f => (f.as || f.table) + '.' + f.name));

    seenFields.sort((a,b) => a.prio - b.prio);
    let seenFieldsIdx = seenFields.reduce((m,c) => m.set(c.name, c), new Map());

    if ((dreq.opts || {}).schemaOnly) {
      if (this.sqlType === SqlLt) {
        // With Sqlite there must be at least one row to fetch column names and types ("types")
        // This result is removed in runSelect
        S = S.emptyWhere().setLimit(1);
      } else  {
        S = S.setLimit(0);
      }
    }

    S = S.travToken(T => {
        let tb = seenFieldsIdx.get(T.token);
        if (tb) {
            return T.toField(tb.as);
        } else if (this.varsWithout$ && typeof vars[T.token] !== 'undefined') {
            return jsToQVal(vars[T.token]);
        } else if (this.inlineStrings) {
          return T.toString();
        } else throw ("Unknown token: " + T.token);
    });

    S = S.travVar(V => {
      if (vars.hasOwnProperty(V.v)) return jsToQVal(vars[V.v]);
      else if (dreq.opts.schemaOnly) return V.toNull();
      else if (!this.varsWithout$) throw ("Variable missing: " + V.v);
    })

    // test for protected fields;
    let needAuth = false, protFields = Set.fromArray(seenFields.filter(e => e.prot).map(e => e.table +'.'+e.name));
    S = S.travField(f => { if (protFields.has(f.table +'.' + f.field)) needAuth = true; return f; });
    if (needAuth && !(await meta.auth())) throw new NeedAuth();

    let metas = {};
    S = await S.travFieldA(async f => {
        if (!metas[f.table] && meta[f.table]) metas[f.table] = await meta[f.table]();
        let T = metas[f.table];
        if (!T && f.table ==='auth') throw new NeedAuth();
        if (!T) return f;
        return jsToQVal(T[f.field] || null);
    });

    return S;
  },

  updateSchema: async function () {
    await this.block;
    this.block = new Promise( ub => this.unblock = ub ) ;
    this._schema = null;
    await this.schema();
    this.unblock();
    return true;
  },

  setSqlFn: function (sqlFn) {
    this.sqlFn = sqlFn;
  },
  setExport: function (exp) {
    this.export = exp;
  },
  addEngine: async function (ext, filepath) {
    try {
      this.engines[ext] = { ext: ext, filepath: filepath, script: await util.promisify(fs.readFile)(filepath, 'utf8') };
    } catch (e) { 
      this.evError(e);
      throw new Exception(501, e, { ext: ext, filepath: filepath });
    }
    try {
      this.engines[ext].script = new VM.Script('(async function () { \n try { ' + this.engines[ext].script + '\n__success__(); } catch (E) { __failure__(E); } })()')
    } catch (e) {
      this.evError(e);
      throw new Exception(502, e, { ext: ext, filepath: filepath });
    }
  },
  addPipe: function (ext, fn) {
    this.pipes[ext] = fn;
  },
  schema: async function () {
    if (!this._schema) {
      await this.conn.learn();
      let dbSch = await this.conn.schema();
      let sch = [];
      let rTables = new Set();
      let wTables = new Set();

      // add rights
      dbSch.slice().reverse().forEach(item => {
        if (item._ === 'field') {
          let appItem = Object.assign({}, item);
          appItem.read  = this.read .match(item.table, item.name);
          appItem.write = this.write.match(item.table, item.name);
          appItem.hide  = this.hide .match(item.table, item.name);
          appItem.prot  = this.prot .match(item.table, item.name) || 0;

          if (appItem.read ) rTables.add(item.table);
          if (appItem.write) wTables.add(item.table);

          if (appItem.read || appItem.write) sch.push(appItem);
        }
        else if (item._ === 'table' && (rTables.has(item.name) || wTables.has(item.name))) {
          let appItem = Object.assign({}, item);
          if (rTables.has(item.name)) appItem.read  = true;
          if (wTables.has(item.name)) appItem.write = true;
          sch.push(appItem);
        }
      });
      this._schema = sch.reverse();

      let lt = await this.lite.conn;
      await lt.exec('BEGIN');
      await lt.exec('DROP TABLE IF EXISTS _schema');
      await lt.exec('CREATE TABLE _schema (name varchar, tablename varchar, rawtype varchar, gentype varchar, comment varchar, read boolean, write boolean, protect boolean)');
      let schIns = await lt.prepare('INSERT INTO _schema VALUES (?, ?, ?, ?, ?, ?, ?, ?)');

      await lt.exec("DROP TABLE IF EXISTS _meta");
      await lt.exec("CREATE TABLE _meta (tablename varchar, field varchar, key varchar, value varchar, type varchar)");
      let metaIns = await lt.prepare("INSERT INTO _meta VALUES (?, ?, ?, ?, ?)");

      for (let i = 0; i < sch.length; i++) {
        let r, item = sch[i];
        if ('table' === item._) r = [ item.name, item.name, 'table', 'table',  item.comment, item.read, item.write, item.prot ];
        if ('field' === item._) r = [ item.name, item.table, item.type, item.genType, item.comment, item.read, item.write, item.prot ]; 
        // if ('field' === item._) r = []; 
        await schIns.run(r);
      }
      await lt.exec('COMMIT');

      // TODO meta
    }
    return this._schema;
  }
}

async function inputRows(dirs, fmt, body) {
  let rd = util.promisify(fs.readdir);
  let rf = util.promisify(fs.readFile);
  let ls = [];
  for (let i = 0; i < dirs.length; i++) (await rd(dirs[i])).forEach(f => ls.push({ dir: dirs[i], file: f}));
  
  let f = ls.find(({file}) => (fmt + '.js') === file);
  if (!f) return { error: "Parser not found (" + fmt + ")" };

  let irows;
  let scriptCode = await rf(f.dir + '/' + f.file, 'utf8');
  let script = new VM.Script('(async function () { try { ' + scriptCode + '\n__success__(); } catch (E) { __failure__(E); } })()');
  return new Promise((ok, bad) => 
    script.runInNewContext({ input: body, console: console, result: r => irows = r, __success__: ok, __failure__: e => { console.error(e); throw e; bad(e); } })
  ).then(() => {
      if (!Array.isArray(irows)) throw "Input rows must be array of objects";
      irows.forEach(i => { if (typeof i !== 'object' || !i) throw "Input rows must be array of objects"; } );
      return irows; 
  }).catch(e => {
    console.error(e);
    this.evError(e);
  });
}


module.exports = App;

// used to throw new NeedAuth()
function NeedAuth() { }



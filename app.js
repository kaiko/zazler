
const VM   = require('vm');
const md5  = require('md5');
const util = require('util');
const path = require('path');
const fs   = require('fs');

const wildcard = require('wildcard');

const {DbConn,urlToConnection} = require('./db');
const AccessRule = require('./access').AccessRule;
const { Select, Filters, WriteRule, jsToQVal, expression } = require('./query')


const AsyncFunction = (async () => {}).constructor;

Object.map = (o, fn) => Object.keys(o).map((k, i) => fn(k, o[k], i));
Object.onValues = (o, fn) => {
  let K = Object.keys(o);
  return K.map((k, i) => fn(o[k], k, i)).reduce((O, o, i) => Object.assign(O, { [ K[i] ]: o }), {});
}
Object.onValuesA = async (o, fn) => {
  let K = Object.keys(o);
  return (await Promise.all(K.map((k, i) => fn(o[k], k, i)))).reduce((O, o, i) => Object.assign(O, { [ K[i] ]: o }), {});
}

Set.fromArray = a => a.reduce((s, e) => s.add(e), new Set());

btoa = s => new Buffer(s, 'base64').toString('binary');
getBody = req => new Promise(ok => { let b = ''; req.on('data', c => b += c); req.on('end', () => ok(b)) });
trace = (v, mark) => { console.log(mark ? mark + '\n' + v + '\n/' + mark : v); return v; }
breakOn = (str, on) => {
  let pos = str.indexOf(on);
  return pos === -1 ? [str] : [str.substr(0,pos), str.substr(pos+1) ];
}

/////////////

class App {
  constructor(conn, conf = null) {
    if (typeof conn === 'string') conn = urlToConnection(conn);
    this.dbName = undefined;
    this.dbSchema = null;
    this.type = conn.type;
    this.sqlType = ({pg: SqlPg, lt: SqlLt, my: SqlMy})[conn.type];
    this.dbName = conf.dbName;

    // if (!conn.internal) // just small speedup for some cases
    this.lite = new DbConn({ filename: ':memory:', type: 'lt' });
    this.conn = new DbConn(conn);
    this.driverName = conn.type; // DriverName :: !String

    this.engines  = {};

    // You can get these from connection but I wanted to have these values available without using any connection


    // this.events = {}; // ![(CtrlEvent, JSVal {- [Value] -> IO Value -} )]
    this.export = {};
    this.sqlFn  = {};
    this.pipes  = {};

    this.expressRequest = this.expressRequest.bind(this);
    this.format    = this.format.bind(this);
    this.runSelect = this.runSelect.bind(this);

    this.emptyResult = { data: [], cols: [], types: [], rawTypes: [], rowsTotal: async () => { } }

    this.events = { onWebRequest: [], onPost: [], onAfterUpload: [],  onSql: [], onError: [] /*, onAfterPost: [], onUpload: [], onEvGet: [] */ };
    this.eventMap = {
        "web-request" : "onWebRequest"
      , "data-post"   : "onPost"
      , "upload-after": "onAfterUpload"
      , "error"       : "onError"
      , "sql"         : "onSql"
    }

    this.logAccess = () => {}
    if (conf.logAccess) fs.open(conf.logAccess, 'a', (err, f) => this.logAccess = m => fs.write(f, m.url + "\n", () =>{} ))
    if (conf.logSql   ) fs.open(conf.logSql   , 'a', (err, f) => this.on('sql'  , e => fs.write(f, '[' + new Date().toString() + '] ' + e.sql + "\n", () => {} )));
    if (conf.logError ) fs.open(conf.logError , 'a', (err, f) => this.on('error', e => fs.write(f, '[' + new Date().toString() + '] ' + e.toString()  +'\n' , () => {} )));

    if (conf) this.setConf(conf);

    this.block = Promise.resolve(true);
  }

  evSql(e)   { this.events.onSql.forEach(fn => fn(e)); }
  evError(e) { this.events.onError.forEach(fn => fn(e)); }

  setConf(conf) {
    this.index = conf.index || '_schema.json'; // TODO: dashboard
    
    this.tmplDirs = [... (conf.templates || []), __dirname + '/templates/'];
    this.prsDirs  = [... (conf.parsers   || []), __dirname + '/parsers/'  ];

    this.read  = new AccessRule(conf.read);
    this.write = new AccessRule(conf.write);
    this.hide  = new AccessRule(conf.hide);
    this.prot  = new AccessRule(conf.prot || conf.protect);

    this.wrules = (conf["write-rules"] || []).map(r => new WriteRule(r));

    if (!conf.auth) this.auth = null;
    else {
      let A = conf.auth;
      if (!conf.auth.table) throw "Auth must have `table` property";
      this.auth = Select.tearUp(conf.auth.table, null, conf.auth);
      if      (A.location) { this.auth.type = 'location'; this.auth.location = A.location; }
      else if (A.content)  { this.auth.type = 'content';  this.auth.content  = A.content; this.auth.contentVars = conf.auth['content-vars'] || {} }
      else if (A.realm  )  { this.auth.type = 'basic';    this.auth.realm = A.realm; }
      else throw "auth must have `realm`, `content` or `location` parameter";
      if (!this.auth.Q.select) throw "auth must have `select` option. NB! everything you put into select can be accessed in the web after authentication";
    }

    this.filt   = (conf.filter||conf.filters||[]).map(f => Select.tearUp(f.table, null, f).Q );
    this.vars   = conf.vars || [];
    this.consts = conf.const || conf.consts || [];

    this.alias = conf.alias || []; // Alias  :: ![QueryAlias]

    this.metaF = {};
    this.metaT = {};
    Object.map(conf.meta || {}, (k, v) => {
      let [t,f] = breakOn(k, '.');
      if (!f && !this.metaT[t]) this.metaT[t] = {};
      if (f) {
        if (!this.metaF[t])    this.metaF[t]    = {};
        if (!this.metaF[t][f]) this.metaF[t][f] = {};
      }
      if (!f) this.metaT[t]    = v;
      if ( f) this.metaF[t][f] = v;
    });

    this.varsWithout$  = conf.hasOwnProperty('varsWithout$' ) ? !!conf.varsWithout$  : false;
    this.inlineStrings = conf.hasOwnProperty('inlineStrings') ? !!conf.inlineStrings : false;
  }

  meta(t, f = null) { return f ? (this.metaF[t]||{})[f] || null : this.metaT[t] || null; }

  async query(tableAs, vars, user, pass) {

    let [tq, fmt] = breakOn(tableAs, '.');
    let [table, as] = breakOn(tq, '@');
    let R = { table, as, format: null, user: null, pass: null, req: { user: null, pass: null, pipe: null, format: null, isMain: true, url: null, isPost: false }, vars: vars, meta: {}, cookie: {} };
    let me = this;

    let _auth = null;
    R.meta = {
      cookie: async () => { return {} }
    , req:    async () => { return Object.onValues(R.req, jsToQVal) }
    , auth:   async () => {
        if (!_auth) {
            if (!me.auth) throw "Authentication needed but not configured!";
            let unprotectedSchema = (await this.schema()).map(f => Object.assign({}, f, {prot: false}));
            _auth = (await me.runSelect(me.auth.Q.from.table, null, me.auth.Q, Object.assign({}, R.meta, { auth: async () => { throw "Auth needed in auth" } }), unprotectedSchema, R)).data[0] || null;
        }
        if (!_auth) throw new NeedAuth();
        return _auth;
      }
    }
    
    let sqlResult;
    try {
        sqlResult = await this.runSelect(R.table, R.as, R.vars, R.meta, await this.schema(), R.req);
        if (fmt) sqlResult = sqlResult.format(fmt);
    } catch (someErr) {
      if (someErr instanceof NeedAuth) { // this is not error, it is to control the flow
        throw "Unauthorized";
      } else {
        this.evError(someErr);
        throw someErr;
      }
    }

    return sqlResult;
  }

  async expressRequest(req, res, next) {
    req.setEncoding('utf8');

    this.logAccess(req);

    await this.block; // wait till schema is learned at the beginning

    let R = { req: { user: null, pass: null, pipe: null, format: null, isMain: true, url: req.originalUrl, isPost: req.method === 'POST' }, vars: {}, meta: {}, cookie: {} };
    let me = this;
    let [_, search] = breakOn(req.originalUrl, '?');
    [R.table, R.format] = breakOn(req.path.split('/').pop(), '.');
    if (!R.table) [R.table, R.format] = breakOn(this.index, '.');
    [R.table, R.as] = breakOn(R.table, '@');

    if (!R.format) {
      res.write('Format missing');
      res.end();
      return;
    }

    // test for pipe
    R.pipe = R.req.pipe = Object.keys(this.pipes).find(p => R.format.substr(-(p.length+1)) === "." + p);
    if (R.pipe) R.format = R.format.substr(0, R.format.length - R.pipe.length - 1); // cut off pipe part (".pdf" for example)

    if (this.auth && this.auth.type === 'basic')
      (([u,p]) => { if (u) { R.user = u; R.pass = p; } })( breakOn( btoa( ((req.get('Authorization') || '').match(/^Basic (.*)/) || [])[1] || ''), ':' ) );
    R.req.user = R.user; // TODO: bad solution (DReq must resolve this)
    R.req.pass = R.pass;

    R.vars =
      (search||'').split('&').filter(Boolean)
      .map(a => { let [k,v] = breakOn(a,'='); return { key: decodeURIComponent(k), val: decodeURIComponent(v)}; })
      .reduce((a,c) => Object.assign(a,{[c.key]: c.val}),{});

    if (this.dbName) R.req.dbName = this.dbName;
    R.req.format = R.format;

    if (R.format === 'soap') {
      R.req.isPost = false;
      let x = require('xml-js').xml2js(await getBody(req));
      try {
        let h = x.elements[0].elements[0];
        let b = x.elements[0].elements[1];
        let r = b.elements[0]; // request 
        R.vars.soapHeader = require('xml-js').js2xml(h);
        let soapVars = r.elements.reduce((a,c) => Object.assign(a, {[c.name]: c.elements[0].text}), {});
        Object.assign(R.vars);
      } catch (e) {
        this.evError(e);
        console.log('Unexpected SOAP structure (' + e + ')');
      }
    }

    let _auth = null, _cookie = null, _req = null;
    R.meta = {
      cookie: async () => { if (!_cookie) _cookie = Object.onValues(req.cookies||{}, jsToQVal); return _cookie; }
    , req:    async () => { return Object.onValues(R.req, jsToQVal) }
    , auth:   async () => {
        if (!_auth) {
            if (!me.auth) throw "Authentication needed but not configured!";
            let unprotectedSchema = (await this.schema()).map(f => Object.assign({}, f, {prot: false}));
            _auth = (await me.runSelect(me.auth.Q.from.table, null, me.auth.Q, Object.assign({}, R.meta, { auth: async () => { return {} } }), unprotectedSchema, {})).data[0] || null;
        }
        if (!_auth) throw new NeedAuth();
        return _auth;
      }
    }

    let arg = { vars: R.vars, table: R.table
      , cookie: req.cookies || {}
      , req: R.req
      , query: async function (qTable, qVars) { // FIXME: this is double in format
          let [qt,f] = breakOn(qTable, '.');
          let [t,as] = breakOn(qt, '@');
          let sch    = await me.schema();
          let sqlRes = await me.runSelect(t, as, qVars, R.meta, sch);
          sqlRes.format = async (qFmt, eVars = {}) => {
              let R;
              try {
                R = await me.format(sqlRes, qFmt, qTable, Object.assign(qVars, eVars), arg, R.meta, sch);
                if (R.error) throw R.error;
              } catch (SF) {
                this.evError(SF);
                throw SF.toString();
              }
              return R.out;
          }
          return f ? await sqlRes.format(f) : sqlRes;
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

    if (req.method === 'POST') {
      if ((req.headers['content-type']||'').toLowerCase().indexOf('multipart/form-data;') === 0) {
        if (!req.body) {
          console.log([
            'Use express-form-data like this: '
           ,"   const app   = require('express')();"
           ,"   const forms = require('express-form-data');"
           ,"   app.use(forms.parse({ autoFiles: true }));"
           ,"   app.use(forms.format());"
           ,"   app.use(forms.stream());"
           ,"   app.use(forms.union());"
          ].join("\n"));
          throw "Fix forms";
        }
        R.post  = [Object.keys(req.body).filter(k => typeof req.body[k] === 'string').reduce((r, k) => Object.assign(r, {[k]: req.body[k]}), {})];
        R.files =  Object.keys(req.body).filter(k => typeof req.body[k] === 'object').reduce((r, k) => Object.assign(r, {[k]: req.body[k]}), {});
      } else {
        R.post = await inputRows(this.prsDirs, R.format, await getBody(req));
      }
    }

    let fmtResult = {}, sqlResult;
    try {
        sqlResult = R.req.isPost
           ? await this.runPost  (R.table, R.vars, R.req, R.meta, await this.schema(), R.post, R, R.files)
           : await this.runSelect(R.table, R.as, R.vars, R.meta, await this.schema(), {...R.req, ...{ cookie: req.cookies} } );
        fmtResult = await this.format(sqlResult, R.format, R.table, Object.assign({}, R.req.vars, R.vars), Object.assign({}, {...R.req, ...{ cookie: req.cookies}}, { vars: R.vars}), R.meta, await this.schema() );
    } catch (someErr) {
      if (someErr instanceof NeedAuth) {
        fmtResult.unAuthorized = true;
      } else {
        this.evError(someErr);
        fmtResult = {error : someErr};
      }
    }

    if (fmtResult.unAuthorized) switch(this.auth.type) {
      case 'basic': res.status(401).setHeader('WWW-Authenticate', 'Basic realm="' + this.auth.realm + '"'); res.send('Unauthorized'); res.end(); return; break;
      case 'location': res.status(307).location(this.auth.location); res.end(); return; break;
      case 'content': {
        let [t,f] = breakOn(auth.content, '.');
        let m  = Object.assing({}, R.meta.req,{ isMain: false, vars: Object.assign({}, R.vars, auth.contentVars||{}) });
        try {
          sqlResult = await this.runQuery(t, Object.assign({}, vars, this.auth.contentVars || {}), R.meta, await this.schema());
          fmtResult = await this.format(sqlResult, f, t, vars, m, R.meta, await this.schema());
        } catch (someE) {
          if (someE instanceof NeedAuth) { throw "auth content wanted to auth again ... look out ;)" }
          this.evError(someE);
          fmtResult.error = someE;
        }
      }
    }

    if (fmtResult.error) {
      res.status(500).type('text').send(fmtResult.error);
    } else {
      if (fmtResult.contentType) res.append('Content-Type', fmtResult.contentType);
      R.pipe
        ? res.send(await (this.pipes[R.pipe])(fmtResult.out, t => res.type(t),Object.assign({}, {...R.req, ...{ cookie: req.cookies}}, { vars: R.vars})))
        : res.send(fmtResult.out);
    }
    res.end();
  }

  on(ev, fn) {
    if (!this.eventMap[ev]) throw "Event `" + ev + '` is unknown (allowed: ' + Object.keys(this.eventMap).join(', ') + ")";
    this.events[this.eventMap[ev]].push(fn);
  }

   // yeah, schema is in 'this' but on auth there is modified schema used 
  async runSelect(table, as, vars, meta, sch, dreq = {}) {
    if (!meta) meta = this.meta;

    if (table === '_empty' ) return this.emptyResult;
    if (table === '_single') return await this.lite.query('SELECT NULL'); // TODO: run on main database

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

    let vr, V, Q;
    V = Select.tearUp(table, as, vr = this.combineVars(table, vars));
    Q = await this.fillSelect(Select.create(table, as, V.Q), V.vars, meta, sc, dreq);

    let rsql = Q.sqlSnippet(tp);
    this.evSql({sql: rsql});
    let res = await cn.query( rsql ); 
    res.rowsTotal = async () => {
      let csql = Q.count().sqlSnippet(tp);
      this.evSql({sql: csql});
      return parseInt((await cn.query(csql)).data[0].count, 10);
    }
    res.format = async (format, extraVars = {}) => {
        let R = await this.format(res, format, table, Object.assign({}, vars, extraVars), dreq, meta, sch);
        if (R.error) throw R.error;
        return R.out;
    }
    return res;
  }
  async runPost(table, initVars, req, meta, sch, irows, dreq, files) {
    let me = this, results = [], rules;
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

    let arg = Object.assign({}, req, {
      vars: vars,
      post: Object.keys(files).length === 0 ? irows : irows.map(r => Object.assign({}, r, Object.onValues(files, f => f.path)))
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

    await Promise.all(rowQ.map(async (Q,i) => { // do queries and get returning rows
      if (!Q) return;         // remove where are no match (don't filter to have array indexes 1:1 rules to input rows)
      let R = rowRule[i];
      let ret;

      let travNew = f => f.table === 'new' ? jsToQVal(irows[i][f.field] || null) : f;

      let cmd, affected, sqlGen = Q.sqlCommands(this.sqlType);
      do {
        cmd = sqlGen.next(affected);
        if (cmd.value) affected = await db.exec(cmd.value);
      } while (cmd.value && !cmd.done);


      if (R.retWhere) {
        let V = Select.tearUp(R.table, null, vars);
        let s = (await this.fillSelect(Select.create(R.table, null, V.Q).tableFilter(R.table, R.retWhere), V.vars, meta, sch, dreq)).travField(travNew);
        let sql = s.sqlSnippet(this.sqlType);
        this.evSql({sql: sql})
        results.push(ret = await db.query( sql ));
      }

      await Promise.all((Q.wrFields ? Q.wrFields() : []).map(field => {
        // console.log('field', field, files[field]);
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

    }));

    await db.commit();

    // as result is list of queryResults but they must have same structure, combine these together
    return  results.length
      ? Object.assign({}, results[0], { data: results.reduce((R,r) => R.concat(r.data), []) })
      : this.emptyResult
  }

  async format(queryResult, format, table, vars, req, meta, sch) {
    let F = await this.findTemplate(format);
    if (F.error) { throw F.error; }
    var result = {out: ''}; // this is returned
    var me = this;

    let evArg = Object.assign(req,
        { req: Object.assign({table, format, vars}, req)
        , vars: vars
        , auth: async () => { try { return await meta.auth(); } catch (e) { if (e instanceof NeedAuth) return null; else throw e; } }
        , newDb: this.templNewDb.bind(this)
        , query: async function (qTable, qVars) {
            let [qt,fmt] = breakOn(qTable, '.');
            let [t,as] = breakOn(qt, '@');
            let sqlRes = await me.runSelect(t, as, qVars, meta, sch, evArg);
            sqlRes.format = async function (qFmt, eVars = {}) {
                let R = await me.format(sqlRes, qFmt, qTable, Object.assign({}, qVars, eVars), evArg, meta, sch);
                if (R.error) throw R.error;
                return R.out;
            }
            return fmt ? await sqlRes.format(fmt) : sqlRes;
        }
        , post: (qTable, getVars, input) => me.runPost(qTable, getVars, evArg, meta, sch, input)
        , opts: o => req.opts[o] || null // TODO
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

    queryResult.format = async (qFmt, evars= {}) => {
      let R = await me.format(queryResult, qFmt, table, Object.assign({}, vars, evars), Object.assign({}, evArg, {isMain: false}), meta, sch); // TODO: this isMain is false here, I guess
      if (R.error) throw R.error;
      return R.out;
    }

    return new Promise((ok, bad) => {
      F.script.runInNewContext(Object.assign({}, evArg, F.ctx
        , Object.onValues(this.export, v => typeof v !== 'function' ? v : function () { return v.apply(null, [evArg].concat(Array.from(arguments))); } )
        , { console: console
        ,   result: queryResult
        ,   contentType: (t,c) => result.contentType = t
        ,   __success__: ok
        ,   __failure__: e => { console.error(e); result.error = e.toString(); bad(e); }
        ,   print:   x => { if (x !== undefined && x !== null) result.out += x; }
        ,   printLn: x => { result.out += x; }
        }));
    })
    .then(() => { return result }).catch(e => { this.evError(e); result.error = e.toString(); return result; });
  }

  templNewDb() {
    let conf = { read : '*', templates: this.tmplDirs, parsers: this.prsDirs, varsWithout$: this.varsWithout$, inlineStrings: this.inlineStrings };
    let a = new App({type: 'lt'}, conf);
    a.engines = this.engines;
    return Object.create({}, {
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
        if (!a._schema) throw "Running query on empty database (or didn't you `await` for newTable)";
        let [table, fmt] = breakOn(tf, '.');
        let [t, as] = breakOn(table, '@');
        let qRes = await a.runSelect(t, as, vars);
        return !fmt ? qRes : await a.format(qRes, fmt, table, vars, { isMain: false, isPost: false, vars: vars, url: tf}, meta, await a.schema() );
      } }
    })
  }

  async findTemplate(fmt) {
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

    return { error: 'Format not found (' + fmt + ')' };
  }

  // vars from conf, user defined vars from url and constants (can't be overwritten)
  combineVars(table, vars) {
    let V = {};
    this.vars  .filter(v => wildcard(v.table, table)).forEach( tv => Object.assign(V, tv) ); // NOTE: "table" is ignored (not removed)
    Object.assign(V, vars);
    this.consts.filter(v => wildcard(v.table, table)).forEach( tv => Object.assign(V, tv) );
    delete V.table;
    return V
  }

  async fillSelect(S, vars, meta, schema = null, dreq={}) {
    if (!schema) schema = await this.schema();
    if (!meta) meta = this.meta;

    S = this.filt.reduce((S, f) => S.tableFilter(f.from, f.where, f.order), S);

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

    // important to have this here, before sort
    if (!S.select) S = S.setSelect(seenFields.map(f => f.as + '.' + f.name));

    seenFields.sort((a,b) => a.prio - b.prio);
    let seenFieldsIdx = seenFields.reduce((m,c) => m.set(c.name, c), new Map());

    let dumbSql = (dreq.url || '').match(/\.wsdl$/);
    if (dumbSql) S = S.setLimit(0)

    S = S.travVar(V => {
      if (typeof vars[V.v] !== 'undefined') return jsToQVal(vars[V.v]);
      else if (dumbSql) return V.toNull();
      else if (!this.varsWithout$) throw ("Variable missing: " + V.v);
    })
    S = S.travToken(T => {
        let tb = seenFieldsIdx.get(T.token);
        if (tb) 
            return T.toField(tb.as)
        else if (this.varsWithout$ && typeof vars[T.token] !== 'undefined')
            return jsToQVal(vars[T.token]);
        else if (dumbSql)
          return T.toNull(); // hack
        else if (this.inlineStrings)
          return T.toString();
        else throw ("Unknown token: " + T.token);
    });

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
  }

  async updateSchema() {
    await this.block;
    this.block = new Promise( ub => this.unblock = ub ) ;
    this._schema = null;
    await this.schema();
    this.unblock();
    return true;
  }

  setSqlFn (sqlFn) {
    this.sqlFn = sqlFn;
  }
  setExport (exp) {
    this.export = exp;
  }
  async addEngine(ext, filepath) {
    try {
      this.engines[ext] = { ext: ext, filepath: filepath, script: await util.promisify(fs.readFile)(filepath, 'utf8') };
      this.engines[ext].script = new VM.Script('(async function () { \n try { ' + this.engines[ext].script + '\n__success__(); } catch (E) { __failure__(E); } })()')
    } catch (e) {
      this.evError(e);
      throw e;
    }
  }
  addPipe(ext, fn) {
    this.pipes[ext] = fn;
  }
  async updSchema() {
    return await this.conn.schema();
  }
  async schema() {
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
    this.logError(e);
  });
}


module.exports = async (con,conf) => {
  let a = new App(con, conf);
  await a.updateSchema();
  return a;
}

// used to throw new NeedAuth()
function NeedAuth() { }



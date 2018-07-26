const url = require('url');
const fs = require('fs');
const Lite = require('sqlite'); //sqlite is wrapper for sqlite3
const Lite3 = require('sqlite3');
const { breakOn, zipObject, uriArgs, parseBool } = require('./toolbox.js');

trace = x => { console.log(x); return x; }

// flatToTree makes keys containing dots to object
// {a: 1, "b.c": 2} ==> { a:1, b: { c: 2 } }
flatToTree = (o, subOn = '.') => {
  let root = {}; // this is object where we copy values
  let filler = (cur, ks, level) => // recursive function to create subjects and fill results
     level === ks.length - 1 // check if last step, no subobjects
        ?         cur[ks[level]] = o[ks.join(subOn)] // fill with value, take it from main object
        : filler( cur[ks[level]] = cur[ks[level]] || {}, ks, level + 1); // create object if needed and continue with filling
  Object.keys(o).forEach(key => filler(root,key.split(subOn), 0));
  return root;
}

// so, these functions with Object.mapDeep helps will make strings to integer where only numbers are present
// for example poolMax=1 in URL is treated as string but drivers are waiting integer
// it's a bit hack but it's better than check all arguments manually
parseIntSoft = str => typeof str === 'string' && str.match(/-?[0-9]/) ? parseInt(str, 10) : str;
mapInt = o => Object.mapDeep(o, parseIntSoft);

sslFiles = c => {
  if (c.ssl && typeof c.ssl === 'object') {
    return Object.assign({}, c, { ssl: Object.assign({} // return new object, don't change old (just nice)
    , !c.ssl.ca   ? {} : { ca  : fs.readFileSync(c.ssl.ca  ) } // only if there is this key, add this to ssl object
    , !c.ssl.key  ? {} : { key : fs.readFileSync(c.ssl.key ) }
    , !c.ssl.cert ? {} : { cert: fs.readFileSync(c.ssl.cert) }
    )})
  } else return c;
}

function urlToConnection(dbUrl) {
  let u = url.parse(dbUrl);
  if (['postgresql:','pg:','psql:'].includes(u.protocol)) {
    return Object.assign({},
      sslFiles(mapInt(flatToTree(uriArgs(u.query)))),
      { type: 'pg'
      , port: parseInt(u.port || 5432) 
      , host: u.hostname
      , database: u.pathname ? u.pathname.split('/')[1] : ''
      },
      !u.auth ? {} : (([u,p]) => ({user: u, password: p}))(breakOn(u.auth, ':')));
  } else
  if (['mysql:','my:'].includes(u.protocol)) {
    return Object.assign({},
      sslFiles(mapInt(flatToTree(uriArgs(u.query)))),
      { type: 'my'
      , port: parseInt(u.port || 3306) 
      , host: u.hostname
      , database: u.pathname ? u.pathname.split('/')[1] : ''
      },
      !u.auth ? {} : (([u,p]) => ({user: u, password: p}))(breakOn(u.auth, ':')));
  } else
  if (['file:','sqlite:','sqlite3:'].includes(u.protocol)) {
    return Object.assign({}, mapInt(flatToTree(uriArgs(u.query))), { type: 'lt' , filename: u.pathname });
  } else
  if (['db2:'].includes(u.protocol)) {
    let c = Object.assign({},
    mapInt(flatToTree(uriArgs(u.query))),
    { type: 'db2'
    , port: parseInt(u.port || 6000) 
    , host: u.hostname
    , database: u.pathname ? u.pathname.split('/')[1] : ''
    },
    !u.auth ? {} : (([u,p]) => ({user: u, password: p}))(breakOn(u.auth, ':')));

    c.connectString = `DATABASE=${c.database};HOSTNAME=${c.host};UID=${c.user};PWD=${c.password};PORT=${c.port};PROTOCOL=TCPIP`;

    return c;
  } else
  if (['oracle:'].includes(u.protocol)) {
    let c = Object.assign({},
      mapInt(flatToTree(uriArgs(u.query))),
      { type: 'or'
      , port: parseInt(u.port || 1521) 
      , host: u.hostname
      , database: u.pathname ? u.pathname.split('/')[1] : ''
      },
      !u.auth ? {} : (([u,p]) => ({user: u, password: p}))(breakOn(u.auth, ':')));

    c.connectString = c.host + ':' + c.port + '/' + c.database;

    return c;
  }
  else throw "Unknown protocol: " + u.protocol;
  return u;
}



async function DbConn(props) {
  let c;
  switch (props.type.toLowerCase()) {
  case 'psql':
  case 'postgresql':
  case 'postgres':
  case 'pg': c = await DbConnPg(props); break;
  case 'maria':
  case 'mariadb':
  case 'mysql':
  case 'my': c = await DbConnMy(props); break;
  case 'file':
  case 'sqlite':
  case 'lt': c = await DbConnLt(props); break;
  case 'or': c = await DbConnOr(props); break;
  case 'db2': c = await DbConnDb2(props); break;
  default: throw new Error("Connection props must have `type` property with value 'sqlite', 'pg', 'mysql' or 'or' (for Oracle)");
  }
  return c;
}

async function DbConnPg(props) {
  const pg = require('pg');

  // numbers must be handled as numbers, not strings
  pg.types.setTypeParser(20, parseInt);
  pg.types.setTypeParser(21, parseInt);
  pg.types.setTypeParser(23, parseInt);
  pg.types.setTypeParser(26, parseInt);
  pg.types.setTypeParser(701, parseFloat);
  pg.types.setTypeParser(700, parseFloat);

  return Object.create(DbConnPg.Proto, { pg: { value: pg }, props: { value: props }, pool: { value: new pg.Pool(props) }, _schema: { value: null, enumerable: true, writable: true } });
}
DbConnPg.Proto = {
  // end:   async function () { return null; /* await this.pool.end() */ },
  query: async function (q, args = []) {
    const client = await this.pool.connect();
    let res = this.runQ(q, args, client);
    client.release();
    return res;
  },
  runQ: async function (q, args = [], client) {
    const R = await client.query(q, args);
    let r = R.rows;
    let o = R.fields.map(f => f.name);
    let t = R.fields.map(f => {
      if (!DbConnPg.types[f.dataTypeID]) {
        console.warn('Warning: no DbConnPg.type ' + f.dataTypeID);
        return ['Unsupported (' + f.dataTypeID + ')', 'str'];
      }
      return DbConnPg.types[f.dataTypeID];
    })
    return { data: r, cols: o, types: t.map(([_,t]) => t), rawTypes: t.map(([t]) => t) };
  },
  transaction: async function () {
    const client = await this.pool.connect();
    let q = this.query.bind(this);
    await client.query('BEGIN');
    return {
      commit: async () => { await client.query('COMMIT'); client.release(); },
      query:  (sql,args = []) => this.runQ(sql, args, client),
      exec:   async (sql, args = []) => {
        try {
          let { rowCount } = await client.query(sql, args);
          return rowCount;
        } catch (SQLE) {
          await client.query('ROLLBACK');
          client.release();
          throw SQLE;
        } 
      }
    }
  },
/*
  exec: async function (sql) {
    const client = await this.pool.connect();
    try {
      return await client.query(sql);
    } finally {
      client.release();
    }
  },
*/
  schema: async function () { 
    if (!this._schema) await this.learn();
    return Promise.resolve(this._schema);
  },

  learn: async function () {
/*
    let [tables, fields, constr] = await Promise.all(
      [ this.query(DbConnPg.queryTables)
      , this.query(DbConnPg.queryFields)
      , this.query(DbConnPg.queryConstraints)]);
*/

    let client = await this.pool.connect();
    let tables = await client.query(DbConnPg.queryTables);
    let fields = await client.query(DbConnPg.queryFields);
    // let constr = this.query(DbConnPg.queryConstraints)]);

    let T = DbConnPg.types;
    let tmap = new Map(); Object.keys(T).forEach(k => tmap.set(T[k][0], T[k][1]));

    let b = { read: false, write: false, hidden: true, prot: false };
    this._schema =
      tables.rows.map( t => Object.assign({}, b, { _: 'table', name: t.name, comment: t.comment })).concat (
      fields.rows.map( f => Object.assign({}, b, { _: 'field', name: f.attname, table: f.relname, comment: f.comment, autonum: false, type: f.type, genType: tmap.get(f.type) || 'str' })))
    // constr.rows.map( r => { _: '
    client.release();
  }
};
// FIXME: real types are not checked and written quite randomly
DbConnPg.types = {
    20   : [ 'int8'  , 'int'   ]
  , 21   : [ 'int2'  , 'int'   ]
  , 23   : [ 'int4'  , 'int'   ]
  , 26   : [ 'oid'   , 'int'   ]
  , 700  : [ 'float4', 'double']
  , 701  : [ 'float8', 'double']
  , 16   : [ 'bit'   , 'bool'  ]
  , 1082 : [ 'date'  , 'date'  ]
  , 1114 : [ 'timestamp without time zone'  , 'datetime'  ]
  , 1184 : [ 'timestamp with time zone', 'datetime']
  , 20   : [ 'bigint'  , 'int'  ]
  , 21   : [ 'smallint', 'int'  ]
  , 23   : [ 'integer' , 'int'  ]  // TODO what's the difference between 23 and 26
  , 26   : [ 'oid'     , 'int'  ]  // OID
  , 1700 : [ 'numeric' , 'float']  // TODO: should this be text
  , 700  : [ 'real'    , 'float' ] // TODO is this really 'real'? check
  , 701  : [ 'double precision', 'float' ]
  , 16   : [ 'boolean', 'bool' ]
  , 1184 : [ 'date', 'date']
  , 25   : [ 'text', 'str']
  , 1043 : [ 'varchar', 'str']
  , 114  : [ 'json', 'str']
  , 1009 : [ '_regproc', 'str']
  , 705  : [ 'text', 'str']    /// TODO NOT SURE
  , 1042 : [ 'bpchar', 'str']
  , 17   : [ 'bytea', 'str']
/*
  , 1000    : parseArray);
  , 1007    : parseArray);
  , 1016    : parseArray);
  , 1008    : parseArray);
  , 1009    : parseArray);
*/

/*
  , 600  : [ parsePoint); // point
  , 651  : [ parseStringArray); // cidr[]
  , 718  : [ parseCircle); // circle
  , 1000 : [ parseBoolArray);
  , 1001 : [ parseByteAArray);
  , 1005 : [ parseIntegerArray); // _int2
  , 1007 : [ parseIntegerArray); // _int4
  , 1028 : [ parseIntegerArray); // oid[]
  , 1016 : [ parseBigIntegerArray); // _int8
  , 1017 : [ parsePointArray); // point[]
  , 1021 : [ parseFloatArray); // _float4
  , 1022 : [ parseFloatArray); // _float8
  , 1231 : [ parseFloatArray); // _numeric
  , 1014 : [ parseStringArray); //char
  , 1015 : [ parseStringArray); //varchar
  , 1008 : [ parseStringArray);
  , 1009 : [ parseStringArray);
  , 1040 : [ parseStringArray); // macaddr[]
  , 1041 : [ parseStringArray); // inet[]
  , 1115 : [ parseDateArray); // timestamp without time zone[]
  , 1182 : [ parseDateArray); // _date
  , 1185 : [ parseDateArray); // timestamp with time zone[]
  , 1186 : [ parseInterval);
  , 17   : [ parseByteA);
  , 114  : [ JSON.parse.bind(JSON)); // json
  , 3802 : [ JSON.parse.bind(JSON)); // jsonb
  , 199  : [ parseJsonArray); // json[]
  , 3807 : [ parseJsonArray); // jsonb[]
  , 3907 : [ parseStringArray); // numrange[]
  , 2951 : [ parseStringArray); // uuid[]
  , 791  : [ parseStringArray); // money[]
  , 1183 : [ parseStringArray); // time[]
  , 1270 : [ parseStringArray); // timetz[]
*/
}
DbConnPg.queryTables = 
  " SELECT c.relname AS name, COALESCE(pg_catalog.obj_description(c.oid, 'pg_class'), '') AS comment" +
  " FROM pg_catalog.pg_class c" +
  "   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace" +
  " WHERE c.relkind IN ('r','v','m')" +
  "   AND n.nspname <> 'pg_catalog'" +
  "   AND n.nspname <> 'information_schema'" +
  "   AND n.nspname !~ '^pg_toast'" +
  "   AND pg_catalog.pg_table_is_visible(c.oid)" +
  " ORDER BY c.relname";
DbConnPg.queryFields = 
  " SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS type, coalesce(pg_catalog.col_description(a.attrelid, a.attnum),'') AS comment, a.attnotnull" +
  " FROM pg_catalog.pg_attribute a" +
  "   JOIN ( " +
  "      SELECT c.oid, c.relname" +
  "      FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace" +
  "      WHERE c.relkind IN ('r','v','m') AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_toast'" +
  "          AND pg_catalog.pg_table_is_visible(c.oid)" +
  "   ) AS c ON c.oid = a.attrelid" +
  "   WHERE a.attnum > 0 AND NOT a.attisdropped" +
  " ORDER BY c.relname, a.attnum";
DbConnPg.queryConstraints =
  " SELECT relname, pg_catalog.pg_get_constraintdef(c.oid) as key" +
  " FROM pg_catalog.pg_constraint c" +
  "    JOIN pg_catalog.pg_namespace  ns ON ns.oid = c.connamespace" +
  "    JOIN pg_catalog.pg_class cl ON cl.oid = c.conrelid" +
  " WHERE cl.relkind IN ('r','v','m')" +
      " AND ns.nspname <> 'pg_catalog'" +
      " AND ns.nspname <> 'information_schema'" +
      " AND ns.nspname !~ '^pg_toast'" + +
      " AND pg_catalog.pg_table_is_visible(cl.oid)";

async function DbConnMy(props) {
  const my = require('mysql');
  return Object.create(DbConnMy.Proto, { props: { value: props }, pool: { value: my.createPool(props) }, _schema: { value: null, writable: true } });
}
DbConnMy.Proto = {
  // end:   async function () { await this.client.end() },
  query: function (q, args = []) {
    return new Promise((return_, onErr) => this.pool.query(q, args, (err, res, fields) => {
      if (err) { onErr( err); return; }
      let t = fields.map(f => DbConnMy.types[f.type] || ['str','str']);
      return_({ data: res, cols: fields.map(f => f.name), types: t.map(t => t[1]), rawTypes: t.map(t => t[0]) })
    }
    ));
  },
  transaction: function () { return new Promise((return_, throw_) => {
    this.pool.getConnection((err, client) =>  {
      if (err) { throw_(err); return; }
      client.query('BEGIN', err => {
        if (err) { throw_(err); return; }
        return_({
          commit: () => { client.query('COMMIT', () => client.release() ) }
        , query: (sql, args = []) => {
            return new Promise(ok => client.query(sql, args, (err,res,fld) => ok({ data: res, cols: fld.map(f => f.Field), types: fields.map(f => f.Type), rawTypes: fields.map(f => f.Type) })));
        }
        , exec : (sql, args = []) => {
            let ok, nok, P;
            P = new Promise((ok_,nok_) => { ok = ok_, nok = nok_; });
            client.query(sql, args, (err,res,fld) => {
              if (err) nok(err); // TODO rollback, close
              else ok(res.affectedRows)
            });
            return P;
          }
        });
      })
    })
  })
  },
  schema: async function () { 
    if (!this._schema) await this.learn();
    return Promise.resolve(this._schema);
  },
  learn: async function () {
    let R = this._schema = [];
    let Q = q => new Promise((ret, problem) => this.pool.query(q, (e, r, f) => { if (e) problem(e); else ret([r,f]) }))
    let [tables,tf] = await Q("SHOW TABLES");
    tables = tables.map(t => t[tf[0].name]);
    for (let i = 0; i < tables.length; i++) {
      let [cols] = await Q('SHOW FULL COLUMNS FROM ' + tables[i]);
      R.push({_: 'table', name: tables[i], comment: null }) // TODO: comment
      cols.forEach(c => 
        R.push({_: 'field', name: c.Field, table: tables[i], comment: c.Comment, autonum: c.Extra.indexOf('auto_increment') > -1, type: c.Type, genType: 'str'}) // FIXME genType
      )
    }
  }
}
// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
DbConnMy.types = {
  [0x00]: [ 'decimal'   , 'float' ]    // MYSQL_TYPE_DECIMAL        0x00   Implemented by ProtocolBinary::MYSQL_TYPE_DECIMAL
, [0x01]: [ 'tiny'      , 'int']       // MYSQL_TYPE_TINY           0x01   Implemented by ProtocolBinary::MYSQL_TYPE_TINY
, [0x02]: [ 'short'     , 'int']       // MYSQL_TYPE_SHORT          0x02   Implemented by ProtocolBinary::MYSQL_TYPE_SHORT
, [0x03]: [ 'long'      , 'int']       // MYSQL_TYPE_LONG           0x03   Implemented by ProtocolBinary::MYSQL_TYPE_LONG
, [0x04]: [ 'float'     , 'float']     // MYSQL_TYPE_FLOAT          0x04   Implemented by ProtocolBinary::MYSQL_TYPE_FLOAT
, [0x05]: [ 'double'    , 'float']     // MYSQL_TYPE_DOUBLE         0x05   Implemented by ProtocolBinary::MYSQL_TYPE_DOUBLE
, [0x06]: [ 'null'      , 'str']       // MYSQL_TYPE_NULL           0x06   Implemented by ProtocolBinary::MYSQL_TYPE_NULL FIXME
, [0x07]: [ 'timestamp' , 'timestamp'] // MYSQL_TYPE_TIMESTAMP      0x07   Implemented by ProtocolBinary::MYSQL_TYPE_TIMESTAMP
, [0x08]: [ 'longlong'  , 'int']       // MYSQL_TYPE_LONGLONG       0x08   Implemented by ProtocolBinary::MYSQL_TYPE_LONGLONG
, [0x09]: [ 'int24'     , 'int']       // MYSQL_TYPE_INT24          0x09   Implemented by ProtocolBinary::MYSQL_TYPE_INT24
, [0x0a]: [ 'date'      , 'date']      // MYSQL_TYPE_DATE           0x0a   Implemented by ProtocolBinary::MYSQL_TYPE_DATE
, [0x0b]: [ 'time'      , 'time']      // MYSQL_TYPE_TIME           0x0b   Implemented by ProtocolBinary::MYSQL_TYPE_TIME
, [0x0c]: [ 'datetime'  , 'datetime']  // MYSQL_TYPE_DATETIME       0x0c   Implemented by ProtocolBinary::MYSQL_TYPE_DATETIME
, [0x0d]: [ 'year'      , 'int']       // MYSQL_TYPE_YEAR           0x0d   Implemented by ProtocolBinary::MYSQL_TYPE_YEAR
, [0x0e]: [ 'date'      , 'date']      // MYSQL_TYPE_NEWDATE [a]    0x0e   see Protocol::MYSQL_TYPE_DATE
, [0x0f]: [ 'varchar'   , 'str']       // MYSQL_TYPE_VARCHAR        0x0f   Implemented by ProtocolBinary::MYSQL_TYPE_VARCHAR
, [0x10]: [ 'bit'       , 'int']       // MYSQL_TYPE_BIT            0x10   Implemented by ProtocolBinary::MYSQL_TYPE_BIT
, [0x11]: [ 'timestamp' , 'datetime']  // MYSQL_TYPE_TIMESTAMP2 [a] 0x11   see Protocol::MYSQL_TYPE_TIMESTAMP
, [0x12]: [ 'datetime'  , 'datetime']  // MYSQL_TYPE_DATETIME2 [a]  0x12   see Protocol::MYSQL_TYPE_DATETIME
, [0x13]: [ 'time'      , 'time']      // MYSQL_TYPE_TIME2 [a]      0x13   see Protocol::MYSQL_TYPE_TIME
, [0xf6]: [ 'decimal'   , 'float']     // MYSQL_TYPE_NEWDECIMAL     0xf6   Implemented by ProtocolBinary::MYSQL_TYPE_NEWDECIMAL
, [0xf7]: [ 'enum'      , 'str']       // MYSQL_TYPE_ENUM           0xf7   Implemented by ProtocolBinary::MYSQL_TYPE_ENUM
, [0xf8]: [ 'set'       , 'str']       // MYSQL_TYPE_SET            0xf8   Implemented by ProtocolBinary::MYSQL_TYPE_SET
, [0xf9]: [ 'tinyblob'  , 'str']       // MYSQL_TYPE_TINY_BLOB      0xf9   Implemented by ProtocolBinary::MYSQL_TYPE_TINY_BLOB
, [0xfa]: [ 'mediumblob', 'str']       // MYSQL_TYPE_MEDIUM_BLOB    0xfa   Implemented by ProtocolBinary::MYSQL_TYPE_MEDIUM_BLOB
, [0xfb]: [ 'longblob'  , 'str']       // MYSQL_TYPE_LONG_BLOB      0xfb   Implemented by ProtocolBinary::MYSQL_TYPE_LONG_BLOB
, [0xfc]: [ 'blob'      , 'str']       // MYSQL_TYPE_BLOB           0xfc   Implemented by ProtocolBinary::MYSQL_TYPE_BLOB
, [0xfd]: [ 'var_string', 'str']       // MYSQL_TYPE_VAR_STRING     0xfd   Implemented by ProtocolBinary::MYSQL_TYPE_VAR_STRING
, [0xfe]: [ 'string'    , 'str']       // MYSQL_TYPE_STRING         0xfe   Implemented by ProtocolBinary::MYSQL_TYPE_STRING
, [0xff]: [ 'geometry'  , 'str']       // MYSQL_TYPE_GEOMETRY       0xff   
}

async function DbConnLt(props) {
  let conn = await Lite.open(props.filename || ':memory:', { mode: Lite3.OPEN_READWRITE });
  let pragma, P = DbConnLt.Pragmas;
  for (pragma in P)
    if (props[pragma])
      await conn.run("PRAGMA " + pragma + " = " + P[pragma](props[pragma]))
  return Object.create(DbConnLt.Proto, { conn: { value: conn }, _schema: { value: null, writable: true }, props: { value: props }  });
}
DbConnLt.Proto = {
  // end:   async function () { await this.client.end() },
  query: async function (q, args = []) {
      const client = this.conn;
      let R = await client.all(q, args);
      let cols = R.length ? Object.keys(R[0]) : {};
      let types = new Array(cols.length);
      let unknown = cols.length;
      let tmap = { number: 'double', "boolean": 'bool' }
      
      // find types
      for (let r = 0; r < R.length && unknown > 0; r ++) 
        for (let c = 0; c < cols.length; c++)
          if (!types[c]) 
            if (R[r][c] !== null) types[c] = typeof R[r][cols[c]];


      // default to text
      let typesGen = [];
      for (let r = 0; r < types.length; r++) typesGen[r] = tmap[types[r]] || 'str';

      return { data: R, cols: R.length === 0 ? [] : Object.keys(R[0]), types: typesGen, rawTypes: types };
  },
  transaction: async function () {
    const client = this.conn; // await Lite.open(this.props.filename || ':memory:', { Promise });
    // await client.run('BEGIN'); // FIXME: how to do transactions in :memory:
    return {
      query: (sql, args = []) => client.query(sql, args)
    , exec:  async (sql, args = []) => { // TODO error handling and rollback
        let {changes, lastID} = await client.run(sql, args);
        return changes;
      }
    , commit: async ()  => {
        // await client.run('COMMIT');
        // await client.close();
      }
    }
  },
  schema: async function () {
    if (!this._schema) await this.learn();
    return this._schema;
  },
  learn: async function () {
    const client = this.conn;
    let tables = await client.all(DbConnLt.queryTables);
    let fields = [];
    let base = { read: false, write: false, hidden: false, prot: false };
    for (let t = 0; t < tables.length; t++) {
      let f = await client.all(DbConnLt.queryFields(tables[t].name))
      fields = fields.concat(f.map( f => Object.assign({_: 'field', name: f.name, table: tables[t].name }, base)));
    }
    this._schema = tables.map(t => Object.assign({_: 'table', name: t.name, comment: '' }, base)).concat(fields);
  }
};
DbConnLt.queryTables = "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name != 'sqlite_sequence'"
DbConnLt.queryFields = t => "PRAGMA table_info('" + t + "')"; // FIXME: escape
DbConnLt.queryConstr = t => "PRAGMA foreign_key_list('" + t + "')";
DbConnLt.Pragmas = {
   application_id:      parseInt
,  auto_vacuum:         parseInt // yeah, NONE/FULL/INCREMENTAL should also be supported
,  automatic_index:     x => parseBool(x)?'1':'0'
,  busy_timeout:        parseInt
,  cache_size:          parseInt
,  cache_spill:         x => parseBool(x)?'1':'0'
,  case_sensitive_like: x => parseBool(x)?'1':'0'
,  cell_size_check:     x => parseBool(x)?'1':'0'
,  checkpoint_fullfsync:x => parseBool(x)?'1':'0'
,  defer_foreign_keys:  x => parseBool(x)?'1':'0'
,  encoding:            x => x
,  foreign_keys:        x => parseBool(x)?'1':'0'
,  fullfsync:           x => parseBool(x)?'1':'0'
,  ignore_check_constraints: x => parseBool(x)?'1':'0'
,  journal_mode:        x => x // user's business to know how to connect ['DELETE', 'TRUNCATE', 'PERSIST','MEMORY','WAL','OFF'].includes(x) ? x : (throw new Error("Invalid journal mode: " + x))
,  journal_size_limit:  parseInt
,  legacy_file_format:  x => parseBool(x)?'1':'0'
,  locking_mode:        x => x
,  max_page_count:      parseInt
,  mmap_size:           parseInt
// ,  optimize:
,  page_size:           parseInt
,  query_only:          x => parseBool(x)?'1':'0'
// ,  quick_check:
,  read_uncommitted:    x => parseBool(x)?'1':'0'
,  recursive_triggers:  x => parseBool(x)?'1':'0'
,  reverse_unordered_selects: x => parseBool(x)?'1':'0'
// ,  schema_version:      
,  secure_delete:       x => (typeof x === 'string' && x.toLowerCase() === 'fast') ? 'FAST' : (parseBool(x)?'1':'0')
,  soft_heap_limit:     parseInt
,  synchronous:         parseInt // TODO, shoud be more sophisticated
,  temp_store:          parseInt // TODO
,  threads:             parseInt
,  user_version:        parseInt
,  wal_autocheckpoint:  parseInt
// ,  wal_checkpoint:     
,  writable_schema:     x => parseBool(x)?'1':'0'
};


async function DbConnOr(props) {
  const oracledb = require('oracledb');
  oracledb.autoCommit = false;

  return Object.create(DbConnOr.Proto, { props: { value: props }, pool: { value: await oracledb.createPool(props) }, _schema: {  value: null, writable: true } } );
}
DbConnOr.Proto = {
  // end:   async function () { await this.client.end() },
  query: async function (q, args = []) {
      let client = await this.pool.getConnection();
      let R = await client.execute(q.trim().replace(/;$/, ''), args, { extendedMetaData: true /* , outFormat: oracledb.OBJECT  */ }); // TODO outFormat object is probably faster (no need to do it by myself here)
      client.release(err => { if (err) console.error(err) } );
      let cols = R.metaData.map(f => f.name);
      let rt = [], gt = []; // raw- and generaly type
      R.metaData
      .map(f => [f.dbType, ... (DbConnOr.types[f.dbType] || [])]) // put all values for type to one array
      .forEach(([dbType, rawType, genType]) => {
        if (!rawType) {
          console.warn('Unsupported Oracle type, dbType: ' + dbType + '; query: ' + q); // TODO: what should be right logging for that
          rt.push(dbType.toString());
          gt.push('str'); // TODO: should convert all columns to string
        } else {
          rt.push(rawType);
          gt.push(genType);
        }
      });
      return { data: R.rows.map(r => zipObject(cols, r )), cols, types: gt, rawTypes: rt };
  },
  transaction: async function () {
    const client = await this.pool.getConnection();
    await client.execute('BEGIN');
    return {
      query: (sql, args = []) => client.execute(sql, args)
    , exec:   async (sql, args = []) => (await client.execute(sql, args)).rowsAffected // TODO error handling and rollback
    , commit: async () => { await client.execute('COMMIT'); client.release(); }
    }
  },
  schema: async function () {
    if (!this._schema) await this.learn();
    return this._schema;
  },
  learn: async function () {
    const client = await this.pool.getConnection();
    let sch = [], curTable = '', base = { read: false, write: false, hidden: false, prot: false };
    (await client.execute(DbConnOr.queryFields)).rows.forEach(([tname, cname, dtype, dlen]) => {
      if (curTable != tname)
      sch.push(Object.assign({_: 'table', comment: '', name:  tname }, base));
      sch.push(Object.assign({_: 'field', comment: '', table: tname, name: cname, type: dtype, genType: 'str', autonum: false }, base)); // FIXME genType
    });
    client.release();
    this._schema = sch;
  }
};
DbConnOr.queryFields = "SELECT table_name, column_name, data_type, data_length FROM USER_TAB_COLUMNS";

// Based on true story: https://github.com/oracle/node-oracledb/blob/master/doc/api.md#oracledbconstantsdbtype
DbConnOr.types = 
{  101 : [ 'BINARY_DOUBLE', 'int' ]
,  100 : [ 'BINARY_FLOAT', 'double']
,  113 : [ 'BLOB', 'str' ]
,   96 : [ 'CHAR', 'str' ]
,  112 : [ 'CLOB', 'str' ]
,   12 : [ 'DATE', 'date' ]
,    8 : [ 'LONG', 'str' ]
,   24 : [ 'LONG RAW', 'str' ]
, 1096 : [ 'NCHAR', 'str']
, 1112 : [ 'NCLOB', 'str']
,    2 : [ 'NUMBER', 'double' ]
, 1001 : [ 'NVARCHAR', 'str' ]
,   23 : [ 'RAW', 'str' ]
,  104 : [ 'ROWID', 'int' ]
,  187 : [ 'TIMESTAMP', 'datetime' ]
,  232 : [ 'TIMESTAMP WITH LOCAL TIME ZONE', 'datetime' ]
,  188 : [ 'TIMESTAMP WITH TIME ZONE', 'datetime' ]
,    1 : [ 'VARCHAR2', 'str' ]
};

async function DbConnDb2(props) {
  const db2Pool = require('ibm_db').Pool;
  const pool = new db2Pool();
  if (props.max) pool.setMaxPoolSize(props.max);

  return Object.create(DbConnDb2.Proto, { pool: { value: pool }, props: { value: props }, _schema: { value: null, writable: true } });
}
DbConnDb2.Proto = {
  // end:   async function () { await this.client.end() },
  query: async function (q, args = []) {
      return new Promise((ok, failure) => {
        this.pool.open(this.props.connectString, (connErr, db) => {
          if (connErr) { failure(connErr); return; }
          db.queryResult(q.trim().replace(/;$/, ''), args, (err, R) => {
            if (err) {
              failure(err);
              return;
            }
            let cols = R.getColumnMetadataSync();
            let data = R.fetchAllSync();
            let rt = [], gt = []; // raw- and generaly type
            cols
            .map(f => [f.SQL_DESC_TYPE_NAME, ... (DbConnDb2.types[f.SQL_DESC_TYPE_NAME] || [])]) // put all values for type to one array
            .forEach(([dbType, rawType, genType]) => {
              if (!rawType) {
                console.warn('Unsupported DB2 type, dbType: ' + dbType + '; query: ' + q); // TODO: what should be right logging for that
                rt.push(dbType.toString());
                gt.push('str'); // TODO: should convert all columns to string
              } else {
                rt.push(rawType);
                gt.push(genType);
              }
            });
            db.close();
            ok({ data: data, cols: cols.map(c => c.SQL_DESC_CONCISE_TYPE), types: gt, rawTypes: rt });
          })
        });
    })
  },
  transaction: async function () {
    const client = this.conn;
    return new Promise((ok, failure) => {
      client.query('BEGIN', [], err => {
        if (err) failure(err);
        else ok({
          query: (sql, args = []) => (new Promise((ok, failure) => client.query(sql, args, (err, R) => err ? failure(err) : ok(R) )))
        , exec:   async (sql, args = []) => ( // TODO error handling and rollback
            new Promise((ok, failure) => {
              client.prepare(sql, (err, stmt) => {
                if (err) failure(err);
                else stmt.execute(args, (err, R) => err ? failure(err) : ok(R)  )
              })
            })
        )
        , commit: async () => (new Promise((ok, failure) => client.query('COMMIT', [], (err, R) => (err ? failure(err) : ok())) ))
        })
      });
    });
  },
  schema: async function () {
    if (!this._schema) await this.learn();
    return this._schema;
  },
  learn: async function () {
    const client = this.conn;
    let sch = [], curTable = '', base = { read: false, write: false, hidden: false, prot: false }, tableDone = 0;
    return new Promise((ok, failure) => {
      client.query(DbConnDb2.queryTables, [], (err, tables) => {
        if (err) { failure(err); return; }
        else {
          for (let t = 0; t < tables.length; t++) {
            let tname = tables[t].TABNAME;
            client.query(DbConnDb2.queryFields, [ tname ], (err, cols) => {
              if (err) { failure(err); return; }

              sch.push(Object.assign({_: 'table', comment: '', name:  tname }, base));
    
              cols.forEach(({NAME, COLTYPE}) => {
                sch.push(Object.assign({_: 'field', comment: '', table: tname, name: NAME, type: COLTYPE.trim(), genType: (DbConnDb2.types[ COLTYPE.trim() ]||['?','?'])[1], autonum: false }, base))
              });
              if (++tableDone === tables.length) ok(this._schema = sch);
            });
          }
        }
      })
    })
  }
};
DbConnDb2.queryTables = "select tabname from syscat.tables";
DbConnDb2.queryFields = "select name, coltype from Sysibm.syscolumns where tbname = ?";
  
DbConnDb2.types = 
{  "SMALLINT": [ "SMALLINT", "int" ]
,  "INTEGER": [ "INTEGER", "int" ]
,  "BIGINT": [ "BIGINT", "int" ]
,  "REAL": [ "REAL", "double" ]
,  "NUMERIC": [ "NUMERIC", "double" ]
,  "DECIMAL": [ "DECIMAL", "double" ]
,  "DOUBLE": [ "DOUBLE", "double" ]
,  "DECFLOAT": [ "DECFLOAT", "double" ]
,  "CHAR": [ "CHAR", "str" ]
,  "VARCHAR": [ "VARCHAR", "str" ]
,  "CBLOB": [ "CBLOB", "str" ]
,  "BLOB": [ "BLOB", "str" ]
,  "CLOB": [ "CLOB", "str" ]
,  "MONEY": [ "MONEY", "double" ]
,  "DISTINCT": [ "DISTINCT", "str" ] // not sure this is str
,  "GRAPHIC": [ "GRAPHIC", "str" ]
,  "VARBINARY" : [ "VARBINARY", "str" ]
,  "VARGRAPH" : [ "VARGRAPH", "str" ]
,  "DATE" : [ "DATE", "date" ]
,  "TIME" : [ "TIME", "time" ]
,  "XML" : [ "XML", "str" ]
,  "TIMESTMP" : [ "TIMESTAMP", "datetime" ]
,  "TIMESTAMP WITHOUT TIME ZONE" : [ "TIMESTAMP WITHOUT TIME ZONE", "datetime" ]
,  "TIMESTAMP WITH TIME ZONE" : [ "TIMESTAMP WITH TIME ZONE", "datetime" ]
};

module.exports = { urlToConnection, DbConn }

const pmon = require('parsimmon');
const util = require('util');
const testSql = require('sqlite').open(':memory:', { Promise });
const Opts = require('./opts');

Object.map = (o, fn) => Object.keys(o).map((k, i) => fn(k, o[k], i));
Object.onValues = function (o, fn) {
  let K = Object.keys(o);
  return K.map((k, i) => fn(o[k], k, i)).reduce((O, o, i) => Object.assign(O, { [ K[i] ]: o }), {});
}
Object.onValuesA = async function (o, fn) {
  let K = Object.keys(o);
  return (await Promise.all(K.map((k, i) => fn(o[k], k, i)))).reduce((O, o, i) => Object.assign(O, { [ K[i] ]: o }), {});
}
if (!Object.values) Object.values = o => Object.keys(o).map(k => o[k]);

Map.prototype.setWith = function (key, val, fn) {
  if (this.has(key)) this.set(key, fn(this.get(key), key));
  else this.set(key, val); 
}

trace = x => { console.log(x); return x; }

opFn = op => (fnName, Sql, inf, args) => args.map(a => a.sqlSnippet(Sql)).join(' ' + op + ' ');
sameFn = (fnName, Sql, inf, args) => fnName + '(' + args.map(a => a.sqlSnippet(Sql)).join(', ') + ')';

QFunctions = {
  "isnull" : [ "=1", (fnName, Sql, inf, [arg]) => '(' + arg.sqlSnippet(Sql) + ') IS NULL']
, "notnull": [ "=1", (fnName, Sql, inf, [arg]) => '(' + arg.sqlSnippet(Sql) + ') IS NOT NULL']
, "not"    : [ "=1", (fnName, Sql, inf, [arg]) => 'NOT (' + arg.sqlSnippet(Sql) + ')']
, "null"   : [ "=0", () => 'NULL' ]
, "true"   : [ "=0", (_, Sql) => Sql.hasBooleans ? 'TRUE' : '1' ]
, "false"  : [ "=0", (_, Sql) => Sql.hasBooleans ? 'FALSE' : '0' ]
, "if"     : [ ">3", ifFn ]
, "nullif" : [ "=2", sameFn ]
, "between": [ "=3", (f, S, i, [s,a,b]) => '(' + s.sqlSnippet(S) + ") BETWEEN (" + a.sqlSnippet(S) + ") AND (" + b.sqlSnippet(S) + ")" ]
, "in"     : [ ">2", (f, S, i, args) =>    '(' + args[0].sqlSnippet(S) +  ') IN (' + args.slice(1).map(a => a.sqlSnippet(S)).join(', ') + ')' ]
, "concat" : [ ">1", (f, S, i, args) => (S === SqlLt || S === SqlPg) ? opFn('||')(f,S,i,args) : sameFn(f,S,i,args) ]
, "choose" : [ ">2", chooseFn ]
, "str"    : [ "=1", castToFn({_: 'CHAR', pg: 'VARCHAR' }) ]
, "num"    : [ "=1", castToFn({_: 'NUMERIC' }) ]
, "int"    : [ "=1", castToFn({_: 'INTEGER', pg: 'BIGINT', my: 'SIGNED' }) ]
, "double" : [ "=1", castToFn({_: 'DECIMAL' }) ]
, "cast"   : [ "=1", castFn ]
, "coalesce":[ ">1", sameFn ]
, "like"   : [ "=2", opFn("LIKE") ]
, "ilike"  : [ "=2", (fn, S, i, [a,b]) => {
      if (S.type == 'pg') return opFn('ILIKE').apply(this, [fn, S, i, [a,b]]);
      else return 'lower(' + a.sqlSnippet(S) + ') LIKE LOWER(' + b.sqlSnippet(S) + ')';

  } ]
, "len"   :  [ "=1",  (fn, S, i, [a]) => "LENGTH(" + a.sqlSnippet(S) + ")" ] // TODO: length should also be supported because it's easier to user
, "length":  [ "=1",  (fn, S, i, [a]) => "LENGTH(" + a.sqlSnippet(S) + ")" ] // TODO: length should also be supported because it's easier to user
, "lower" :  [ "=1",  sameFn ]
, "upper" :  [ "=1",  sameFn ]
, "trim"  :  [ "=1",  sameFn ] // FIXME: database differences
, "ltrim" :  [ "=1",  sameFn ]
, "rtrim" :  [ "=1",  sameFn ]
//  ),( "strpos",  More 2, [ (dbAll, sameFn) ] // FIXME: SQLite doesn't have strpos
, "substr":  [ ">2",  sameFn ] // FIXME: 2 OR 3 arguments
, "replace": [ "=3",  sameFn ]
, "lpad"  :  [ ">2",  sameFn ] // FIXME: 2 OR 3
, "rpad"  :  [ ">2",  sameFn ] // FIXME: 2 OR 3

, "date"  :  [ "=1",  sameFn ]
, "now"   :  [ ">-1", (fn, S, i, args) => ({
      lt: "datetime('now', 'localtime')",
      pg: "current_timestamp::timestamp(0)"
    })[S.type] || 'current_timestamp' ]
// , "dt",      [ ">0",  dtFn  ) ] TODO
// , "extr",    [ "=1",  extrFn) ] TODO

, "countif": [ "=1",  (fn, S, i, [a]) => "COUNT(CASE WHEN " + a.sqlSnippet(S) + " THEN 1 ELSE NULL END)" ]
, "count"  : [ "=1",  sameFn ]
, "ceil"   : [ "=1",  sameFn ]
, "floor"  : [ "=1",  sameFn ]
, "random" : [ "=0",  sameFn ]
, "round"  : [ ">1",  sameFn ] // FIXME 1 OR 2 arguments
, "avg"    : [ "=1",  sameFn ]
, "exp"    : [ "=1",  sameFn ]
, "abs"    : [ "=1",  sameFn ]
, "md5"    : [ "=1",  sameFn ]
, "greatest":[ "=1",  sameFn ]
, "least"   :[ "=1",  sameFn ]
, "min"    : [ "=1",  sameFn ]
, "max"    : [ "=1",  sameFn ]
, "sum"    : [ "=1",  sameFn ]
, "every"  : [ "=1",  sameFn ]

, "and"    : [ ">1",  opFn('AND') ]
, "or"     : [ ">1",  opFn('OR') ]
, "xor"    : [ ">1",  opFn('XOR') ]
, "add"    : [ ">1",  opFn('+') ]
, "div"    : [ ">1",  opFn('/') ]
, "sub"    : [ ">1",  opFn('-') ]
, "mul"    : [ ">1",  opFn('*') ]

, "desc":[ "=1",  (_, S, i, [a]) => a.sqlSnippet(S) + " DESC"]
, "asc": [ "=1",  (_, S, i, [a]) => a.sqlSnippet(S) + " ASC" ]
}
// let QValue = () => throw "QValue 

QShortFn = {
    ":": (a,b) => new QOp(a, [["=", b]])
  , "!": (a,b) => new QOp(a, [["!=", b]])
  , ">": (a,b) => new QOp(a, [["!=", b]])
  , "<": (a,b) => new QOp(a, [["!=", b]])
  , "~": (a,b) => new QFn("ilike", null, [a,b])
}

function QValue() { }
QValue.prototype = {
  sqlSnippet: function () { throw new Error("sqlSnippet in QValue") }
, travToken : function () { return this }
, travField : function () { return this }
, travFunc  : function () { return this }
, travTokenA : async function () { return this }
, travFieldA : async function () { return this }
, travFuncA  : async function () { return this }
, isQValue   : true // is used to detect if it's type is parsed value
, describe   : function () { throw new Error("describe in QValue") }
, isSame     : function (el) { return this === el }
}
protoQ = o => Object.assign({}, QValue.prototype, o); // I don't like it

// QEmpty is just to write more convinitent code
function QEmpty() {}
QEmpty.prototype = protoQ({
  sqlSnippet: () => ''
, isEmpty: true
, describe: () => { return { "empty": "empty" } }
, isSame: function (e) { return e === this }
})
qEmpty = new QEmpty();

function QRaw(v) { this.v = v }
QRaw.prototype = protoQ({
  sqlSnippet : function () { return this.v }
, describe   : function () { return { "raw": this.v } }
, isSame     : function (el) { el === this || el.v === this.v }
});

function QNull() { }
QNull.prototype = protoQ({
  sqlSnippet: () => "NULL"
, describe: () => { return { "null": "null" } }
, isSame: function (el) { return el === this || QNull.sqlSnippet === el.sqlSnippet }
})
qNull = new QNull();

function QBool(v) { this.v = v }
QBool.prototype = protoQ({
  sqlSnippet: function (Sql) { return Sql.hasBooleans ? (this.v ? "TRUE" : "FALSE") : (this.v ? '1' : '0') }
, describe: function () { return { "bool": this.v } }
, isSame  : function (el) { return el === this || el.v === this.v }
});
qTrue  = new QBool(true);
qFalse = new QBool(false);

function QString(v) { this.v = v; }
QString.prototype = protoQ( {
  sqlSnippet : function (Sql) { return Sql.esc(this.v) }
, describe: function () { return {"str": this.v } }
, isSame: function (el) { return el === this || el.v === this.v }
});

function QInt(v) { this.v = v }
QInt.prototype = protoQ({
  sqlSnippet : function () { return this.v.toString() }
, describe: function () { return { "int": this.v } }
, isSame: function (el) { return this === el || el.v === this.v }
});

function QFloat(v) { this.v = v }
QFloat.prototype = protoQ({
  sqlSnippet : function () { return this.v.toString() }
, describe: function () { return { "float": this.v } }
, isSame: function (el) { return this === el || el.v === this.v }
});

function QField(table, field) { this.table = table; this.field = field; }
QField.prototype = protoQ({
  sqlSnippet : function (Sql) { return this.table ? [ Sql.name(this.table), Sql.name(this.field)].join('.') : Sql.name(this.field) }
, travField  : function (fn)  { return fn(this); }
, travFieldA : async function (fn)  { return await fn(this); }
, describe: function () { return { "field":  this.field, table: this.table } }
, isSame: function (el) { return this === el || (this.table == el.table && el.field === this.field) }
});

function QPar(v) { this.v = v }
QPar.prototype = protoQ({
   sqlSnippet : function (Sql) { return '(' + this.v.sqlSnippet(Sql) + ')' }
,  travToken  : function (fn) { return new QPar(this.v.travToken(fn)); }
,  travField  : function (fn) { return new QPar(this.v.travField(fn)); }
,  travFunc   : function (fn) { return new QPar(this.v.travFunc (fn)); }
,  travTokenA  : async function (fn) { return new QPar(await this.v.travTokenA(fn)); }
,  travFieldA  : async function (fn) { return new QPar(await this.v.travFieldA(fn)); }
,  travFuncA   : async function (fn) { return new QPar(await this.v.travFuncA (fn)); }
,  describe    : function () { return { "parenthesis": this.v.describe() } }
,  isSame      : function (el) { return this === el || this.v.isSame(el.v) } // FIXME: check if `el` is QPar
});

function QFn (name, inf, args = []) { this.name = name; this.inf = inf; this.args = args; }
QFn.prototype = protoQ({
   sqlSnippet : function (Sql) {
      if (!QFunctions[this.name]) throw "Function '" + this.name + "' not defined";
      let [argCount, fn] = QFunctions[this.name];
      return fn(this.name, Sql, this.inf, this.args);
  }
, travToken : function (fn) { return new QFn(this.name, this.inf, this.args.map(a => a.travToken(fn))); }
, travField : function (fn) { return new QFn(this.name, this.inf, this.args.map(a => a.travField(fn))); }
, travFunc  : function (fn) { let fn_ = fn(this); return fn_ === this ? new QFn(this.name, this.info, this.args.map(a => a.travFunc(fn))) : fn_; }
, travTokenA: async function (fn) { return new QFn(this.name, this.inf, await Promise.all(this.args.map(a => a.travTokenA(fn)))); }
, travFieldA: async function (fn) { return new QFn(this.name, this.inf, await Promise.all(this.args.map(a => a.travFieldA(fn)))); }
, travFuncA : async function (fn) { let fn_ = await fn(this); return fn_ === this ? new QFn(this.name, this.inf, await Promise.all(this.args.map(a => a.travFuncA(fn)))) : fn_; }
, describe  : function () { return { "func": this.name, "inf": this.inf, args: this.args.map(a => a.describe()) } }
, isSame    : function (el) { return el === this ||
                (this.name === el.name && this.inf === el.inf &&
                 Array.isArray(el.args) && el.args.length === this.args.length && this.args.every((a, i) => a.isSame(el.args[i]))) }
//,  sqlSnippet : function (Sql) { return this.name + '(' + this.args.map(a => a.sqlSnippet(Sql)).join (', ') + ')' }
})

function QAs(v, as) { this.v = v; this.as = as; }
QAs.prototype = protoQ({
   sqlSnippet : function (Sql) { return this.v.sqlSnippet(Sql) + (this.as ? ' AS ' + this.as.sqlSnippet(Sql) : ''); }
,  travToken  : function (fn) { return new QAs(this.v.travToken(fn), this.as); }
,  travField  : function (fn) { return new QAs(this.v.travField(fn), this.as); }
,  travFunc   : function (fn) { return new QAs(this.v.travFunc (fn), this.as);  }
,  travTokenA : async function (fn) { return new QAs(await this.v.travTokenA(fn), this.as); }
,  travFieldA : async function (fn) { return new QAs(await this.v.travFieldA(fn), this.as); }
,  travFuncA  : async function (fn) { return new QAs(await this.v.travFuncA (fn), this.as);  }
,  describe   : function () { return Object.assing({}, this.v.describe(), { as: this.as }) }
,  isSame     : function (el) { return el === this || (this.v.isSame(el.v) && this.as === el.as) } // TODO: check if `el` is QAs
})

function QLimit(limit, offset) { this.limit = limit || qEmpty; this.offset = offset || qEmpty; }
QLimit.prototype = protoQ({
   sqlSnippet : function (Sql) { return this.limit.sqlSnippet(Sql) + (this.offset && !this.offset.isEmpty ? ' OFFSET ' + this.offset.sqlSnippet(Sql) : '') }
,  travToken  : function (fn) { return new QLimit(this.limit.travToken(fn), this.offset.travToken(fn)); }
,  travField  : function (fn) { return new QLimit(this.limit.travField(fn), this.offset.travField(fn)); }
,  travFunc   : function (fn) { return new QLimit(this.limit.travFunc (fn), this.offset.travFunc (fn));  }
,  travTokenA : async function (fn) { return new QLimit(await this.limit.travTokenA(fn), await this.offset.travTokenA(fn)); }
,  travFieldA : async function (fn) { return new QLimit(await this.limit.travFieldA(fn), await this.offset.travFieldA(fn)); }
,  travFuncA  : async function (fn) { return new QLimit(await this.limit.travFuncA (fn), await this.offset.travFuncA (fn));  }
,  describe   : function () { return { limit: this.limit ? this.limit.describe() : null, offset: this.offset ? this.offset.describe() : null } }
,  isSame     : function (el) { return el === this || (this.limit.isSame(el.limit) && this.offset.isSame(el.offset)) }
});

function QOp(fst, ls) { this.fst = fst; this.ls = ls; }
QOp.opMap = {
  ":"  : "AND"
, "|"  : "OR"
, "==" : "IS NOT DISTINCT FROM"
, "!==": "IS DISTINCT FROM"
}
QOp.prototype = protoQ({
  sqlSnippet : function (Sql) {
    var ls = this.ls.map(([op, val]) => [ QOp.opMap[op] || op, val ]);
    return this.fst.sqlSnippet(Sql) + ls.map(([op, val]) => ' ' + op + ' ' + val.sqlSnippet(Sql)).join('')
  }
, travToken: function (fn) { return new QOp(this.fst.travToken(fn), this.ls.map(([op, val]) => [op, val.travToken(fn)])) }
, travField: function (fn) { return new QOp(this.fst.travField(fn), this.ls.map(([op, val]) => [op, val.travField(fn)])) }
, travFunc : function (fn) { return new QOp(this.fst.travFunc (fn), this.ls.map(([op, val]) => [op, val.travFunc (fn)])) }
, travTokenA: async function (fn) { return new QOp(await this.fst.travTokenA(fn), await Promise.all(this.ls.map(([op, val]) => val.travTokenA(fn).then(v => [op, v]) ))) }
, travFieldA: async function (fn) { return new QOp(await this.fst.travFieldA(fn), await Promise.all(this.ls.map(([op, val]) => val.travFieldA(fn).then(v => [op, v]) ))) }
, travFuncA : async function (fn) { return new QOp(await this.fst.travFuncA (fn), await Promise.all(this.ls.map(([op, val]) => val.travFuncA (fn).then(v => [op, v]) ))) }
, describe  : function () { return { "expression": this.fst.describe(), "op": this.ls.map(([op,val]) => { return Object.assign({}, val.describe(), { "op": op }) })  } }
, isSame    : function (el) { return this === el || (Array.isArray(el.ls) && el.ls.length === this.ls.length && this.fst.isSame(el.fst) && this.ls.every(([op,val], i) => op === el.ls[i][0] && val.isSame(el.ls[i][1]) ) ) }
});

// list of other QValues and sqlSnippet gives comma listed result (for order by and group by)
function QList(ls) { this.ls = ls; }
QList.prototype = protoQ({
   sqlSnippet : function (Sql) { return this.ls.map(el => el.sqlSnippet(Sql)).join(', ') }
,  travToken  : function (fn) { return new QList(this.ls.map(i => i.travToken(fn))) }
,  travField  : function (fn) { return new QList(this.ls.map(i => i.travField(fn))) }
,  travFunc   : function (fn) { return new QList(this.ls.map(i => i.travFunc (fn))) }
,  travTokenA : async function (fn) { return new QList(await Promise.all(this.ls.map(i => i.travTokenA(fn)))) }
,  travFieldA : async function (fn) { return new QList(await Promise.all(this.ls.map(i => i.travFieldA(fn)))) }
,  travFuncA  : async function (fn) { return new QList(await Promise.all(this.ls.map(i => i.travFuncA (fn)))) }
,  append     : function (qList) { return new QList(this.ls.concat(qList.ls)) } // only QList object expected
,  map        : function (fn) { return new QList(this.ls.map(el => fn(el))) }
,  describe   : function () { return this.ls.map(e => e.describe()) }
,  isSame     : function (el) { return this === el || (Array.isArray(el.ls) && this.ls.every((val, i) => val.isSame(el.ls[i]))) }
});

// QSet if for update and insert, given object key is fieldname and value is QValue
function QSet(table, sets = {}) { this.table, this.sets = sets; }
QSet.prototype = protoQ({
   sqlSnippet : function (Sql) { return Object.map(this.sets, (f, v) => new QField(this.table, f).sqlSnippet(Sql) + ' = ' + v.sqlSnippet(Sql)).join(', ') }
,  travToken  : function (fn) { return new QSet(this.table, Object.onValues(this.sets, v => v.travToken(fn))); }
,  travField  : function (fn) { return new QSet(this.table, Object.onValues(this.sets, v => v.travField(fn))); }
,  travFunc   : function (fn) { return new QSet(this.table, Object.onValues(this.sets, v => v.travFunc(fn))); }
,  travTokenA : async function (fn) { return new QSet(this.table, await Object.onValuesA(this.sets, v => v.travTokenA(fn))); }
,  travFieldA : async function (fn) { return new QSet(this.table, await Object.onValuesA(this.sets, v => v.travFieldA(fn))); }
,  travFuncA  : async function (fn) { return new QSet(this.table, await Object.onValuesA(this.sets, v => v.travFuncA(fn))); }
,  describe   : function () { return Object.onValues(this.sets, e => e.describe()) }
,  append1    : function (key, val) { return new QSet(this.table, Object.assign(this.sets, {[key]: val})); }
,  appendSet  : function (set) { return new QSet(this.table||set.table, Object.assign({}, this.sets||{}, set.sets||{})); }
,  keys       : function () { return new QList( Object.keys(this.sets).map(f => new QField(this.table, f)) ) }
,  values     : function () { return new QList( Object.map(this.sets, (_, v) => v) ); }
,  filter     : function (ls) {
    let filtFn = ls.has ? x => ls.has(x) : x => ls.includes(x); // array of Set; (don't use ls[ ls.has ? 'has' : 'include'] because of speed) 
    return new QSet( this.table, Object.keys(this.sets).filter(filtFn).reduce((S,s) => Object.assign(S, {[s]: this.sets[s]}), {}) );
    }
,  isSame     : function (el) { return this === el || (this.table === el.table && typeof el.sets === 'object'
                    && Object.keys(this.sets).length === Object.keys(el.sets).length && Object.keys(el.sets).every((k,i) => this.sets[k].isSame(el.sets[k])) ) }
});
QSet.fromObject     = function (table, o) { return new QSet(table, Object.onValues(o, jsToQVal)) }
QSet.fromObjectExpr = function (table, o) { return new QSet(table, Object.onValues(o, v => typeof v === 'string' ? QParser.value.tryParse(v) : jsToQVal(v))) }

// QToken 
function QToken(token) { this.token = token; if (typeof token !== 'string') throw new Error('BUG: token is not string'); }
QToken.prototype = protoQ({
   sqlSnippet : () => { throw "BUG: QToken must always be converted to string, variable or field (value: " + this.token + ")" }
,  travToken: function (fn) { return fn(this); }
,  travTokenA: async function (fn) { return await fn(this); }
,  toString: function (s) { return new QString(s ? s.replace(/%s/g, this.token) : this.token) }
,  toField:  function (table) { return new QField(table, this.token) }
,  toNull :  () => qNull
,  toAs: function (as) { return new QAs(this, new QName(as)); }
,  describe: function () { return { "token": this.token } }
,  isSame: function (el) { return this === el || (el.token === this.token) }
});

// QVar is known that this is variable
function QVar(v) { this.v = v }
QVar.prototype = protoQ({
    sqlSnippet: function (Sql) { return new QString(this.v).sqlSnippet(Sql) }
  , describe  : function () { return { "var": this.v } }
  , isSame    : function (el) { return this === el || (el.v === this.v) }
});

// QName is for escaped as `select ... as "name"`
function QName(v) { this.v = v }
QName.prototype = protoQ({
    sqlSnippet : function (Sql) { return Sql.name(this.v) }
  , describe   : function () { return { "name": this.v } }
  , isSame     : function (el) { return this === el || el.v === this.v }
})

function QTempl(name, templ, values) { this.name = name; this.templ = templ; this.values = values; }
QTempl.prototype = protoQ({
   sqlSnippet : function (Sql) { 
    var vals = this.values;
    return this.templ.replace(/\$(\d+|\$|\*|,)/g, function (match, arg) {
      if (match === '$$') return '$';
      if (match === '$,') return vals.map(v => v.sqlSnippet(Sql)).join(', ');
      if (match === '$*') return vals.map(v => v.sqlSnippet(Sql)).join(', ');
      return vals[parseInt(arg)].sqlSnippet(Sql);
    })
  }
, travToken: function (fn) { return new QTempl(this.name,this.templ,this.values.map(v => v.travToken(fn))); }
, travField: function (fn) { return new QTempl(this.name,this.templ,this.values.map(v => v.travField(fn))); }
, travFunc : function (fn) { return new QTempl(this.name,this.templ,this.values.map(v => v.travFunc (fn))); }
, travTokenA: async function (fn) { return new QTempl(this.name,this.templ,await Promise.all(this.values.map(v => v.travTokenA(fn)))); }
, travFieldA: async function (fn) { return new QTempl(this.name,this.templ,await Promise.all(this.values.map(v => v.travFieldA(fn)))); }
, travFuncA : async function (fn) { return new QTempl(this.name,this.templ,await Promise.all(this.values.map(v => v.travFuncA (fn)))); }
, describe  : function () { return { "template": this.templ, "id": this.name, "values": this.values.map(e => e.describe()) } }
, isSame    : function (el) { return this === el || (this.name === el.name && this.templ === el.templ && Array.isArray(el.values) && this.values.length === el.values.length && this.values.every((v,i) => v.isSame(el.values[i])) ) }
});

function QFrom(table, as = null, joins = []) {
  this.table = table;
  this.as = as;
  this.joins = joins;
}
QFrom.prototype = protoQ({
  sqlSnippet: function (Sql) {
    return (new QName(this.table).sqlSnippet(Sql)) +
           (this.as ? ' AS ' + ( new QName(this.as).sqlSnippet(Sql) ) : '') +
           prefIf(' ', this.joins.map(j => j.sqlSnippet(Sql)).join(' '));
  }
  // tableFilter is filter ment specifically to some table, filter added to where or join
  , tableFilter: function (from, where) {
    // TODO: join filter should result `JOIN (a JOIN b ON a.x=b.x) ON a.x = 1` (and take care about names if joined twice)
    if (from.table === this.table)
      return new QFrom(
        this.table
      , this.as
      , this.joins.map(j => j.type === 'cross' ? j : new QJoin(j.table, j.type, j.as, and([j.on, where])))
      );
    else 
      return this;
  }
  , tables: function () { return [ { name: this.table, as: this.as || this.table} ].concat(this.joins.map(j => { return {name: j.table, as: j.as ? j.as.v : j.name}})) }
  , addJoins: function (join_ls) { this.joins = this.joins.concat(join_ls) }
  , travToken: function (fn) { return new QFrom(this.table,this.as,this.joins.map(j => j.travToken(fn))); }
  , travField: function (fn) { return new QFrom(this.table,this.as,this.joins.map(j => j.travField(fn))); }
  , travFunc : function (fn) { return new QFrom(this.table,this.as,this.joins.map(j => j.travFunc (fn))); }
  , travTokenA: async function (fn) { return new QFrom(this.table,this.as,await Promise.all(this.joins.map(j => j.travTokenA(fn)))); }
  , travFieldA: async function (fn) { return new QFrom(this.table,this.as,await Promise.all(this.joins.map(j => j.travFieldA(fn)))); }
  , travFuncA : async function (fn) { return new QFrom(this.table,this.as,await Promise.all(this.joins.map(j => j.travFuncA (fn)))); }
  , describe: function () { return { "table": this.table, "as": this.as, "joins": this.joins.map(j => j.describe()) } }
  , isSame  : function (el) { return this === el || (el.table === this.table && el.as === this.as && Array.isArray(el.joins) && this.joins.every((j,i) => j.isSame(el.joins[i]))) }
});

function QJoin(table, type = 'inner', as, on) {
  if (type.length === 1) type = ({ c: 'cross', l: 'left', r: 'right' })[type.toLowerCase()];
  if (!['inner', 'left', 'right', 'cross'].includes(type)) throw "Unknown join type"; 
  this.table = table; // :: String
  this.type = type;   // :: String
  this.as = as;       // :: QValue
  this.on = on ? on : qEmpty;  // :: QValue
}
QJoin.prototype = protoQ({
   sqlSnippet : function (Sql) { 
    let T = (new QName(this.table)).sqlSnippet(Sql);
    if (this.type === 'cross')
      return 'CROSS JOIN ' + T
    else 
      return this.type.toUpperCase() + ' JOIN ' + T + prefIf(' AS ', this.as.sqlSnippet(Sql)) + ' ON ' + this.on.sqlSnippet(Sql);
  }
, travToken: function (fn) { return new QJoin(this.table,this.type,this.as, this.on.travToken(fn)); }
, travField: function (fn) { return new QJoin(this.table,this.type,this.as, this.on.travField(fn)); }
, travFunc : function (fn) { return new QJoin(this.table,this.type,this.as, this.on.travFunc (fn)); }
, travTokenA: async function (fn) { return new QJoin(this.table,this.type,this.as, await this.on.travTokenA(fn)); }
, travFieldA: async function (fn) { return new QJoin(this.table,this.type,this.as, await this.on.travFieldA(fn)); }
, travFuncA : async function (fn) { return new QJoin(this.table,this.type,this.as, await this.on.travFuncA (fn)); }
, describe  : function () { return { "table": this.table, "type": this.type, "as": this.as.describe(), "on": this.on.describe() } }
, isSame    : function (el) { return el === this || (this.table === el.table && this.type === el.type && this.as.isSame(el.as) && this.on.isSame(el.on)) }
});

function ifFn (fnName, Sql, inf, args) {
  var i, cond = [], els = args.length % 2 ? ' ELSE ' + args[args.length - 1].sqlSnippet(Sql) : '';
  
  for (i = 0; i < args.length - 1; i += 2)
    cond.push('WHEN ' + args[i].sqlSnippet(Sql) + ' THEN ' + args[i + 1].sqlSnippet(Sql));
  return 'CASE ' + cond.join(' ') + els + ' END';
}

function chooseFn (fnName, Sql, inf, args) {
  return 'CASE ' + args[0].sqlSnippet(Sql) + args.slice(1).map((a, idx) => 'WHEN ' + idx + ' THEN ' + a.sqlSnippet(Sql)).join(' ') + ' END';
}

function castToFn(types) {
  return (fnName, Sql, inf, [arg]) => 'CAST(' + arg.sqlSnippet(Sql) + ' AS ' + (types[Sql.type] || types._) + ')';
}
function castFn(fnName, Sql, inf, [arg]) {
  inf = (inf||'').toLowerCase();

  if (!inf || !castFn.typemap[inf]) {
    throw "Invalid type to cast: " + inf;
  }
  else return castToFn(castFn.typemap[inf]).apply(this, arguments);
}
castFn.typemap = {
   "int"     : {_: 'INTEGER', pg: 'BIGINT' , my: 'SIGNED' }
  ,'time'    : {_: 'TIME'}
  ,'date'    : {_: 'DATE'}
  ,'datetime': {_: 'DATETIME'}
  ,'num'     : {_: 'NUMERIC'}
  ,'str'     : {_: 'CHAR', pg: 'VARCHAR' }
  ,'money'   : {_: 'MONEY' }
}

SqlPg = {
    type: "pg"
  , hasBooleans: true
  , esc:  v => ("'" + v.replace(/'/g, '\\\'') +  "'")
  , name: v => ('"' + v.replace(/"/g, '""') + '"')
}

SqlMy = {
    type: "my"
  , hasBooleans: true
  , esc: v => ("'" + v.replace(/'/g, '\\\'') +  "'")
  , name: v => ('`' + v.replace(/`/g, '``') + '`')      // TODO  check
}

SqlLt = {
    type: "lt"
  , hasBooleans: false
  , esc : v => ("'" + v.replace(/'/g, '\'\'') + "'")
  , name: v => ('"' + v.replace(/"/g,   '""') + '"')
}

SqlJsPseudo = { // This is used to get SQL expression without escaping
    type: "js"
  , hasBooleans: true
  , esc : v => v
  , name: v => v
}

let QParser = pmon.createLanguage({
  // lists and stuff
    select: r => r.selectItem.sepBy(r.comma).map( ls => new QList(ls) )
  , list  : r => r.value     .sepBy(r.comma).map( ls => new QList(ls) )
  , limit : r => pmon.alt( r.tokenv, r.nrpos ).sepBy(r.comma).map(v => new QLimit(v[0], v[1]))

  // values
  , value : r => pmon.alt( r.not, r.op, r.par, r.cnt, r.fni, r.fn, r.nr, r.true_, r.false_, r.null_, r.fld, r.tokenv )
  , true_ : () => pmon.string("true" ).result(qTrue)
  , false_: () => pmon.string("false").result(qFalse)
  , null_ : () => pmon.string("null" ).result(qNull)
  , cnt   : () => pmon.string('count(*)').result(new QRaw('count(*)'))
  , nr    : () => pmon.regexp(/-?(0|[1-9][0-9]*)([.][0-9]+)?([eE][+-]?[0-9]+)?/).map(Number).map(n => new QFloat(n))
  , nrpos : () => pmon.regexp(/[0-9]+/).map(Number).map(n => new QInt(n))
  , fn    : r => pmon.seq(r.token,                            pmon.string("("), r.value.sepBy(r.comma), pmon.string(")")).map(([name,    a,args]) => new QFn(name, null, args))
  , fni   : r => pmon.seq(r.token, pmon.string("."), r.token, pmon.string("("), r.value.sepBy(r.comma), pmon.string(")")).map(([name,p,i,a,args]) => new QFn(name, i   , args))
  , op    : r => pmon.seq(r.opVal, pmon.seq(r.opStr, r.opVal).atLeast(1)).map(([fst, ls]) => new QOp(fst, ls) )
  , par   : r => pmon.string("(").then(r.value).skip(pmon.string(')')).map(v => new QPar(v))
  , fld   : r => pmon.seq(r.token, pmon.string("."), r.token).map(([t,p,f]) => new QField(t,f))
  , not   : r => pmon.string("!").then(r.value).map(v => new QFn("not", null, [v]))

  // helpers
  , selectItem : r => pmon.alt( pmon.seq(r.value, pmon.string("@"), r.token).map(([v,at,as]) => new QAs(v, new QName(as))), r.value)
  , token : r => pmon.regexp(/[a-z_]([a-z0-9_])*/i)
  , tokenv: r => r.token.map(x => new QToken(x))
  , comma : () => pmon.string(",")
  , opStr : () => pmon.alt( pmon.string("=="), pmon.string("!=="), pmon.string("!="), pmon.string("<="), pmon.string(">="), pmon.oneOf('+-*/%=<>|:') )
  , opVal : r => pmon.alt( r.not, r.par, r.cnt, r.fni, r.fn, r.nr, r.true_, r.false_, r.null_, r.fld, r.tokenv) // everything like in value but without 'op' to avoid recursion
});

// use QSelect.tearUp to get right arguments from object
function QSelect(sel, from, where, group, having, ord, lim, opts) {
  this.select = sel;
  this.from   = from;
  this.where  = where;
  this.group  = group;
  this.having = having;
  this.order  = ord;
  this.limit  = lim;  

  this.opts = opts;
}
QSelect.prototype = protoQ({
  tableFilter: function (from, where, order) { // :: QFrom, QValue, QList
    if (typeof from === 'string') from = new QFrom(from);
    let sameTable = this.from.table === from.table;
    return new QSelect(
      this.select
    , this.from.tableFilter(from, where, order)
    , and( [ this.where // my where
           , !sameTable ? null : where // new where
           , !sameTable ? null : and(from.joins.filter(j => j.type === 'cross').map(j => j.on)) // all cross join whers
           ].filter(Boolean))
    , this.group, this.having
    , this.order
      ? (order ? this.order.append(order) : this.order)
      : (order ? order : null)
    , this.limit
    , this.opts);
  },
  setSelect: function (fields) {
    return new QSelect(
      new QList(fields.map(f => typeof f === 'string' ? QParser.value.tryParse(f) : jsToQVal(f)))
      , this.from, this.where, this.group, this.having, this.order, this.limit, this.opts);
  },
  tables: function () { return this.from.tables(); },
  sqlSnippet: function (Sql) {
    if (!this.select) throw new Error('query bug: select must be defined');
    let x = 'SELECT ' + (this.opts.distinct ? 'DISTINCT ' : '') +  (this.select).sqlSnippet(Sql) +
      prefIf(' FROM '    , (this.from   || new QEmpty() ).sqlSnippet(Sql)) + // can be missing in INSERT INTO ... SELECT ...
      prefIf(' WHERE '   , (this.where  || new QEmpty() ).sqlSnippet(Sql)) +
      prefIf(' GROUP BY ', (this.group  || new QEmpty() ).sqlSnippet(Sql)) +
      prefIf(' HAVING '  , (this.having || new QEmpty() ).sqlSnippet(Sql)) +
      prefIf(' ORDER BY ', (this.order  || new QEmpty() ).sqlSnippet(Sql)) +
      prefIf(' LIMIT '   , (this.limit  || new QEmpty() ).sqlSnippet(Sql));
    return x;
  },
  describe: function () {
    return { select: this.select.describe() } // TODO, at the moment only for template parseQuery function
  },
  setLimit: function (l = 0) {
    return new QSelect(this.select, this.from, this.where, this.group, this.having, null, new QLimit(l),this.opts  );
  },
  count: function () {
    return new QSelect(new QList([ new QAs(new QRaw('count(*)'), new QName('count')) ]), this.from, this.where, this.group, this.having, null, null, Object.assign({}, this.opts, { distinct: false }));
  },
  // here  is possible to make one traverse function but as far as I know using this.select[fn] is much slower than this.select.travToken
  mapSelect(fn) {
    if (!this.select) return this;
    return new QSelect(this.select.map(el => fn(el)) , this.from, this.where, this.group, this.having, this.order, this.limit, this.opts);
  },
  travToken (fn) {
    return new QSelect
    (!this.select  ? null : this.select.travToken(fn)
    ,!this.from    ? null : this.from  .travToken(fn)
    ,!this.where   ? null : this.where .travToken(fn)
    ,!this.group   ? null : this.group .travToken(fn)
    ,!this.having  ? null : this.having.travToken(fn)
    ,!this.order   ? null : this.order .travToken(fn)
    ,!this.limit   ? null : this.limit .travToken(fn)
    , this.opts);
  },
  travField (fn) {
    return new QSelect
    (!this.select  ? null : this.select.travField(fn)
    ,!this.from    ? null : this.from  .travField(fn)
    ,!this.where   ? null : this.where .travField(fn)
    ,!this.group   ? null : this.group .travField(fn)
    ,!this.having  ? null : this.having.travField(fn)
    ,!this.order   ? null : this.order .travField(fn)
    ,!this.limit   ? null : this.limit .travField(fn)
    , this.opts);
  },
  travFunc (fn) {
    return new QSelect
    (!this.select  ? null : this.select.travFunc(fn)
    ,!this.from    ? null : this.from  .travFunc(fn)
    ,!this.where   ? null : this.where .travFunc(fn)
    ,!this.group   ? null : this.group .travFunc(fn)
    ,!this.having  ? null : this.having.travFunc(fn)
    ,!this.order   ? null : this.order .travFunc(fn)
    ,!this.limit   ? null : this.limit .travFunc(fn)
    , this.opts);
  },
  async travTokenA (fn) {
    return new QSelect
    (!this.select  ? null : await this.select.travTokenA(fn)
    ,!this.from    ? null : await this.from  .travTokenA(fn)
    ,!this.where   ? null : await this.where .travTokenA(fn)
    ,!this.group   ? null : await this.group .travTokenA(fn)
    ,!this.having  ? null : await this.having.travTokenA(fn)
    ,!this.order   ? null : await this.order .travTokenA(fn)
    ,!this.limit   ? null : await this.limit .travTokenA(fn)
    , this.opts);
  },
  async travFieldA (fn) {
    return new QSelect
    (!this.select  ? null : await this.select.travFieldA(fn)
    ,!this.from    ? null : await this.from  .travFieldA(fn)
    ,!this.where   ? null : await this.where .travFieldA(fn)
    ,!this.group   ? null : await this.group .travFieldA(fn)
    ,!this.having  ? null : await this.having.travFieldA(fn)
    ,!this.order   ? null : await this.order .travFieldA(fn)
    ,!this.limit   ? null : await this.limit .travFieldA(fn)
    , this.opts);
  },
  async travFuncA (fn) {
    return new QSelect
    (!this.select  ? null : await this.select.travFuncA(fn)
    ,!this.from    ? null : await this.from  .travFuncA(fn)
    ,!this.where   ? null : await this.where .travFuncA(fn)
    ,!this.group   ? null : await this.group .travFuncA(fn)
    ,!this.having  ? null : await this.having.travFuncA(fn)
    ,!this.order   ? null : await this.order .travFuncA(fn)
    ,!this.limit   ? null : await this.limit .travFuncA(fn)
    , this.opts);
  }
});

// Function takes bunch of variables and finds variables that are special, parses them.
// Returns object with three properties:
// `drv` - extra variables, `vars` - variables that are not special
// and `Q` where are parsed variables { select, where, having, group, order, limit, let.*, where.* ...}
// if were no `select` then out has select neither
QSelect.tearUp = function (table, as = null, inputVars = {}) {
  if (arguments.length === 1) throw new Error("tearUp takes table, alias and object as vars");
  let { vars, drv } = Object.keys(inputVars).reduce((res, name) => {
    let isDrv = 
          ['select','where', 'having', 'order', 'group', 'limit'].includes(name)
      || name.match(/^(join|(r|c|l)join|where|having)\./)
      || name === 'opts'
      || name.match(/[:!~<>]$/) ; // special where: id:=1
  
    isDrv ? res.drv [name] = inputVars[name]
          : res.vars[name] = inputVars[name];

    return res;
  }, {vars: {}, drv: {}});

  // one exception
  if (vars.from && vars.from.isQValue) {
    drv.from = vars.from;
    delete vars.from;
  }

  let pval = QParser.value .tryParse.bind(QParser.value );
  let pls  = QParser.list  .tryParse.bind(QParser.list  );
  let psel = QParser.select.tryParse.bind(QParser.select);
  let onStr = fn => v => {
    if (typeof v === 'string') return fn(v);
    else return (typeof v === 'object' && v.isQValue) ? v : jsToQVal(v);
  }

  let D = Object.keys(drv);
  let w = D.filter(k => k.match(/^where\.?/ )).map(w => drv[w]).map(onStr(pval));
  let h = D.filter(k => k.match(/^having\.?/)).map(w => drv[w]).map(onStr(pval));

  // shortcuts
  D.map(k => k.match(/(.*)([:!~<>])$/)).filter(Boolean).forEach( ([key, left, op]) =>
    w.push(  QShortFn[op](  pval(left), typeof inputVars[key] === 'string' ? new QString(inputVars[key]) : jsToQVal(inputVars[key]) ) )
  );

  let jexp = /^(r|l|c|)join\.([a-zA-Z0-9_-]*)(@(.*))?/;
  let lim = drv.limit ? onStr(QParser.limit.tryParse.bind(QParser.limit))(drv.limit) : null;

  let Q = Object.assign(
    { from: drv.from ||
         new QFrom(table, as,
                D.map(v => v.match(jexp)).filter(Boolean)
                 .map(([key,typ,table,_x,as]) => new QJoin(table, typ || 'inner', as ? new QName(as) : new QEmpty(), onStr(pval)(drv[key]) )))
    }
    , !drv.select ? {} : { select : onStr(psel)(drv.select) }
    , !w.length   ? {} : { where  : and(w) }
    , !h.length   ? {} : { having : and(h) }
    , !drv.order  ? {} : { order  : onStr(pls)(drv.order) }
    , !drv.group  ? {} : { group  : onStr(pls)(drv.group) }
    , !lim        ? {} : { limit  : lim }
    , { opts: typeof drv.opts === 'string' ? Opts.parse(drv.opts, Opts.def) : drv.opts || Opts.def }
    );
  return { drv, vars, Q };
}
QSelect.create = function (table, as, vars) {
  let {Q} = QSelect.tearUp(table, as, vars);
  return new QSelect(Q.select, Q.from, Q.where, Q.group, Q.having, Q.order, Q.limit, Q.opts);
}

function QInsert (table, set) { // :: string, QValue
  this.table = table;
  this.set = set;
}
QInsert.prototype = protoQ({
  sqlSnippet(Sql) {
    return 'INSERT INTO ' + new QName(this.table).sqlSnippet(Sql) + ' (' + this.set.keys().sqlSnippet(Sql) + ') VALUES (' + this.set.values().sqlSnippet(Sql) + ')';
  }
  , sqlCommands: function* (Sql) { yield this.sqlSnippet(Sql) }
  , travToken: function (fn) { return new QInsert(this.table, this.set.travToken(fn)); }
  , travField: function (fn) { return new QInsert(this.table, this.set.travField(fn)); }
  , travFunc : function (fn) { return new QInsert(this.table, this.set.travFunc (fn)); }
  , travTokenA: async function (fn) { return new QInsert(this.table, await this.set.travTokenA(fn)); }
  , travFieldA: async function (fn) { return new QInsert(this.table, await this.set.travFieldA(fn)); }
  , travFuncA : async function (fn) { return new QInsert(this.table, await this.set.travFuncA (fn)); }
  , tableFilter: function () { return this }
  , wrFields: function () { return Object.keys(this.set.sets) }
});

function QUpdate (table, set, where) {
  this.table = table;
  this.set = set;
  this.where = where;
}
QUpdate.prototype = protoQ({
    sqlSnippet(Sql) { return 'UPDATE ' + new QName(this.table).sqlSnippet(Sql) + ' SET ' + this.set.sqlSnippet(Sql) + ' WHERE ' + this.where.sqlSnippet(Sql) }
  , sqlCommands: function* (Sql) { yield this.sqlSnippet(Sql); }
  , travToken: function (fn) { return new QUpdate(this.table, this.set.travToken(fn), this.where.travToken(fn)); }
  , travField: function (fn) { return new QUpdate(this.table, this.set.travField(fn), this.where.travField(fn)); }
  , travFunc : function (fn) { return new QUpdate(this.table, this.set.travFunc (fn), this.where.travFunc (fn)); }
  , travTokenA: async function (fn) { return new QUpdate(this.table, await this.set.travTokenA(fn), await this.where.travTokenA(fn)); }
  , travFieldA: async function (fn) { return new QUpdate(this.table, await this.set.travFieldA(fn), await this.where.travFieldA(fn)); }
  , travFuncA : async function (fn) { return new QUpdate(this.table, await this.set.travFuncA (fn), await this.where.travFuncA (fn)); }
  , wrFields: function () { return Object.keys(this.set.sets) }
});

function QDelete (table, where) { // :: string, QValue
  this.table = table;
  this.where = where;
}
QDelete.prototype = protoQ({
    sqlSnippet(Sql) { return 'DELETE FROM ' + new QName(this.table).sqlSnippet(Sql) + ' WHERE ' + this.where.sqlSnippet(Sql); }
  , sqlCommands: function* (Sql) { yield this.sqlSnippet(Sql); }
  , travToken: function (fn) { return new QDelete(this.table, this.where.travToken(fn)); }
  , travField: function (fn) { return new QDelete(this.table, this.where.travField(fn)); }
  , travFunc : function (fn) { return new QDelete(this.table, this.where.travFunc (fn)); }
  , travTokenA: async function (fn) { return new QDelete(this.table, await this.where.travTokenA(fn)); }
  , travFieldA: async function (fn) { return new QDelete(this.table, await this.where.travFieldA(fn)); }
  , travFuncA : async function (fn) { return new QDelete(this.table, await this.where.travFuncA (fn)); }
});

function QReplace (table, set, where) {
  this.table = table;
  this.set = set;
  this.where = where;
}
QReplace.prototype = protoQ({
    sqlSnippet(Sql) { throw new Error('sqlSnippet can not be used on QReplace because it has many commands; use sqlCommands instead') }
  , sqlCommands: function* (Sql) {
      let affected = yield 'UPDATE ' + new QName(this.table).sqlSnippet(Sql) + ' SET ' + this.set.sqlSnippet(Sql) + ' WHERE ' + this.where.sqlSnippet(Sql);
      if (affected === 0) 
        yield 'INSERT INTO ' + new QName(this.table).sqlSnippet(Sql) + ' (' + this.set.keys().sqlSnippet(Sql) + ') VALUES (' + this.set.values().sqlSnippet(Sql) + ')';
      else yield null;
  }
  , travToken: function (fn) { return new QReplace(this.table, this.set.travToken(fn), this.where.travToken(fn)); }
  , travField: function (fn) { return new QReplace(this.table, this.set.travField(fn), this.where.travField(fn)); }
  , travFunc : function (fn) { return new QReplace(this.table, this.set.travFunc (fn), this.where.travFunc(fn)); }
  , travTokenA: async function (fn) { return new QReplace(this.table, await this.set.travTokenA(fn), await this.where.travTokenA(fn)); }
  , travFieldA: async function (fn) { return new QReplace(this.table, await this.set.travFieldA(fn), await this.where.travFieldA(fn)); }
  , travFuncA : async function (fn) { return new QReplace(this.table, await this.set.travFuncA (fn), await this.where.travFuncA (fn)); }
  , wrFields: function () { return Object.keys(this.set.sets) }
});


function makeSet (set, table, schema, inputRow) {
    let allFields = schema.filter(f => f._ === 'field' && f.table === table).map(f => f.name);
    let wrFields  = schema.filter(f => f._ === 'field' && f.table === table && f.write).map(f => f.name);

    return QSet.fromObject(table, inputRow).filter(wrFields).appendSet(set).filter(allFields);
}

async function fillUp(q, vars, custFn, meta, evArg) {
    let lets = letReplacer(vars);

    q = q
      .travFunc (lets.func)
      .travToken(lets.vars)
      .travFunc (custFnReplacer(custFn, evArg))
      .travToken(T => typeof vars[T.token] === 'undefined' ? T : jsToQVal(vars[T.token]));

    q = await q.travFieldA(async f => {
        let T = meta[f.table] ? await meta[f.table]() : null;
        if (!T && f.table ==='auth') throw new NeedAuth();
        if (!T) return f;
        return jsToQVal(T[f.field] || null);
    });

    return q;
}

class WriteRule {
  constructor (inf) {
    Object.assign(this, inf);
    if (!inf.table ) throw "Write-rule must have `table` (" + JSON.stringify(inf) + ')';
    if (!inf.on    ) throw "Write-rule must have `on` (" + JSON.stringify(inf) + ')';
    if (!inf.action) throw "Write-rule must have `action` (" + JSON.stringify(inf) + ')';
    if (!([ "replace", "upsert", "insert", "update", "delete", "error"].includes(this.action))) 
      throw "Write rule `action` must be either `insert`, `update`, `delete`, `replace`, `upsert` or `error`, not `" + this.action + '`';

    if (typeof this.on !== 'string') throw "Write-rule `on` must be string (currently: " + JSON.stringify(this.on) + ")";

    this.message = inf.message; // error message
    this.on  = QParser.value.tryParse(this.on);
    this.set = QSet.fromObjectExpr(inf.table, inf.set || {})
    if (this.where) {
      if (typeof this.where !== 'string') throw "Write-rule `where` must be string (currently: " + JSON.stringify(this.where) + ")";
      this.where = QParser.value.tryParse(this.where);
    }

    if (this.returning) {
      let m, R = this.returning;
      if (m = R.match(/seq +(\w+) +(\w+)/)) this.retWhere = new QTempl('returning', '$0 = currval($1)'     , [ new QField(this.table, m[1]), new QString(m[2]) ]); else
      if (m = R.match(/autonum +(\w+)/))    this.retWhere = new QTempl('returning', '$0 = last_insert_id()', [ new QField(this.table, m[1]) ]);
      else this.retWhere = QParser.value.tryParse(this.returning);
    }
  }
  async match(inp, schema, vars = {}, custFn = {}, meta, evArg = {}) {
    if (!this.tester) this.tester = await testSql;

    let allFields = Set.fromArray(schema.filter(f => f._ === 'field' && f.table === this.table).map(f => f.name));
    vars = Object.assign({}, vars, this.vars);

    let newRow = Object.onValues(inp, v => jsToQVal(v));
    let travNew = f => f.table === 'new' ? newRow[f.field] || new QNull() : f;

    let tokenFn = T => {
      if (allFields.has(T.token)) return T.toField(this.table);
      if (typeof vars[T.token] !== 'undefined') return jsToQVal(vars[T.token]);
      else return T.toString();
    }
    let fill = async q => (await fillUp(q, vars, custFn, meta, evArg)).travField(travNew).travToken(tokenFn);
    let isMatch = (await this.tester.all('SELECT ' + new QAs(this.on.travField(travNew).travToken(tokenFn), new QName('t')).sqlSnippet(SqlLt))) [0].t;
    if (!isMatch) return null;
    else switch(this.action) {
      case 'delete' : return await fill(new QDelete (this.table, this.where)); break;
      case 'insert' : return await fill(new QInsert (this.table, makeSet(this.set, this.table, schema, inp))); break;
      case 'update' : return await fill(new QUpdate (this.table, makeSet(this.set, this.table, schema, inp), this.where)); break;
      case 'replace': return await fill(new QReplace(this.table, makeSet(this.set, this.table, schema, inp), this.where)); break;
      // case 'upsert' : return new Error('TODO: upsert');
      case 'error' : throw this.message; break;
      default: throw new Error("BUG");
    }
  }
}

let prefIf = (pref, txt)  => txt ? pref + txt : '';

function and(ls) {
  if (!ls) return new QEmpty();
  ls = ls.filter(x => !x.isEmpty);
  if (ls.length === 0) return new QEmpty();  else
  if (ls.length === 1) return ls[0];         else
  return new QOp(new QPar(ls[0]), ls.slice(1).map(w => [ 'AND', new QPar(w)]));
}

// Takes object, return function that replaces all variables in QValue
// Use as someValue.travToken(letVarReplacer({"let.foo":"blahh", ...}))
letReplacer = function (vars) {
  let D = Object.keys(vars);
  let letVar = D
      .map(k => k.match(/^let\.([a-zA-Z0-9_]+)$/)).filter(Boolean) // match and filter only vars
      .map(([key, l]) => { return { key: l, val: typeof vars[key] === 'string' ? QParser.value.tryParse(vars[key]) : jsToQVal(vars[key]) } })
      .reduce((a,c) => Object.assign(a,{[c.key]: c.val}), {}); // make one object

  let letFn = D.map(k => k.match(/^let\.([a-zA-Z0-9_]+)\(([a-zA-Z0-9_,]+)\)$/)).filter(Boolean).map(([key, name, args]) => {
    // TODO: it would be good if I parse here and later on use this value but I should have QValue.copy first
    return { fn: name, val: QParser.value.tryParse(vars[key]), args: args.split(',') }
  }).reduce((a,c) => Object.assign(a, {[c.fn]: c}), {});

  let repl = {
    vars: T => { while (letVar[T.token]) { T = letVar[T.token] }; return T; }, // Recursive 
    func: function (fn) {
      let lfn = letFn[fn.name];
      if (!lfn) return fn; // return self if nothing to change
      if (fn.args.length !== lfn.args.length) throw "Let function " + fn.name + " has different number of arguments";
      // lfn.args is list of strins (for `let.x(a,b,c)` it is ["a","b","c"]
      // map these strings and put called arguments into place where argument token is in place 
      // and then put let-in-let into work
      return lfn.args.reduce((expr, arg, i) => expr.travToken(T => T.token === arg ? fn.args[i].travToken(repl.vars).travFunc(repl.func) : T), lfn.val);
    }
  }

  // let in let
  letFn  = Object.onValues(letFn , ({fn, val, args}) => {return {fn: fn, args: args, val: val.travToken(repl.vars).travFunc(repl.func) }});
  letVar = Object.onValues(letVar, v => v.travFunc(repl.func).travToken(repl.vars));

  // FIXME hack
  letFn  = Object.onValues(letFn , ({fn, val, args}) => {return {fn: fn, args: args, val: val.travToken(repl.vars).travFunc(repl.func) }});
  letVar = Object.onValues(letVar, v => v.travFunc(repl.func).travToken(repl.vars));
  letFn  = Object.onValues(letFn , ({fn, val, args}) => {return {fn: fn, args: args, val: val.travToken(repl.vars).travFunc(repl.func) }});
  letVar = Object.onValues(letVar, v => v.travFunc(repl.func).travToken(repl.vars));
  letFn  = Object.onValues(letFn , ({fn, val, args}) => {return {fn: fn, args: args, val: val.travToken(repl.vars).travFunc(repl.func) }});
  letVar = Object.onValues(letVar, v => v.travFunc(repl.func).travToken(repl.vars));

  // as let rules can be recursive we don't know when we are done by replacing, therefore we do it until nothing replaced
  // this code is trying to optimize code by not doing traversion on elements that has no tokens and it counts tokens
  // not sure if this really is faster :/ (compared to serializing stuff or smth)
  // TODO: letFn is still not covered
  // TODO: should do tree of let dependencies letDepends = [ v: letExpr, depends: { ... }]
  let changes, lv = Object.values(letVar).map(v => { let tc = 0; v.travToken(t => { tc++; return t }); return { val: v, tokens: tc } });
  do {
    changes = 0;
    lv = lv.map(el => {
      if (el.tokens) {
        let el_ = el.val.travToken(repl.vars); // TODO repl.vars should give way to count tokens on the fly
        if (!el_.isSame(el.val)) {
          el.tokens = 0;
          el.val = el_;
          el_.travToken(T => { el.tokens++; return T; });
          changes++;
        }
      }
      return el;
    });
  } while (changes > 0);
  letVar = Object.onValues(letVar, (v,k,i) => lv[i].val );

  return repl;
}

/*
letReplacer= function (vars) {
  let letVar = new Map(), letFn = new Map();

  let D = Object.keys(vars).filter(k => k.substr(0,4) === 'let.');
  let L = new Map();
  D .map(k => k.match(/^let\.([a-zA-Z0-9_]+)$/)).filter(Boolean) // match and filter only vars
    .forEach(([key, l]) => L.set('$' + l, QParser.value.tryParse(vars[key])));

  D .map(k => k.match(/^let\.([a-zA-Z0-9_]+)\(([a-zA-Z0-9_,]+)\)$/)).filter(Boolean)
    .forEach(([key, name, args]) => L.set('!' + name, { name: name, val: QParser.value.tryParse(vars[key]), args: args.split(',') }));

  // interface to add dependencies and say it is resolved (rmDepen
  let deps = new Map();
  let addDep = (what, dependsOn) => deps.set(what, (deps.get(what) || new Set()).add(dependsOn));
  let remDep = (what, dependsOn) => { let s = deps.get(what); s.delete(dependsOn); if (s.size === 0) deps.delete(what); }

  // find and set dependecies
  L.forEach((val, key) => {
    let typ = key.substr(0,1);
    let D = deps.get(key) || new Set();
    if      (typ === '!') val.val.travToken(T => { if (L.has('$' + T.token)) addDep(key, '$' + T.token); return T; })
    else if (typ === '$') val    .travToken(T => { if (L.has('$' + T.token)) addDep(key, '$' + T.token); return T; })

    if      (typ === '!') val.val.travFunc(F => { if (L.has('!' + F.name)) addDep(key, '!' + F.name); return F; })
    else if (typ === '$') val    .travFunc(F => { if (L.has('!' + F.name)) addDep(key, '!' + F.name); return F; })
  })

  let repl = {
    vars: T => letVar.get(T.token) || T,
    func: function (fn) {
      let lfn = letFn.get(fn.name);
      if (!lfn) return fn; // return self if nothing to change
      if (fn.args.length !== lfn.args.length) throw "Let function " + fn.name + " has different number of arguments";
      // lfn.args is list of strins (for `let.x(a,b,c)` it is ["a","b","c"]
      // map these strings and put called arguments into place where argument token is in place 
      // and then put let-in-let into work
      return lfn.args.reduce((expr, arg, i) => expr.travToken(T => T.token === arg ? fn.args[i].travToken(repl.vars).travFunc(repl.func) : T), lfn.val);
    }
  }

  // deepToken looks for dependencies and if there is any then does dependencies first, holding return back
  let deepToken = T => {
    let N = '$' + T.token, l, d, n;    // N as name, let value, dependencies and new value
    if (! (l = L   .get(N))) return T; // check if there is this let variable; if not, return token as it is
    if (! (d = deps.get(N))) return l; // ask if something depends on this; if not, return let value as it is

    L.set(N, n = l.travToken(deepToken)); // if depencies, we are going deeper and replace value in L and ask later it again
    return n;
  }
  let deepFunc = F => {
    let N = '!' + F.name, l, d, n;
    if (! (l = L   .get(N))) return F;
    if (d) L.set(N, l = Object.assign(F, { val: F.val.travToken(deepToken).travFunc(deepFunc) }));
    return l.args.reduce((V, arg, i) => V.travToken(T => T.token === arg ? l.args[i].travToken(repl.vars).travFunc(repl.func) : T), l.val);
  }

  L.forEach((val, key) => {
    let typ = key.substr(0,1);
    if (typ === '$') letVar.set(key.substr(1), val.travToken(deepToken));
    if (typ === '!') letFn .set(key.substr(1), Object.assign(val, {val: val.val.travToken(deepToken) }) );
  });

  return repl;

  // let in let
  // letFn  = Object.onValues(letFn , ({fn, val, args}) => {return {fn: fn, args: args, val: val.travToken(repl.vars).travFunc(repl.func) }});
  // letVar = Object.onValues(letVar, v => v.travFunc(repl.func).travToken(repl.vars));

  // as let rules can be recursive we don't know when we are done by replacing, therefore we do it until nothing replaced
  // this code is trying to optimize code by not doing traversion on elements that has no tokens and it counts tokens
  // not sure if this really is faster :/ (compared to serializing stuff or smth)
  // TODO: letFn is still not covered
  // TODO: should do tree of let dependencies letDepends = [ v: letExpr, depends: { ... }]
}
*/

custFnReplacer = function (vars, arg) {
  let rfn = fn => {
    let F = vars[fn.name];
    if (typeof F === 'string'  ) return new QTempl(fn.name, F , fn.args.map(a => a.travFunc(rfn)));
    if (typeof F === 'function') return new QTempl(fn.name, F(arg, fn.inf, fn.args.length), fn.args.map(a => a.travFunc(rfn))); // TODO async
    else return fn;
  }
  return rfn;
}

function jsToQVal(v) {
  if (v === null) return new QNull();
  if (v === true) return new QBool(true);
  if (v === false) return new QBool(false);
  if (typeof v === 'string') return new QString(v);
  if (typeof v === 'number') return new QFloat(v);
  if (typeof v === 'object' && v.isQValue) return v;
  if (typeof v === 'undefined') return new QNull(); // oh, well...
  throw "Can't convert value to QVal (" + JSON.stringify(v) + ")";
}

module.exports.Select  = QSelect;
module.exports.WriteRule = WriteRule;
module.exports.jsToQVal = jsToQVal;
module.exports.expression = s => QParser.value.tryParse(s)
/*
module.exports.SqlPg   = SqlPg;
module.exports.SqlLt   = SqlLt;
module.exports.SqlMy   = SqlMy;
*/


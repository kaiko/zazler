

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
, "concat" : [ ">1", (f, S, i, args) => (S.type === 'lt' || S.type === 'pg') ? opFn('||')(f,S,i,args) : sameFn(f,S,i,args) ]
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

function ifFn (fnName, Sql, inf, args) {
  var i, cond = [], els = args.length % 2 ? ' ELSE ' + args[args.length - 1].sqlSnippet(Sql) : '';
  
  for (i = 0; i < args.length - 1; i += 2)
    cond.push('WHEN ' + args[i].sqlSnippet(Sql) + ' THEN ' + args[i + 1].sqlSnippet(Sql));
  return 'CASE ' + cond.join(' ') + els + ' END';
}

module.exports.QFunctions = QFunctions;


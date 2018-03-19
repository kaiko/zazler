
var refMatch = /\(([^\)]+)\) REFERENCES ([^(]+)\(([^)]+)\)/;

if (!Object.assign) {
  Object.defineProperty(Object, 'assign', {
    enumerable: false,
    configurable: true,
    writable: true,
    value: function(target, firstSource) {
      'use strict';
      if (target === undefined || target === null) {
        throw new TypeError('Cannot convert first argument to object');
      }

      var to = Object(target);
      for (var i = 1; i < arguments.length; i++) {
        var nextSource = arguments[i];
        if (nextSource === undefined || nextSource === null) {
          continue;
        }

        var keysArray = Object.keys(Object(nextSource));
        for (var nextIndex = 0, len = keysArray.length; nextIndex < len; nextIndex++) {
          var nextKey = keysArray[nextIndex];
          var desc = Object.getOwnPropertyDescriptor(nextSource, nextKey);
          if (desc !== undefined && desc.enumerable) {
            to[nextKey] = nextSource[nextKey];
          }
        }
      }
      return to;
    }
  });
}

/*
// Not used in this format
function getRef(res) { 
  return Object.keys(res.tableMeta).
    filter(function (k) { return k.substr(0, 5) == 'fkey:' }).
    map(function(k) {
        var m = res.tableMeta[k].match(refMatch);
        return !m ? null :
        { fTable : m[2]
        , fKeys  : m[3].split(', ')
        , myKeys : m[1].split(', ')
        };
    }).filter(function (n) { return n !== null })
}
*/

/*
 * Make list of references to given table.
 */
function fkeysTo(table) {
  return query("_meta",
    { where: "like(value,REF)", REF: "%) REFERENCES " + table + "(%"
    , select: "tablename,value"
    , opts: "map"}).
  data.
  map(function (r) { r['fkey'] = r.value.match(refMatch); return r; }).
  filter(function (r) { return r.fkey !== null }).
  map(function (r) {
    return {
        fromTable:  r.tablename
      , fromFields: r.fkey[1].split(', ')
      , toFields:   r.fkey[3].split(', ') } });
}

// Filters variables with given namespace
function filterNS(vars, ns) {
  var r = {};
  Object.keys(vars).forEach(function(key) {
    if (key.substr(0, ns.length + 1) == ns + ":") 
      r[key] = vars[key];
  });
  return r;
}

// Variable that must be defined
if (!vars.sub) throw "tree.json expect variable 'sub' to be described. Use json if tree is not needed.";

// ref[table] = [ references-to-this-table ]
var ref = {}
var sub = (vars.sub || '').split(',')
sub.concat([req.table]).forEach(function (t) {
  var fk = fkeysTo(t);
  if (fk.length) ref[t] = fk;
})

/*
 * subWhere           -- describes subqueries that have to be made
 *  [table]           -- for what table it describes
 *   [fromTable]      -- from what table queries have to be made
 *    = { varId:    _ -- variable identificator (`"V" + num` is used to set variables)
 *      , refField: _ -- remote field with table
 *      , valField: _ -- where value is taken from here
 *      }
 */
var varC = 0       // identification variable counter
var subWhere = {} 
// fill subWhere, walk thru related tables
Object.keys(ref).forEach(function (forTable) {

  // initialize subWhere strukture as it will be
  subWhere[forTable] = {};
  // ref[forTable].forEach(function(refDesc) { subWhere[forTable][refDesc.fromTable] = {} });
  
  ref[forTable].
    // filter(function (r) { return r.fromTable == forTable }).
    forEach(function (r) {
      subWhere[forTable][r.fromTable] = []
      r.toFields.forEach(function (valField, idx) {
        subWhere[forTable][r.fromTable].push( { varId: ++varC, valField: valField, refField: r.fromTable + "." + r.fromFields[idx] } )
      })
    });
})

// TODO: check if all tables exist and all refernces work well

function fillSub(res) {
  
  // if no subqueries found, return
  var wheres = subWhere[res.tableName];
  if (!wheres) return;
  var whereTables = Object.keys(wheres);

  // for each row, for each where table for every field
  res.data.forEach(function (row) {
    whereTables.forEach(function (table) {
      var wVar = {}
      wheres[table].forEach(function (w) {
        wVar['where.' + w.varId] = w.refField + '=V' + w.varId;
        wVar['V' + w.varId] = row[ w.valField ];
      });
      var subResult = query(table, Object.assign(wVar, filterNS(vars, table)));
      fillSub(subResult);
      row['@' + table] = subResult.data;
    })
  })
}
fillSub(result);

print(JSON.stringify(result.data));
// print(JSON.stringify({sub: sub, ref: ref, subWhere: subWhere, result: result}));



let optAlias = {
  'dist': 'distinct'
}
let def = {
  askpw : true
, body  : false
, head  : true
, map   : true
, save  : false
, text  : false
, point : true
, comma : false
, distinct: false
, transaction: false
}

module.exports = {
  def: def,
  parse: (o, d = {}) => 
    o.split(/,/g).map(x => {
      let [_, s, opt] = x.match(/([ +-]?)(.+)/);
      return {[optAlias[opt] || opt]:  s === '-' ? false : true };
    }).reduce((a, o) => Object.assign(a, o), Object.assign({}, d)) // Object.assign to avoid changing d object (often default)
}


const wildcard = require('wildcard');

class AccessRule {
  constructor(rule) {
    this.rule = rule;
    this.rules = [];
    if (!rule) this.rules = [];
    else this.doParsing();
  }
  doParsing() {
    var m = this.rule.match(/-?[a-z0-9_*-]+(\(.*?\))?\s*/g).map(r => {
      let rm = r.trim().match(/(-?)([a-z0-9_*-]+)(\((.*)\))?/);
      let R = { adding: rm[1] === '-' ? false : true, table: rm[2], fields: rm[4] ? rm[4].trim().split(/[, ]+/) : ['*'], points: 0 };
      // points, lower gets rule more prioritized
      // specific and negative rules first
      if (R.adding === false         ) R.points -= 1000;
      if (R.table.indexOf('*') === -1) R.points -= 100;
      this.rules.push( R );
    });
    this.rules.sort((a,b) => a.points - b.points);
  }
  match(table, field, def = null) {
    for (let i = 0; i < this.rules.length; i++) {
      let r = this.rules[i];
      if (wildcard(r.table, table)) 
        for (let f = 0; f < r.fields.length; f++)
          if (wildcard(r.fields[f], field)) {
            // console.log(JSON.stringify(r,null,3));
            return r.adding;
          }
    }
    return def;
  }
}

module.exports.AccessRule = AccessRule;


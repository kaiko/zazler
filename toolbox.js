

if (!Object.values) Object.values = o => Object.keys(o).map(k => o[k]);
Object.map  =       (o, fn) => { let i = 0, r = {}; for (let k in o) r[k] =       fn(o[k], k, i++); return r; }
Object.mapA = async (o, fn) => { let i = 0, r = {}; for (let k in o) r[k] = await fn(o[k], k, i++); return r; }
Set.fromArray = a => a.reduce((s, e) => s.add(e), new Set());

Map.prototype.setWith = function (key, val, fn) {
  if (this.has(key)) this.set(key, fn(this.get(key), key));
  else this.set(key, val); 
}

Map.prototype.fromArray = (ls, key, valFn) => ls.reduce((m, i) => m.set(i[key], valFn ? valFn(i) : val), new Map())

module.exports = {
  trace:  (v, mark) => { console.log(mark ? mark + '\n' + v + '\n/' + mark : v); return v; },
  breakOn: (str, on) => (pos => pos === -1 ? [str] : [str.substr(0,pos), str.substr(pos+1) ])( str.indexOf(on) ),
  zipObject: (lsKeys, lsValues) => lsKeys.reduce((k,o,i) => Object.assign(o, {[k]: lsValues[i]}), {}),
  getBody: req => new Promise(ok => { let b = ''; req.on('data', c => b += c); req.on('end', () => ok(b)) }),
  btoa: s => new Buffer(s, 'base64').toString('binary')
}


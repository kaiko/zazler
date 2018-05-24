

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
Object.map = Object.onValues;

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


/*
var fn = new Function("obj",
   "var p=[],print=function(){p.push.apply(p,arguments);};" +
   "with(obj){p.push('" +
    template
       .replace(/[\r\t\n]/g, " ")
       .split("<%").join("\t")
       .replace(/((^|%>)[^\t]*)'/g, "$1\r")
       .replace(/\t=(.*?)%>/g, "',$1,'")
       .split("\t").join("');")
       .split("%>").join("p.push('")
       .split("\r").join("\\'")
   + "');}return p.join('');");

print(fn( this ));
*/

fnBody = '' +
    'function _out( val ) { if (! (val === null || typeof val === "undefined")) print(val.toString()); }\n' +
    'function escHtml(str) { return !str ? \'\' : String(str) .replace(/&/g, "&amp;") .replace(/"/g, "&quot;") .replace(/\'/g, "&#39;") .replace(/</g, "&lt;") .replace(/>/g, "&gt;"); }\n' +
    'encUrl = encodeURIComponent;\n' +
    template.split("%>").map(function (p) {
        return p.split("<%").map(function (pp, i) {
          if (!pp) return ""; else
          if (i && pp.charAt(0) == "=") return "_out(" + pp.substr(1) + ");"; else // TODO: zazler should take any value
          if (i && pp.charAt(0) == "~") return "print( escHtml(" + pp.substr(1) + ") );"; else
          if (!i) return "print( " + JSON.stringify(pp) + " );"; else
          return pp;
        }).join("\n");
      }).join("\n");


// contentType('text/plain', 'utf-8'); print(fnBody);
contentType("text/html", "utf-8");
let AFn = Object.getPrototypeOf(async function(){}).constructor;
let f = new AFn(fnBody);
await f();


contentType('text/xml', 'utf-8');
header('SOAPAction', "http://www.w3.org/2003/05/soap-envelope");
function escHtml(str) { return str; return !str ? '' : String(str).replace(/&/g, "&amp;").replace(/"/g, "&quot;") .replace(/'/g, "&#39;") .replace(/</g, "&lt;") .replace(/>/g, "&gt;"); }
(function () { 
var escXml = escHtml;

var T = { str: 'string' };
var cols = JSON.parse(JSON.stringify(result.cols));
var S = escXml(req.table + "Response");

var O = '<?xml version="1.0" encoding="UTF-8"?>' +
'\n<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://producer.x-road.eu" xmlns:iden="http://x-road.eu/xsd/identifiers" xmlns:xrd="http://x-road.eu/xsd/xroad.xsd">' +
'\n    <SOAP-ENV:Header>' +
vars.soapHeader + 
'\n    </SOAP-ENV:Header>' +
'\n    <SOAP-ENV:Body>' +
'\n        <ns1:' + S + '>' +
'\n' +
  result.data.map(r =>
    '<row>\n' +
      cols.map((c,ci) => '<' + c + ' type="' + ((x => T[x] || x)(result.types[ci])) + '">' + escHtml(r[c]) + '</' + c + '>').join('\n') +
    '\n</row>'
  ).join("\n") +
'\n        </ns1:' + S + '>' +
'\n    </SOAP-ENV:Body>' +
'\n</SOAP-ENV:Envelope>';

print(O);
})()

/*
print('<?xml version="1.0"?>');
print('<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:m="http://www.example.org/stock/Manikandan">');
print('<soap:Header>');
print('</soap:Header>');
print('<soap:Body>');
print('     <m:getResponse>');
print('      </m:getResponse>');
print('</soap:Body>');
print('</soap:Envelope>');
*/

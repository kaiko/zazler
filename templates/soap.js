header('SOAPAction', '""');
contentType('text/xml');

normXml = x => typeof x === 'string' ? x.replace(/[^a-z0-9]+/gi, '_') : x
escXml = unsafe => typeof unsafe === 'string' ? unsafe.replace(/&/g, '&amp;').replace(/[<>'"]/g, c => escXml.chars[c]) : unsafe;
escXml.chars = {
    '<': '&lt'
  , '>': '&gt'
  , "'": '&apos;'
  , '"': '&quot;'}

var T = { str: 'string' };
var cols = JSON.parse(JSON.stringify(result.cols));

var tableName = req.tableAs || req.table;
var S = normXml(tableName) + 'Response';

var O = '<?xml version="1.0" encoding="UTF-8"?>' +
'\n<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://producer.x-road.eu" xmlns:iden="http://x-road.eu/xsd/identifiers" xmlns:xrd="http://x-road.eu/xsd/xroad.xsd">' +
'\n    <SOAP-ENV:Header>' +
vars.soapHeader + 
'\n    </SOAP-ENV:Header>' +
'\n    <SOAP-ENV:Body>' +
'\n        <ns1:' + S + '>' +
'\n';

if (! ['insert','update','delete'].includes(vars.servtype)) {
  O += result.data.map(r => (
    '<row>\n' +
      cols.map((c,ci) => '<' + c + ' type="' + ((x => T[x] || x)(result.types[ci])) + '">' + escXml(r[c]) + '</' + c + '>').join('\n') +
    '\n</row>'
  )).join("\n")
} else {
    let P = await post(req.table, vars, [vars]);
    //O += '<!-- ' + JSON.stringify(P) + ' -->\n'
    O += '<result>' + (P.affected.length ? P.affected[0] : '0') + '</result>';
}

O+= '\n        </ns1:' + S + '>' +
    '\n    </SOAP-ENV:Body>' +
    '\n</SOAP-ENV:Envelope>';

print(O);

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

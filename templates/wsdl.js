
// contentType('application/wsdl+xml');
contentType('text/xml');

normXml = x => typeof x === 'string' ? x.replace(/[^a-z0-9]+/ig, '_') : x;
toString = b => (typeof b !== 'undefined' && b !== null && b.toString) ? b.toString() : b; // this is because of buffer actually
escXml = unsafe =>
  typeof unsafe !== 'string' ? unsafe : // if not string, leave it as is
     unsafe.replace(/[<>'"&]/g, c => escXml.chars[c]) // usual xml entities
            .replace(/[\000-\032]/g, c => "&#x" + c.charCodeAt().toString(16).toUpperCase() + ';'); // control characters
escXml.chars = {
    '<': '&lt;'
  , '>': '&gt;'
  , "&": '&amp;'
  , "'": '&apos;'
  , '"': '&quot;'}

function getVars(expr) {
  var strs = [];
  var walk = function (e) {
    if (!e) return; // not nice
    if (Array.isArray(e)) e.map(walk);
    // else if (e.str        ) { strs.push( e.str );  } // goal -- collect str
    else if (e.operators) { walk(e.expression); e.operators.forEach(e => walk(e)); }
    else if (e.var        ) { strs.push( e.var );  }
    else if (e.parentheses) { walk(e.parentheses); }
    else if (e.func       ) { func.args.map(walk); }
    else if (e.expression) {
      walk(e.expression[0]); // left side of operator
      walk(e.expression[2]); // right side of operator
    }
  }
  walk(expr);
  return strs;
}

function getFields(expr) {
  var strs = [];
  var walk = function (e) {
    if (!e) return; // not nice
    if (Array.isArray(e)) e.map(walk);
    else if (e.operators) { walk(e.expression); e.operators.forEach(e => walk(e)); }
    else if (e.field      ) { strs.push( e.field );  }
    else if (e.parentheses) { walk(e.parentheses); }
    else if (e.func       ) { func.args.map(walk); }
    else if (e.expression) {
      walk(e.expression[0]); // left side of operator
      walk(e.expression[2]); // right side of operator
    } else if (Array.isArray(e.select)) {
        e.select.forEach(walk);
    }
  }
  walk(expr);
  return strs;
}

var neededArgs, typeAlias, NS, TableX, ReqName, ResName;
var tableName = (r => r.as || r.table)(result.explainQuery().from);

TableX = normXml(tableName);
ReqName = TableX;
ResName = TableX + "Response";

typeAlias = { str: 'string' };
isWrite =  ['insert','update','delete'].includes((vars||{}).servtype);

NS = (this.schema || 'https') + '://' + (this.hostname || '127.0.0.1') + req.url;

if (!isWrite) {
neededArgs = getVars(result.explainQuery().where);

//////////////////////////////////////
var O = '<?xml version="1.0" encoding="UTF-8"?>' +
'\n<wsdl:definitions targetNamespace="http://producer.x-road.eu" ' +
'\n        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"' +
'\n        xmlns:tns="http://producer.x-road.eu"' +
'\n        xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"' +
'\n        xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"' +
'\n        xmlns:xmime="http://www.w3.org/2005/05/xmlmime"' +
'\n        xmlns:ref="http://ws-i.org/profiles/basic/1.1/xsd"' +
'\n        xmlns:xs="http://www.w3.org/2001/XMLSchema"' +
'\n        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">' +

'\n    <wsdl:types>' +
'\n        <schema targetNamespace="http://producer.x-road.eu" xmlns="http://www.w3.org/2001/XMLSchema">' +
'\n        <import namespace="http://x-road.eu/xsd/xroad.xsd" schemaLocation="http://x-road.eu/xsd/xroad.xsd"/>' +

'\n    <xs:complexType name="fault">' +
'\n      <xs:sequence>' +
'\n        <xs:element name="faultCode" type="xs:string">' +
'\n          <xs:annotation>' +
'\n            <xs:appinfo>' +
'\n              <xrd:title xml:lang="et">Fault Code</xrd:title>' +
'\n              <xrd:title xml:lang="en">Fault Code</xrd:title>' +
'\n            </xs:appinfo>' +
'\n          </xs:annotation>' +
'\n        </xs:element>' +
'\n        <xs:element name="faultString" type="xs:string">' +
'\n          <xs:annotation>' +
'\n            <xs:appinfo>' +
'\n              <xrd:title xml:lang="et">Vea kirjeldus</xrd:title>' +
'\n              <xrd:title xml:lang="en">Fault explanation</xrd:title>' +
'\n            </xs:appinfo>' +
'\n          </xs:annotation>' +
'\n        </xs:element>' +
'\n      </xs:sequence>' +
'\n    </xs:complexType>' +

'\n    <xs:element name="' + ReqName + '">' +
"\n      <xs:complexType> " +
"\n        <xs:sequence>" +
                neededArgs.map(a =>
                    "\n <xs:element name='" + a + "' type='string'>" +
                    '   <xs:annotation><xs:appinfo>' +
                    "   <xrd:title xml:lang='en'>Input " + escXml(a) + "</xrd:title>" +
                    "   <xrd:title xml:lang='et'>Sisend " + escXml(a) + "</xrd:title>" +
                    "</xs:appinfo></xs:annotation>" +
                    " </xs:element>"
                ).join('\n') +
"\n        </xs:sequence> " +
"\n      </xs:complexType>" +
"\n    </xs:element>" +

'\n     <xs:element name="' + ResName  + '">' +
"\n       <xs:complexType>" +
"\n         <xs:sequence>" +
'\n          <xs:element name="row" maxOccurs="unbounded">' +
"\n             <xs:annotation>" +
"\n               <xs:appinfo>" +
'\n                 <xrd:title xml:lang="et">väljundkirje teenusest ' + escXml(tableName) + '</xrd:title>' +
'\n                 <xrd:title xml:lang="en">output row of ' + escXml(tableName) + '</xrd:title>' +
"\n               </xs:appinfo>" +
"\n             </xs:annotation>" +
"\n             <xs:complexType><xs:sequence>" +
// "\n             <!-- " + JSON.stringify(result) + "  -->" +
 result.cols.map((colName, i) =>
    " <xs:element name='" + escXml(colName) + "' type='xs:" + (typeAlias[result.types[i]] || result.types[i]) + "'>" +
    "\n   <xs:annotation><xs:appinfo>" +
    '\n     <xrd:title xml:lang="en">' + escXml(result.cols[i]) + '</xrd:title>' +
    '\n     <xrd:title xml:lang="et">' + escXml(result.cols[i]) + '</xrd:title>' +
    "\n   </xs:appinfo></xs:annotation>" +
    " </xs:element>"
    ).join("\n") +
"\n             </xs:sequence></xs:complexType>" +
"\n             </xs:element>" +
'\n             <xs:element name="fault" type="tns:fault" minOccurs="0"/>' +
"\n         </xs:sequence>" +
"\n       </xs:complexType>" +
"\n    </xs:element>" +

"\n </schema>" +
"\n </wsdl:types>" +

'\n<wsdl:message name="' + ReqName + '">' + '<wsdl:part name="' + ReqName  + '" element="tns:' + ReqName + '" /></wsdl:message>' +
'\n<wsdl:message name="' + ResName + '">' + '<wsdl:part name="' + ResName  + '" element="tns:' + ResName + '" /></wsdl:message>' +

'\n  <wsdl:message name="requestHeader">' +
'\n    <wsdl:part name="client"          element="xrd:client"/>' +
'\n    <wsdl:part name="service"         element="xrd:service"/>' +
'\n    <wsdl:part name="id"              element="xrd:id"/>' +
'\n    <wsdl:part name="userId"          element="xrd:userId"/>' +
'\n    <wsdl:part name="issue"           element="xrd:issue"/>' +
'\n    <wsdl:part name="protocolVersion" element="xrd:protocolVersion"/>' +
'\n  </wsdl:message>' +

'\n <wsdl:portType name="' + TableX + 'PortType">' +
'\n    <wsdl:operation name="' + TableX + '"> ' +
'\n    <wsdl:documentation>' +
'\n      <xrd:title xml:lang="et">Teenus '  + escXml(tableName) + '</xrd:title>' +
'\n      <xrd:title xml:lang="en">Service ' + escXml(tableName) + '</xrd:title>' +
'\n      <xrd:notes xml:lang="et">Teenuse kirjeldus</xrd:notes>' +
'\n      <xrd:notes xml:lang="en">Notes/Description of service</xrd:notes>' +
'\n      <xrd:techNotes xml:lang="et">Technical notes</xrd:techNotes>' +
'\n      <xrd:techNotes xml:lang="en">Technical notes</xrd:techNotes>' +
'\n    </wsdl:documentation>' +
'\n      <wsdl:input  name="' + ReqName + '" message="tns:' + ReqName + '"/> ' +
'\n      <wsdl:output name="' + ResName + '" message="tns:' + ResName + '"/> ' +
'\n    </wsdl:operation> ' +
'\n  </wsdl:portType> ' +

"\n<wsdl:binding name='" + TableX + "Binding' type='tns:" + TableX + "PortType'>" +
"\n    <soap:binding style='document' transport='http://schemas.xmlsoap.org/soap/http' />" +
"\n    <wsdl:operation name='" + TableX + "'>" +
"\n        <soap:operation soapAction='" + /*tableName + */ "' style='document' />" +
"\n        <xrd:version>v1</xrd:version>" +
"\n        <wsdl:input name='" + ReqName + "'>" +
"\n            <soap:body use='literal'/>" +
'\n            <soap:header use="literal" message="tns:requestHeader" part="client"/> ' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="service"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="id"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="userId"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="issue"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="protocolVersion"/>' +
'\n        </wsdl:input>' +
"\n        <wsdl:output name='" + ResName + "'>" +
"\n           <soap:body use='literal' />" +
'\n           <soap:header use="literal" message="tns:requestHeader" part="client"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="service"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="id"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="userId"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="issue"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="protocolVersion"/>' +
"\n        </wsdl:output>" +
"\n    </wsdl:operation>" +
"\n</wsdl:binding>" +

'\n' +

'\n    <wsdl:service name="' + TableX + 'Service">' +
'\n        <wsdl:port name="' + TableX + 'Port" binding="tns:' + TableX + 'Binding">' +
'\n            <soap:address location="' + NS.replace(/\.wsdl/, '.soap')+ '"/>' +
'\n        </wsdl:port>' +
'\n    </wsdl:service>'  +
'\n    </wsdl:definitions>' +
'\n';

} else {

if (vars.servtype === 'insert') neededArgs = getFields(result.explainQuery(true).select); else
if (vars.servtype === 'delete') neededArgs = getVars(result.explainQuery().where); else
if (vars.servtype === 'update') neededArgs = getVars(result.explainQuery().where).concat(getFields(result.explainQuery(true).select)) ;

var O = '<?xml version="1.0" encoding="UTF-8"?>' +
// '\n<!-- neededArgs (' + vars.servtype + '): ' + JSON.stringify(neededArgs) + ' -->' +
// '\n<!-- neededArgs (' + vars.servtype + '): ' + JSON.stringify((result.explainQuery().where)) + ' -->' +
'\n<wsdl:definitions targetNamespace="http://producer.x-road.eu" ' +
'\n        xmlns:wsdl="http://schemas.xmlsoap.org/wsdl/"' +
'\n        xmlns:tns="http://producer.x-road.eu"' +
'\n        xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"' +
'\n        xmlns:mime="http://schemas.xmlsoap.org/wsdl/mime/"' +
'\n        xmlns:xmime="http://www.w3.org/2005/05/xmlmime"' +
'\n        xmlns:ref="http://ws-i.org/profiles/basic/1.1/xsd"' +
'\n        xmlns:xs="http://www.w3.org/2001/XMLSchema"' +
'\n        xmlns:soap="http://schemas.xmlsoap.org/wsdl/soap/">' +

'\n    <wsdl:types>' +
'\n        <schema targetNamespace="http://producer.x-road.eu" xmlns="http://www.w3.org/2001/XMLSchema">' +
'\n        <import namespace="http://x-road.eu/xsd/xroad.xsd" schemaLocation="http://x-road.eu/xsd/xroad.xsd"/>' +

'\n    <xs:complexType name="fault">' +
'\n      <xs:sequence>' +
'\n        <xs:element name="faultCode" type="xs:string">' +
'\n          <xs:annotation>' +
'\n            <xs:appinfo>' +
'\n              <xrd:title xml:lang="et">Fault Code</xrd:title>' +
'\n              <xrd:title xml:lang="en">Fault Code</xrd:title>' +
'\n            </xs:appinfo>' +
'\n          </xs:annotation>' +
'\n        </xs:element>' +
'\n        <xs:element name="faultString" type="xs:string">' +
'\n          <xs:annotation>' +
'\n            <xs:appinfo>' +
'\n              <xrd:title xml:lang="et">Vea kirjeldus</xrd:title>' +
'\n              <xrd:title xml:lang="en">Fault explanation</xrd:title>' +
'\n            </xs:appinfo>' +
'\n          </xs:annotation>' +
'\n        </xs:element>' +
'\n      </xs:sequence>' +
'\n    </xs:complexType>' +

'\n    <xs:element name="' + ReqName + '">' +
"\n      <xs:complexType> " +
"\n        <xs:sequence>" +
                neededArgs.map(a =>
                    "\n <xs:element name='" + a + "' type='string'>" +
                    '   <xs:annotation><xs:appinfo>' +
                    "   <xrd:title xml:lang='en'>Input " + escXml(a) + "</xrd:title>" +
                    "   <xrd:title xml:lang='et'>Sisend " + escXml(a) + "</xrd:title>" +
                    "</xs:appinfo></xs:annotation>" +
                    " </xs:element>"
                ).join('\n') +
"\n        </xs:sequence> " +
"\n      </xs:complexType>" +
"\n    </xs:element>" +

'\n     <xs:element name="' + ResName  + '">' +
"\n       <xs:complexType>" +
"\n         <xs:sequence>" +
'\n          <xs:element name="result" type="xs:int">' +
"\n             <xs:annotation>" +
"\n               <xs:appinfo>" +
'\n                 <xrd:title xml:lang="et">muudetud ridu</xrd:title>' +
'\n                 <xrd:title xml:lang="en">affected rows</xrd:title>' +
"\n               </xs:appinfo>" +
"\n             </xs:annotation>" +
"\n          </xs:element>" +
"\n         </xs:sequence>" +
"\n       </xs:complexType>" +
"\n    </xs:element>" +

"\n </schema>" +
"\n </wsdl:types>" +

'\n<wsdl:message name="' + ReqName + '">' + '<wsdl:part name="' + ReqName  + '" element="tns:' + ReqName + '" /></wsdl:message>' +
'\n<wsdl:message name="' + ResName + '">' + '<wsdl:part name="' + ResName  + '" element="tns:' + ResName + '" /></wsdl:message>' +

'\n  <wsdl:message name="requestHeader">' +
'\n    <wsdl:part name="client"          element="xrd:client"/>' +
'\n    <wsdl:part name="service"         element="xrd:service"/>' +
'\n    <wsdl:part name="id"              element="xrd:id"/>' +
'\n    <wsdl:part name="userId"          element="xrd:userId"/>' +
'\n    <wsdl:part name="issue"           element="xrd:issue"/>' +
'\n    <wsdl:part name="protocolVersion" element="xrd:protocolVersion"/>' +
'\n  </wsdl:message>' +

'\n <wsdl:portType name="' + TableX + 'PortType">' +
'\n    <wsdl:operation name="' + TableX + '"> ' +
'\n    <wsdl:documentation>' +
'\n      <xrd:title xml:lang="et">Teenus '  + escXml(tableName) + '</xrd:title>' +
'\n      <xrd:title xml:lang="en">Service ' + escXml(tableName) + '</xrd:title>' +
'\n      <xrd:notes xml:lang="et">Teenuse kirjeldus</xrd:notes>' +
'\n      <xrd:notes xml:lang="en">Notes/Description of service</xrd:notes>' +
'\n      <xrd:techNotes xml:lang="et">Tehniline info</xrd:techNotes>' +
'\n      <xrd:techNotes xml:lang="en">Technical notes</xrd:techNotes>' +
'\n    </wsdl:documentation>' +
'\n      <wsdl:input  name="' + ReqName + '" message="tns:' + ReqName + '"/> ' +
'\n      <wsdl:output name="' + ResName + '" message="tns:' + ResName + '"/> ' +
'\n    </wsdl:operation> ' +
'\n  </wsdl:portType> ' +

"\n<wsdl:binding name='" + TableX + "Binding' type='tns:" + TableX + "PortType'>" +
"\n    <soap:binding style='document' transport='http://schemas.xmlsoap.org/soap/http' />" +
"\n    <wsdl:operation name='" + TableX + "'>" +
"\n        <soap:operation soapAction='" + /* tableName + */ "' style='document' />" +
"\n        <xrd:version>v1</xrd:version>" +
"\n        <wsdl:input name='" + ReqName + "'>" +
"\n            <soap:body use='literal'/>" +
'\n            <soap:header use="literal" message="tns:requestHeader" part="client"/> ' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="service"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="id"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="userId"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="issue"/>' +
'\n            <soap:header use="literal" message="tns:requestHeader" part="protocolVersion"/>' +
'\n        </wsdl:input>' +
'\n        <wsdl:output name="' + ResName + '">' +
"\n           <soap:body use='literal' />" +
'\n           <soap:header use="literal" message="tns:requestHeader" part="client"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="service"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="id"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="userId"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="issue"/>' +
'\n           <soap:header use="literal" message="tns:requestHeader" part="protocolVersion"/>' +
"\n        </wsdl:output>" +
"\n    </wsdl:operation>" +
"\n</wsdl:binding>" +

'\n' +

'\n    <wsdl:service name="' + TableX + 'Service">' +
'\n        <wsdl:port name="' + TableX + 'Port" binding="tns:' + TableX + 'Binding">' +
'\n            <soap:address location="' + NS.replace(/\.wsdl/, '.soap')+ '"/>' +
'\n        </wsdl:port>' +
'\n    </wsdl:service>'  +
'\n    </wsdl:definitions>' +
'\n';

}

print (O);


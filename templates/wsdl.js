
contentType('application/wsdl+xml');
function escXML (x) { return x }
function getVars(expr) {
  var strs = [];
  var walk = function (e) {
    if (Array.isArray(e)) e.map(walk);
    // else if (e.str        ) { strs.push( e.str );  } // goal -- collect str
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
var neededArgs = Object.keys(req.vars).filter(w => w.match(/:?where$/)).map(w => [w.split(':')[0], req.vars[w]]).map(([t,w]) => getVars(parseQuery(w, t)));
var typeAlias = { str: 'string' };
var NS = 'http://' + hostname + ':' + port + req.url; // $proto://$_SERVER[SERVER_NAME]:$_SERVER[SERVER_PORT]$_SERVER[REQUEST_URI]

var ReqName = req.table + "Request";
var ResName = req.table + "Response";

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
//     <import namespace="http://x-road.eu/xsd/xroad.xsd"
// '\n            <xs:import namespace="http://x-road.eu/xsd/xroad.xsd" schemaLocation="http://x-road.eu/xsd/xroad.xsd" />' +
// '\n            <xs:import namespace="http://ws-i.org/profiles/basic/1.1/xsd" schemaLocation="http://ws-i.org/profiles/basic/1.1/swaref.xsd" />' +
// '\n            <xs:import namespace="http://www.w3.org/2005/05/xmlmime" schemaLocation="http://www.w3.org/2005/05/xmlmime" />' +

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
                    "   <xrd:title xml:lang='en'>Input " + escXML(a) + "</xrd:title>" +
                    "   <xrd:title xml:lang='et'>Sisend " + escXML(a) + "</xrd:title>" +
                    "</xs:appinfo></xs:annotation>" +
                    " </xs:element>"
                ).join('\n') +
"\n        </xs:sequence> " +
"\n      </xs:complexType>" +
"\n    </xs:element>" +

'\n     <xs:element name="' + ResName  + '">' +
"\n       <xs:complexType>" +
"\n         <xs:sequence>" +
'\n          <xs:element name="row">' +
"\n             <xs:annotation>" +
"\n               <xs:appinfo>" +
'\n                 <xrd:title xml:lang="et">väljundkirje teenusest ' + req.table + '</xrd:title>' +
'\n                 <xrd:title xml:lang="en">output row of ' + req.table + '</xrd:title>' +
"\n               </xs:appinfo>" +
"\n             </xs:annotation>" +
"\n             <xs:complexType><xs:sequence>" +
 result.cols.map((colName, i) =>
    " <xs:element name='" + colName + "' type='xs:" + (typeAlias[result.types[i]] || result.types[i]) + "'>" +
    "\n   <xs:annotation><xs:appinfo>" +
    '\n     <xrd:title xml:lang="en">' + result.cols[i] + '</xrd:title>' +
    '\n     <xrd:title xml:lang="et">' + result.cols[i] + '</xrd:title>' +
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

/*
"\n     <element name='getResponse'>" +
"\n        <xs:complexType>" +
"\n            <xs:sequence>" +
"\n                <element maxOccurs='unbounded' minOccurs='0' name='row' type='tns1:row' />" +
"\n            </xs:sequence>" +
"\n        </xs:complexType>" +
"\n     </element>" +
*/

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

'\n <wsdl:portType name="' + req.table + 'PortType">' +
'\n    <wsdl:operation name="' + req.table + '"> ' +
'\n    <wsdl:documentation>' +
'\n      <xrd:title xml:lang="et">Teenus ' + req.table  + '</xrd:title>' +
'\n      <xrd:title xml:lang="en">Service ' + req.table + '</xrd:title>' +
'\n      <xrd:notes xml:lang="et">Notes/Description of service</xrd:notes>' +
'\n      <xrd:notes xml:lang="en">Notes/Description of service</xrd:notes>' +
'\n      <xrd:techNotes xml:lang="et">Technical notes</xrd:techNotes>' +
'\n      <xrd:techNotes xml:lang="en">Technical notes</xrd:techNotes>' +
'\n    </wsdl:documentation>' +
'\n      <wsdl:input  name="' + ReqName + '" message="tns:' + ReqName + '"/> ' +
'\n      <wsdl:output name="' + ResName + '" message="tns:' + ResName + '"/> ' +
'\n    </wsdl:operation> ' +
'\n  </wsdl:portType> ' +

"\n<wsdl:binding name='" + req.table + "Binding' type='tns:" + req.table + "PortType'>" +
"\n    <soap:binding style='document' transport='http://schemas.xmlsoap.org/soap/http' />" +
"\n    <wsdl:operation name='" + req.table + "'>" +
"\n        <soap:operation soapAction='" + /*req.table + */ "' style='document' />" +
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
"\n        <wsdl:output name='" + req.table + "Response'>" +
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

'\n    <wsdl:service name="' + req.table + 'Service">' +
'\n        <wsdl:port name="' + req.table + 'Port" binding="tns:' + req.table + 'Binding">' +
'\n            <soap:address location="' + NS.replace(/\.wsdl/, '.soap').replace(/([&?])limit=[0-9]+/, '$1') + '"/>' +
'\n        </wsdl:port>' +
'\n    </wsdl:service>'  +
'\n    </wsdl:definitions>';

print (O);


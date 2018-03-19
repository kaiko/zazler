function escHtml(str) {
  return String(str)
          .replace(/&/g, '&amp;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
}

// headers = {"Content-Type": "text/xml; charset=utf-8"};
rowTag = vars.rowTag || "row";
hTable = escHtml(req.table);

xsdMap = {
  "str"      : "string",
  "bool"     : "boolean",
  "float"    : "double",
  "double"   : "double",
  "dbl"      : "double",
  "int"      : "integer",
  "date"     : "date",
  "datetime" : "datetime",
  "time"     : "time"};
table = escHtml(req.table);

function xsdType(t) { return xsdMap[t] || "string" }

print('<?xml version="1.0" encoding="utf-8"?>'
+"\n"+'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">'
+"\n"+'<xs:element name="' + table + '">'
+"\n"+'<xs:complexType>'
+"\n"+'<xs:sequence>'
+"\n"+'<xs:element name="' + rowTag + '" maxOccurs="unbounded"><xs:complexType><xs:sequence>');

result.cols.forEach(function(col, colId) {
   print('<xs:element name="' + escHtml(col) + '" type="xs:' + xsdType( result.types[colId] ) + '"/>')
});

print ('</xs:sequence>'
+"\n"+'</xs:complexType>'
+"\n"+'</xs:element>'
+"\n"+'</xs:sequence>'
+"\n"+'</xs:complexType>'
+"\n"+'</xs:element>'
+"\n"+'</xs:schema>');

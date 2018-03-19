function escHtml(str) {
  return String(str)
          .replace(/&/g, '&amp;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
}

// headers = {"Content-Type": "text/xml; charset=utf-8"};
// FIXME: pass all query
hTable = escHtml(req.table);
rowTag = vars.rowTag || "row";

print('<?xml version="1.0" encoding="utf-8"?>\n')
print('<' + hTable + ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="' + hTable + '.xsd">\n');
result.data.forEach(function (row) {
  print("\n<" + rowTag + ">");
  result.cols.forEach(function (col, colId) {
    print("\n  <" + col + ">" + escHtml(row[col]) + "</" + col + ">");
  })
  print("\n</" + rowTag + ">");
})

print('</' + hTable + '>\n');

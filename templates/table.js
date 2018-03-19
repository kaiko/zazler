function escHtml(str) {
  return String(str)
          .replace(/&/g, '&amp;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
}

print ('<table>');
// if (opts("head")) {
if (true) {
  print("<thead><tr>");
  for (i = 0; i < result.cols.length; i++)
    print("<th>" + escHtml(result.cols[i]) + "</th>");
  print("</tr></thead>\n");
}

print('<tbody>');
result.data.forEach(function (row) {
  print("\n<tr>");
  result.cols.forEach(function (fieldN, fieldId) {
    var n = "";
    if (row[fieldN] === null)
         print("<td class=\"null " + result.types[fieldId] + "\">N</td>\n");
    else print("<td class=\"" + result.types[fieldId] + "\">" + escHtml(row[fieldN]) + "</td>\n");
  });
  print("</tr>");
});

print('</tbody>\n');
print('</table>');

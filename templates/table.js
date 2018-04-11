escHtml = unsafe => typeof unsafe === 'string' ? unsafe.replace(/&/g, '&amp;').replace(/[<>'"]/g, c => escHtml.chars[c]) : unsafe;
escHtml.chars = {
    '<': '&lt'
  , '>': '&gt'
  , "'": '&apos;'
  , '"': '&quot;'}

print ('<table>');
// if (opts("head")) {
if (true) {
  print("<thead><tr>");
  for (i = 0; i < result.cols.length; i++)
    print("<th>" + escHtml(result.cols[i]) + "</th>");
  print("</tr></thead>\n");
}

print('<tbody>');
result.data.forEach(row => {
  print("\n<tr>");
  result.cols.forEach((fieldN, fieldId) => {
    var n = "";
    if (row[fieldN] === null)
         print("<td class=\"null " + result.types[fieldId] + "\">N</td>\n");
    else print("<td class=\"" + result.types[fieldId] + "\">" + escHtml(row[fieldN]) + "</td>\n");
  });
  print("</tr>");
});

print('</tbody>\n');
print('</table>');

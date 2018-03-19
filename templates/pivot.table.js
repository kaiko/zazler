function escHtml(str) {
  return String(str)
          .replace(/&/g, '&amp;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
}

print('<table class="pivot">');

result.cols.forEach(function (col, colId) {
  print("\n<tr><th>" + escHtml(col) + "</th>");
  result.data.forEach(function (row) {
     if (row[col] === null)
          print("<td class=\"null " + result.types[colId] + "\">N</td>\n");
     else print("<td class=\"" + result.types[colId] + "\">" + escHtml(row[col]) + "</td>\n");
  });
  print("</tr>");
})

print ('</table>');

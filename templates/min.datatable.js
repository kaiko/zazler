print('<!DOCTYPE html>'
+ "\n" + '<html>'
+ "\n" + '<head>'
+ "\n" + '<title>' + req.table + '</title>'
+ "\n" + '<meta charset="utf-8"/>'
+ "\n" + '<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />'
+ "\n" + '<meta name="robots" content="noindex, nofollow">'
+ "\n" + '<script src="//code.jquery.com/jquery-1.11.1.min.js"></script>'
+ "\n" + '<script src="//cdn.datatables.net/1.10.3/js/jquery.dataTables.min.js"></script>'
+ "\n" + '<!--<link rel="stylesheet" type="text/css" href="//cdn.datatables.net/1.10.3/css/jquery.dataTables.min.css">-->'
+ "\n" + '<link rel="stylesheet" type="text/css" href="//cdn.datatables.net/1.10.3/css/jquery.dataTables.css">'
+ "\n" + '<style type="text/css">'
+ "\n" + '* { font-family: "Trebutech MS", Verana, Arial, sans-serif; }'
+ "\n" + 'table { border-collapse: collapse; }'
+ "\n" + 'td, th { padding: 2px 4px; border: 1px solid #e5e5e5; }'
+ "\n" + 'tr:hover, thead tr { background-color: rgb(245,245,245); }'
+ "\n" + '.int, .double { text-align: right }'
+ "\n" + '.null { color: #eee; font-style: italic; }'
+ "\n" + '.str { text-align: left }'
+ "\n" + '.date .time .datetime { white-space: nowrap }'
+ "\n" + '.bool { text-align: right }'
+ "\n" + '</style>'
+ "\n" + '<body>'
+ "\n" + '<table></table>'
+ "\n" + '</body>'
+ "\n" + '<script>');

columns = result.cols.map(function (c, i) { return { title: c, "class": result.types[i] } });

print(   'rowCount = ' + result.data.length +';'
  +"\n"+ 'options = {lengthMenu: [50, 100, 500, 1000, 1500, 2000], pageLength: 500, columns: ' + JSON.stringify(columns) + ' }'
  +"\n"+ 't = $("table").DataTable(options);'
  +"\n"+ 'function a(r) { setTimeout(function () {'
  +"\n"+ '  if (++a.counter == rowCount || (options.pageLength > a.counter && !(a.counter % a.group))) {'
  +"\n"+ '         a.group = a.group * 2;'
  +"\n"+ '         t.row.add(r).draw();'
  +"\n"+ '  } else t.row.add(r);'
  +"\n"+ '}, 0); }'
  +"\n"+ 'a.group = 20;'
  +"\n"+ 'a.counter = 0;'
);

Object.values = function (o) { return Object.keys(o).map(function (k) { return o[k] } ) }

result.data.forEach(function (row) { print("a(" + JSON.stringify(Object.values(row)) + ")\n"); });

print(
'</script>\n' +
'</html>');

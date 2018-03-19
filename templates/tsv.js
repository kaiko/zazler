headers = {"Content-Type": "text/tab-separated-values; charset=utf-8"};
fsep = "\t";
rsep = "\n";

if (true) {
  result.cols.forEach(function (col, i) {
    print(col);
    if (i + 1 < result.cols.length) print(fsep);
  });
  print(rsep);
}

result.data.forEach(function (row, rowId) {
  result.cols.forEach(function (col, colId) {
    print(row[col]);
    if (colId + 1 < result.cols.length) print(fsep);
  })
  print(rsep);
})

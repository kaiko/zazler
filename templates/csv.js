headers = {"Content-Type": "text/csv; charset=utf-8"};
fsep = ",";
rsep = "\n";

// TODO: same as tsv
// if (opts("head")) {
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

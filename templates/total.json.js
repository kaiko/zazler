contentType("application/json");
let t = await (result.rowsTotal());
print(JSON.stringify({data: result.data, total: t }));

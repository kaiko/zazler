let r;
try {
  r = JSON.parse(input)
} catch (e) {
  throw new Error("Invalid JSON input\n" + input.substring(0, 400));
}

result(r);

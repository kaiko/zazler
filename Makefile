
pack:
	mkdir /tmp/zjs/
	cp -r README.md opts.js access.js db.js query.js query-functions.js package.json LICENSE parsers engines templates /tmp/zjs/
	cp app.js /tmp/zjs/index.js
	tar cvzf zazler.tgz -C /tmp/zjs/ .
	rm -rf /tmp/zjs/


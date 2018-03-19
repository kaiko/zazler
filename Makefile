
pack:
	mkdir /tmp/zjs/
	cp -r opts.js access.js db.js query.js package.json README.md LICENSE parsers engines templates /tmp/zjs/
	cp app.js /tmp/zjs/index.js
	tar cvzf zazlerjs.tgz -C /tmp/zjs/ .
	rm -rf /tmp/zjs/


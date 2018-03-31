
pack:
	rm -rf /tmp/zjs/
	mkdir /tmp/zjs/
	cp -r README.md app.js opts.js access.js db.js query.js query-functions.js package.json LICENSE parsers engines templates /tmp/zjs/
	cd /tmp/zjs && npm pack
	mv /tmp/zjs/zazler-*.tgz .
	# tar cvzf zazler.tgz -C /tmp/zjs/ .


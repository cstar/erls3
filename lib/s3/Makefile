main:	clean
	erlc -o ebin -I include src/*.erl

clean:
	mkdir -p ebin
	rm -f ebin/*

VERSION=s3erl-0.2
release:
	mkdir disttmp
	svn export `svn info . | grep '^URL:'| cut -d' ' -f2` disttmp/${VERSION}
	tar -Cdisttmp -zcvf  ${VERSION}.tar.gz ${VERSION}
	rm -rf disttmp
	echo Distribution is ${VERSION}.tar.gz

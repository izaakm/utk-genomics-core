.PHONY: all tags clean

all: tags

tags:
	ctags -R src

clean:
	rm -rf bin/

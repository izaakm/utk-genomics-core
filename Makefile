CODEBOOKS_SRC := $(shell find $(CODEBOOKS_HOME) -maxdepth 1 -type d -name '*gensvc*' -or -name '*genomics-core*')
CODEBOOKS_DST := $(subst $(CODEBOOKS_HOME),codebooks,$(CODEBOOKS_SRC))

$(info CODEBOOKS_SRC: $(CODEBOOKS_SRC))
$(info CODEBOOKS_DST: $(CODEBOOKS_DST))

.PHONY: all tags clean

all: tags

.PHONY: codebooks
codebooks: | $(CODEBOOKS_DST)
codebooks/%: $(CODEBOOKS_HOME)/%
	mkdir -p codebooks
	ln -s $< $@

tags:
	ctags -R src

clean:
	rm -rf bin/

SHELL := /bin/bash

CODEBOOKS_SRC := $(shell find $(CODEBOOKS_HOME) -maxdepth 1 -type d -name '*gensvc*' -or -name '*genomics-core*')
CODEBOOKS_DST := $(subst $(CODEBOOKS_HOME),codebooks,$(CODEBOOKS_SRC))

CONDA_ENV_NAME := $(shell grep '^\s*name:' environment.yml | awk '{print $$2}')

# $(info CODEBOOKS_SRC: $(CODEBOOKS_SRC))
# $(info CODEBOOKS_DST: $(CODEBOOKS_DST))

.PHONY: all tags init install uninstall clean j

all: tags

.PHONY: codebooks
codebooks: | $(CODEBOOKS_DST)
codebooks/%: $(CODEBOOKS_HOME)/%
	mkdir -p codebooks
	ln -s $< $@

tags:
	ctags -R src

init: .env .work

.env:
	@echo "export CODEBOOKS_HOME=\"$(CODEBOOKS_HOME)\"" >> $(@)
	@echo "export CONDA_ENV_NAME=\"$(CONDA_ENV_NAME)\"" >> $(@)

.work:
	@echo "conda activate $(CONDA_ENV_NAME)" >> $(@)
	@echo "source .env" >> $(@)

install:
	conda env create -f environment.yml
	eval "$$(conda shell.bash hook)" && conda activate $(CONDA_ENV_NAME) && pip install -e .

uninstall:
	conda env remove -n $(CONDA_ENV_NAME)

clean:
	rm -rf bin/

j:
	/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --app=$(JUPYTER_WORKSPACE)


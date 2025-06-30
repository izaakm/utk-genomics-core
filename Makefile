SHELL := /bin/bash

CODEBOOKS_SRC := $(shell find $(CODEBOOKS_HOME) -maxdepth 1 -type d -name '*gensvc*' -or -name '*genomics-core*')
CODEBOOKS_DST := $(subst $(CODEBOOKS_HOME),codebooks,$(CODEBOOKS_SRC))

CONDA_ENV_NAME := $(shell grep '^\s*name:' environment.yml | awk '{print $$2}')

# $(info CODEBOOKS_SRC: $(CODEBOOKS_SRC))
# $(info CODEBOOKS_DST: $(CODEBOOKS_DST))

.PHONY: all tags init install uninstall j codebooks testdata clean

usage:
	@echo "Usage: make [target]"
	@echo "Available targets:"
	@echo "  tags         # Generate tags for the source code"
	@echo "  codebooks    # Create symbolic links to codebooks in the current directory"
	@echo "  init         # Create .env and .work files for environment setup"
	@echo "  install      # Install the conda environment and Python package"
	@echo "  uninstall    # Remove the conda environment"
	@echo "  j            # Open Jupyter workspace in Chrome"
	@echo "  testdata     # Generate test data for unit tests"
	@echo "  clean        # Clean up test data"

tags:
	ctags -R src

codebooks: | $(CODEBOOKS_DST)
codebooks/%: $(CODEBOOKS_HOME)/%
	mkdir -p codebooks
	ln -s $< $@

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

j:
	test -n "$(JUPYTER_WORKSPACE)" && /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --app=$(JUPYTER_WORKSPACE)


# ************************************************************
# Tests
# ************************************************************

TESTS_DIR := tests
TEST_DATA := $(TESTS_DIR)/_data

testdata: $(TEST_DATA)/Illumina $(TEST_DATA)/processed $(TEST_DATA)/globus
$(TEST_DATA)/%:
	@mkdir -p $(@D)
	python src/gensvc/testing/make_test_data.py $(*) $(@D)


# ************************************************************
# Clean up
# ************************************************************

clean:
	rm -rf $(TEST_DATA)

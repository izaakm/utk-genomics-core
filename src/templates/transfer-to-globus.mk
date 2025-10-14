SHELL := /bin/bash

UTK0192 := /lustre/isaac24/proj/UTK0192
GLOBUS_DATA := $(UTK0192)/data/globus
PROCESSED_DATA := $(UTK0192)/data/processed

runid := $(shell basename $(realpath ..))
sample_project := $(shell find external unknown_project -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
$(info $(sample_project))

globus_projects := $(addprefix $(GLOBUS_DATA)/$(runid)/,$(sample_project))
collections := $(GLOBUS_DATA)/$(runid)/COLLECTIONS

# $(info $(runid))
# $(info $(sample_project))
# $(info $(globus_projects))

all: $(globus_projects)

$(GLOBUS_DATA)/$(runid)/%: $(PROCESSED_DATA)/$(runid)/transfer/external/% | $(GLOBUS_DATA)/$(runid) $(collections)
	@echo "External: $(*)"
	cp -lr $(<) $(@D)/
	echo "$(runid) - $(*)" >> $(@D)/COLLECTIONS
	touch "$(<D)/$(*).GlobusTransferComplete"

$(GLOBUS_DATA)/$(runid)/%: $(PROCESSED_DATA)/$(runid)/transfer/unknown_project/% | $(GLOBUS_DATA)/$(runid) $(collections)
	@echo "Unknown Project: $(*)"
	cp -lr $(<) $(@D)/
	echo "$(runid) - $(*)" >> $(@D)/COLLECTIONS
	touch "$(<D)/$(*).GlobusTransferComplete"

$(collections):
	@echo "<PI Last Name> UTK Illumina Data <YYYYMMDD> [(<Collaborator Last Name>)]" > $(@)

$(GLOBUS_DATA)/$(runid):
	test -d $(GLOBUS_DATA) && mkdir -p $(@)

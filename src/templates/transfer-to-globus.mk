SHELL := /bin/bash

UTK0192 := /lustre/isaac24/proj/UTK0192
GLOBUS_DATA := $(UTK0192)/data/globus
PROCESSED_DATA := $(UTK0192)/data/processed

RUNID := $(shell basename $(realpath ..))

# */<SAMPLE_PROJECT>
SAMPLE_PROJECT := $(shell find * -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

# */*.GlobusTransferComplete
GLOBUS_TRANSFER_COMPLETE := $(addsuffix .GlobusTransferComplete,$(wildcard */*))

# $(info $(SAMPLE_PROJECT))

# .../UTK0192/data/globus/<RUNID>/<SAMPLE_PROJECT>
GLOBUS_COLLECTION := $(addprefix $(GLOBUS_DATA)/$(RUNID)/,$(SAMPLE_PROJECT))

# .../UTK0192/data/globus/<RUNID>/COLLECTIONS
COLLECTION_INFO := $(GLOBUS_DATA)/$(RUNID)/COLLECTIONS


# $(info $(RUNID))
# $(info $(SAMPLE_PROJECT))
# $(info $(GLOBUS_COLLECTION))

# all: $(GLOBUS_COLLECTION)
all: $(GLOBUS_TRANSFER_COMPLETE)

$(GLOBUS_DATA)/$(RUNID)/%: | $(PROCESSED_DATA)/$(RUNID)/transfer/external/% $(GLOBUS_DATA)/$(RUNID) $(COLLECTION_INFO)
	@echo "External: $(*)"
	cp -lr $(<) $(@D)/
	echo "$(RUNID) - $(*)" >> $(@D)/COLLECTIONS
	touch "$(<D)/$(*).GlobusTransferComplete"

unknown_project/%.GlobusTransferComplete: | $(GLOBUS_DATA)/$(RUNID)/%
	touch $(@)

$(GLOBUS_DATA)/$(RUNID)/%: | $(PROCESSED_DATA)/$(RUNID)/transfer/unknown_project/% $(GLOBUS_DATA)/$(RUNID) $(COLLECTION_INFO)
	@echo "Unknown Project: $(*)"
	cp -lr $(<) $(@D)/
	echo "$(RUNID) - $(*)" >> $(@D)/COLLECTIONS
	touch "$(<D)/$(*).GlobusTransferComplete"

$(COLLECTION_INFO):
	@echo "<PI Last Name> UTK Illumina Data <YYYYMMDD> [(<Collaborator Last Name>)]" > $(@)

$(GLOBUS_DATA)/$(RUNID):
	test -d $(GLOBUS_DATA) && mkdir -p $(@)

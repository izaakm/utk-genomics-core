# Example "transfer/" directory:
#
#   ./external
#   ./external/External_ExtPIName_260313
#   ./unknown_project
#   ./unknown_project/UTK_Micro679_Spring2026_260313
#   ./unknown_project/UTK_PINameFoo_260313
#   ./unknown_project/UTK_PINameBar_260313
#
# Mark complete:
#
#   ./external/External_ExtPIName_260313.GlobusTransferComplete
#   ./unknown_project/UTK_Micro679_Spring2026_260313.GlobusTransferComplete
#   ./unknown_project/UTK_PINameBar_260313.GlobusTransferComplete
#   ./unknown_project/UTK_PINameFoo_260313.GlobusTransferComplete

SHELL := /bin/bash

UTK0192 := /lustre/isaac24/proj/UTK0192
GLOBUS_DATA := $(UTK0192)/data/globus
PROCESSED_DATA := $(UTK0192)/data/processed

RUNID := $(shell basename $(realpath ..))

# PROJECT_DIRS             ->  external/External_PIName_Date  unknown_project/UTK_PIName_Date
# TRANSFER_GROUPS          ->  external                       unknown_project
# SAMPLE_PROJECT           ->           External_PIName_Date                  UTK_PIName_Date
PROJECT_DIRS := $(shell find */* -maxdepth 0 -type d )
TRANSFER_GROUPS := $(shell find * -maxdepth 0 -type d -exec basename {} \;)
SAMPLE_PROJECT := $(shell find * -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

# GLOBUS_TRANSFER_COMPLETE -> */*.GlobusTransferComplete
GLOBUS_TRANSFER_COMPLETE := $(foreach name,$(PROJECT_DIRS),$(name).GlobusTransferComplete)

# GLOBUS_COLLECTION ->  .../UTK0192/data/globus/<RUNID>/<SAMPLE_PROJECT>
GLOBUS_COLLECTION := $(addprefix $(GLOBUS_DATA)/$(RUNID)/,$(SAMPLE_PROJECT))
.SECONDARY: $(GLOBUS_COLLECTION)

# COLLECTION_INFO ->  .../UTK0192/data/globus/<RUNID>/COLLECTIONS
COLLECTION_INFO := $(GLOBUS_DATA)/$(RUNID)/COLLECTIONS

$(info RUNID                    = $(RUNID))
$(info TRANSFER_GROUPS          = $(TRANSFER_GROUPS))
$(info SAMPLE_PROJECT           = $(SAMPLE_PROJECT))
$(info PROJECT_DIRS             = $(PROJECT_DIRS))
$(info GLOBUS_COLLECTION        = $(GLOBUS_COLLECTION))
$(info COLLECTION_INFO          = $(COLLECTION_INFO))
$(info GLOBUS_TRANSFER_COMPLETE = $(GLOBUS_TRANSFER_COMPLETE))

# all: $(GLOBUS_COLLECTION)
all: $(GLOBUS_TRANSFER_COMPLETE)

# ============================================================
# All project directories
# ============================================================

# EG -> ./external/External_PIName.GlobusTransferComplete: <GLOBUS>/<RUNID>/External_PIName
#                  %%%%%%%%%%%%%%%                                          %%%%%%%%%%%%%%%
$(foreach name,$(TRANSFER_GROUPS),$(name)/%.GlobusTransferComplete): | $(GLOBUS_DATA)/$(RUNID)/%
	touch "$(@)"

$(GLOBUS_DATA)/$(RUNID)/%: | $(GLOBUS_DATA)/$(RUNID) $(COLLECTION_INFO)
	@echo "Sample Project: $(*)"
	cp -lr */"$(*)" "$(@D)/"
	echo "$(RUNID) - $(*)" >> "$(@D)/COLLECTIONS"

$(COLLECTION_INFO):
	@echo "<PI Last Name> UTK Illumina Data <YYYYMMDD> [(<Collaborator Last Name>)]" > $(@)

$(GLOBUS_DATA)/$(RUNID): | $(GLOBUS_DATA)
	mkdir -p "$(@)"

# Dummy.
$(GLOBUS_DATA):
	test -d "$(GLOBUS_DATA)"

# ============================================================
clean:
	rm -f */*.GlobusTransferComplete

# END

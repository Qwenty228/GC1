# The system lives in SystemCode/. This file just forwards every target there,
# so `make run` works from the repository root as well as from SystemCode/.

.PHONY: help
help:
	@$(MAKE) -s -C SystemCode help

# Command-line variables (T=..., LIMIT=...) propagate to the sub-make on their own.
%:
	@$(MAKE) -C SystemCode $@

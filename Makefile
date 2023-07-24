sgr0 := $(shell tput sgr0)
err := $(shell tput setab 1 && tput setaf 7 && tput bold)
info := $(shell tput setab 4 && tput setaf 7 && tput bold)
done := $(shell tput setab 2 && tput setaf 7 && tput bold)

log_info = @(echo "$(info)>>$(1)$(sgr0)")
log_error = @(echo "$(err)>>$(1)$(sgr0)") && exit 1
log_done = @(echo "$(done)>> Done!$(sgr0)")

.PHONY: setup
setup:
	$(call log_info, Installing ifacemaker...)
	go install github.com/vburenin/ifacemaker@latest
	$(call log_done)

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

generate_sqs:
	$(call log_info, Generating SQS interface...)
	@mkdir tmp_aws
	@git clone --no-checkout --filter=tree:0 https://github.com/aws/aws-sdk-go-v2 tmp_aws
	@cd tmp_aws; git sparse-checkout set --no-cone service/sqs && git checkout
	@ifacemaker -f "tmp_aws/service/sqs/*.go" -i "SQSClient" -p "sqsextendedclient" -s "Client" -o "sqs.go" -c "Generated from $$(cd tmp_aws; git tag -l 'service/sqs/v*' --sort -creatordate | head -n 1)" -D -y 'SQSClient is a wrapper interface for the [github.com/aws/aws-sdk-go-v2/service/sqs.Client].'
	@sed -i '' 's/*/*sqs./gi' sqs.go
	@sed -i '' 's|"context"|"context"\n\n\t"github.com/aws/aws-sdk-go-v2/service/sqs"|gi' sqs.go
	@rm -rf tmp_aws
	$(call log_done)

test:
	$(call log_info, Running tests...)
	go test -v ./... -cover
	$(call log_done)
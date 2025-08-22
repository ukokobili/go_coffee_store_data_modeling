#######################################################################################
# dbt commands

docs:
	dbt docs generate && dbt docs serve

gen:
	python code/model_generate.py

#######################################################################################
# git commit, push, and pull requests

push:
	git checkout -B feature/jacob_feature
	@read -p "Enter commit message: " msg; \
	git add .; \
	git commit -m "$$msg"; \
	git push origin feature/jacob_feature

pull:
	git checkout main && git pull origin main

#######################################################################################
# code formatter
fmt:
	sqlfmt models

#######################################################################################
# Make the env_vars.sh script executable
chmodx:
	chmod +x env_vars.sh 

# Source the env_vars.sh script to set environment variables
script:
	. ./env_vars.sh

# Echo the MOTHERDUCK_TOKEN environment variable
echoes:
	@echo "MOTHERDUCK_TOKEN: $$MOTHERDUCK_TOKEN"

# Run dbt debug after setting up environment variables
debug: chmodx script echoes
	dbt debug
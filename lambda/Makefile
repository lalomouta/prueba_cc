build-FuelPricesPipelineFunction:
	pip install \
		--platform manylinux2014_x86_64 \
		--target $(ARTIFACTS_DIR) \
		--implementation cp \
		--python-version 3.12 \
		--only-binary=:all: \
		-r requirements.txt
	cp lambda_function.py $(ARTIFACTS_DIR)/

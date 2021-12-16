mkfile_path := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

default:
	cd $(mkfile_path) ;\
	python3.8 -m venv venv ;\
	venv/bin/python3 -m pip install --upgrade pip ;\
	venv/bin/python3 -m pip install .

dev:
	cd $(mkfile_path) ;\
	python3.8 -m venv venv ;\
	venv/bin/python3 -m pip install --upgrade pip ;\
	venv/bin/python3 -m pip install -e .[dev]

pylint:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	pylint --rcfile .pylintrc cicada/

pytest:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	pytest tests/ --verbose --cov=cicada --cov-fail-under=70 --cov-report term-missing

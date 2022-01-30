mkfile_path := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

default:
	cd $(mkfile_path) ;\
	python3.8 -m venv venv ;\
	venv/bin/python3 -m pip install --upgrade pip ;\
	venv/bin/python3 -m pip install -e .


dev:
	cd $(mkfile_path) ;\
	python3.8 -m venv venv ;\
	venv/bin/python3 -m pip install --upgrade pip ;\
	venv/bin/python3 -m pip install -e .[dev]


pytest:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	pytest tests/ --verbose --cov=cicada --cov-fail-under=70 --cov-report term-missing


pylint:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	pylint --rcfile .pylintrc cicada/


flake8:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	flake8 cicada/ --count --select=E9,F63,F7,F82 --max-line-length=120 --statistics  --show-source


black:
	cd $(mkfile_path) ;\
	. venv/bin/activate ;\
	black --check --verbose --skip-string-normalization cicada/ tests/

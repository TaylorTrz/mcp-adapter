IMAGE_NAME := mcp-adapter
LOG_LEVEL := DEBUG
DOCKER_IMAGE := library/apigateway/mcp-adapter
# RELEASE_VERSION := 0.0.2-102710
RELEASE_VERSION := 0.1.1
TEST_ENV := gate
REGISTRY := registry-jinan-lab.inspurcloud.cn

test:
	LOG_LEVEL=$(LOG_LEVEL) mcp-adapter -f src/samples/openapi-v3-demo1.json  -f src/samples/openapi-v3-demo2.json

build:
	uv build --wheel
	uv pip install -e  .
	# uv pip install --find-links dist/ .

clean:
	uv pip uninstall mcp-adapter -q
	test -d build/ && rm -rf build/ || true
	test -d dist/ && rm -rf dist/ || true
	find . -type d -iname __pycache__ -o -iname mcp_adapter.egg-info -o -iname  mcp-adapter.tar.gz | xargs -I {} rm -rf {}

base:
	docker build --rm --force-rm --network host -f ./Dockerfile -t mcp-adapter:base  .

release:
	docker build --rm --force-rm --network host -f ./Dockerfile-release -t mcp-adapter:$(RELEASE_VERSION) --no-cache .


.PHONY: run build clean docker release

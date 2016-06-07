VERSION := `head -1 debian/changelog | awk '{print $2}' | grep -o -e '\([0-9\.]\+\)' | tr -d '()'`
UNIQUE_TAG ?= `date +%s`
REPO ?=

all: pkg

pkg:
	dpkg-deb --build debian	

docker-client:
	docker build -t "${REPO}qasino-client:${VERSION}-${UNIQUE_TAG}" -f Dockerfile.client .
	docker push "${REPO}qasino-client:${VERSION}-${UNIQUE_TAG}"
	echo "Client container image created."

docker-server:
	docker build -t "${REPO}qasino-server:${VERSION}-${UNIQUE_TAG}" .
	docker push "${REPO}qasino-server:${VERSION}-${UNIQUE_TAG}"
	echo "Server container image created"


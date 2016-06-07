VERSION := `head -1 debian/changelog | awk '{print $2}' | grep -o -e '\([0-9\.]\+\)' | tr -d '()'`
UNIQUE_TAG ?= `date +%s`

all: pkg

pkg:
	dpkg-deb --build debian	

docker-client:
	docker build -t "qasino-client:${VERSION}-${UNIQUE_TAG}" -f Dockerfile.client .
	echo "Client container image created."

docker-server:
	docker build -t "qasino-server:${VERSION}-${UNIQUE_TAG}" .
	echo "Server container image created"

VERSION := `head -1 debian/changelog | awk '{print $2}' | grep -o -e '\([0-9\.]\+\)' | tr -d '()'`

all: docker-client docker-server

pkg:
	dpkg-deb --build debian	

docker-client:
	docker build -t "qasino-client:${VERSION}" -f Dockerfile.client .
	echo "Client container image created."

docker-server:
	docker build -t "qasino-server:${VERSION}" .
	echo "Server container image created"

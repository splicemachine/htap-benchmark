NAME = splicemachine/benchmark_htap
VERSION = 0.0.1
SPLICE_BASE_IMAGE_VERSION = 0.0.3
.PHONY: all build clean push realclean

all: build

run:
	docker run $(NAME):$(VERSION)

build:
	-mvn -f pom.xml clean install
	docker build --rm --build-arg splice_base_image_version=${SPLICE_BASE_IMAGE_VERSION} \
		-t $(NAME):$(VERSION) .
	docker tag $(NAME):$(VERSION) $(NAME):latest

push:
	docker push $(NAME):$(VERSION)
	docker push $(NAME):latest

clean:
	-docker rmi $(NAME):$(VERSION)
	-docker rmi $(NAME):latest

realclean:
	-docker kill $(shell docker ps -q)
	-docker rm $(shell docker ps -a -q)
	-docker rmi $(shell docker images -q)
	-docker system prune -f -a


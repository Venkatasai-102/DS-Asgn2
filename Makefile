start:
	# docker-compose up -d metadb
	docker-compose up lb

build:
	docker-compose build 


clean:
	make clean_servers
	-docker-compose down

clean_servers:
	-docker rm -f $$(docker ps -aqf "ancestor=serverimg")

remove_images:
	-docker rmi -f serverimg lb

# to get logs of any container use:      docker logs -f CONTAINER_ID OR NAME
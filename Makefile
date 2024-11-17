build: 
	echo 'build the app' && go fmt ./... && go build -o ./bin/app ./app

run: build
	./bin/app

run-dir: build
	./bin/app --dir /home/eco/codecrafters-redis-go --dbfilename dump.rdb

run-wdir: build
	./bin/app --dir /home/eco/codecrafters-redis-go --dbfilename nofile.rdb
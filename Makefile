build: 
	echo 'build the app' && go fmt ./... && go build -o ./bin/app ./app

run: build
	./bin/app
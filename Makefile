.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/archiveextract-worker ./cmd/archiveextract-worker

pack:
	bash scripts/pack.sh dist

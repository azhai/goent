SINGLETON =
COMMANDS  = goent-gen goent-tools


ifndef GOAMD64
	GOAMD64 = v2
endif
GOARCH  = $(shell uname -m | tr [A-Z] [a-z])
ifeq ($(GOARCH), amd64)
	GOARGS = GOAMD64=$(GOAMD64)
else
	GOARGS =
endif

GOBIN    = go
UPXBIN   = upx
RELEASE  = "-s -w"
GOBUILD  = $(GOARGS) $(GOBIN) build -ldflags=$(RELEASE)
BINFILES = $(SINGLETON) $(COMMANDS)


.PHONY: one all build clean upx upxx $(BINFILES)

one:
	@echo "Compile goent-gen ..."
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GOBUILD) -o ./bin/goent-gen ./cmd/goent-gen
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GOBUILD) -o ./bin/goent-tools ./cmd/goent-tools

all: clean one build

build: $(BINFILES)
	@echo "✅ Build success."

$(SINGLETON):
	@echo "Compile $@ ..."
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GOBUILD) -o ./bin/$@.darwin-arm64 ./
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) -o ./bin/$@.darwin-amd64 ./
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GOBUILD) -o ./bin/$@.linux-arm64 ./
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o ./bin/$@.linux-amd64 ./
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) -o ./bin/$@.windows-amd64 ./

$(COMMANDS):
	@echo "Compile $@ ..."
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 $(GOBUILD) -o ./bin/$@.darwin-arm64 ./cmd/$@
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) -o ./bin/$@.darwin-amd64 ./cmd/$@
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GOBUILD) -o ./bin/$@.linux-arm64 ./cmd/$@
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o ./bin/$@.linux-amd64 ./cmd/$@
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) -o ./bin/$@.windows-amd64 ./cmd/$@

clean:
	rm -f $(BINFILES:%=./bin/%)
	@echo "✅ Clean complete."

upx: clean build
	$(UPXBIN) $(BINFILES:%=./bin/%)

upxx: clean build
	$(UPXBIN) --ultra-brute $(BINFILES:%=./bin/%)

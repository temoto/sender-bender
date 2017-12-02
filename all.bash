#!/bin/bash
set -e

cmd=${*-test}
protoc_flags=(
  -I=.
  -I=$GOPATH/src
  -I=$GOPATH/src/github.com/gogo/protobuf/protobuf
  --gogoslick_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types:./
)
protoc_flags="${protoc_flags[@]}"

go get \
  github.com/alecthomas/gometalinter \
  github.com/kovetskiy/manul \
  github.com/gogo/protobuf/proto \
  github.com/gogo/protobuf/protoc-gen-gogoslick \
  github.com/gogo/protobuf/gogoproto
git submodule update --init

go get -tags trace ./vendor/github.com/temoto/go-sqlite3

( cd talk ; protoc $protoc_flags *.proto )

if [[ "$cmd" == "test" ]] ; then
  rm -f coverage.txt
  for d in $(go list ./... |fgrep -v 'vendor/') ; do
  	tmp=/tmp/coverage-one.txt
    go test -tags trace -race -coverprofile=$tmp -covermode=atomic $d
    [[ -f "$tmp" ]] && cat $tmp >>coverage.txt
  done
elif [[ "$cmd" == "build" ]] ; then
  go build -tags trace ./...
  GOBIN=$PWD go get -tags trace ./...
else
  go $cmd ./...
fi

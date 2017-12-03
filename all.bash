#!/bin/bash
set -eu

# setup
cd "$( dirname "${BASH_SOURCE[0]}" )"
export GOBIN=${GOBIN-$PWD/build}
export PATH=$PWD/build:$PATH
cmd=${*-test}
protoc_flags=(
  -I=.
  -I=$PWD/vendor/github.com/gogo/protobuf/protobuf
  -I=$GOPATH/src
  --gogoslick_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types:./
)
protoc_flags="${protoc_flags[@]}"

# check / install dependencies
if ! which protoc >/dev/null ; then
  echo 'Please install external dependency: protobuf (protoc program)' >&2
  exit 1
fi

go get \
  github.com/alecthomas/gometalinter \
  github.com/gogo/protobuf/proto \
  github.com/gogo/protobuf/protoc-gen-gogoslick \
  github.com/gogo/protobuf/gogoproto

git submodule update --init --recursive --checkout

# hopefully this is a temporary measure and in future we will
# use original fiorix/go-smpp with all relevant patches included
git config submodule.vendor/github.com/temoto/go-smpp.ignore dirty
sed -i'' -e 's/github.com\/fiorix\/go-smpp/github.com\/temoto\/go-smpp/' ./vendor/github.com/temoto/go-smpp/**/*.go

go get -tags trace ./vendor/github.com/temoto/go-sqlite3

# essence
( cd talk ; protoc $protoc_flags *.proto )
[[ "$cmd" == "proto" ]] && exit 0

if [[ "$cmd" == "test" ]] ; then
  rm -f coverage.txt
  for d in $(go list ./... |fgrep -v 'vendor/') ; do
  	tmp=/tmp/coverage-one.txt
    go test -tags trace -race -coverprofile=$tmp -covermode=atomic $d
    [[ -f "$tmp" ]] && cat $tmp >>coverage.txt
  done
elif [[ "$cmd" == "build" ]] ; then
  go get -tags trace ./...
else
  go $cmd ./...
fi


FROM golang:1.21-alpine AS build

RUN apk add --no-cache make git bash

WORKDIR /go/src/github.com/replicatedhq/pvmigrate
COPY cmd cmd
COPY pkg pkg
COPY go.mod go.mod
COPY go.sum go.sum
COPY Makefile Makefile
COPY .git .git

RUN make build

FROM scratch
COPY --from=build /go/src/github.com/replicatedhq/pvmigrate/bin/pvmigrate /pvmigrate
ENTRYPOINT ["/pvmigrate"]

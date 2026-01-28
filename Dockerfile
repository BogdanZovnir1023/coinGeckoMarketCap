FROM golang:alpine AS app-builder
RUN apk add git

WORKDIR /go/src/app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /go/bin/app ./

FROM alpine:latest AS alpine-with-tz
RUN apk --no-cache add tzdata zip
WORKDIR /usr/share/zoneinfo
RUN zip -q -r -0 /zoneinfo.zip .

# Use scratch to create a minimal image
FROM scratch
COPY --from=app-builder /go/bin/app /app

ENV ZONEINFO=/zoneinfo.zip
COPY --from=alpine-with-tz /zoneinfo.zip /
COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/app"]

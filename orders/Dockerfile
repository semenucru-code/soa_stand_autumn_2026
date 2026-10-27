FROM alpine:3.20 AS build
RUN apk add --no-cache build-base libpq-dev curl-dev libmicrohttpd-dev cjson-dev
WORKDIR /src
COPY Makefile *.c *.h ./
RUN make

FROM alpine:3.20
RUN apk add --no-cache libpq libcurl libmicrohttpd cjson \
    && adduser -D -H -u 10001 app
COPY --from=build /src/orders /orders
EXPOSE 8080
USER app
ENTRYPOINT ["/orders"]

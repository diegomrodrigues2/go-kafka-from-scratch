FROM golang:1.22 as build
WORKDIR /app
COPY . .
RUN go build -o /out/broker ./cmd/broker

FROM gcr.io/distroless/base-debian12
WORKDIR /app
ENV DATA_DIR=/data
VOLUME ["/data"]
COPY --from=build /out/broker /usr/local/bin/broker
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/broker"]

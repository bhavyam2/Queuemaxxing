FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /queuemaxxing ./cmd/server

FROM scratch
COPY --from=build /queuemaxxing /queuemaxxing
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/queuemaxxing"]
CMD ["--addr", ":8080", "--data-dir", "/data"]

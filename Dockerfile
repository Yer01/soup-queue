FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build every command into one image; compose picks the binary per service.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
RUN adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080
CMD ["/app/api"]

FROM golang:1.24-alpine AS builder
RUN apk add --no-cache git ca-certificates tzdata
WORKDIR /build
COPY go.mod .
COPY main.go .
COPY index.html .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o raj .

# Switched from `FROM scratch` to alpine so we can install ffmpeg/ffprobe,
# needed for the in-page audio-language switcher (detects extra audio
# tracks and remuxes each into its own single-track file). If you don't
# need that feature, you can revert to `FROM scratch` and drop the
# ffmpeg line below to keep the image minimal.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata ffmpeg
COPY --from=builder /build/raj /raj
COPY --from=builder /build/index.html /index.html
EXPOSE 8080
ENTRYPOINT ["/raj"]

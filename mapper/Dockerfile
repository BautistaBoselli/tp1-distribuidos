FROM golang:alpine AS builder
LABEL intermediateStageToBeDeleted=true

# Change the working directory to /app instead of /build
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Set GOCACHE
# ENV GOCACHE=/root/.cache/go-build

# Build the application
RUN CGO_ENABLE=0 go build -o bin/mapper ./mapper

FROM alpine:latest
COPY --from=builder /app/bin/mapper /mapper
ENTRYPOINT ["/bin/sh"]
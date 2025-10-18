FROM golang:1.25

# Source code will be stored in /app
WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build App and put gokv executable in root folder
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /gokv

EXPOSE 8080

# Run executable
CMD ["/gokv"]
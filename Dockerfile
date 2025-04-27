FROM golang:1.20-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o main .

EXPOSE 8080 8081 8082 8083

CMD ["./main"] 
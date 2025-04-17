FROM golang:1.21-alpine

WORKDIR /app
COPY . .

RUN go mod tidy
RUN go build -o kvstore

EXPOSE 8000 9000
CMD ["./kvstore"]


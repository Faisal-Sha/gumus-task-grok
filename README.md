# Grok Scraper

## Setup

### Install Protobuf (if not already installed)

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     proto/crawler.proto proto/notification.proto
```

1. Install dependencies:
go mod tidy

2. Start services:
docker compose up -d

3. Run application:
go run main.go

4. Fetch products:
curl http://localhost:8080/fetch

5. Check database:
docker exec -it grok-sraper-postgres-1 psql -U trendyol_user -d trendyol_tracker -c "SELECT name, seller, is_active FROM products;"


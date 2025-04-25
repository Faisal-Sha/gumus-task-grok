package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "scraper/proto"

	"bytes"

	"github.com/IBM/sarama"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Product represents the product data structure
type Product struct {
	gorm.Model
	ID                 uint `gorm:"primaryKey"`
	CategoryPath       string
	Name               string
	Images             datatypes.JSON `gorm:"type:jsonb"`
	Video              string
	Seller             string
	Brand              string
	RatingScore        datatypes.JSON `gorm:"type:jsonb"`
	FavoritesCount     int
	CommentsCount      int
	AddToCartEvents    int
	Views              int
	Orders             int
	TopReviews         datatypes.JSON `gorm:"type:jsonb"`
	SizeRecommendation string
	EstimatedDelivery  string
	StockInfo          datatypes.JSON `gorm:"type:jsonb"`
	PriceInfo          datatypes.JSON `gorm:"type:jsonb"`
	SimilarProducts    datatypes.JSON `gorm:"type:jsonb"`
	Attributes         datatypes.JSON `gorm:"type:jsonb"`
	OtherSellers       datatypes.JSON `gorm:"type:jsonb"`
	IsActive           bool           `gorm:"default:true"`
	IsFavorite         bool           `gorm:"default:false"`
}

type TrendyolResponse struct {
	Data struct {
		Contents []struct {
			ID          int    `json:"id"`
			Name        string `json:"name"`
			RatingScore struct {
				AverageRating float32 `json:"averageRating"`
				TotalCount    int     `json:"totalCount"`
			} `json:"ratingScore"`
			Brand      string `json:"brand"`
			BrandID    int    `json:"brandId"`
			WebBrand   string `json:"webBrand"`
			MerchantID int    `json:"merchantId"`
			Category   struct {
				Name string `json:"name"`
				ID   int    `json:"id"`
			} `json:"category"`
		} `json:"contents"`
		Title        string `json:"title"`
		RelevancyKey string `json:"relevancyKey"`
	} `json:"data"`
	StatusCode int  `json:"statusCode"`
	IsSuccess  bool `json:"isSuccess"`
}

// PriceStockLog logs price and stock changes
type PriceStockLog struct {
	gorm.Model
	ProductID  uint
	OldPrice   string
	NewPrice   string
	OldStock   string
	NewStock   string
	ChangeTime time.Time
}

// Crawler Service gRPC definition
type CrawlerServiceClient interface {
	FetchProducts(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error)
}

type FetchRequest struct{}
type FetchResponse struct {
	Products []byte
}

// Notification Service gRPC definition
type NotificationServiceClient interface {
	SendNotification(ctx context.Context, in *pb.NotificationRequest, opts ...grpc.CallOption) (*pb.NotificationResponse, error)
}

type NotificationRequest struct {
	UserID    string
	ProductID uint
	Message   string
}

type NotificationResponse struct {
	Success bool
}

// Database setup
func setupDB() *gorm.DB {
	dsn := "host=localhost user=trendyol_user password=trendyol_password dbname=trendyol_tracker port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	db.AutoMigrate(&Product{}, &PriceStockLog{})
	return db
}

// Kafka Producer setup
func setupKafkaProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

// Kafka Consumer setup
func setupKafkaConsumer(topic string, handler func([]byte)) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Error creating partition consumer:", err)
	}

	log.Printf("Started consuming from topic: %s", topic)
	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("Received message from topic %s", topic)
				handler(msg.Value)
			case err := <-partitionConsumer.Errors():
				log.Printf("Error consuming from topic %s: %v", topic, err)
			}
		}
	}()
}

// readMockData reads product data from data.json file
func readMockData() ([]Product, error) {
	data, err := os.ReadFile("data.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read mock data: %v", err)
	}

	var response TrendyolResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mock data: %v", err)
	}

	// Convert TrendyolResponse to []Product
	products := make([]Product, len(response.Data.Contents))
	for i, item := range response.Data.Contents {
		// Convert maps to JSON strings first, then to datatypes.JSON
		ratingJSON, _ := json.Marshal(map[string]interface{}{
			"averageRating": item.RatingScore.AverageRating,
			"totalCount":    item.RatingScore.TotalCount,
		})
		stockJSON, _ := json.Marshal(map[string]interface{}{"stock": 10})
		priceJSON, _ := json.Marshal(map[string]interface{}{"price": 99.99, "currency": "USD"})

		products[i] = Product{
			Name:         item.Name,
			CategoryPath: item.Category.Name,
			Brand:        item.Brand,
			Seller:       fmt.Sprintf("Merchant-%d", item.MerchantID),
			RatingScore:  datatypes.JSON(ratingJSON),
			IsActive:     true,
			StockInfo:    datatypes.JSON(stockJSON),
			PriceInfo:    datatypes.JSON(priceJSON),
		}
	}

	return products, nil
}

// Crawler Service
func startCrawlerService() {
	e := echo.New()
	// db := setupDB()
	producer := setupKafkaProducer()

	e.GET("/fetch", func(c echo.Context) error {
		// Read mock products from data.json
		mockProducts, err := readMockData()
		if err != nil {
			return c.JSON(500, map[string]string{"error": fmt.Sprintf("Failed to read mock data: %v", err)})
		}

		productsJSON, err := json.Marshal(mockProducts)
		if err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to marshal products"})
		}

		msg := &sarama.ProducerMessage{
			Topic: "products",
			Value: sarama.ByteEncoder(productsJSON),
		}

		if _, _, err := producer.SendMessage(msg); err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to send message to Kafka"})
		}

		return c.JSON(200, map[string]string{"status": "Products fetched and sent to Kafka"})
	})

	go func() {
		s, lis := startGRPCServer("crawler")
		log.Fatal(s.Serve(lis))
	}()

	e.Logger.Fatal(e.Start(":8080"))
}

// Product Analysis Service
func startProductAnalysisService() {
	db := setupDB()
	// Add a ping to verify connection
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("Failed to get database connection:", err)
	}
	if err := sqlDB.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}
	producer := setupKafkaProducer()

	// gRPC client to Notification Service
	conn, err := grpc.Dial("localhost:8082", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	notificationClient := pb.NewNotificationServiceClient(conn)

	setupKafkaConsumer("products", func(data []byte) {
		log.Printf("Received Kafka message: %s", string(data))
		var products []Product
		err := json.Unmarshal(data, &products)
		if err != nil {
			log.Printf("Error unmarshaling products: %v", err)
			return
		}
		log.Printf("Unmarshaled %d products", len(products))

		for _, p := range products {
			log.Printf("Processing product: %s, Seller: %s", p.Name, p.Seller)
			var existing Product
			if err := db.Where("name = ? AND seller = ?", p.Name, p.Seller).First(&existing).Error; err != nil {
				log.Printf("Creating new product: %s (Seller: %s)", p.Name, p.Seller)
				result := db.Create(&p)
				if result.Error != nil {
					log.Printf("Error creating product: %v", result.Error)
				} else {
					log.Printf("Successfully created product with ID: %d", p.ID)
				}
			} else {
				log.Printf("Found existing product: %s (ID: %d)", existing.Name, existing.ID)
				// Existing Product
				var stockMap map[string]interface{}
				if err := json.Unmarshal(p.StockInfo, &stockMap); err == nil {
					if stock, ok := stockMap["stock"].(float64); ok && stock == 0 {
						db.Model(&existing).Update("is_active", false)
					}
				} else {
					log.Printf("Error unmarshaling stock info: %v", err)
				}
				// Check critical fields and log changes
				if !bytes.Equal(existing.PriceInfo, p.PriceInfo) || !bytes.Equal(existing.StockInfo, p.StockInfo) {
					logEntry := PriceStockLog{
						ProductID:  existing.ID,
						OldPrice:   string(existing.PriceInfo),
						NewPrice:   string(p.PriceInfo),
						OldStock:   string(existing.StockInfo),
						NewStock:   string(p.StockInfo),
						ChangeTime: time.Now(),
					}
					db.Create(&logEntry)

					// Check for price drop using unmarshal
					var oldPriceMap, newPriceMap map[string]interface{}
					json.Unmarshal(existing.PriceInfo, &oldPriceMap)
					json.Unmarshal(p.PriceInfo, &newPriceMap)

					oldPrice, _ := oldPriceMap["price"].(float64)
					newPrice, _ := newPriceMap["price"].(float64)

					if newPrice < oldPrice && existing.IsFavorite {
						_, err = notificationClient.SendNotification(context.Background(), &pb.NotificationRequest{
							UserId:    "admin", // Replace with actual user ID
							ProductId: uint32(p.ID),
							Message:   "Price change detected!",
						})
						if err != nil {
							log.Fatal(err)
						}
					}
				}
				// Update DB
				db.Model(&existing).Updates(p)
			}
		}
	})

	// Prioritize favorited products
	go func() {
		for {
			var favorites []Product
			db.Where("is_favorite = ?", true).Find(&favorites)
			// Re-fetch and update favorited products more frequently
			for _, product := range favorites {
				// Simulate fetching updated data for the favorited product
				updatedProduct := Product{
					Name:            product.Name,
					CategoryPath:    product.CategoryPath,
					Images:          product.Images,
					Seller:          product.Seller,
					Brand:           product.Brand,
					RatingScore:     product.RatingScore,
					FavoritesCount:  product.FavoritesCount,
					CommentsCount:   product.CommentsCount,
					AddToCartEvents: product.AddToCartEvents,
					Views:           product.Views,
					Orders:          product.Orders,
					StockInfo:       product.StockInfo,
					PriceInfo:       product.PriceInfo,
					Attributes:      product.Attributes,
					IsActive:        product.IsActive,
				}

				productsJSON, _ := json.Marshal([]Product{updatedProduct})
				msg := &sarama.ProducerMessage{
					Topic: "products",
					Value: sarama.ByteEncoder(productsJSON),
				}
				producer.SendMessage(msg)
			}
			time.Sleep(5 * time.Minute)
		}
	}()
}

// Notification Service
func startNotificationService() {
	e := echo.New()

	go func() {
		s, lis := startGRPCServer("notification")
		log.Fatal(s.Serve(lis))
	}()

	e.Logger.Fatal(e.Start(":8082"))
}

// gRPC Server Setup (Simplified for brevity)
func startGRPCServer(service string) (*grpc.Server, net.Listener) {
	port := ":8081"
	if service == "notification" {
		port = ":8083"
	}
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	if service == "crawler" {
		crawlerServer := &CrawlerServer{}
		pb.RegisterCrawlerServiceServer(s, crawlerServer)
	} else if service == "notification" {
		notificationServer := &NotificationServer{}
		pb.RegisterNotificationServiceServer(s, notificationServer)
	}
	return s, lis
}

func main() {
	go startCrawlerService()
	go startProductAnalysisService()
	go startNotificationService()
	select {}
}

type CrawlerServer struct {
	pb.UnimplementedCrawlerServiceServer
}

func (s *CrawlerServer) FetchProducts(ctx context.Context, in *pb.FetchRequest) (*pb.FetchResponse, error) {
	// Read mock data from data.json
	mockProducts, err := readMockData()
	if err != nil {
		return nil, fmt.Errorf("failed to read mock data: %v", err)
	}

	// Convert mock products to JSON
	productsJSON, err := json.Marshal(mockProducts)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mock products: %v", err)
	}

	return &pb.FetchResponse{Products: productsJSON}, nil
}

type NotificationServer struct {
	pb.UnimplementedNotificationServiceServer
}

func (s *NotificationServer) SendNotification(ctx context.Context, in *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	// Log the notification
	log.Printf("Notification for User %s about Product %d: %s", in.UserId, in.ProductId, in.Message)

	// Here you would typically:
	// 1. Save notification to database
	// 2. Send push notification/email/SMS
	// 3. Update notification status

	// For now, we'll just simulate successful notification
	return &pb.NotificationResponse{
		Success: true,
	}, nil
}

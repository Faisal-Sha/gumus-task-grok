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

	"github.com/IBM/sarama"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Product represents the product data structure
type Product struct {
	gorm.Model
	ID                 uint `gorm:"primaryKey"`
	CategoryPath       string
	Name               string
	Images             []string `gorm:"type:jsonb" json:"images"`
	Video              string
	Seller             string
	Brand              string
	RatingScore        float32
	FavoritesCount     int
	CommentsCount      int
	AddToCartEvents    int
	Views              int
	Orders             int
	TopReviews         []interface{} `gorm:"type:jsonb" json:"top_reviews,omitempty"`
	SizeRecommendation string
	EstimatedDelivery  string
	StockInfo          map[string]interface{} `gorm:"type:jsonb" json:"stock_info"`
	PriceInfo          map[string]interface{} `gorm:"type:jsonb" json:"price_info"`
	SimilarProducts    []interface{}          `gorm:"type:jsonb" json:"similar_products,omitempty"`
	Attributes         map[string]interface{} `gorm:"type:jsonb" json:"attributes"`
	OtherSellers       []interface{}          `gorm:"type:jsonb" json:"other_sellers,omitempty"`
	IsActive           bool                   `gorm:"default:true"`
	IsFavorite         bool                   `gorm:"default:false"`
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

func (p *Product) BeforeSave(tx *gorm.DB) error {
	if p.Images != nil {
		imagesJSON, err := json.Marshal(p.Images)
		if err != nil {
			return err
		}
		tx.Statement.SetColumn("images", string(imagesJSON))
	}

	if p.StockInfo != nil {
		stockJSON, err := json.Marshal(p.StockInfo)
		if err != nil {
			return err
		}
		tx.Statement.SetColumn("stock_info", string(stockJSON))
	}

	if p.PriceInfo != nil {
		priceJSON, err := json.Marshal(p.PriceInfo)
		if err != nil {
			return err
		}
		tx.Statement.SetColumn("price_info", string(priceJSON))
	}

	if p.Attributes != nil {
		attrJSON, err := json.Marshal(p.Attributes)
		if err != nil {
			return err
		}
		tx.Statement.SetColumn("attributes", string(attrJSON))
	}

	return nil
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
		products[i] = Product{
			Name:         item.Name,
			CategoryPath: item.Category.Name,
			Brand:       item.Brand,
			RatingScore: item.RatingScore.AverageRating,
			IsActive:    true,
			// Add other fields as needed with default values
			StockInfo: map[string]interface{}{"stock": 10},
			PriceInfo: map[string]interface{}{"price": 99.99, "currency": "USD"},
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
			var existing Product
			if err := db.Where("name = ? AND seller = ?", p.Name, p.Seller).First(&existing).Error; err != nil {
				// New Product
				db.Create(&p)
			} else {
				// Existing Product
				stock, ok := p.StockInfo["stock"].(float64)
				if ok && stock == 0 {
					db.Model(&existing).Update("is_active", false)
				} else {
					// Check critical fields and log changes
					oldPriceJSON, _ := json.Marshal(existing.PriceInfo)
					newPriceJSON, _ := json.Marshal(p.PriceInfo)
					oldStockJSON, _ := json.Marshal(existing.StockInfo)
					newStockJSON, _ := json.Marshal(p.StockInfo)

					if string(oldPriceJSON) != string(newPriceJSON) || string(oldStockJSON) != string(newStockJSON) {
						logEntry := PriceStockLog{
							ProductID:  existing.ID,
							OldPrice:   string(oldPriceJSON),
							NewPrice:   string(newPriceJSON),
							OldStock:   string(oldStockJSON),
							NewStock:   string(newStockJSON),
							ChangeTime: time.Now(),
						}
						db.Create(&logEntry)

						// Check for price drop
						var oldPrice, newPrice struct{ Price float64 }
						json.Unmarshal([]byte(oldPriceJSON), &oldPrice)
						json.Unmarshal([]byte(newPriceJSON), &newPrice)
						if newPrice.Price < oldPrice.Price && existing.IsFavorite {
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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pb "scraper/proto"

	"bytes"
	"crypto/tls"
	"html/template"
	"net/smtp"

	// "net/smtp"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

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

// User represents a user of the application
type User struct {
	gorm.Model
	Email       string `gorm:"uniqueIndex;not null"`
	Username    string
	Password    string // Should be stored hashed
	Name        string
	IsActive    bool `gorm:"default:true"`
	LastLoginAt time.Time
}

// UserFavorite represents the relationship between a user and their favorite products
type UserFavorite struct {
	gorm.Model
	UserID    uint `gorm:"index:idx_user_product,unique"`
	ProductID uint `gorm:"index:idx_user_product,unique"`
	AddedAt   time.Time
}

// Database setup
func setupDB() *gorm.DB {
	host := os.Getenv("DB_HOST")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_NAME")
	port := os.Getenv("DB_PORT")

	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		host, user, password, dbname, port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Migrate all tables
	db.AutoMigrate(&Product{}, &PriceStockLog{}, &User{}, &UserFavorite{})

	// Create a default user if none exists
	var count int64
	db.Model(&User{}).Count(&count)
	if count == 0 {
		db.Create(&User{
			Email:    "usmaaslam187@gmail.com",
			Username: "admin",
			Password: "admin123", // In production, use bcrypt to hash passwords
			Name:     "Admin User",
		})
	}

	return db
}

// Kafka Producer setup
func setupKafkaProducer() sarama.SyncProducer {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		brokers = []string{"localhost:9092"}
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	return producer
}

// Kafka Consumer setup
func setupKafkaConsumer(topic string, handler func([]byte)) {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		brokers = []string{"localhost:9092"}
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
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

// Product represents the product data structure
type Product struct {
	gorm.Model
	ID                 uint `gorm:"primaryKey"`
	CategoryPath       string
	Name               string
	Images             datatypes.JSON `gorm:"type:jsonb"`
	Video              string
	Seller             datatypes.JSON `gorm:"type:jsonb"`
	Brand              datatypes.JSON `gorm:"type:jsonb"`
	RatingScore        datatypes.JSON `gorm:"type:jsonb"`
	FavoritesCount     string
	CommentsCount      string
	AddToCartEvents    string
	Views              string
	Orders             string
	TopReviews         datatypes.JSON `gorm:"type:jsonb"`
	SizeRecommendation string
	EstimatedDelivery  datatypes.JSON `gorm:"type:jsonb"`
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
			ProductCode string `json:"productCode"`
			InStock     bool   `json:"inStock"`
			AllVariants []struct {
				Barcode    string  `json:"barcode"`
				Currency   string  `json:"currency"`
				InStock    bool    `json:"inStock"`
				ItemNumber int     `json:"itemNumber"`
				Price      float64 `json:"price"`
				Value      string  `json:"value"`
			} `json:"allVariants"`
			IsFavorited bool `json:"isPeopleLikeThisProduct"`
			Brand       struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			} `json:"brand"`
			Category struct {
				Hierarchy string `json:"hierarchy"`
				ID        int    `json:"id"`
				Name      string `json:"name"`
			} `json:"category"`
			RatingScore struct {
				AverageRating float32 `json:"averageRating"`
				TotalCount    int     `json:"totalCount"`
				CommentCount  int     `json:"commentCount"`
			} `json:"ratingScore"`
			WinnerVariant struct {
				Price struct {
					Currency        string  `json:"currency"`
					DiscountedPrice float64 `json:"discountedPrice"`
					SellingPrice    float64 `json:"sellingPrice"`
				} `json:"price"`
				Stock struct {
					Quantity int  `json:"quantity"`
					Disabled bool `json:"disabled"`
				} `json:"stock"`
			} `json:"winnerVariant"`
			WinnerMerchantListing struct {
				Merchant struct {
					ID   int    `json:"id"`
					Name string `json:"name"`
				} `json:"merchant"`
			} `json:"winnerMerchantListing"`
			Images []struct {
				Org       string `json:"org"`
				Preview   string `json:"preview"`
				MainImage string `json:"mainImage"`
				Zoom      string `json:"zoom"`
			} `json:"images"`
			SellerInfo struct {
				Address                string `json:"address"`
				BusinessType           string `json:"businessType"`
				CodEligible            bool   `json:"codEligible"`
				OfficialName           string `json:"officialName"`
				RegisteredEmailAddress string `json:"registeredEmailAddress"`
				RegistrationNumber     string `json:"registrationNumber"`
				TaxNumber              string `json:"taxNumber"`
				TaxOffice              string `json:"taxOffice"`
			} `json:"sellerInfo"`
			Delivery struct {
				DeliveryEndDate   string `json:"deliveryEndDate"`
				DeliveryStartDate string `json:"deliveryStartDate"`
			} `json:"winnerMerchantListing"`
			Attributes []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
				Type  string `json:"type"`
			} `json:"attributes"`
			Description struct {
				ContentDescriptions []struct {
					Description string `json:"description"`
					Type        string `json:"type"`
				} `json:"contentDescriptions"`
			} `json:"description"`
			SocialProof []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"socialProof"`
			OrderCount           int `json:"orderCount"`
			OtherSellersVariants []struct {
				Barcode    string  `json:"barcode"`
				Currency   string  `json:"currency"`
				InStock    bool    `json:"inStock"`
				ItemNumber int     `json:"itemNumber"`
				Price      float64 `json:"price"`
				Value      string  `json:"value"`
			} `json:"otherMerchantVariants"`
		} `json:"contents"`
		Title        string `json:"title"`
		RelevancyKey string `json:"relevancyKey"`
	} `json:"data"`
	StatusCode int  `json:"statusCode"`
	IsSuccess  bool `json:"isSuccess"`
}

// readMockData reads product data from data.json file
func readMockData() ([]Product, error) {
	data, err := os.ReadFile("data.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read mock data: %v", err)
	}
	var trendyolResp TrendyolResponse
	err = json.Unmarshal(data, &trendyolResp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal mock data: %v", err)
	}
	return ConvertTrendyolToProduct(&trendyolResp), nil
}

func ConvertTrendyolToProduct(item *TrendyolResponse) []Product {
	products := make([]Product, len(item.Data.Contents))

	for i, content := range item.Data.Contents {
		// Marshal JSON fields
		ratingJSON, _ := json.Marshal(map[string]interface{}{
			"averageRating": content.RatingScore.AverageRating,
			"commentCount":  content.RatingScore.CommentCount,
			"totalCount":    content.RatingScore.TotalCount,
		})
		comments := content.RatingScore.CommentCount
		stockJSON, _ := json.Marshal(map[string]interface{}{
			"stock":    content.WinnerVariant.Stock.Quantity,
			"disabled": content.WinnerVariant.Stock.Disabled,
		})

		priceJSON, _ := json.Marshal(map[string]interface{}{
			"price":         content.WinnerVariant.Price.DiscountedPrice,
			"originalPrice": content.WinnerVariant.Price.SellingPrice,
			"currency":      content.WinnerVariant.Price.Currency,
		})

		sellerJSON, _ := json.Marshal(map[string]interface{}{
			"address":                content.SellerInfo.Address,
			"businessType":           content.SellerInfo.BusinessType,
			"codEligible":            content.SellerInfo.CodEligible,
			"officialName":           content.SellerInfo.OfficialName,
			"registeredEmailAddress": content.SellerInfo.RegisteredEmailAddress,
			"registrationNumber":     content.SellerInfo.RegistrationNumber,
			"taxNumber":              content.SellerInfo.TaxNumber,
			"taxOffice":              content.SellerInfo.TaxOffice,
		})

		deliveryJSON, _ := json.Marshal(map[string]interface{}{
			"deliveryEndDate":   content.Delivery.DeliveryEndDate,
			"deliveryStartDate": content.Delivery.DeliveryStartDate,
		})

		otherSellersVariant := make(map[string]interface{})
		for _, attr := range content.OtherSellersVariants {
			otherSellersVariant["barcode"] = attr.Barcode
			otherSellersVariant["currency"] = attr.Currency
			otherSellersVariant["inStock"] = attr.InStock
			otherSellersVariant["itemNumber"] = attr.ItemNumber
			otherSellersVariant["price"] = attr.Price
			otherSellersVariant["value"] = attr.Value
		}
		otherSellersVariantsJSON, _ := json.Marshal(otherSellersVariant)
		brandJSON, _ := json.Marshal(content.Brand)

		// Create attributes map
		attrMap := make(map[string]interface{})
		for _, attr := range content.Attributes {
			attrMap[attr.Key] = attr.Value
		}
		attributesJSON, _ := json.Marshal(attrMap)

		// Process images
		var images []string
		for _, img := range content.Images {
			images = append(images, img.MainImage)
		}
		imagesJSON, _ := json.Marshal(images)

		var orders, favorites, views, addToBasket string
		for _, count := range content.SocialProof {
			if count.Key == "orderCount" {
				orders = count.Value
			} else if count.Key == "favoriteCount" {
				favorites = count.Value
			} else if count.Key == "pageViewCount" {
				views = count.Value
			} else if count.Key == "basketCount" {
				addToBasket = count.Value
			}
		}
		products[i] = Product{
			ID:                uint(content.ID),
			Name:              content.Name,
			CategoryPath:      content.Category.Hierarchy,
			Brand:             datatypes.JSON(brandJSON),
			Seller:            datatypes.JSON(sellerJSON),
			RatingScore:       datatypes.JSON(ratingJSON),
			IsActive:          content.InStock,
			StockInfo:         datatypes.JSON(stockJSON),
			PriceInfo:         datatypes.JSON(priceJSON),
			Attributes:        datatypes.JSON(attributesJSON),
			Images:            datatypes.JSON(imagesJSON),
			Orders:            orders,
			FavoritesCount:    favorites,
			Views:             views,
			IsFavorite:        content.IsFavorited,
			CommentsCount:     strconv.Itoa(comments),
			AddToCartEvents:   addToBasket,
			EstimatedDelivery: datatypes.JSON(deliveryJSON),
			OtherSellers:      datatypes.JSON(otherSellersVariantsJSON),
		}
	}

	log.Printf("Converted trendyol response to products `%s`", products)
	return products
}

// Utility function to find an available port
func findAvailablePort(basePort int, serviceName string) int {
	port := basePort
	maxAttempts := 10 // Try up to 10 ports

	for attempt := 0; attempt < maxAttempts; attempt++ {
		addr := fmt.Sprintf(":%d", port)
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			// Found an available port
			listener.Close()
			log.Printf("%s using port %d", serviceName, port)
			return port
		}

		log.Printf("Port %d is in use, trying port %d for %s", port, port+1, serviceName)
		port++
	}

	log.Printf("Failed to find available port after %d attempts for %s, using %d", maxAttempts, serviceName, port)
	return port
}

// Crawler Service
func startCrawlerService() {
	e := echo.New()
	producer := setupKafkaProducer()

	// Find available port for HTTP server
	port := findAvailablePort(8080, "Crawler HTTP")

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
			Topic: "PRODUCTS",
			Value: sarama.ByteEncoder(productsJSON),
		}

		if _, _, err := producer.SendMessage(msg); err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to send message to Kafka"})
		}

		return c.JSON(200, map[string]string{"status": "Products fetched and sent to Kafka"})
	})

	e.POST("/favorites", func(c echo.Context) error {
		var req struct {
			UserID    uint `json:"user_id"`
			ProductID uint `json:"product_id"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid request"})
		}

		db := setupDB()
		if err := AddFavorite(db, req.UserID, req.ProductID); err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to add favorite"})
		}

		return c.JSON(200, map[string]string{"status": "Product added to favorites"})
	})

	e.DELETE("/favorites", func(c echo.Context) error {
		var req struct {
			UserID    uint `json:"user_id"`
			ProductID uint `json:"product_id"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid request"})
		}

		db := setupDB()
		if err := RemoveFavorite(db, req.UserID, req.ProductID); err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to remove favorite"})
		}

		return c.JSON(200, map[string]string{"status": "Product removed from favorites"})
	})

	e.GET("/favorites/:user_id", func(c echo.Context) error {
		userID, err := strconv.ParseUint(c.Param("user_id"), 10, 32)
		if err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid user ID"})
		}

		db := setupDB()
		favorites, err := GetUserFavorites(db, uint(userID))
		if err != nil {
			return c.JSON(500, map[string]string{"error": "Failed to get favorites"})
		}

		return c.JSON(200, favorites)
	})

	e.POST("/users", func(c echo.Context) error {
		// Define the request structure
		var req struct {
			Email    string `json:"email" validate:"required,email"`
			Username string `json:"username" validate:"required"`
			Password string `json:"password" validate:"required,min=6"`
			Name     string `json:"name" validate:"required"`
		}

		if err := c.Bind(&req); err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid request"})
		}

		// Basic validation
		if req.Email == "" || req.Username == "" || req.Password == "" {
			return c.JSON(400, map[string]string{"error": "Email, username and password are required"})
		}

		if len(req.Password) < 6 {
			return c.JSON(400, map[string]string{"error": "Password must be at least 6 characters"})
		}

		db := setupDB()

		// Check if user with this email already exists
		var count int64
		db.Model(&User{}).Where("email = ?", req.Email).Count(&count)
		if count > 0 {
			return c.JSON(409, map[string]string{"error": "User with this email already exists"})
		}

		// Check if username is taken
		db.Model(&User{}).Where("username = ?", req.Username).Count(&count)
		if count > 0 {
			return c.JSON(409, map[string]string{"error": "Username is already taken"})
		}

		// Create the user
		// In production, hash the password before storing
		// passwordHash, _ := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)

		user := User{
			Email:       req.Email,
			Username:    req.Username,
			Password:    req.Password, // Should use passwordHash in production
			Name:        req.Name,
			IsActive:    true,
			LastLoginAt: time.Now(),
		}

		result := db.Create(&user)
		if result.Error != nil {
			return c.JSON(500, map[string]string{"error": "Failed to create user"})
		}

		// Don't return the password in the response
		user.Password = ""

		return c.JSON(201, user)
	})

	e.GET("/users/:id", func(c echo.Context) error {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid user ID"})
		}

		db := setupDB()

		var user User
		if err := db.First(&user, id).Error; err != nil {
			return c.JSON(404, map[string]string{"error": "User not found"})
		}

		// Don't return the password
		user.Password = ""

		return c.JSON(200, user)
	})

	go func() {
		s, lis := startGRPCServer("crawler")
		log.Fatal(s.Serve(lis))
	}()

	// Start the HTTP server on the available port
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}

// Product Analysis Service
// Function to start the Analyzer Service with proper flow
func startProductAnalysisService() {
	e := echo.New()
	db := setupDB()
	producer := setupKafkaProducer()

	// Get port from environment variable
	port := os.Getenv("ANALYZER_PORT")
	if port == "" {
		port = "8085"
	}

	log.Printf("Starting Product Analysis Service on port %s", port)

	// Add health check endpoint
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

	// Start HTTP server in a goroutine
	go func() {
		if err := e.Start("0.0.0.0:" + port); err != nil && err != http.ErrServerClosed {
			log.Printf("Analyzer service shutdown: %v", err)
		}
	}()

	productsTopic := os.Getenv("KAFKA_PRODUCTS_TOPIC")
	if productsTopic == "" {
		productsTopic = "PRODUCTS"
	}

	favoritesTopic := os.Getenv("KAFKA_FAVORITES_TOPIC")
	if favoritesTopic == "" {
		favoritesTopic = "FAVORITE_PRODUCTS"
	}

	setupKafkaConsumer(productsTopic, func(data []byte) {
		log.Printf("Product Analysis Service received product data")
		var products []Product
		if err := json.Unmarshal(data, &products); err != nil {
			log.Printf("Error unmarshaling products: %v", err)
			return
		}
		log.Printf("Product Analysis Service received product data `%s`", string(data))
		var favoritedProducts []Product // For collecting favorites

		for _, p := range products {
			// Check if product already exists - EXISTING PRODUCT path
			var existing Product
			result := db.First(&existing, p.ID)

			if result.Error != nil {
				if result.Error == gorm.ErrRecordNotFound {
					// NEW PRODUCT path in diagram
					log.Printf("[Product Analysis] NEW PRODUCT: %s (ID: %d)", p.Name, p.ID)
					db.Create(&p)

					// Check if newly created product should go to Favorite service
					var favoriteCount int64
					db.Model(&UserFavorite{}).Where("product_id = ?", p.ID).Count(&favoriteCount)
					if favoriteCount > 0 && p.IsActive && p.IsFavorite {
						favoritedProducts = append(favoritedProducts, p)
					}
				} else {
					log.Printf("Error checking for existing product: %v", result.Error)
				}
			} else {
				// EXISTING PRODUCT path in diagram
				log.Printf("[Product Analysis] EXISTING PRODUCT: %s (ID: %d)", p.Name, p.ID)

				// Check stock for INACTIVE PRODUCT path in diagram
				var stockInfo map[string]interface{}
				if err := json.Unmarshal(p.StockInfo, &stockInfo); err == nil {
					if stock, ok := stockInfo["stock"].(float64); ok && stock == 0 {
						// MARK IS_ACTIVE = FALSE path in diagram
						log.Printf("[Product Analysis] INACTIVE PRODUCT: %s (ID: %d) - Out of stock", p.Name, p.ID)
						p.IsActive = false
					}
				}

				// Check if this is a favorite product that needs special handling
				isFavorited := false
				if p.IsFavorite && p.IsActive {
					var favoriteCount int64
					db.Model(&UserFavorite{}).Where("product_id = ?", p.ID).Count(&favoriteCount)
					if favoriteCount > 0 {
						isFavorited = true
						log.Printf("[Product Analysis] FAVORITED PRODUCT: %s (ID: %d) - Forwarding to Favorite Service", p.Name, p.ID)
						favoritedProducts = append(favoritedProducts, p)
					}
				}

				if !isFavorited {
					// REGULAR PRODUCT path in diagram - just update DB
					log.Printf("[Product Analysis] REGULAR PRODUCT: %s (ID: %d) - Updating in DB", p.Name, p.ID)
					db.Model(&existing).Updates(map[string]interface{}{
						"name":                p.Name,
						"category_path":       p.CategoryPath,
						"images":              p.Images,
						"seller":              p.Seller,
						"brand":               p.Brand,
						"rating_score":        p.RatingScore,
						"favorites_count":     p.FavoritesCount,
						"views":               p.Views,
						"orders":              p.Orders,
						"stock_info":          p.StockInfo,
						"price_info":          p.PriceInfo,
						"attributes":          p.Attributes,
						"is_active":           p.IsActive,
						"is_favorite":         p.IsFavorite,
						"comments_count":      p.CommentsCount,
						"add_to_cart_events":  p.AddToCartEvents,
						"size_recommendation": p.SizeRecommendation,
						"estimated_delivery":  p.EstimatedDelivery,
						"other_sellers":       p.OtherSellers,
					})
				}
			}
		}

		// Send all collected favorited products to the Favorite Service
		if len(favoritedProducts) > 0 {
			log.Printf("[Product Analysis] Forwarding %d favorited products to Favorite Service", len(favoritedProducts))
			productsJSON, err := json.Marshal(favoritedProducts)
			if err != nil {
				log.Printf("Error marshaling favorited products: %v", err)
			} else {
				msg := &sarama.ProducerMessage{
					Topic: favoritesTopic,
					Value: sarama.ByteEncoder(productsJSON),
				}
				_, _, err := producer.SendMessage(msg)
				if err != nil {
					log.Printf("Error sending favorited products to Kafka: %v", err)
				} else {
					log.Printf("Successfully forwarded favorited products to Favorite Service")
				}
			}
		}
	})
}

// Notification Service
func startNotificationService() {
	e := echo.New()

	// Find available port for HTTP server
	port := findAvailablePort(8082, "Notification HTTP")

	go func() {
		s, lis := startGRPCServer("notification")
		log.Fatal(s.Serve(lis))
	}()

	// Start the HTTP server on the available port
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}

// gRPC Server Setup with port conflict handling
func startGRPCServer(service string) (*grpc.Server, net.Listener) {
	var basePort int
	var serviceName string

	if service == "crawler" {
		basePort = 8081
		serviceName = "Crawler gRPC"
	} else if service == "notification" {
		basePort = 8083
		serviceName = "Notification gRPC"
	}

	// Find available port
	port := findAvailablePort(basePort, serviceName)

	// Try to listen on the port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}

	s := grpc.NewServer()
	if service == "crawler" {
		crawlerServer := &CrawlerServer{}
		pb.RegisterCrawlerServiceServer(s, crawlerServer)
	} else if service == "notification" {
		db := setupDB()
		emailService := NewEmailService(db)
		notificationServer := &NotificationServer{
			emailService: emailService,
		}
		pb.RegisterNotificationServiceServer(s, notificationServer)
	}

	log.Printf("Starting %s service on port %d", serviceName, port)
	return s, lis
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found, using environment variables")
	}

	go startCrawlerService()
	go startProductAnalysisService()
	go startFavoriteProductService()
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
	emailService *EmailService
}

func (s *NotificationServer) SendNotification(ctx context.Context, in *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	// Log the notification
	log.Printf("Notification for User %s about Product %d: %s", in.UserId, in.ProductId, in.Message)

	// Get database connection
	db := setupDB()

	// Create email service if not exists
	if s.emailService == nil {
		s.emailService = NewEmailService(db)
	}

	// Parse user ID from string to uint
	userID, err := strconv.ParseUint(in.UserId, 10, 32)
	if err != nil {
		log.Printf("Error parsing user ID: %v", err)
		return &pb.NotificationResponse{Success: false}, nil
	}

	// Get password from environment variable
	password := os.Getenv("EMAIL_APP_PASSWORD")
	if password == "" {
		log.Printf("Warning: EMAIL_APP_PASSWORD environment variable not set")
		return nil, fmt.Errorf("email password not configured")
	}

	// Extract price information from the message
	// Assuming message format: "Price dropped from X to Y for ProductName!"
	var oldPrice, newPrice float64
	_, err = fmt.Sscanf(in.Message, "Price dropped from %f to %f for", &oldPrice, &newPrice)
	if err != nil {
		log.Printf("Error parsing price info from message: %v", err)
		// Send simple notification with whatever info we have
		_, err = s.emailService.SendPriceDropNotification(uint(userID), uint(in.ProductId), 0, 0)
	} else {
		// Send detailed price drop notification
		_, err = s.emailService.SendPriceDropNotification(uint(userID), uint(in.ProductId), oldPrice, newPrice)
	}

	if err != nil {
		log.Printf("Error sending email notification: %v", err)
	}

	return &pb.NotificationResponse{
		Success: true,
	}, nil
}

// AddFavorite adds a product to a user's favorites
func AddFavorite(db *gorm.DB, userID, productID uint) error {
	favorite := UserFavorite{
		UserID:    userID,
		ProductID: productID,
		AddedAt:   time.Now(),
	}

	result := db.Create(&favorite)
	return result.Error
}

// RemoveFavorite removes a product from a user's favorites
func RemoveFavorite(db *gorm.DB, userID, productID uint) error {
	result := db.Where("user_id = ? AND product_id = ?", userID, productID).Delete(&UserFavorite{})
	return result.Error
}

// GetUserFavorites gets all favorite products for a user
func GetUserFavorites(db *gorm.DB, userID uint) ([]Product, error) {
	var favorites []UserFavorite
	result := db.Where("user_id = ?", userID).Find(&favorites)
	if result.Error != nil {
		return nil, result.Error
	}

	var productIDs []uint
	for _, fav := range favorites {
		productIDs = append(productIDs, fav.ProductID)
	}

	var products []Product
	result = db.Where("id IN ?", productIDs).Find(&products)
	return products, result.Error
}

// IsProductFavorited checks if a product is in a user's favorites
func IsProductFavorited(db *gorm.DB, userID, productID uint) bool {
	var count int64
	db.Model(&UserFavorite{}).Where("user_id = ? AND product_id = ?", userID, productID).Count(&count)
	return count > 0
}

// EmailService handles email sending and message generation
type EmailService struct {
	db *gorm.DB
}

// NewEmailService creates a new EmailService with a database connection
func NewEmailService(db *gorm.DB) *EmailService {
	return &EmailService{db: db}
}

// SendMail sends an email with the given HTML content
func (es *EmailService) SendMail(toEmail string, htmlContent, subject string) error {
	log.Printf("Attempting to send email to %s with subject: %s", toEmail, subject)

	senderMail := os.Getenv("EMAIL_SENDER")
	password := os.Getenv("EMAIL_APP_PASSWORD")
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")

	// Use defaults if env vars not set
	if senderMail == "" {
		senderMail = "your-mailtrap-username" // Replace with default
	}
	if smtpHost == "" {
		smtpHost = "sandbox.smtp.mailtrap.io"
	}
	if smtpPort == "" {
		smtpPort = "2525"
	}

	// MIME headers
	headers := make(map[string]string)
	headers["From"] = senderMail
	headers["To"] = toEmail
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/html; charset=\"utf-8\""
	log.Printf("Sending email to %s with subject: %s from %s", toEmail, subject, senderMail)

	// Add explicit TLS configuration
	config := &tls.Config{ServerName: smtpHost}

	// Auth
	auth := smtp.PlainAuth("", senderMail, password, smtpHost)

	// Connect to the server and start TLS
	client, err := smtp.Dial(smtpHost + ":" + smtpPort)
	if err != nil {
		log.Printf("Error dialing SMTP server: %v", err)
		return err
	}
	defer client.Close()

	if err = client.StartTLS(config); err != nil {
		log.Printf("Error starting TLS: %v", err)
		return err
	}

	if err = client.Auth(auth); err != nil {
		log.Printf("Error authenticating: %v", err)
		return err
	}

	// Set up message and send
	if err = client.Mail(senderMail); err != nil {
		log.Printf("Error setting sender: %v", err)
		return err
	}

	if err = client.Rcpt(toEmail); err != nil {
		log.Printf("Error setting recipient: %v", err)
		return err
	}

	w, err := client.Data()
	if err != nil {
		log.Printf("Error creating data writer: %v", err)
		return err
	}

	// Build message
	var msg bytes.Buffer
	for k, v := range headers {
		msg.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	msg.WriteString("\r\n")
	msg.WriteString(htmlContent)

	_, err = w.Write(msg.Bytes())
	if err != nil {
		return err
	}

	err = w.Close()
	if err != nil {
		return err
	}

	log.Printf("Email sent successfully to %s", toEmail)
	return nil
}

// SendPriceDropNotification sends a notification when a product price drops
func (es *EmailService) SendPriceDropNotification(userID uint, productID uint, oldPrice, newPrice float64) (bool, error) {
	if es.db == nil {
		return false, fmt.Errorf("database connection is nil")
	}

	// Get user email
	var user User
	if err := es.db.First(&user, userID).Error; err != nil {
		return false, fmt.Errorf("failed to find user: %w", err)
	}

	// Get product details
	var product Product
	if err := es.db.First(&product, productID).Error; err != nil {
		return false, fmt.Errorf("failed to find product: %w", err)
	}

	// Get product name and other details
	name := product.Name

	// Unmarshal the price info
	var priceInfo map[string]interface{}
	if err := json.Unmarshal(product.PriceInfo, &priceInfo); err != nil {
		return false, fmt.Errorf("failed to unmarshal price info: %w", err)
	}

	currency := "AED"
	if curr, ok := priceInfo["currency"].(string); ok {
		currency = curr
	}

	// Price drop notification email template
	tmpl := `
	<html>
	<body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
		<div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
			<h2 style="color: #e91e63; margin-bottom: 20px;">Price Drop Alert!</h2>
			
			<p>Hi <b>{{.UserName}}</b>,</p>
			
			<p>Good news! A product you've favorited has dropped in price:</p>
			
			<div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
				<h3 style="margin-top: 0; color: #333;">{{.ProductName}}</h3>
				<p><b>Price dropped from:</b> <span style="text-decoration: line-through;">{{.OldPrice}} {{.Currency}}</span></p>
				<p><b>New price:</b> <span style="color: #e91e63; font-weight: bold; font-size: 1.2em;">{{.NewPrice}} {{.Currency}}</span></p>
				<p><b>You save:</b> <span style="color: #4caf50;">{{.Savings}} {{.Currency}} ({{.SavingsPercent}}%)</span></p>
			</div>
			
			<p>Don't miss out on this great deal!</p>
			
			<a href="http://localhost:8080/products/{{.ProductID}}" style="display: inline-block; background-color: #e91e63; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; margin-top: 15px;">View Product</a>
			
			<p style="margin-top: 30px; font-size: 0.9em; color: #777;">
				This notification was sent because you've favorited this product. 
				<br>Happy Shopping!
			</p>
		</div>
	</body>
	</html>`

	t, err := template.New("priceDropEmail").Parse(tmpl)
	if err != nil {
		return false, fmt.Errorf("failed to parse email template: %w", err)
	}

	// Calculate savings
	savings := oldPrice - newPrice
	savingsPercent := (savings / oldPrice) * 100

	var buf bytes.Buffer
	data := struct {
		UserName       string
		ProductName    string
		OldPrice       float64
		NewPrice       float64
		Currency       string
		Savings        float64
		SavingsPercent float64
		ProductID      uint
	}{
		UserName:       user.Name,
		ProductName:    name,
		OldPrice:       oldPrice,
		NewPrice:       newPrice,
		Currency:       currency,
		Savings:        savings,
		SavingsPercent: float64(int(savingsPercent*100)) / 100, // Round to 2 decimal places
		ProductID:      productID,
	}

	err = t.Execute(&buf, data)
	if err != nil {
		return false, fmt.Errorf("failed to execute email template: %w", err)
	}

	htmlContent := buf.String()
	subject := fmt.Sprintf("Price Drop Alert! %s is now cheaper", name)

	// Send the email
	err = es.SendMail(user.Email, htmlContent, subject)
	if err != nil {
		return false, err
	}

	return true, nil
}

// Function to start the Favorite Product Service with proper flow
func startFavoriteProductService() {
	e := echo.New()
	db := setupDB()

	// Get port from environment variable
	port := os.Getenv("FAVORITE_PORT")
	if port == "" {
		port = "8084"
	}

	log.Printf("Starting Favorite Product Service on port %s", port)

	// Add health check endpoint
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

	// Start HTTP server in a goroutine
	go func() {
		if err := e.Start("0.0.0.0:" + port); err != nil && err != http.ErrServerClosed {
			log.Printf("Favorite service shutdown: %v", err)
		}
	}()

	// Connect to notification service via gRPC
	notificationGrpcPort := os.Getenv("NOTIFICATION_GRPC_PORT")
	if notificationGrpcPort == "" {
		notificationGrpcPort = "8083"
	}

	conn, err := grpc.DialContext(context.Background(),
		fmt.Sprintf("0.0.0.0:%s", notificationGrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("Failed to connect to notification service:", err)
	}
	notificationClient := pb.NewNotificationServiceClient(conn)

	// Get topic from environment
	favoritesTopic := os.Getenv("KAFKA_FAVORITES_TOPIC")
	if favoritesTopic == "" {
		favoritesTopic = "FAVORITE_PRODUCTS"
	}

	// Consume messages from FAVORITE_PRODUCTS topic
	setupKafkaConsumer(favoritesTopic, func(data []byte) {
		log.Printf("[Favorite Service] Received favorited product update `%s`", string(data))

		var products []Product
		if err := json.Unmarshal(data, &products); err != nil {
			log.Printf("Error unmarshaling favorited products: %v", err)
			return
		}

		for _, updatedProduct := range products {
			// Find the existing product in the database
			var existingProduct Product
			if err := db.First(&existingProduct, updatedProduct.ID).Error; err != nil {
				log.Printf("Error finding existing product %d: %v", updatedProduct.ID, err)
				continue
			}

			// PRIORITY UPDATE STOCK & PRICE step in diagram
			log.Printf("[Favorite Service] Priority update for product %s (ID: %d)", existingProduct.Name, existingProduct.ID)

			// Extract price and stock information for comparison
			var oldPriceInfo, newPriceInfo map[string]interface{}
			var oldStockInfo, newStockInfo map[string]interface{}

			if err := json.Unmarshal(existingProduct.PriceInfo, &oldPriceInfo); err != nil {
				log.Printf("Error unmarshaling old price: %v", err)
				continue
			}

			if err := json.Unmarshal(updatedProduct.PriceInfo, &newPriceInfo); err != nil {
				log.Printf("Error unmarshaling new price: %v", err)
				continue
			}

			if err := json.Unmarshal(existingProduct.StockInfo, &oldStockInfo); err != nil {
				log.Printf("Error unmarshaling old stock: %v", err)
				continue
			}

			if err := json.Unmarshal(updatedProduct.StockInfo, &newStockInfo); err != nil {
				log.Printf("Error unmarshaling new stock: %v", err)
				continue
			}

			// Check for price and stock changes
			oldPrice, oldPriceOk := oldPriceInfo["originalPrice"].(float64)
			newPrice, newPriceOk := newPriceInfo["originalPrice"].(float64)
			oldStock, oldStockOk := oldStockInfo["stock"].(float64)
			newStock, newStockOk := newStockInfo["stock"].(float64)
			
			if !oldPriceOk || !newPriceOk || !oldStockOk || !newStockOk {
				log.Printf("Could not extract price or stock values as float64")
				continue
			}

			// LOG STOCK & PRICE CHANGES step in diagram
			if oldPrice != newPrice || oldStock != newStock {
				log.Printf("[Favorite Service] Change detected: Price: %.2f -> %.2f, Stock: %.0f -> %.0f",
					oldPrice, newPrice, oldStock, newStock)

				// Create a log entry
				priceLog := PriceStockLog{
					ProductID:  existingProduct.ID,
					OldPrice:   fmt.Sprintf("%.2f", oldPrice),
					NewPrice:   fmt.Sprintf("%.2f", newPrice),
					OldStock:   fmt.Sprintf("%.0f", oldStock),
					NewStock:   fmt.Sprintf("%.0f", newStock),
					ChangeTime: time.Now(),
				}
				db.Create(&priceLog)

				// Update the product in database
				db.Model(&existingProduct).Updates(map[string]interface{}{
					"price_info": updatedProduct.PriceInfo,
					"stock_info": updatedProduct.StockInfo,
				})

				// PRICE DROP DETECTED? step in diagram
				if newPrice < oldPrice {
					log.Printf("[Favorite Service] PRICE DROP DETECTED for %s!", existingProduct.Name)

					// Find all users who favorited this product
					var favorites []UserFavorite
					if err := db.Where("product_id = ?", existingProduct.ID).Find(&favorites).Error; err != nil {
						log.Printf("Error finding favorites for product %d: %v", existingProduct.ID, err)
						continue
					}

					log.Printf("[Favorite Service] Found %d users to notify", len(favorites))

					// NOTIFICATION SERVICE step in diagram - notify each user about the price drop
					for _, favorite := range favorites {
						var user User
						if err := db.First(&user, favorite.UserID).Error; err != nil {
							log.Printf("Error finding user %d: %v", favorite.UserID, err)
							continue
						}

						log.Printf("[Favorite Service] Sending notification to %s about price drop", user.Email)
						_, err := notificationClient.SendNotification(context.Background(),
							&pb.NotificationRequest{
								UserId:    fmt.Sprintf("%d", user.ID),
								ProductId: uint32(existingProduct.ID),
								Message:   fmt.Sprintf("Price dropped from %.2f to %.2f for %s!", oldPrice, newPrice, existingProduct.Name),
							})

						if err != nil {
							log.Printf("Error sending notification: %v", err)
						} else {
							log.Printf("Successfully sent notification to user %d", user.ID)
						}
					}
				}
			} else {
				log.Printf("[Favorite Service] No changes detected for product %s", existingProduct.Name)
			}
		}
	})
}

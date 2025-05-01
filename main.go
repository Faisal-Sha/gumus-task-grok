package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"scraper/models"
	pb "scraper/proto"

	"bytes"
	"crypto/tls"
	"html/template"
	"net/smtp"
)

var jobIDs = make(map[string]cron.EntryID)

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
	db.AutoMigrate(&models.Product{}, &models.PriceStockLog{}, &models.User{}, &models.UserFavorite{})

	// Create a default user if none exists
	var count int64
	db.Model(&models.User{}).Count(&count)
	if count == 0 {
		db.Create(&models.User{
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

func fetchProductDetails(productID int) map[string]interface{} {
	url := fmt.Sprintf("https://apigw.trendyol.com/discovery-sfint-product-service/api/product-detail/?contentId=%d&campaignId=null&storefrontId=36&culture=en-AE", productID)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Error creating request for product %d: %v", productID, err)
		return nil
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("accept-language", "en-US,en;q=0.9")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("if-none-match", `W/"2b9d-hQQAkXZ6TfXuSxKb4d/3BO1fF/4"`)
	req.Header.Set("origin", "https://www.trendyol.com")
	req.Header.Set("priority", "u=1, i")
	req.Header.Set("sec-ch-ua", `"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"macOS"`)
	req.Header.Set("sec-fetch-dest", "empty")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("sec-fetch-site", "same-site")
	req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("cookie", "platform=web; anonUserId=c88ca0a0-202a-11f0-b282-e524755a5054; OptanonAlertBoxClosed=2025-04-23T10:07:40.439Z; pid=c88ca0a0-202a-11f0-b282-e524755a5054; storefrontId=36; countryCode=AE; language=en; _gcl_au=1.1.140209073.1745402880; _scid=kApt02EUMDwDAF4F9vfx7LsfKVHQYORB; _fbp=fb.1.1745402880754.810322442114590411; _ScCbts=%5B%5D; _pin_unauth=dWlkPU5UZ3hNREptTWpJdE1qZ3hPUzAwT0RFMExXSXlOR010TlRreFpESXdOMlZoWTJVNA; _tt_enable_cookie=1; _ttp=01JSH1WRWS90XBGHC34CSJZ9SE_.tt.1; AbTestingCookies=A_82-B_38-C_43-D_9-E_38-F_99-G_63-H_9-I_13-J_94-K_50-L_55-M_17-N_13-O_80; hvtb=1; VisitCount=1; WebAbTesting=A_16-B_93-C_88-D_50-E_32-F_13-G_69-H_13-I_35-J_51-K_50-L_84-M_21-N_65-O_66-P_10-Q_76-R_88-S_88-T_73-U_72-V_88-W_6-X_53-Y_65-Z_31; msearchAb=ABAdvertSlotPeriod_1-ABDsNlp_2-ABQR_B-ABSearchFETestV1_B-ABBSA_D-ABSuggestionLC_B; AbTesting=pdpAiReviewSummaryUat_B-SFWBAA_V1_B-SFWDBSR_A-SFWDQL_B-SFWDRS_A-SFWDSAOFv2_B-SFWDSFAG_B-SFWDTKV2_A-SSTPRFL_B-STSBuynow_B-STSCouponV2_A-STSImageSocialProof_A-STSRecoRR_B-STSRecoSocialProof_A-WCBsQckFiltTestv2_B-WCOnePageCheckout_B-WEBSFAATest1_A-WebSFAATest2_B-WebSFAATest3_A%7C1745405135%7Cc88ca0a0-202a-11f0-b282-e524755a5054; navbarGenderId=1; guest_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cm46dHJlbmR5b2w6YW5vbmlkIjoiNDMwNmY3M2MyMDQxMTFmMGEwNDNmNjEyMjNiNDc3MGMiLCJyb2xlIjoiYW5vbiIsImF0d3J0bWsiOiI0MzA2ZjczOS0yMDQxLTExZjAtYTA0My1mNjEyMjNiNDc3MGMiLCJhcHBOYW1lIjoidHkiLCJhdWQiOiJzYkF5ell0WCtqaGVMNGlmVld5NXR5TU9MUEpXQnJrYSIsImV4cCI6MTkwMzIwMDUxMSwiaXNzIjoiYXV0aC50cmVuZHlvbC5jb20iLCJuYmYiOjE3NDU0MTI1MTF9.EltSK08NpXAye9_vA86ZAcN_-pIBafYFkFS0uwKe244; csrf-secret=jx65ssufAbKAOyuFhlR442UG; functionalConsent=true; performanceConsent=true; targetingConsent=true; WebRecoTss=collectionRecoVersion%2F1%7CpdpGatewayVersion%2F1%7CsimilarRecoAdsVersion%2F1%7CbasketRecoVersion%2F1%7CsimilarRecoVersion%2F1%7CcompleteTheLookVersion%2F1%7CshopTheLookVersion%2F1%7CcrossRecoAdsVersion%2F1%7CsimilarSameBrandVersion%2F1%7CcrossSameBrandVersion%2F1%7CallInOneRecoVersion%2F1%7CcrossRecoVersion%2F1%7ChomepageVersion%2FfirstComponent%3AinitialNewTest_1.sorter%3AhomepageSorterNewTest_d(M)%7CnavigationSectionVersion%2Fsection%3AazSectionTest_1(M)%7CnavigationSideMenuVersion%2FsideMenu%3AinitialTest_1(M)%7CfirstComponent_V1%2F1%7Csorter_V1%2Fb%7Csection_V1%2F1%7CsideMenu_V1%2F1%7CtopWidgets_V1%2F1; __cf_bm=yAcNpc9.Z023H2FTR6J2dRbKaDczMk24CNVZsuBSnqM-1745517402-1.0.1.1-qjYSPztv9v8GvHS5G0uYTaLZdwoPLOUaz13d12HfxDNO92vJq5WWPX7ZPuSZW4llzjTwiB20ukqB8OOTRtwaJKMxKQA6ZbY2F_GITvRb0yU; _cfuvid=Absmqzycbc8IZ65QvA_J.1CTRkwYDuZ1k_jFWAsDg74-1745517402311-0.0.1.1-604800000; __cflb=04dToYCH9RsdhPpttacYW22gpq3mLXZXuCfT4Kmdad; UserInfo=%7B%22Gender%22%3Anull%2C%22UserTypeStatus%22%3Anull%2C%22ForceSet%22%3Afalse%7D; sid=859166fc-0fcb-478e-a642-7b36856ec13d; _gid=GA1.2.1275501495.1745517404; _dc_gtm_UA-13174585-70=1; ttcsid_CJ5M5PJC77U7DSNBELOG=1745517404716::SmLe8QbNf0ibzXveWLA3.4.1745517406958; ttcsid=1745517404716::bIhYpvqxso6xBLcBmryx.4.1745517406958; tss=firstComponent_V1_1%2Csorter_V1_b%2Csection_V1_1%2CsideMenu_V1_1%2CtopWidgets_V1_1%2CFSA_B%2CProductCardVariantCount_B%2CSuggestionPopular_B%2CRR_2%2CGRRLO_B%2CGRRIn_B%2CVisualCategorySlider_B%2CSuggestionTermActive_B%2CKB_B%2CDGB_B%2CSB_B%2CSuggestion_B%2COFIR_B; _scid_r=rYpt02EUMDwDAF4F9vfx7LsfKVHQYORBse8OXA; _ga=GA1.2.2084360745.1745402880; _uetsid=7c2e4db0213511f0bca4851cb46f5236|1i11fu8|2|fvc|0|1940; _sc_cspv=https%3A%2F%2Ftr.snapchat.com%2Fconfig%2Fcom%2F5df43118-abd2-4cd8-89b9-8cf942d1ee25.js%3Fv%3D3.44.6-2504241707; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Apr+24+2025+22%3A56%3A55+GMT%2B0500+(Pakistan+Standard+Time)&version=202402.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=92cde421-80dd-4b6c-9752-f4bc74338ad3&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0009%3A1%2CC0007%3A1%2CC0003%3A1%2CC0001%3A1%2CC0004%3A1&hosts=H138%3A1%2CH29%3A1%2CH111%3A1%2CH129%3A1%2CH93%3A1%2CH128%3A1%2CH112%3A1%2CH147%3A1%2CH148%3A1%2CH56%3A1%2CH58%3A1%2CH59%3A1%2CH91%3A1%2CH20%3A1%2CH104%3A1%2CH115%3A1%2CH75%3A1%2CH86%3A1%2CH25%3A1%2CH90%3A1%2CH32%3A1%2CH116%3A1%2CH124%3A1%2CH7%3A1%2CH152%3A1%2CH37%3A1%2CH42%3A1%2CH43%3A1%2CH153%3A1%2CH149%3A1%2CH145%3A1%2CH134%3A1%2CH139%3A1%2CH144%3A1&genVendors=V77%3A1%2CV67%3A1%2CV79%3A1%2CV71%3A1%2CV69%3A1%2CV7%3A1%2CV5%3A1%2CV9%3A1%2CV1%3A1%2CV70%3A1%2CV3%3A1%2CV68%3A1%2CV78%3A1%2CV17%3A1%2CV76%3A1%2CV80%3A1%2CV16%3A1%2CV72%3A1%2CV10%3A1%2CV40%3A1%2C&geolocation=PK%3BPB&AwaitingReconsent=false; cto_bundle=MqRYUl9DNGtMb3BmVDRnNWVnNWJkMTVQcmFHUTIlMkYwdGJSV3NmVTBiWklUM1dsUDQlMkI1M094QjN2QnFtVGNxeXhiWDJzZnc5dm1MckQlMkZYYlNhODAydm1uZkdzMiUyQnNGYnp5TjN1c2lUcndjJTJCdExiRzBjSTRDMXpadXFiNDlBZXhHRnM2eFk5RVdyQ0VwNDA3UTRpbllQaDJrVzElMkJNd1klMkJJRWFaZyUyQnNlZjdRWER6TG5LOVhISG16JTJGUUR0RVFwZDRVSGxza2FoZzFvWXJvbmJWNENJSGtZaGhoY0dBJTNEJTNE; _uetvid=d67945d0202a11f0817e972d9bec397e|19nng9p|1745517415909|2|1|bat.bing.com/p/insights/c/a; _ga_9J2BFGDX1E=GS1.1.1745517404.5.1.1745517462.2.0.0")
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error fetching product %d: %v", productID, err)
		return nil
	}
	defer resp.Body.Close()
	bodyText, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response for product %d: %v", productID, err)
		return nil
	}
	var jsonResponse map[string]interface{}
	if err := json.Unmarshal(bodyText, &jsonResponse); err != nil {
		log.Printf("Error unmarshaling response for product %d: %v", productID, err)
		return nil
	}
	return jsonResponse
}

func findAvailablePort(basePort int, serviceName string) int {
	port := basePort
	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		addr := fmt.Sprintf(":%d", port)
		listener, err := net.Listen("tcp", addr)
		if err == nil {
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

func readMockData() ([]models.Product, error) {
	data, err := os.ReadFile("data.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read mock data: %v", err)
	}
	var trendyolResp []models.TrendyolResponse
	err = json.Unmarshal(data, &trendyolResp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal mock data: %v", err)
	}
	return ConvertTrendyolToProduct(&trendyolResp), nil
}

func ConvertTrendyolToProduct(item *[]models.TrendyolResponse) []models.Product {
	products := make([]models.Product, len(*item))

	for i, content := range *item {
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
		attrMap := make(map[string]interface{})
		for _, attr := range content.Attributes {
			attrMap[attr.Key] = attr.Value
		}
		attributesJSON, _ := json.Marshal(attrMap)
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
		products[i] = models.Product{
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

// Crawler Service
func startCrawlerService() {
	e := echo.New()
	producer := setupKafkaProducer()

	// Find available port for HTTP server
	port := findAvailablePort(8080, "Crawler HTTP")

	// Set up cron scheduler
	c := cron.New()
	id, err := c.AddFunc("* * * * *", func() {
		productIDs, err := fetchProductIDsFromDB()
		if err != nil {
			log.Printf("Failed to fetch favorited product IDs: %v", err)
			return
		}
		log.Printf("Found %d active favorited products to update", len(productIDs))
		if len(productIDs) > 0 {
			runTask(producer)
		}
	})
	if err != nil {
		log.Fatal("Invalid cron expression for runTask:", err)
	}
	jobIDs["productDetails"] = id
	c.Start()
	log.Println("✅ Scheduler started for product details fetching")

	e.GET("/fetch", func(c echo.Context) error {

		// {"flag" : true}
		var req struct {
			Flag bool `json:"flag"`
		}
		if err := c.Bind(&req); err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid request"})
		}
		if req.Flag {
			client := &http.Client{}
			start := 1
			end := 160
			file, err := os.Create("data.json")
			if err != nil {
				log.Fatal("Failed to create JSON file:", err)
			}
			defer file.Close()
			file.WriteString("[\n")
			first := true
			for wc := start; wc <= end; wc++ {
				url := fmt.Sprintf("https://apigw.trendyol.com/discovery-sfint-browsing-service/api/search-feed/products?source=sr?wc=%d&size=60", wc)
				fmt.Println("Fetching products for wc:", wc)
				req, err := http.NewRequest("GET", url, nil)
				if err != nil {
					log.Fatal(err)
				}
				req.Header.Set("accept", "application/json")
				req.Header.Set("accept-language", "en-US,en;q=0.9")
				req.Header.Set("baggage", "ty.kbt.name=ViewSearchResult,ty.platform=Web,ty.business_unit=Core%20Commerce,ty.channel=INT,com.trendyol.observability.business_transaction.name=ViewSearchResult,ty.source.service.name=WEB%20Storefront%20International,ty.source.deployment.environment=production,ty.source.service.version=4f92d141,ty.source.client.path=unknown,ty.source.service.type=client")
				req.Header.Set("content-type", "application/json")
				req.Header.Set("origin", "https://www.trendyol.com")
				req.Header.Set("platform", "Web")
				req.Header.Set("priority", "u=1, i")
				req.Header.Set("sec-ch-ua", `"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"`)
				req.Header.Set("sec-ch-ua-mobile", "?0")
				req.Header.Set("sec-ch-ua-platform", `"macOS"`)
				req.Header.Set("sec-fetch-dest", "empty")
				req.Header.Set("sec-fetch-mode", "cors")
				req.Header.Set("sec-fetch-site", "same-site")
				req.Header.Set("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
				req.Header.Set("x-section-id", "null")
				req.Header.Set("cookie", "csrf_secret=u2ANLHbZzCsSCZr5y4L3ziOG; platform=web; _cfuvid=JS6Mjd5imwMhUE.DEVCgMHGZxxyaowoBsHkO6AJXN.w-1745402857084-0.0.1.1-604800000; anonUserId=c88ca0a0-202a-11f0-b282-e524755a5054; __cflb=04dToYCH9RsdhPpttDDEnPngTWcVjd8n2VS1BNR3Xj; OptanonAlertBoxClosed=2025-04-23T10:07:40.439Z; pid=c88ca0a0-202a-11f0-b282-e524755a5054; sid=RN4GQHDizD; storefrontId=36; countryCode=AE; language=en; functionalConsent=true; performanceConsent=true; targetingConsent=true; WebRecoTss=similarRecoAdsVersion%2F1%7CallInOneRecoVersion%2F1%7CbasketRecoVersion%2F1%7CshopTheLookVersion%2F1%7CpdpGatewayVersion%2F1%7CsimilarSameBrandVersion%2F1%7CcrossSameBrandVersion%2F1%7CcrossRecoVersion%2F1%7CsimilarRecoVersion%2F1%7CcompleteTheLookVersion%2F1%7CcollectionRecoVersion%2F1%7CcrossRecoAdsVersion%2F1%7CnavigationSideMenuVersion%2FsideMenu%3AinitialTest_1(M)%7ChomepageVersion%2Fsorter%3AhomepageSorterNewTest_d.firstComponent%3AinitialNewTest_1(M)%7CnavigationSectionVersion%2Fsection%3AazSectionTest_1(M)%7CfirstComponent_V1%2F1%7Csorter_V1%2Fb%7Csection_V1%2F1%7CsideMenu_V1%2F1%7CtopWidgets_V1%2F1; UserInfo=%7B%22Gender%22%3Anull%2C%22UserTypeStatus%22%3Anull%2C%22ForceSet%22%3Afalse%7D; navbarGenderId=1; _gcl_au=1.1.140209073.1745402880; tss=firstComponent_V1_1%2Csorter_V1_b%2Csection_V1_1%2CsideMenu_V1_1%2CtopWidgets_V1_1%2CFSA_B%2CProductCardVariantCount_B%2CSuggestionPopular_B%2CRR_2%2CGRRLO_B%2CGRRIn_B%2CVisualCategorySlider_B%2CSuggestionTermActive_B%2CKB_B%2CDGB_B%2CSB_B%2CSuggestion_B%2COFIR_B; _gid=GA1.2.1617466369.1745402880; _scid=kApt02EUMDwDAF4F9vfx7LsfKVHQYORB; _fbp=fb.1.1745402880754.810322442114590411; _ScCbts=%5B%5D; _pin_unauth=dWlkPU5UZ3hNREptTWpJdE1qZ3hPUzAwT0RFMExXSXlOR010TlRreFpESXdOMlZoWTJVNA; _tt_enable_cookie=1; _ttp=01JSH1WRWS90XBGHC34CSJZ9SE_.tt.1; AbTestingCookies=A_82-B_38-C_43-D_9-E_38-F_99-G_63-H_9-I_13-J_94-K_50-L_55-M_17-N_13-O_80; hvtb=1; VisitCount=1; SearchMode=1; WebAbTesting=A_16-B_93-C_88-D_50-E_32-F_13-G_69-H_13-I_35-J_51-K_50-L_84-M_21-N_65-O_66-P_10-Q_76-R_88-S_88-T_73-U_72-V_88-W_6-X_53-Y_65-Z_31; ForceUpdateSearchAbDecider=forced; WebRecoAbDecider=ABbasketRecoVersion_1-ABcrossRecoVersion_1-ABcrossRecoAdsVersion_1-ABsimilarRecoVersion_1-ABsimilarSameBrandVersion_1-ABcrossSameBrandVersion_1-ABpdpGatewayVersion_1-ABallInOneRecoVersion_1-ABattributeRecoVersion_1-ABcollectionRecoVersion_1-ABshopTheLookVersion_1-ABsimilarRecoAdsVersion_1-ABcompleteTheLookVersion_1-ABhomepageVersion_firstComponent%3AinitialNewTest_1.performanceSorting%3AinitialTest_3.sorter%3AhomepageSorterNewTest_d%28M%29-ABnavigationSideMenuVersion_sideMenu%3AinitialTest_1%28M%29-ABnavigationSectionVersion_section%3AazSectionTest_1%28M%29; FirstSession=0; msearchAb=ABAdvertSlotPeriod_1-ABDsNlp_2-ABQR_B-ABSearchFETestV1_B-ABBSA_D-ABSuggestionLC_B; WebAbDecider=ABres_B-ABBMSA_B-ABRRIn_B-ABSCB_B-ABSuggestionHighlight_B-ABBP_A-ABCatTR_B-ABSuggestionTermActive_A-ABAZSmartlisting_63-ABBH2_B-ABMB_B-ABMRF_1-ABARR_B-ABMA_B-ABSP_B-ABPastSearches_B-ABSuggestionJFYProducts_B-ABSuggestionQF_B-ABBadgeBoost_A-ABRelevancy_1-ABFilterRelevancy_1-ABSuggestionBadges_B-ABProductGroupTopPerformer_B-ABOpenFilterToggle_2-ABRR_2-ABBS_2-ABSuggestionPopularCTR_B; AbTesting=pdpAiReviewSummaryUat_B-SFWBAA_V1_B-SFWDBSR_A-SFWDQL_B-SFWDRS_A-SFWDSAOFv2_B-SFWDSFAG_B-SFWDTKV2_A-SSTPRFL_B-STSBuynow_B-STSCouponV2_A-STSImageSocialProof_A-STSRecoRR_B-STSRecoSocialProof_A-WCBsQckFiltTestv2_B-WCOnePageCheckout_B-WEBSFAATest1_A-WebSFAATest2_B-WebSFAATest3_A%7C1745405135%7Cc88ca0a0-202a-11f0-b282-e524755a5054; __cf_bm=NWXTz_0PnBOy.pCO9V30IFpkTw2Qo2Rje1YU4o55T9k-1745403356-1.0.1.1-iyIpH5eFby7QY4AOTWZym26XHjefMDMuC379iE7HNBCR9f5EbPoplMYfMFa5Me.CvsRrbR079unZJVptaKiMm4qrDsLxMA1Jbq3tngIegkc; _ga_9J2BFGDX1E=GS1.1.1745402880.1.1.1745403367.7.0.0; _scid_r=qopt02EUMDwDAF4F9vfx7LsfKVHQYORBse8OLA; _ga=GA1.2.2084360745.1745402880; _dc_gtm_UA-13174585-70=1; _sc_cspv=https%3A%2F%2Ftr.snapchat.com%2Fconfig%2Fcom%2F5df43118-abd2-4cd8-89b9-8cf942d1ee25.js%3Fv%3D3.44.3-2504222057; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Apr+23+2025+15%3A16%3A07+GMT%2B0500+(Pakistan+Standard+Time)&version=202402.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=92cde421-80dd-4b6c-9752-f4bc74338ad3&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0009%3A1%2CC0007%3A1%2CC0003%3A1%2CC0001%3A1%2CC0004%3A1&hosts=H138%3A1%2CH29%3A1%2CH111%3A1%2CH129%3A1%2CH93%3A1%2CH128%3A1%2CH112%3A1%2CH147%3A1%2CH148%3A1%2CH56%3A1%2CH58%3A1%2CH59%3A1%2CH91%3A1%2CH20%3A1%2CH104%3A1%2CH115%3A1%2CH75%3A1%2CH86%3A1%2CH25%3A1%2CH90%3A1%2CH32%3A1%2CH116%3A1%2CH124%3A1%2CH7%3A1%2CH152%3A1%2CH37%3A1%2CH42%3A1%2CH43%3A1%2CH153%3A1%2CH149%3A1%2CH145%3A1%2CH134%3A1%2CH139%3A1%2CH144%3A1&genVendors=V77%3A1%2CV67%3A1%2CV79%3A1%2CV71%3A1%2CV69%3A1%2CV7%3A1%2CV5%3A1%2CV9%3A1%2CV1%3A1%2CV70%3A1%2CV3%3A1%2CV68%3A1%2CV78%3A1%2CV17%3A1%2CV76%3A1%2CV80%3A1%2CV16%3A1%2CV72%3A1%2CV10%3A1%2CV40%3A1%2C&geolocation=PK%3BPB&AwaitingReconsent=false; _uetsid=d6791910202a11f097b9bfaf0c52dfc1|n1586e|2|fvb|0|1939; cto_bundle=Bag8ol9DNGtMb3BmVDRnNWVnNWJkMTVQcmFDb0olMkYlMkIwNmZuZGpqR2JqS2VmamxjZWltJTJCWEUxdmw3cXJwTWhEeGFzelBNdU83dXFjSG1CemZpOFJrVkpFJTJGelMyNEk0SkhWZG9DcyUyQnZmYlpmM0JBd1JLMDg2TmIzYzR3MDJMMDdDczJaaHRnQnpoWCUyRm1ib1RHTGV1QzZVTVlqOGNOQm51THYwSEdmeUI5aVZENDdDeVhuRkslMkJtUkJwV2Q5cm1NYWlyQWFwaTFrY3c2cmtZWiUyRm8xN05QNVZEZiUyRmRBJTNEJTNE; ttcsid=1745402880923::pm5yFa6t5a51-RWmpBHn.1.1745403368456; ttcsid_CJ5M5PJC77U7DSNBELOG=1745402880923::I82TPjx5GfCIEgRdpsw1.1.1745403368663; _uetvid=d67945d0202a11f0817e972d9bec397e|1jcuqwt|1745403369059|3|1|bat.bing.com/p/insights/c/u")
				resp, err := client.Do(req)
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()
				bodyText, err := io.ReadAll(resp.Body)
				if err != nil {
					log.Fatal(err)
				}
				var result models.Root
				if err := json.Unmarshal(bodyText, &result); err != nil {
					log.Fatal(err)
				}
				fmt.Println("Total products found:", len(result.Data.Contents))
				if len(result.Data.Contents) == 0 {
					fmt.Println("No more products found ")
					continue
				}
				for _, p := range result.Data.Contents {
					time.Sleep(4 * time.Second)
					fmt.Println("Fetching details for product ID:", p.ID)
					detailedProduct := fetchProductDetails(p.ID)
					encoder := json.NewEncoder(file)
					if !first {
						file.WriteString(",\n")
					}
					first = false
					if err := encoder.Encode(detailedProduct); err != nil {
						log.Printf("Failed to write product ID %d to file: %v", p.ID, err)
						continue
					}
				}
			}
			file.WriteString("\n]")
			fmt.Println("Done!")
		}

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

// 		{
//     "user_id" : 1,
//     "product_id" : 449950861
// }
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
		var req struct {
			Email    string `json:"email" validate:"required,email"`
			Username string `json:"username" validate:"required"`
			Password string `json:"password" validate:"required,min=6"`
			Name     string `json:"name" validate:"required"`
		}

		if err := c.Bind(&req); err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid request"})
		}

		if req.Email == "" || req.Username == "" || req.Password == "" {
			return c.JSON(400, map[string]string{"error": "Email, username and password are required"})
		}

		if len(req.Password) < 6 {
			return c.JSON(400, map[string]string{"error": "Password must be at least 6 characters"})
		}

		db := setupDB()
		var count int64
		db.Model(&models.User{}).Where("email = ?", req.Email).Count(&count)
		if count > 0 {
			return c.JSON(409, map[string]string{"error": "User with this email already exists"})
		}
		db.Model(&models.User{}).Where("username = ?", req.Username).Count(&count)
		if count > 0 {
			return c.JSON(409, map[string]string{"error": "Username is already taken"})
		}

		user := models.User{
			Email:       req.Email,
			Username:    req.Username,
			Password:    req.Password,
			Name:        req.Name,
			IsActive:    true,
			LastLoginAt: time.Now(),
		}

		result := db.Create(&user)
		if result.Error != nil {
			return c.JSON(500, map[string]string{"error": "Failed to create user"})
		}

		user.Password = ""
		return c.JSON(201, user)
	})

	e.GET("/users/:id", func(c echo.Context) error {
		id, err := strconv.ParseUint(c.Param("id"), 10, 32)
		if err != nil {
			return c.JSON(400, map[string]string{"error": "Invalid user ID"})
		}

		db := setupDB()
		var user models.User
		if err := db.First(&user, id).Error; err != nil {
			return c.JSON(404, map[string]string{"error": "User not found"})
		}

		user.Password = ""
		return c.JSON(200, user)
	})

	go func() {
		s, lis := startGRPCServer("crawler")
		log.Fatal(s.Serve(lis))
	}()

	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}

// runTask fetches product details for predefined product IDs and saves to data.json
func runTask(producer sarama.SyncProducer) {
	fmt.Println("🚀 Running scheduled task at", time.Now())
	var newProducts []map[string]interface{}

	productIDs, err := fetchProductIDsFromDB()
	if err != nil {
		log.Printf("Error fetching product IDs: %v", err)
		return
	}

	log.Printf("Fetching details for %d products", len(productIDs))

	for _, productID := range productIDs {
		fmt.Printf("🔎 Fetching product ID: %d\n", productID)
		time.Sleep(2 * time.Second)
		detail := fetchProductDetails(productID)
		if detail != nil {
			newProducts = append(newProducts, detail)
		}
	}

	if len(newProducts) == 0 {
		fmt.Println("No new products fetched.")
		return
	}

	filePath := "data.json"
	var existing []map[string]interface{}
	if file, err := os.ReadFile(filePath); err == nil {
		json.Unmarshal(file, &existing)
	}
	existing = append(existing, newProducts...)

	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", " ")
	if err := enc.Encode(existing); err != nil {
		log.Printf("Failed to encode products to file: %v", err)
		return
	}

	fmt.Println("✅ Product details saved to data.json")

	// Convert to models.Product and send to Kafka
	trendyolResp := make([]models.TrendyolResponse, len(newProducts))
	for i, p := range newProducts {
		data, _ := json.Marshal(p)
		var resp models.TrendyolResponse
		json.Unmarshal(data, &resp)
		trendyolResp[i] = resp
	}
	products := ConvertTrendyolToProduct(&trendyolResp)
	productsJSON, err := json.Marshal(products)
	if err != nil {
		log.Printf("Failed to marshal products for Kafka: %v", err)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: "PRODUCTS",
		Value: sarama.ByteEncoder(productsJSON),
	}
	if _, _, err := producer.SendMessage(msg); err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
	} else {
		fmt.Println("✅ Products sent to Kafka topic PRODUCTS")
	}
}

// Product Analysis Service
func startProductAnalysisService() {
	e := echo.New()
	db := setupDB()
	producer := setupKafkaProducer()

	port := os.Getenv("ANALYZER_PORT")
	if port == "" {
		port = "8085"
	}

	log.Printf("Starting Product Analysis Service on port %s", port)

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

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
		var products []models.Product
		if err := json.Unmarshal(data, &products); err != nil {
			log.Printf("Error unmarshaling products: %v", err)
			return
		}
		log.Printf("Product Analysis Service received product data `%s`", string(data))
		var favoritedProducts []models.Product

		for _, p := range products {
			var existing models.Product
			result := db.First(&existing, p.ID)

			if result.Error != nil {
				if result.Error == gorm.ErrRecordNotFound {
					log.Printf("[Product Analysis] NEW PRODUCT: %s (ID: %d)", p.Name, p.ID)
					db.Create(&p)

					var favoriteCount int64
					db.Model(&models.UserFavorite{}).Where("product_id = ?", p.ID).Count(&favoriteCount)
					if favoriteCount > 0 && p.IsActive && p.IsFavorite {
						favoritedProducts = append(favoritedProducts, p)
					}
				} else {
					log.Printf("Error checking for existing product: %v", result.Error)
				}
			} else {
				log.Printf("[Product Analysis] EXISTING PRODUCT: %s (ID: %d)", p.Name, p.ID)

				var stockInfo map[string]interface{}
				if err := json.Unmarshal(p.StockInfo, &stockInfo); err == nil {
					if stock, ok := stockInfo["stock"].(float64); ok && stock == 0 {
						log.Printf("[Product Analysis] INACTIVE PRODUCT: %s (ID: %d) - Out of stock", p.Name, p.ID)
						p.IsActive = false
					}
				}

				isFavorited := false
				if p.IsFavorite && p.IsActive {
					var favoriteCount int64
					db.Model(&models.UserFavorite{}).Where("product_id = ?", p.ID).Count(&favoriteCount)
					if favoriteCount > 0 {
						isFavorited = true
						log.Printf("[Product Analysis] FAVORITED PRODUCT: %s (ID: %d) - Forwarding to Favorite Service", p.Name, p.ID)
						favoritedProducts = append(favoritedProducts, p)
					}
				}

				if !isFavorited {
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
	port := findAvailablePort(8082, "Notification HTTP")

	go func() {
		s, lis := startGRPCServer("notification")
		log.Fatal(s.Serve(lis))
	}()

	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", port)))
}

// gRPC Server Setup
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

	port := findAvailablePort(basePort, serviceName)
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

type NotificationServer struct {
	pb.UnimplementedNotificationServiceServer
	emailService *EmailService
}

func (s *NotificationServer) SendNotification(ctx context.Context, in *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	log.Printf("Notification for User %s about Product %d: %s", in.UserId, in.ProductId, in.Message)
	db := setupDB()

	if s.emailService == nil {
		s.emailService = NewEmailService(db)
	}

	userID, err := strconv.ParseUint(in.UserId, 10, 32)
	if err != nil {
		log.Printf("Error parsing user ID: %v", err)
		return &pb.NotificationResponse{Success: false}, nil
	}

	password := os.Getenv("EMAIL_APP_PASSWORD")
	if password == "" {
		log.Printf("Warning: EMAIL_APP_PASSWORD environment variable not set")
		return nil, fmt.Errorf("email password not configured")
	}

	var oldPrice, newPrice float64
	_, err = fmt.Sscanf(in.Message, "Price dropped from %f to %f for", &oldPrice, &newPrice)
	if err != nil {
		log.Printf("Error parsing price info from message: %v", err)
		_, err = s.emailService.SendPriceDropNotification(uint(userID), uint(in.ProductId), 0, 0)
	} else {
		_, err = s.emailService.SendPriceDropNotification(uint(userID), uint(in.ProductId), oldPrice, newPrice)
	}

	if err != nil {
		log.Printf("Error sending email notification: %v", err)
	}

	return &pb.NotificationResponse{Success: true}, nil
}

func AddFavorite(db *gorm.DB, userID, productID uint) error {
	favorite := models.UserFavorite{
		UserID:    userID,
		ProductID: productID,
		AddedAt:   time.Now(),
	}
	result := db.Create(&favorite)
	return result.Error
}

func RemoveFavorite(db *gorm.DB, userID, productID uint) error {
	result := db.Where("user_id = ? AND product_id = ?", userID, productID).Delete(&models.UserFavorite{})
	return result.Error
}

func GetUserFavorites(db *gorm.DB, userID uint) ([]models.Product, error) {
	var favorites []models.UserFavorite
	result := db.Where("user_id = ?", userID).Find(&favorites)
	if result.Error != nil {
		return nil, result.Error
	}

	var productIDs []uint
	for _, fav := range favorites {
		productIDs = append(productIDs, fav.ProductID)
	}

	var products []models.Product
	result = db.Where("id IN ?", productIDs).Find(&products)
	return products, result.Error
}

func IsProductFavorited(db *gorm.DB, userID, productID uint) bool {
	var count int64
	db.Model(&models.UserFavorite{}).Where("user_id = ? AND product_id = ?", userID, productID).Count(&count)
	return count > 0
}

type EmailService struct {
	db *gorm.DB
}

func NewEmailService(db *gorm.DB) *EmailService {
	return &EmailService{db: db}
}

func (es *EmailService) SendMail(toEmail string, htmlContent, subject string) error {
	log.Printf("Attempting to send email to %s with subject: %s", toEmail, subject)

	senderMail := os.Getenv("EMAIL_SENDER")
	password := os.Getenv("EMAIL_APP_PASSWORD")
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")

	if senderMail == "" {
		senderMail = "your-mailtrap-username"
	}
	if smtpHost == "" {
		smtpHost = "sandbox.smtp.mailtrap.io"
	}
	if smtpPort == "" {
		smtpPort = "2525"
	}

	headers := make(map[string]string)
	headers["From"] = senderMail
	headers["To"] = toEmail
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/html; charset=\"utf-8\""
	log.Printf("Sending email to %s with subject: %s from %s", toEmail, subject, senderMail)

	config := &tls.Config{ServerName: smtpHost}
	auth := smtp.PlainAuth("", senderMail, password, smtpHost)

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

func (es *EmailService) SendPriceDropNotification(userID uint, productID uint, oldPrice, newPrice float64) (bool, error) {
	if es.db == nil {
		return false, fmt.Errorf("database connection is nil")
	}

	var user models.User
	if err := es.db.First(&user, userID).Error; err != nil {
		return false, fmt.Errorf("failed to find user: %w", err)
	}

	var product models.Product
	if err := es.db.First(&product, productID).Error; err != nil {
		return false, fmt.Errorf("failed to find product: %w", err)
	}

	name := product.Name
	var priceInfo map[string]interface{}
	if err := json.Unmarshal(product.PriceInfo, &priceInfo); err != nil {
		return false, fmt.Errorf("failed to unmarshal price info: %w", err)
	}

	currency := "AED"
	if curr, ok := priceInfo["currency"].(string); ok {
		currency = curr
	}

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
		SavingsPercent: float64(int(savingsPercent*100)) / 100,
		ProductID:      productID,
	}

	err = t.Execute(&buf, data)
	if err != nil {
		return false, fmt.Errorf("failed to execute email template: %w", err)
	}

	htmlContent := buf.String()
	subject := fmt.Sprintf("Price Drop Alert! %s is now cheaper", name)
	err = es.SendMail(user.Email, htmlContent, subject)
	if err != nil {
		return false, err
	}

	return true, nil
}

func startFavoriteProductService() {
	e := echo.New()
	db := setupDB()

	port := os.Getenv("FAVORITE_PORT")
	if port == "" {
		port = "8084"
	}

	log.Printf("Starting Favorite Product Service on port %s", port)

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(200, map[string]string{"status": "ok"})
	})

	go func() {
		if err := e.Start("0.0.0.0:" + port); err != nil && err != http.ErrServerClosed {
			log.Printf("Favorite service shutdown: %v", err)
		}
	}()

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

	favoritesTopic := os.Getenv("KAFKA_FAVORITES_TOPIC")
	if favoritesTopic == "" {
		favoritesTopic = "FAVORITE_PRODUCTS"
	}

	setupKafkaConsumer(favoritesTopic, func(data []byte) {
		log.Printf("[Favorite Service] Received favorited product update `%s`", string(data))

		var products []models.Product
		if err := json.Unmarshal(data, &products); err != nil {
			log.Printf("Error unmarshaling favorited products: %v", err)
			return
		}

		for _, updatedProduct := range products {
			var existingProduct models.Product
			if err := db.First(&existingProduct, updatedProduct.ID).Error; err != nil {
				log.Printf("Error finding existing product %d: %v", updatedProduct.ID, err)
				continue
			}

			log.Printf("[Favorite Service] Priority update for product %s (ID: %d)", existingProduct.Name, existingProduct.ID)

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

			oldPrice, oldPriceOk := oldPriceInfo["originalPrice"].(float64)
			newPrice, newPriceOk := newPriceInfo["originalPrice"].(float64)
			oldStock, oldStockOk := oldStockInfo["stock"].(float64)
			newStock, newStockOk := newStockInfo["stock"].(float64)

			if !oldPriceOk || !newPriceOk || !oldStockOk || !newStockOk {
				log.Printf("Could not extract price or stock values as float64")
				continue
			}

			if oldPrice != newPrice || oldStock != newStock {
				log.Printf("[Favorite Service] Change detected: Price: %.2f -> %.2f, Stock: %.0f -> %.0f",
					oldPrice, newPrice, oldStock, newStock)

				priceLog := models.PriceStockLog{
					ProductID:  existingProduct.ID,
					OldPrice:   fmt.Sprintf("%.2f", oldPrice),
					NewPrice:   fmt.Sprintf("%.2f", newPrice),
					OldStock:   fmt.Sprintf("%.0f", oldStock),
					NewStock:   fmt.Sprintf("%.0f", newStock),
					ChangeTime: time.Now(),
				}
				db.Create(&priceLog)

				db.Model(&existingProduct).Updates(map[string]interface{}{
					"price_info": updatedProduct.PriceInfo,
					"stock_info": updatedProduct.StockInfo,
				})

				if newPrice < oldPrice {
					log.Printf("[Favorite Service] PRICE DROP DETECTED for %s!", existingProduct.Name)

					var favorites []models.UserFavorite
					if err := db.Where("product_id = ?", existingProduct.ID).Find(&favorites).Error; err != nil {
						log.Printf("Error finding favorites for product %d: %v", existingProduct.ID, err)
						continue
					}

					log.Printf("[Favorite Service] Found %d users to notify", len(favorites))

					for _, favorite := range favorites {
						var user models.User
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

func fetchProductIDsFromDB() ([]int, error) {
	db := setupDB()
	var productIDs []int

	// Join with user_favorites table to get only favorited products
	result := db.Model(&models.Product{}).
		Select("DISTINCT products.id").
		Joins("JOIN user_favorites ON products.id = user_favorites.product_id").
		Where("products.is_active = ? AND products.is_favorite = ?", true, true).
		Pluck("products.id", &productIDs)

	if result.Error != nil {
		return nil, result.Error
	}
	if len(productIDs) == 0 {
		log.Println("No active favorited products found in database")
		return nil, fmt.Errorf("no active favorited products")
	}
	return productIDs, nil
}

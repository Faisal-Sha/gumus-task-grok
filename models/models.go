package models

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type WebCategory struct {
	Name  string `json:"name"`
	ID    int    `json:"id"`
	Level int    `json:"level"`
}

type ProductPrice struct {
	SellingPrice        float64 `json:"sellingPrice"`
	BuyingPrice         float64 `json:"buyingPrice"`
	DiscountedPrice     float64 `json:"discountedPrice"`
	DiscountedPriceText string  `json:"discountedPriceText"`
	OriginalPrice       float64 `json:"originalPrice"`
	OriginalPriceText   string  `json:"originalPriceText"`
	SellingPriceText    string  `json:"sellingPriceText"`
	BuyingPriceText     string  `json:"buyingPriceText"`
	Currency            string  `json:"currency"`
	DiscountPercentage  int     `json:"discountPercentage"`
	RRP                 float64 `json:"rrp"`
	ProfitMargin        float64 `json:"profitMargin"`
}

type Item struct {
	ID       int    `json:"id"`
	Url      string `json:"productDetailUrl"`
	Name     string `json:"name"`
	Code     string `json:"productCode"`
	Category struct {
		ID        int    `json:"id"`
		Name      string `json:"name"`
		Hierarchy string `json:"hierarchy"`
	} `json:"category"`
	Seller struct {
		TaxOffice              string `json:"taxOffice"`
		OfficialName           string `json:"officialName"`
		RegisteredEmailAddress string `json:"registeredEmailAddress"`
		RegistrationNumber     string `json:"registrationNumber"`
		TaxNumber              string `json:"taxNumber"`
		Address                string `json:"address"`
		BusinessType           string `json:"businessType"`
		CodEligible            bool   `json:"codEligible"`
	} `json:"sellerInfo"`
	WebCategoryTree []WebCategory `json:"webCategoryTree"`
	RatingScore     struct {
		AverageRating float64 `json:"averageRating"`
		CommentCount  int     `json:"commentCount"`
		TotalCount    int     `json:"totalCount"`
	} `json:"ratingScore"`
	Price    ProductPrice `json:"price"`
	Quantity int          `json:"quantity"`
}

type ProductItem struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Brand       string `json:"brand"`
	Image       string `json:"image"`
	RatingScore struct {
		AverageRating float64 `json:"averageRating"`
	} `json:"ratingScore"`
	Price struct {
		SellingPrice float64 `json:"sellingPrice"`
	} `json:"price"`
}

type Root struct {
	Data struct {
		Contents []ProductItem `json:"contents"`
	} `json:"data"`
}

type PriceStockLog struct {
	gorm.Model
	ProductID  uint
	OldPrice   string
	NewPrice   string
	OldStock   string
	NewStock   string
	ChangeTime time.Time
}

type User struct {
	gorm.Model
	Email       string `gorm:"uniqueIndex;not null"`
	Username    string
	Password    string
	Name        string
	IsActive    bool `gorm:"default:true"`
	LastLoginAt time.Time
}

type UserFavorite struct {
	gorm.Model
	UserID    uint `gorm:"index:idx_user_product,unique"`
	ProductID uint `gorm:"index:idx_user_product,unique"`
	AddedAt   time.Time
}

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

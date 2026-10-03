package application

import (
	"encoding/json"
	"math"
	"search-service/internal/domain"
	"strings"
	"time"
)

const gigIndexedSubject = "search.gig.indexed"

type gigPublishedEvent struct {
	GigID          string                `json:"gig_id"`
	FreelancerID   string                `json:"freelancer_id"`
	SellerUsername string                `json:"seller_username"`
	Slug           string                `json:"slug"`
	Title          string                `json:"title"`
	Description    string                `json:"description"`
	PictureFileID  string                `json:"picture_file_id"`
	PublishedAt    *time.Time            `json:"published_at,omitempty"`
	Packages       []gigPublishedPackage `json:"packages"`
}

type gigPublishedPackage struct {
	PriceCents int64 `json:"price_cents"`
}

func parseGigPublished(payload []byte) (*gigPublishedEvent, error) {
	var event gigPublishedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func buildDocument(event *gigPublishedEvent) domain.GigDocument {
	doc := domain.GigDocument{
		ID:             strings.TrimSpace(event.GigID),
		FreelancerID:   strings.TrimSpace(event.FreelancerID),
		SellerUsername: strings.TrimSpace(event.SellerUsername),
		Slug:           strings.TrimSpace(event.Slug),
		Title:          strings.TrimSpace(event.Title),
		Description:    strings.TrimSpace(event.Description),
		PictureFileID:  strings.TrimSpace(event.PictureFileID),
		PublishedAt:    "",
	}
	if event.PublishedAt != nil {
		doc.PublishedAt = event.PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	minPrice := int64(^uint64(0) >> 1)
	for _, pkg := range event.Packages {
		if pkg.PriceCents <= 0 {
			continue
		}
		if pkg.PriceCents < minPrice {
			minPrice = pkg.PriceCents
		}
	}
	if minPrice == math.MaxInt64 {
		minPrice = 0
	}
	doc.MinPrice = minPrice
	return doc
}

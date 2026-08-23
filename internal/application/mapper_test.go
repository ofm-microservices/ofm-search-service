package application

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildDocumentIncludesSellerUsernameAndPictureFileID(t *testing.T) {
	now := time.Date(2026, 6, 30, 10, 15, 0, 123456000, time.UTC)
	doc := buildDocument(&gigPublishedEvent{
		GigID:          " gig-1 ",
		FreelancerID:   " freelancer-1 ",
		SellerUsername: " seller-1 ",
		Slug:           " slug-1 ",
		Title:          " Gig title ",
		Description:    " Gig description ",
		PictureFileID:  " cover-file ",
		PublishedAt:    &now,
		Packages: []gigPublishedPackage{
			{PriceCents: 0},
			{PriceCents: 2500},
			{PriceCents: 1500},
		},
	})

	if doc.ID != "gig-1" {
		t.Fatalf("unexpected gig id: %q", doc.ID)
	}
	if doc.PictureFileID != "cover-file" {
		t.Fatalf("unexpected picture file id: %q", doc.PictureFileID)
	}
	if doc.SellerUsername != "seller-1" {
		t.Fatalf("unexpected seller username: %q", doc.SellerUsername)
	}
	if doc.MinPrice != 1500 {
		t.Fatalf("unexpected min price: %d", doc.MinPrice)
	}
	if doc.PublishedAt != now.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("unexpected published at: %q", doc.PublishedAt)
	}
}

func TestBuildDocumentOmitsEmptyPublishedAtForElasticsearch(t *testing.T) {
	doc := buildDocument(&gigPublishedEvent{GigID: "gig-1", Title: "Draft gig"})
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" {
		t.Fatalf("empty published_at must not be indexed: %s", raw)
	}
	if _, ok := value["published_at"]; ok {
		t.Fatalf("empty published_at must not be indexed: %s", raw)
	}
}

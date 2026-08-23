package domain

import "context"

// GigDocument is the minimal search document stored in Elasticsearch.
type GigDocument struct {
	ID             string  `json:"gig_id"`
	Title          string  `json:"title"`
	Description    string  `json:"description"`
	FreelancerID   string  `json:"freelancer_id"`
	SellerUsername string  `json:"seller_username"`
	Slug           string  `json:"slug"`
	PublishedAt    string  `json:"published_at,omitempty"`
	MinPrice       int64   `json:"min_price"`
	Picture        string  `json:"picture"`
	PictureFileID  string  `json:"picture_file_id"`
	ReviewsCount   int64   `json:"reviews_count"`
	Rating         float64 `json:"rating"`
}

// SearchQuery captures the public search request.
type SearchQuery struct {
	Query  string
	Sort   int32
	Order  int32
	Cursor string
	Limit  int
}

// SearchResult is one public search row.
type SearchResult struct {
	ID             string  `json:"id"`
	Title          string  `json:"title"`
	Description    string  `json:"description"`
	Picture        string  `json:"picture"`
	ReviewsCount   int64   `json:"reviews_count"`
	Rating         float64 `json:"rating"`
	MinPrice       int64   `json:"min_price"`
	Slug           string  `json:"slug"`
	FreelancerID   string  `json:"freelancer_id"`
	SellerUsername string  `json:"seller_username"`
	PublishedAt    string  `json:"published_at"`
}

// SearchPage represents a page of search results.
type SearchPage struct {
	Services []SearchResult
	Cursor   string
	HasMore  bool
}

// SearchRepository stores and queries search documents.
type SearchRepository interface {
	UpsertGig(ctx context.Context, doc GigDocument) error
	UpdateGigPicture(ctx context.Context, gigID, picture string) error
	DeleteGig(ctx context.Context, gigID string) error
	Search(ctx context.Context, q SearchQuery) (*SearchPage, error)
}

package elasticsearch

import (
	"context"
	"encoding/json"
	"search-service/internal/domain"
	"strings"
)

type repository struct {
	cl *client
}

// ClaimEvent atomically records an event in Elasticsearch and returns false
// when the event was already projected.
func (r *repository) ClaimEvent(ctx context.Context, eventID string) (bool, error) {
	return r.cl.claimEvent(ctx, eventID)
}

// ReleaseEvent removes a failed projection claim so Kafka retry can retry it.
func (r *repository) ReleaseEvent(ctx context.Context, eventID string) error {
	return r.cl.releaseEvent(ctx, eventID)
}

// NewRepository constructs the Elasticsearch-backed search repository.
func NewRepository(cfg Config) (Repository, error) {
	cl, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &repository{cl: cl}, nil
}

func (r *repository) UpsertGig(ctx context.Context, doc domain.GigDocument) error {
	return r.cl.upsert(ctx, doc)
}

func (r *repository) UpdateGigPicture(ctx context.Context, gigID, picture string) error {
	return r.cl.updatePicture(ctx, gigID, picture)
}

func (r *repository) DeleteGig(ctx context.Context, gigID string) error {
	return r.cl.delete(ctx, gigID)
}

func (r *repository) Search(ctx context.Context, q domain.SearchQuery) (*domain.SearchPage, error) {
	if q.Limit <= 0 {
		q.Limit = 20
	}
	body := buildSearchBody(q, q.Limit+1)
	hits, err := r.cl.search(ctx, body)
	if err != nil {
		return nil, err
	}
	page := &domain.SearchPage{Services: make([]domain.SearchResult, 0, len(hits))}
	for i, hit := range hits {
		if i >= q.Limit {
			page.HasMore = true
			page.Cursor = encodeCursor(hit.Sort)
			break
		}
		page.Services = append(page.Services, toResult(hit.Source))
		if i == len(hits)-1 {
			page.Cursor = encodeCursor(hit.Sort)
		}
	}
	return page, nil
}

func toResult(doc domain.GigDocument) domain.SearchResult {
	return domain.SearchResult{
		ID:             doc.ID,
		Title:          doc.Title,
		Description:    doc.Description,
		Picture:        doc.Picture,
		ReviewsCount:   doc.ReviewsCount,
		Rating:         doc.Rating,
		MinPrice:       doc.MinPrice,
		Slug:           doc.Slug,
		FreelancerID:   doc.FreelancerID,
		SellerUsername: doc.SellerUsername,
		PublishedAt:    doc.PublishedAt,
	}
}

func buildSearchBody(q domain.SearchQuery, size int) []byte {
	type sortItem map[string]any
	sort := make([]sortItem, 0, 2)
	desc := q.Order != 1

	if q.Query != "" {
		sort = append(sort, sortItem{"_score": map[string]any{"order": "desc"}})
	}

	switch q.Sort {
	case 1:
		order := "desc"
		if !desc {
			order = "asc"
		}
		sort = append(sort, sortItem{"min_price": map[string]any{"order": order}})
	default:
		order := "desc"
		if !desc {
			order = "asc"
		}
		sort = append(sort, sortItem{"published_at": map[string]any{"order": order}})
	}

	sort = append(sort, sortItem{"gig_id": map[string]any{"order": "desc"}})

	query := map[string]any{
		"size":    size,
		"sort":    sort,
		"_source": []string{"title", "description", "gig_id", "freelancer_id", "seller_username", "slug", "published_at", "min_price", "picture", "picture_file_id", "reviews_count", "rating"},
	}
	if strings.TrimSpace(q.Query) != "" {
		query["query"] = map[string]any{
			"multi_match": map[string]any{
				"query":     q.Query,
				"fields":    []string{"title^3", "description"},
				"fuzziness": "AUTO",
				"type":      "best_fields",
			},
		}
	}
	if strings.TrimSpace(q.Cursor) != "" {
		if fields, err := decodeCursor(q.Cursor); err == nil {
			query["search_after"] = fields
		}
	}
	data, _ := json.Marshal(query)
	return data
}

func (r *repository) Close() error { return nil }

package grpc

import (
	"strconv"

	searchv1 "github.com/ofm-microservices/ofm-common/proto/search/v1"
	"search-service/internal/application"
)

type searchMapper struct{}

func newSearchMapper() SearchMapper { return &searchMapper{} }

func (m *searchMapper) ToQuery(req *searchv1.SearchRequest) application.SearchQuery {
	if req == nil {
		return application.SearchQuery{}
	}
	return application.SearchQuery{
		Query:  req.GetQuery(),
		Sort:   req.GetSort(),
		Order:  req.GetOrder(),
		Cursor: req.GetCursor(),
	}
}

func (m *searchMapper) ToResponse(res *application.SearchResultPage) *searchv1.SearchResponse {
	if res == nil {
		return &searchv1.SearchResponse{}
	}
	out := &searchv1.SearchResponse{
		Cursor:  res.Cursor,
		HasMore: res.HasMore,
	}
	out.Services = make([]*searchv1.SearchResult, 0, len(res.Services))
	for _, item := range res.Services {
		out.Services = append(out.Services, &searchv1.SearchResult{
			Id:             item.ID,
			Title:          item.Title,
			Description:    item.Description,
			Picture:        item.Picture,
			ReviewsCount:   item.ReviewsCount,
			Rating:         item.Rating,
			MinPrice:       item.MinPrice,
			Slug:           item.Slug,
			FreelancerId:   item.FreelancerID,
			SellerUsername: item.SellerUsername,
			PublishedAt:    item.PublishedAt,
		})
	}
	return out
}

func (m *searchMapper) parseInt32(s string) int32 {
	v, _ := strconv.ParseInt(s, 10, 32)
	return int32(v)
}

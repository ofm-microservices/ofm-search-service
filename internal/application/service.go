package application

import (
	"context"
	"search-service/internal/domain"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type searchService struct {
	repo   domain.SearchRepository
	broker EventBroker
	files  FileURLClient
	log    Logger
}

// ClaimEvent records a projection event in the repository's durable marker
// store before Elasticsearch projection work begins.
func (s *searchService) ClaimEvent(ctx context.Context, eventID string) (bool, error) {
	claimer, ok := s.repo.(interface {
		ClaimEvent(context.Context, string) (bool, error)
	})
	if !ok {
		return true, nil
	}
	return claimer.ClaimEvent(ctx, eventID)
}

// ReleaseEvent removes a failed projection claim before broker retry.
func (s *searchService) ReleaseEvent(ctx context.Context, eventID string) error {
	claimer, ok := s.repo.(interface {
		ReleaseEvent(context.Context, string) error
	})
	if !ok {
		return nil
	}
	return claimer.ReleaseEvent(ctx, eventID)
}

// New constructs the application service responsible for search indexing and queries.
func New(repo domain.SearchRepository, broker EventBroker, files FileURLClient, log Logger) (SearchService, error) {
	if repo == nil {
		return nil, ErrNilRepository
	}
	if broker == nil {
		return nil, ErrNilBroker
	}
	if files == nil {
		return nil, ErrNilFileClient
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &searchService{
		repo:   repo,
		broker: broker,
		files:  files,
		log:    log.With(logging.String("module", "application")),
	}, nil
}

func (s *searchService) ApplyGigPublished(ctx context.Context, payload []byte) error {
	event, err := parseGigPublished(payload)
	if err != nil {
		return err
	}
	doc := buildDocument(event)
	if strings.TrimSpace(doc.ID) == "" {
		return domain.ErrInvalidQuery
	}

	if err := s.repo.UpsertGig(ctx, doc); err != nil {
		return err
	}
	if fileID := strings.TrimSpace(doc.PictureFileID); fileID != "" {
		s.resolvePictureAsync(doc.ID, fileID)
	}

	encoded, err := jsonMarshal(doc)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, gigIndexedSubject, encoded)
}

func (s *searchService) ApplyGigDeleted(ctx context.Context, payload []byte) error {
	gigID := strings.TrimSpace(string(payload))
	if gigID == "" {
		return nil
	}
	return s.repo.DeleteGig(ctx, gigID)
}

func (s *searchService) Search(ctx context.Context, q domain.SearchQuery) (*domain.SearchPage, error) {
	if q.Limit <= 0 {
		q.Limit = 20
	}
	return s.repo.Search(ctx, q)
}

func (s *searchService) resolvePictureAsync(gigID, fileID string) {
	go func() {
		for attempt, delay := range []time.Duration{0, time.Second, 3 * time.Second} {
			if delay > 0 {
				time.Sleep(delay)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			url, err := s.files.GetFileURL(ctx, fileID)
			cancel()
			if err != nil {
				s.log.Error("resolve gig picture url failed",
					logging.Operation("search.gig.resolve_picture_url"),
					logging.Attempt(attempt+1),
					logging.String("gig_id", gigID),
					logging.String("file_id", fileID),
					logging.Err(err),
				)
				continue
			}
			url = strings.TrimSpace(url)
			if url == "" {
				s.log.Error("resolve gig picture url returned empty url",
					logging.Operation("search.gig.resolve_picture_url"),
					logging.Attempt(attempt+1),
					logging.String("gig_id", gigID),
					logging.String("file_id", fileID),
				)
				continue
			}

			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			err = s.repo.UpdateGigPicture(ctx, gigID, url)
			cancel()
			if err != nil {
				s.log.Error("update gig picture failed",
					logging.Operation("search.gig.update_picture"),
					logging.Attempt(attempt+1),
					logging.String("gig_id", gigID),
					logging.String("file_id", fileID),
					logging.String("picture_url", url),
					logging.Err(err),
				)
				continue
			}

			s.log.Info("gig picture resolved",
				logging.Operation("search.gig.resolve_picture_url"),
				logging.String("gig_id", gigID),
				logging.String("file_id", fileID),
			)
			return
		}
		s.log.Warn("gig picture enrichment exhausted retries",
			logging.Operation("search.gig.resolve_picture_url"),
			logging.String("gig_id", gigID),
			logging.String("file_id", fileID),
		)
	}()
}

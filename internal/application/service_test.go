package application

import (
	"context"
	"sync"
	"testing"
	"time"

	"search-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type repoFake struct {
	mu             sync.Mutex
	upserts        []domain.GigDocument
	pictureUpdates []pictureUpdate
	updateCh       chan pictureUpdate
}

type pictureUpdate struct {
	gigID   string
	picture string
}

func newRepoFake() *repoFake {
	return &repoFake{updateCh: make(chan pictureUpdate, 1)}
}

func (r *repoFake) UpsertGig(_ context.Context, doc domain.GigDocument) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserts = append(r.upserts, doc)
	return nil
}

func (r *repoFake) UpdateGigPicture(_ context.Context, gigID, picture string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := pictureUpdate{gigID: gigID, picture: picture}
	r.pictureUpdates = append(r.pictureUpdates, item)
	r.updateCh <- item
	return nil
}

func (r *repoFake) DeleteGig(context.Context, string) error { return nil }

func (r *repoFake) Search(context.Context, domain.SearchQuery) (*domain.SearchPage, error) {
	return &domain.SearchPage{}, nil
}

type brokerFake struct {
	mu        sync.Mutex
	published [][]byte
}

func (b *brokerFake) Publish(_ context.Context, _ string, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, append([]byte(nil), payload...))
	return nil
}

func (b *brokerFake) Close() error { return nil }

type fileClientFake struct {
	url   string
	err   error
	calls []string
	mu    sync.Mutex
}

func (f *fileClientFake) GetFileURL(_ context.Context, fileID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fileID)
	return f.url, f.err
}

func TestApplyGigPublishedResolvesPictureURLInBackground(t *testing.T) {
	logger, err := logging.New("search-service", "test", "error")
	if err != nil {
		t.Fatalf("logger: %v", err)
	}

	repo := newRepoFake()
	broker := &brokerFake{}
	files := &fileClientFake{url: "https://cdn.example/cover.png"}

	svc, err := New(repo, broker, files, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	payload := []byte(`{
		"gig_id":"gig-1",
		"freelancer_id":"freelancer-1",
		"seller_username":"seller-1",
		"slug":"gig-1",
		"title":"Gig title",
		"description":"Gig description",
		"picture_file_id":"cover-file",
		"packages":[{"price_cents":2000}]
	}`)

	if err := svc.ApplyGigPublished(context.Background(), payload); err != nil {
		t.Fatalf("apply gig published: %v", err)
	}

	repo.mu.Lock()
	if len(repo.upserts) != 1 {
		repo.mu.Unlock()
		t.Fatalf("unexpected upsert count: %d", len(repo.upserts))
	}
	if repo.upserts[0].SellerUsername != "seller-1" {
		saved := repo.upserts[0].SellerUsername
		repo.mu.Unlock()
		t.Fatalf("unexpected seller username: %q", saved)
	}
	repo.mu.Unlock()

	select {
	case upd := <-repo.updateCh:
		if upd.gigID != "gig-1" {
			t.Fatalf("unexpected gig id: %q", upd.gigID)
		}
		if upd.picture != "https://cdn.example/cover.png" {
			t.Fatalf("unexpected picture url: %q", upd.picture)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for picture enrichment")
	}

	files.mu.Lock()
	defer files.mu.Unlock()
	if len(files.calls) != 1 || files.calls[0] != "cover-file" {
		t.Fatalf("unexpected file client calls: %#v", files.calls)
	}
}

# OFM Search Service

## Purpose

The Search Service owns the search index and search query behavior for publicly searchable marketplace data. It consumes indexed entity changes but does not own the source gig, user, or order records. Status: active.

## Interfaces and flow

The gateway sends search queries to the service. Indexing consumers receive Kafka/CDC entity events, transform the source representation into search documents, and update the index idempotently. Source services remain authoritative; a missing or stale index is repaired by replay or reindex tooling.

## Configuration

.env.example groups are Elasticsearch/OpenSearch endpoint and index settings, Kafka brokers/topics/consumer group, HTTP/gRPC listeners, retry settings, and observability. Search endpoint values select the index cluster; Kafka values select indexing events; retry values control replay behavior.

## Local development

    cp .env.example .env
    go run ./cmd/search-service
    go test ./...

## Build and operations

Dockerfile builds ofm/search-service:<tag>. Helm and ofm-infra provide the index cluster and deployment. Diagnose consumer lag, indexing errors, document versions, and query traces.


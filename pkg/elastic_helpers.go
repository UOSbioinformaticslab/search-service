package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

var (
	aggregationFieldOverrides   = make(map[string]string)
	aggregationFieldOverridesMu sync.RWMutex
	fielddataRegex              = regexp.MustCompile(`\[(?P<field>[^\[\]]+)\]`)
)

func resolveAggregationField(key string) string {
	aggregationFieldOverridesMu.RLock()
	defer aggregationFieldOverridesMu.RUnlock()

	if override, ok := aggregationFieldOverrides[key]; ok {
		return override
	}

	return key
}

func setAggregationFieldOverride(key, override string) {
	aggregationFieldOverridesMu.Lock()
	defer aggregationFieldOverridesMu.Unlock()

	if existing, ok := aggregationFieldOverrides[key]; ok && existing == override {
		return
	}

	aggregationFieldOverrides[key] = override
}

func executeElasticQuery(index string, elasticQuery gin.H) (SearchResponse, []byte) {
	var (
		elasticResp SearchResponse
		body        []byte
	)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(elasticQuery); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticQuery,
			err.Error()),
		)
		return elasticResp, body
	}

	response, err := ElasticClient.Search(
		ElasticClient.Search.WithIndex(index),
		ElasticClient.Search.WithBody(&buf),
	)

	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		return elasticResp, body
	}

	if response == nil {
		slog.Warn(fmt.Sprintf("Elastic returned nil response for index %s", index))
		return elasticResp, body
	}

	defer response.Body.Close()

	body, err = io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
		return elasticResp, body
	}

	json.Unmarshal(body, &elasticResp)

	return elasticResp, body
}

func executeSearchWithRetry(index string, buildQuery func() gin.H) (SearchResponse, []byte) {
	var (
		lastResp SearchResponse
		lastBody []byte
	)

	for attempt := 0; attempt < 2; attempt++ {
		elasticQuery := buildQuery()
		resp, body := executeElasticQuery(index, elasticQuery)
		lastResp, lastBody = resp, body

		if resp.Hits.Hits != nil || len(body) == 0 {
			break
		}

		if !updateAggregationOverridesFromError(body) {
			break
		}
	}

	return lastResp, lastBody
}

func updateAggregationOverridesFromError(body []byte) bool {
	if len(body) == 0 {
		return false
	}

	var elasticError SearchErrorResponse
	if err := json.Unmarshal(body, &elasticError); err != nil {
		return false
	}

	rootCauses, ok := elasticError.Error["root_cause"]
	if !ok || len(rootCauses) == 0 {
		return false
	}

	updated := false
	for _, cause := range rootCauses {
		if !(strings.Contains(cause.Reason, "Text fields are not optimised") ||
			strings.Contains(cause.Reason, "Fielddata is disabled")) {
			continue
		}

		matches := fielddataRegex.FindAllStringSubmatch(cause.Reason, -1)
		for _, match := range matches {
			if len(match) < 2 {
				continue
			}
			field := strings.TrimSpace(match[1])
			if field == "" || strings.Contains(field, " ") {
				continue
			}

			field = strings.TrimSuffix(field, ".keyword")

			override := fmt.Sprintf("%s.keyword", field)

			aggregationFieldOverridesMu.RLock()
			_, exists := aggregationFieldOverrides[field]
			aggregationFieldOverridesMu.RUnlock()
			if exists {
				// Override already exists, but we still consider this an update
				// because it means we detected the error and have a solution
				updated = true
				continue
			}

			setAggregationFieldOverride(field, override)
			slog.Debug(fmt.Sprintf("Updated aggregation field override: %s -> %s", field, override))
			updated = true
		}
	}

	return updated
}

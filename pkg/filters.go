package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type Aggregations struct {
	Took         int                    `json:"took"`
	TimedOut     bool                   `json:"timed_out"`
	Aggregations map[string]interface{} `json:"aggregations"`
}

type FilterRequest struct {
	Filters []map[string]interface{} `json:"filters"`
}

/*
ListFilters lists all the values available for the filter type and key pairs
in the given FilterRequest.
The `type` must match an existing elasticsearch index.
The `keys` must match a field name in that index.
The expected structure of a FilterRequest is:

```

	{
		"filters": [
			{
				"type": "dataset",
				"keys": "publisherName"
			},
			{
				"type": "dataset",
				"keys": "containsTissue"
			}
		]
	}

```
*/
func ListFilters(c *gin.Context) {
	var filterRequest FilterRequest
	if err := c.BindJSON(&filterRequest); err != nil {
		slog.Warn(fmt.Sprintf("Could not bind filter request: %s", c.Request.Body))
	}

	var allFilters []gin.H

	for _, filter := range filterRequest.Filters {
		filterType, ok := filter["type"].(string)
		if !ok {
			slog.Debug(fmt.Sprintf("Filter type in %s not recognised", filter))
		}
		var index string
		if filterType == "dataUseRegister" || filterType == "dataProvider" {
			index = strings.ToLower(filterType)
		} else if filterType == "paper" {
			index = "publication"
		} else {
			index = filterType
		}

		filterKey, ok := filter["keys"].(string)
		if !ok {
			slog.Debug(fmt.Sprintf("Filter keys in %s not recognised", filter))
		}

		var (
			filterData gin.H
			success    bool
		)

		for attempt := 0; attempt < 2; attempt++ {
			var buf bytes.Buffer
			elasticQuery := filtersRequest(filter)
			if err := json.NewEncoder(&buf).Encode(elasticQuery); err != nil {
				slog.Info(fmt.Sprintf("Failed to encode filters request: %s", err.Error()))
			}

			response, err := ElasticClient.Search(
				ElasticClient.Search.WithIndex(index),
				ElasticClient.Search.WithBody(&buf),
			)

			if err != nil {
				slog.Warn(err.Error())
				break
			}

			if response == nil {
				slog.Warn(fmt.Sprintf("No response received for filter: %s - %s", filterType, filterKey))
				break
			}

			if response.StatusCode >= http.StatusBadRequest {
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if attempt == 0 && updateAggregationOverridesFromError(body) {
					slog.Debug(fmt.Sprintf(
						"Retrying filter query for %s - %s after updating field override",
						filterType,
						filterKey,
					))
					continue
				}
				// Only log warning if retry failed or this was the second attempt
				slog.Warn(fmt.Sprintf(
					"Elastic returned status %d for filter: %s - %s with body: %s",
					response.StatusCode,
					filterType,
					filterKey,
					string(body),
				))
				break
			}

			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				slog.Warn(err.Error())
				break
			}

			var elasticResp SearchResponse
			json.Unmarshal(body, &elasticResp)

			if len(elasticResp.Aggregations) == 0 {
				if attempt == 0 && updateAggregationOverridesFromError(body) {
					slog.Debug(fmt.Sprintf(
						"Retrying filter query for %s - %s after updating field override",
						filterType,
						filterKey,
					))
					continue
				}
				// Only log warning if retry failed or this was the second attempt
				slog.Warn(fmt.Sprintf("No aggreations returned for filter: %s - %s", filterType, filterKey))
				break
			}

			if filterKey == "dateRange" || filterKey == "publicationDate" {
				startDate, ok := safeAggValue(elasticResp.Aggregations, "startDate")
				if !ok {
					slog.Warn(fmt.Sprintf("No startDate aggregation returned for filter: %s - %s", filterType, filterKey))
					break
				}
				endDate, ok := safeAggValue(elasticResp.Aggregations, "endDate")
				if !ok {
					slog.Warn(fmt.Sprintf("No endDate aggregation returned for filter: %s - %s", filterType, filterKey))
					break
				}
				filterData = gin.H{
					filterType: gin.H{
						filterKey: gin.H{
							"buckets": []gin.H{
								{
									"key":   "startDate",
									"value": startDate,
								},
								{
									"key":   "endDate",
									"value": endDate,
								},
							},
						},
					},
				}
			} else {
				filterData = gin.H{filterType: elasticResp.Aggregations}
			}

			success = true
			break
		}

		if success {
			allFilters = append(allFilters, filterData)
		}
	}

	c.JSON(http.StatusOK, gin.H{"filters": allFilters})
}

func filtersRequest(filter map[string]interface{}) gin.H {
	filterKey, ok := filter["keys"].(string)
	var aggs gin.H
	if !ok {
		slog.Info(fmt.Sprintf("Filter key in %s not recognised", filter["keys"]))
	}
	if filterKey == "dateRange" {
		aggs = gin.H{
			"size": 0,
			"aggs": gin.H{
				"startDate": gin.H{
					"min": gin.H{"field": "startDate"},
				},
				"endDate": gin.H{
					"max": gin.H{"field": "endDate"},
				},
			},
		}
	} else if filterKey == "publicationDate" {
		aggs = gin.H{
			"size": 0,
			"aggs": gin.H{
				"startDate": gin.H{
					"min": gin.H{"field": "publicationDate"},
				},
				"endDate": gin.H{
					"max": gin.H{"field": "publicationDate"},
				},
			},
		}
	} else if filterKey == "populationSize" {
		ranges := populationRanges()
		aggs = gin.H{
			"size": 0,
			"aggs": gin.H{
				"populationSize": gin.H{
					"range": gin.H{"field": filterKey, "ranges": ranges},
				},
			},
		}
	} else {
		aggs = gin.H{
			"size": 0,
			"aggs": gin.H{
				filterKey: gin.H{
					"terms": gin.H{
						"field": resolveAggregationField(filterKey),
						"size":  1000,
					},
				},
			},
		}
	}
	return aggs
}

func safeAggValue(aggs map[string]interface{}, key string) (string, bool) {
	if aggs == nil {
		return "", false
	}

	rawAgg, ok := aggs[key]
	if !ok || rawAgg == nil {
		return "", false
	}

	aggMap, ok := rawAgg.(map[string]interface{})
	if !ok {
		return "", false
	}

	if value, ok := aggMap["value_as_string"].(string); ok && value != "" {
		return value, true
	}

	if value, exists := aggMap["value"]; exists && value != nil {
		return fmt.Sprintf("%v", value), true
	}

	return "", false
}

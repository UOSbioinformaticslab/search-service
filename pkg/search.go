package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"

	bigqueryclient "hdruk/search-service/utils/bigquery"
	"hdruk/search-service/utils/elastic"
)

var (
	ElasticClient  *elasticsearch.Client
	BigQueryClient *bigquery.Client
	BQUpload       = uploadSearchAnalytics

	// Env vars read once at startup to avoid repeated syscalls on every request.
	searchNoRecords            int
	searchNoRecordsAggregation int
	searchNoRecordsSimilar     int
	explanationEnabled         bool
	populationRangesCache      []gin.H
)

func init() {
	populationRangesCache = buildPopulationRanges()
}

func DefineElasticClient() {
	ElasticClient = elastic.DefaultClient()
	BigQueryClient = bigqueryclient.DefaultBigQueryClient()

	// Read env vars here, after godotenv.Load() has been called in main().
	// init() runs before main(), so env vars from .env are not yet available there.
	searchNoRecords, _ = strconv.Atoi(os.Getenv("SEARCH_NO_RECORDS"))
	searchNoRecordsAggregation, _ = strconv.Atoi(os.Getenv("SEARCH_NO_RECORDS_AGGREGATION"))
	searchNoRecordsSimilar, _ = strconv.Atoi(os.Getenv("SEARCH_NO_RECORDS_SIMILAR_SEARCH"))
	_, explanationEnabled = os.LookupEnv("SEARCH_EXPLANATION_EXTRACTOR")
}

/*
Query represents the search query incoming from the gateway-api

The body of the request is expected to have the following structure
```

	{
		"query": <query_term>,
		"filters": {
			<type>: {
				<key>: [
					<value1>,
					...
				]
			}
		}
	}

```
where:
- query_term is a string e.g. "asthma"
- type is a string matching the name of an elasticsearch index e.g. "dataset"
- key is a string matching a field in the elastic search index specified e.g. "publisherName"
- value1 is a value matching values in the specified fields of the elastic index e.g. "publisher A"
*/
type Query struct {
	QueryString    string                            `json:"query"`
	Filters        map[string]map[string]interface{} `json:"filters"`
	Aggregations   []map[string]interface{}          `json:"aggs"`
	IDs            []string                          `json:"ids"`
	PartnerContext *string                           `json:"partnerContext"`
}

type SimilarSearch struct {
	ID string `json:"id"`
}

// SearchResponse represents the expected structure of results returned by ElasticSearch
type SearchResponse struct {
	Took         int                    `json:"took"`
	TimedOut     bool                   `json:"timed_out"`
	Shards       map[string]interface{} `json:"_shards"`
	Hits         HitsField              `json:"hits"`
	Aggregations map[string]interface{} `json:"aggregations"`
}

type HitsField struct {
	Total    map[string]interface{} `json:"total"`
	MaxScore float64                `json:"max_score"`
	Hits     []Hit                  `json:"hits"`
}

type Hit struct {
	Explanation map[string]interface{} `json:"_explanation"`
	Id          string                 `json:"_id"`
	Score       float64                `json:"_score"`
	Source      map[string]interface{} `json:"_source"`
	Highlight   map[string][]string    `json:"highlight"`
}

type SearchErrorResponse struct {
	Error  map[string][]RootCause `json:"error"`
	Status int                    `json:"status"`
}

type RootCause struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	Index  string `json:"index"`
}

type SearchAnalytics struct {
	UUID             string
	Timestamp        string
	EntityType       string
	SearchTerm       string
	FilterUsed       string
	PageResults      string
	EntitiesReturned int
}

func (a *SearchAnalytics) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"UUID":             a.UUID,
		"Timestamp":        a.Timestamp,
		"SearchTerm":       a.SearchTerm,
		"FilterUsed":       a.FilterUsed,
		"PageResults":      a.PageResults,
		"EntitiesReturned": a.EntitiesReturned,
	}, "", nil
}

func HealthCheck(c *gin.Context) {
	results := make(map[string]interface{})
	responseStatus := http.StatusOK

	elasticResponse, err := ElasticClient.Info()
	if err != nil {
		slog.Debug(fmt.Sprintf("%v", err.Error()))
		responseStatus = http.StatusServiceUnavailable
		results["elastic_status"] = http.StatusServiceUnavailable
	} else {
		defer elasticResponse.Body.Close()
		results["elastic_status"] = elasticResponse.StatusCode
		if elasticResponse.StatusCode != 200 {
			responseStatus = elasticResponse.StatusCode
			body, err := io.ReadAll(elasticResponse.Body)
			if err != nil {
				slog.Debug(fmt.Sprintf("Failed to read elastic response with %s", err.Error()))
			}
			var elasticError SearchErrorResponse
			if json.Unmarshal(body, &elasticError) == nil {
				if rootCauses, ok := elasticError.Error["root_cause"]; ok && len(rootCauses) > 0 {
					results["elastic_error"] = rootCauses[0].Type
				}
			}
		}
	}

	ctx := context.Background()
	_, bqErr := BigQueryClient.Dataset(os.Getenv("BQ_DATASET_NAME")).Metadata(ctx)
	if bqErr != nil {
		var e *googleapi.Error
		if errors.As(bqErr, &e) {
			results["bigquery_status"] = e.Code
			results["bigquery_message"] = e.Message
			responseStatus = e.Code
		} else {
			results["bigquery_status"] = http.StatusInternalServerError
			responseStatus = http.StatusInternalServerError
		}
	} else {
		results["bigquery_status"] = 200
	}

	results["search_service_status"] = "OK"
	c.JSON(responseStatus, results)
}

func EnsureTableExists() error {
	ctx := context.Background()
	dataset := BigQueryClient.Dataset(os.Getenv("BQ_DATASET_NAME"))
	table := dataset.Table(os.Getenv("BQ_TABLE_NAME"))

	schema := bigquery.Schema{
		{Name: "UUID", Required: true, Type: bigquery.StringFieldType},
		{Name: "Timestamp", Required: false, Type: bigquery.DateTimeFieldType},
		{Name: "EntityType", Required: true, Type: bigquery.StringFieldType},
		{Name: "SearchTerm", Required: false, Type: bigquery.StringFieldType},
		{Name: "FilterUsed", Repeated: false, Type: bigquery.JSONFieldType},
		{Name: "PageResults", Required: false, Type: bigquery.JSONFieldType},
		{Name: "EntitiesReturned", Required: true, Type: bigquery.IntegerFieldType},
	}

	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		var e *googleapi.Error
		if errors.As(err, &e) && e.Code == 409 {
			slog.Debug(fmt.Sprintf("%s", err.Error()))
			return nil
		}
		slog.Info(fmt.Sprintf("Could not create table: %s", err.Error()))
		return err
	}
	return nil
}

// executeSearch is the shared implementation for all entity index searches.
// It encodes the query, calls Elastic, parses the response, and applies
// explanation stripping and aggregation flattening.
func executeSearch(ctx context.Context, index string, elasticQuery gin.H, query Query, entityType string, searchUuid string) SearchResponse {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(elasticQuery); err != nil {
		slog.Debug(fmt.Sprintf("Failed to encode elastic query with %s", err.Error()))
	}

	response, err := ElasticClient.Search(
		ElasticClient.Search.WithContext(ctx),
		ElasticClient.Search.WithIndex(index),
		ElasticClient.Search.WithBody(&buf),
	)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to execute elastic query on index %s: %s", index, err.Error()))
		return SearchResponse{}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("Failed to read elastic response with %s", err.Error()))
	}

	var elasticResp SearchResponse
	if err := json.Unmarshal(body, &elasticResp); err != nil {
		slog.Debug(fmt.Sprintf("Failed to unmarshal elastic response with %s", err.Error()))
	}

	if elasticResp.Hits.Hits == nil {
		var elasticError SearchErrorResponse
		if json.Unmarshal(body, &elasticError) == nil {
			if rootCauses, ok := elasticError.Error["root_cause"]; ok && len(rootCauses) > 0 && rootCauses[0].Reason != "" {
				slog.Warn(fmt.Sprintf("Search query returned elastic error: %s", rootCauses[0].Reason))
			} else {
				slog.Warn("Hits from elastic are null, query may be malformed")
			}
		} else {
			slog.Warn("Hits from elastic are null, query may be malformed")
		}
		slog.Debug(fmt.Sprintf("Null result elastic query: %v", elasticQuery))
	}

	stripExplanation(elasticResp, query, entityType, searchUuid)
	elasticResp.Aggregations = flattenAggs(elasticResp)
	return elasticResp
}

// SearchGeneric performs searches across all entity indices concurrently.
// A 10-second timeout is applied; if any index is unresponsive the request
// returns 504 rather than hanging indefinitely.
// Results are returned grouped by entity type.
func SearchGeneric(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	searchUuid := uuid.New().String()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Buffered channels so goroutines can send and exit even if we return early.
	datasetResults := make(chan SearchResponse, 1)
	toolResults := make(chan SearchResponse, 1)
	collectionResults := make(chan SearchResponse, 1)
	dataUseResults := make(chan SearchResponse, 1)
	publicationResults := make(chan SearchResponse, 1)
	dataProviderResults := make(chan SearchResponse, 1)
	dataCustodianNetworkResults := make(chan SearchResponse, 1)

	go func() {
		datasetResults <- executeSearch(ctx, "dataset", datasetElasticConfig(query), query, "dataset", searchUuid)
	}()
	go func() {
		toolResults <- executeSearch(ctx, "tool", toolsElasticConfig(query), query, "tool", searchUuid)
	}()
	go func() {
		collectionResults <- executeSearch(ctx, "collection", collectionsElasticConfig(query), query, "collection", searchUuid)
	}()
	go func() {
		dataUseResults <- executeSearch(ctx, "datauseregister", dataUseElasticConfig(query), query, "dur", searchUuid)
	}()
	go func() {
		publicationResults <- executeSearch(ctx, "publication", publicationElasticConfig(query), query, "publication", searchUuid)
	}()
	go func() {
		dataProviderResults <- executeSearch(ctx, "dataprovider", dataProviderElasticConfig(query), query, "dataProvider", searchUuid)
	}()
	go func() {
		dataCustodianNetworkResults <- executeSearch(ctx, "datacustodiannetwork", dataCustodianNetworkElasticConfig(query), query, "datacustodiannetwork", searchUuid)
	}()

	results := make(map[string]interface{})
	for i := 0; i < 7; i++ {
		select {
		case <-ctx.Done():
			slog.Warn("SearchGeneric timed out waiting for results")
			c.JSON(http.StatusGatewayTimeout, gin.H{"error": "search timed out"})
			return
		case datasets := <-datasetResults:
			results["dataset"] = datasets
		case tools := <-toolResults:
			results["tool"] = tools
		case collections := <-collectionResults:
			results["collection"] = collections
		case data_uses := <-dataUseResults:
			results["dataUseRegister"] = data_uses
		case publications := <-publicationResults:
			results["publication"] = publications
		case dataProviders := <-dataProviderResults:
			results["dataProvider"] = dataProviders
		case dataCustodianNetworks := <-dataCustodianNetworkResults:
			results["datacustodiannetwork"] = dataCustodianNetworks
		}
	}

	c.JSON(http.StatusOK, results)
}

func DatasetSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "dataset", datasetElasticConfig(query), query, "dataset", searchUuid)
	go BQUpload(query, results, "dataset", searchUuid)
	c.JSON(http.StatusOK, results)
}

// datasetElasticConfig defines the body of the query to the elastic datasets index
func datasetElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		searchableFields := []string{
			"abstract",
			"keywords",
			"description",
			"shortTitle",
			"title",
			"named_entities",
			"datasetDOI",
			"datasetAliases",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"analyzer":  "medterms_search_analyzer",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"analyzer":  "medterms_search_analyzer",
				"operator":  "and",
				"boost":     2,
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":    query.QueryString,
				"type":     "phrase",
				"fields":   searchableFields,
				"analyzer": "medterms_search_analyzer",
				"boost":    3,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["dataset"] {
		var filter gin.H
		if key == "dateRange" {
			filter = gin.H{
				"bool": gin.H{
					"must": []gin.H{
						{"range": gin.H{"startDate": gin.H{"lte": terms.([]interface{})[1]}}},
						{"range": gin.H{"endDate": gin.H{"gte": terms.([]interface{})[0]}}},
					},
				},
			}
		} else if key == "populationSize" {
			includeNull := terms.(map[string]interface{})["includeUnreported"].(bool)
			from := terms.(map[string]interface{})["from"]
			to := terms.(map[string]interface{})["to"]
			if includeNull {
				filter = gin.H{
					"bool": gin.H{
						"should": []gin.H{
							{"range": gin.H{key: gin.H{"gte": from, "lte": to}}},
							{"term": gin.H{"populationSize": -1}},
						},
					},
				}
			} else {
				filter = gin.H{
					"bool": gin.H{
						"must": []gin.H{
							{"range": gin.H{key: gin.H{"gte": from, "lte": to}}},
						},
					},
				}
			}
		} else {
			filters := []gin.H{}
			for _, t := range terms.([]interface{}) {
				filters = append(filters, gin.H{"term": gin.H{key: t}})
			}
			filter = gin.H{"bool": gin.H{"should": filters}}
		}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	finalQuery := mainQuery
	if query.PartnerContext != nil {
		var partnerFilter gin.H
		if *query.PartnerContext == "HDRUK" {
			partnerFilter = gin.H{
				"bool": gin.H{
					"should": []gin.H{
						{"term": gin.H{"partnerContext": "HDRUK"}},
						{"bool": gin.H{"must_not": []gin.H{{"exists": gin.H{"field": "partnerContext"}}}}},
					},
					"minimum_should_match": 1,
				},
			}
		} else {
			partnerFilter = gin.H{"term": gin.H{"partnerContext": *query.PartnerContext}}
		}
		finalQuery = gin.H{
			"bool": gin.H{
				"must":   []gin.H{mainQuery},
				"filter": []gin.H{partnerFilter},
			},
		}
	}

	response := gin.H{
		"size":    searchNoRecords,
		"query":   finalQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"description": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"abstract": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func ToolSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "tool", toolsElasticConfig(query), query, "tool", searchUuid)
	go BQUpload(query, results, "tool", searchUuid)
	c.JSON(http.StatusOK, results)
}

// toolsElasticConfig defines the body of the query to the elastic tools index
func toolsElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		searchableFields := []string{
			"tags",
			"programmingLanguage",
			"name",
			"link",
			"description",
			"resultsInsights",
			"license",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"operator":  "and",
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"fields": searchableFields,
				"type":   "phrase",
				"boost":  2,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["tool"] {
		filters := []gin.H{}
		for _, t := range terms.([]interface{}) {
			filters = append(filters, gin.H{"term": gin.H{key: t}})
		}
		filter := gin.H{"bool": gin.H{"should": filters}}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	response := gin.H{
		"size":    searchNoRecords,
		"query":   mainQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"name": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"description": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func CollectionSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "collection", collectionsElasticConfig(query), query, "collection", searchUuid)
	go BQUpload(query, results, "collection", searchUuid)
	c.JSON(http.StatusOK, results)
}

// collectionsElasticConfig defines the body of the query to the elastic collections index
func collectionsElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		relatedObjectFields := []string{
			"datasetTitles",
			"datasetAbstracts",
		}
		searchableFields := []string{
			"description",
			"name",
			"keywords",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    relatedObjectFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"boost":     2,
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"type":   "phrase",
				"fields": searchableFields,
				"boost":  3,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["collection"] {
		filters := []gin.H{}
		for _, t := range terms.([]interface{}) {
			filters = append(filters, gin.H{"term": gin.H{key: t}})
		}
		filter := gin.H{"bool": gin.H{"should": filters}}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	response := gin.H{
		"size":    searchNoRecords,
		"query":   mainQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"description": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"name": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"keywords": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func DataUseSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "datauseregister", dataUseElasticConfig(query), query, "dur", searchUuid)
	go BQUpload(query, results, "datauseregister", searchUuid)
	c.JSON(http.StatusOK, results)
}

// dataUseElasticConfig defines the body of the query to the elastic data uses index
func dataUseElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		searchableFields := []string{
			"projectTitle",
			"laySummary",
			"publicBenefitStatement",
			"technicalSummary",
			"fundersAndSponsors",
			"datasetTitles",
			"keywords",
			"collectionNames",
			"publisherName",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"operator":  "and",
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"fields": searchableFields,
				"type":   "phrase",
				"boost":  2,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["dataUseRegister"] {
		filters := []gin.H{}
		for _, t := range terms.([]interface{}) {
			filters = append(filters, gin.H{"term": gin.H{key: t}})
		}
		filter := gin.H{"bool": gin.H{"should": filters}}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	response := gin.H{
		"size":    searchNoRecords,
		"query":   mainQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"laySummary": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func PublicationSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "publication", publicationElasticConfig(query), query, "publication", searchUuid)
	go BQUpload(query, results, "publication", searchUuid)
	c.JSON(http.StatusOK, results)
}

// publicationElasticConfig defines the body of the query to the elastic publications index.
// The publications index consists of publications hosted on the Gateway — not a federated search.
func publicationElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		searchableFields := []string{
			"title",
			"journalName",
			"abstract",
			"publicationType",
			"authors",
			"datasetTitles",
			"doi",
			"keywords",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"operator":  "and",
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"fields": searchableFields,
				"type":   "phrase",
				"boost":  2,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["paper"] {
		var filter gin.H
		if key == "publicationDate" {
			filter = gin.H{
				"bool": gin.H{
					"must": []gin.H{
						{"range": gin.H{"publicationDate": gin.H{"gte": terms.([]interface{})[0]}}},
						{"range": gin.H{"publicationDate": gin.H{"lte": terms.([]interface{})[1]}}},
					},
				},
			}
		} else {
			filters := []gin.H{}
			for _, t := range terms.([]interface{}) {
				filters = append(filters, gin.H{"term": gin.H{key: t}})
			}
			filter = gin.H{"bool": gin.H{"should": filters}}
		}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	response := gin.H{
		"size":    searchNoRecords,
		"query":   mainQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"title": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"abstract": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func DataProviderSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "dataprovider", dataProviderElasticConfig(query), query, "dataProvider", searchUuid)
	go BQUpload(query, results, "dataprovider", searchUuid)
	c.JSON(http.StatusOK, results)
}

// dataProviderElasticConfig defines the body of the query to the elastic data providers index
func dataProviderElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	var sortQuery []gin.H

	if query.QueryString == "" {
		if len(query.IDs) == 0 {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query":        gin.H{"match_all": gin.H{}},
					"random_score": gin.H{},
				},
			}
		} else {
			mainQuery = gin.H{
				"function_score": gin.H{
					"query": gin.H{
						"function_score": gin.H{
							"query": gin.H{
								"bool": gin.H{
									"filter": []gin.H{
										{"terms": gin.H{"_id": query.IDs}},
									},
								},
							},
							"random_score": gin.H{},
						},
					},
				},
			}
			sortQuery = []gin.H{
				{
					"_script": gin.H{
						"type": "number",
						"script": gin.H{
							"lang":   "painless",
							"source": "params.order.indexOf(doc['_id'].value)",
							"params": gin.H{"order": query.IDs},
						},
						"order": "asc",
					},
				},
			}
		}
	} else {
		searchableFields := []string{
			"name",
			"datasetTitles",
			"geographicLocation",
			"publicationTitles",
			"collectionNames",
			"durTitles",
			"toolNames",
			"teamAliases",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"operator":  "and",
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"fields": searchableFields,
				"type":   "phrase",
				"boost":  2,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["dataProvider"] {
		filters := []gin.H{}
		for _, t := range terms.([]interface{}) {
			filters = append(filters, gin.H{"term": gin.H{key: t}})
		}
		filter := gin.H{"bool": gin.H{"should": filters}}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	response := gin.H{
		"size":        searchNoRecords,
		"query":       mainQuery,
		"explain":     explanationEnabled,
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
	if len(query.IDs) > 0 {
		response["sort"] = sortQuery
	}
	return response
}

func DataCustodianNetworkSearch(c *gin.Context) {
	var query Query
	if err := c.BindJSON(&query); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	searchUuid := uuid.New().String()
	results := executeSearch(c.Request.Context(), "datacustodiannetwork", dataCustodianNetworkElasticConfig(query), query, "datacustodiannetwork", searchUuid)
	go BQUpload(query, results, "datacustodiannetwork", searchUuid)
	c.JSON(http.StatusOK, results)
}

// dataCustodianNetworkElasticConfig defines the body of the query to the elastic datacustodiannetwork index
func dataCustodianNetworkElasticConfig(query Query) gin.H {
	var mainQuery gin.H
	if query.QueryString == "" {
		mainQuery = gin.H{
			"function_score": gin.H{
				"query":        gin.H{"match_all": gin.H{}},
				"random_score": gin.H{},
			},
		}
	} else {
		relatedObjectFields := []string{
			"publisherNames",
			"datasetTitles",
			"durTitles",
			"toolNames",
			"publicationTitles",
			"collectionNames",
		}
		searchableFields := []string{
			"name",
			"summary",
		}
		mm1 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    relatedObjectFields,
				"fuzziness": "AUTO:5,7",
			},
		}
		mm2 := gin.H{
			"multi_match": gin.H{
				"query":     query.QueryString,
				"fields":    searchableFields,
				"fuzziness": "AUTO:5,7",
				"boost":     2,
			},
		}
		mm3 := gin.H{
			"multi_match": gin.H{
				"query":  query.QueryString,
				"type":   "phrase",
				"fields": searchableFields,
				"boost":  3,
			},
		}
		mainQuery = gin.H{
			"bool": gin.H{"should": []gin.H{mm1, mm2, mm3}},
		}
	}

	mustFilters := []gin.H{}
	mustFiltersByKey := map[string]gin.H{}
	for key, terms := range query.Filters["datacustodiannetwork"] {
		filters := []gin.H{}
		for _, t := range terms.([]interface{}) {
			filters = append(filters, gin.H{"term": gin.H{key: t}})
		}
		filter := gin.H{"bool": gin.H{"should": filters}}
		mustFilters = append(mustFilters, filter)
		mustFiltersByKey[key] = filter
	}

	return gin.H{
		"size":    searchNoRecords,
		"query":   mainQuery,
		"explain": explanationEnabled,
		"highlight": gin.H{
			"fields": gin.H{
				"name": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
				"summary": gin.H{
					"boundary_scanner": "sentence",
					"fragment_size":    0,
					"no_match_size":    0,
				},
			},
		},
		"post_filter": gin.H{"bool": gin.H{"must": mustFilters}},
		"aggs":        buildAggregations(query, mustFiltersByKey),
	}
}

// buildAggregations constructs the "aggs" part of an elastic search query.
// mustFiltersByKey maps each filter's field key to its gin.H filter clause.
// For each aggregation, all filters except the one for that field are applied,
// enabling faceted counts that reflect the current selection state.
func buildAggregations(query Query, mustFiltersByKey map[string]gin.H) gin.H {
	agg1 := gin.H{}
	for _, agg := range query.Aggregations {
		k, ok := agg["keys"].(string)
		if !ok {
			log.Printf("Filter key in %v not recognised", agg)
			continue
		}
		aggInner := gin.H{}
		if k == "dateRange" {
			aggInner["startDate"] = gin.H{"min": gin.H{"field": "startDate"}}
			aggInner["endDate"] = gin.H{"max": gin.H{"field": "endDate"}}
		} else if k == "publicationDate" {
			aggInner["startDate"] = gin.H{"min": gin.H{"field": "publicationDate"}}
			aggInner["endDate"] = gin.H{"max": gin.H{"field": "publicationDate"}}
		} else if k == "populationSize" {
			aggInner[k] = gin.H{
				"range": gin.H{"field": k, "ranges": populationRangesCache},
			}
		} else {
			aggInner[k] = gin.H{"terms": gin.H{"field": k, "size": searchNoRecordsAggregation}}
		}

		// Include all active filters except the one for this aggregation key,
		// so that facet counts reflect the full unfiltered set for each facet.
		filters := []gin.H{}
		for filterKey, fil := range mustFiltersByKey {
			if filterKey != k {
				filters = append(filters, fil)
			}
		}
		agg1[k] = gin.H{
			"aggs":   aggInner,
			"filter": gin.H{"bool": gin.H{"must": filters}},
		}
	}
	return agg1
}

func buildPopulationRanges() []gin.H {
	ranges := []gin.H{{"from": -1.0, "to": 1.0, "key": "Unreported"}}
	for i := 0; i < 9; i++ {
		ranges = append(ranges, gin.H{
			"from": math.Pow(10, float64(i)),
			"to":   math.Pow(10, float64(i+1)),
		})
	}
	return ranges
}

func flattenAggs(elasticResp SearchResponse) map[string]any {
	newAggs := make(map[string]any)
	for k, agg := range elasticResp.Aggregations {
		aggMap, ok := agg.(map[string]any)
		if !ok {
			slog.Debug(fmt.Sprintf("Unexpected aggregation type for key %s", k))
			continue
		}
		if k == "dateRange" || k == "publicationDate" {
			newAggs["startDate"] = aggMap["startDate"]
			newAggs["endDate"] = aggMap["endDate"]
		} else {
			newAggs[k] = aggMap[k]
		}
	}
	return newAggs
}

// stripExplanation removes the explanation field from each hit to reduce response size,
// and forwards the explanation data to the extractor service if configured.
func stripExplanation(elasticResp SearchResponse, query Query, entityType string, searchUuid string) {
	_, expEnabled := os.LookupEnv("SEARCH_EXPLANATION_EXTRACTOR")
	if expEnabled && entityType == "dataset" && !reflect.ValueOf(query).IsZero() {
		respCopy := copyResponseHits(elasticResp)
		go extractExplanation(respCopy, query, searchUuid)
	}
	for i := range elasticResp.Hits.Hits {
		elasticResp.Hits.Hits[i].Explanation = make(map[string]interface{}, 0)
	}
}

func copyResponseHits(r SearchResponse) SearchResponse {
	var hits []Hit
	hits = append(hits, r.Hits.Hits...)
	return SearchResponse{
		Hits: HitsField{Hits: hits},
	}
}

func extractExplanation(elasticResp SearchResponse, query Query, searchUuid string) {
	bodyContent := gin.H{
		"data":              elasticResp,
		"query":             fmt.Sprintf("%v", query),
		"destination_table": os.Getenv("SEARCH_EXPLANATION_TABLE"),
		"search_uuid":       searchUuid,
	}

	body, err := json.Marshal(bodyContent)
	if err != nil {
		slog.Info(fmt.Sprintf("Failed to marshal search explanation payload: %s", err.Error()))
		return
	}

	urlPath := fmt.Sprintf("%s/process_data", os.Getenv("SEARCH_EXPLANATION_EXTRACTOR"))
	req, err := http.NewRequest("POST", urlPath, bytes.NewBuffer(body))
	if err != nil {
		slog.Info(fmt.Sprintf("Failed to build search explanation request: %s", err.Error()))
		return
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(os.Getenv("SEARCH_EXPLANATION_USER"), os.Getenv("SEARCH_EXPLANATION_PASSWORD"))

	response, err := Client.Do(req)
	if err != nil {
		slog.Info(fmt.Sprintf("Failed to send search explanation request: %s", err.Error()))
		return
	}
	defer response.Body.Close()

	respBody, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Info(fmt.Sprintf("Failed to read search explanation response: %s", err.Error()))
	}

	slog.Debug(fmt.Sprintf(
		"Search explanation extraction routine exited with response: %s, uuid: %s", respBody, searchUuid,
	))
}

// SearchSimilarDatasets returns the top datasets similar to the document with the provided id.
func SearchSimilarDatasets(c *gin.Context) {
	var querySimilar SimilarSearch
	if err := c.BindJSON(&querySimilar); err != nil {
		slog.Debug(fmt.Sprintf("Failed to interpret search query with %s", err.Error()))
		return
	}
	results := similarSearch(c.Request.Context(), querySimilar.ID, "dataset")
	c.JSON(http.StatusOK, results)
}

func similarSearch(ctx context.Context, id string, index string) SearchResponse {
	var buf bytes.Buffer
	elasticQuery := gin.H{
		"size": searchNoRecordsSimilar,
		"query": gin.H{
			"more_like_this": gin.H{
				"like": []gin.H{
					{"_index": index, "_id": id},
				},
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(elasticQuery); err != nil {
		slog.Debug(fmt.Sprintf("Failed to encode elastic query with %s", err.Error()))
	}

	response, err := ElasticClient.Search(
		ElasticClient.Search.WithContext(ctx),
		ElasticClient.Search.WithIndex(index),
		ElasticClient.Search.WithBody(&buf),
	)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to execute similar search with %s", err.Error()))
		return SearchResponse{}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf("Failed to read elastic response with %s", err.Error()))
	}

	var elasticResp SearchResponse
	if err := json.Unmarshal(body, &elasticResp); err != nil {
		slog.Debug(fmt.Sprintf("Failed to unmarshal elastic response with %s", err.Error()))
	}

	if elasticResp.Hits.Hits == nil {
		slog.Warn("Hits from elastic are null, query may be malformed")
		slog.Debug(fmt.Sprintf("Null result elastic query: %v", elasticQuery))
	}

	return elasticResp
}

func uploadSearchAnalytics(query Query, results SearchResponse, entityType string, searchUuid string) {
	ctx := context.Background()
	analyticsDataset := BigQueryClient.Dataset(os.Getenv("BQ_DATASET_NAME"))
	table := analyticsDataset.Table(os.Getenv("BQ_TABLE_NAME"))
	u := table.Inserter()

	var datasetIds []string
	for _, r := range results.Hits.Hits {
		datasetIds = append(datasetIds, r.Id)
	}
	pageResults, err := json.Marshal(gin.H{"entity_ids": datasetIds})
	if err != nil {
		slog.Info(fmt.Sprintf("Could not marshal page results: %s", err.Error()))
	}

	filterUsed, err := json.Marshal(query.Filters)
	if err != nil {
		slog.Info(fmt.Sprintf("Could not marshal filters: %s", err.Error()))
	}

	total, _ := results.Hits.Total["value"].(float64)
	searchResult := SearchAnalytics{
		UUID:             searchUuid,
		Timestamp:        time.Now().Format("2006-01-02 15:04:05"),
		EntityType:       entityType,
		SearchTerm:       query.QueryString,
		FilterUsed:       string(filterUsed),
		PageResults:      string(pageResults),
		EntitiesReturned: int(total),
	}

	if err := u.Put(ctx, searchResult); err != nil {
		slog.Info(fmt.Sprintf("Failed to upload search analytics to BigQuery: %s", err.Error()))
	}

	slog.Debug(fmt.Sprintf("Search analytics upload complete for UUID: %s", searchUuid))
}

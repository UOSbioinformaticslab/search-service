package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gin-gonic/gin"
)

// DefineDatasetMappings initialises the datasets index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefineDatasetMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"settings": gin.H{
			"index": gin.H{
				"analysis": gin.H{
					"analyzer": gin.H{
						//index analyzer
						"medterms_index_analyzer": gin.H{
							"tokenizer": "standard",
							"filter": []string{
								"lowercase",
								"english_stemmer",
							},
							"char_filter": []string{
								"punctuation_removal",
							},
						},
						//search analyzer
						"medterms_search_analyzer": gin.H{
							"tokenizer": "standard",
							"filter": []string{
								"lowercase",
								"medterms_synonyms",
								"english_stemmer",
							},
							"char_filter": []string{
								"punctuation_removal",
							},
						},
					},
					"filter": gin.H{
						"english_stemmer": gin.H{
							"type":     "stemmer",
							"language": "english",
						},
						"medterms_synonyms": gin.H{
							"type":         "synonym_graph",
							"synonyms_set": "hdr_synonyms_set",
							"updateable":   true,
						},
					},
					"char_filter": gin.H{
						"punctuation_removal": gin.H{
							"type":        "pattern_replace",
							"pattern":     "[^\\w\\s]",
							"replacement": "",
						},
					},
				},
			},
		},
		"mappings": gin.H{
			"properties": gin.H{
				"title": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"shortTitle": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"abstract": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"description": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"keywords": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"named_entities": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
				"publisherName":      gin.H{"type": "keyword"},
				"partnerContext":     gin.H{"type": "keyword"},
				"dataProvider":       gin.H{"type": "keyword"},
				"dataProviderColl":   gin.H{"type": "keyword"},
				"dataUseTitles":      gin.H{"type": "keyword"},
				"collectionName":     gin.H{"type": "keyword"},
				"geographicLocation": gin.H{"type": "keyword"},
				"accessService":      gin.H{"type": "keyword"},
				"sampleAvailability": gin.H{"type": "keyword"},
				"dataType":           gin.H{"type": "keyword"},
				"dataSubType":        gin.H{"type": "keyword"},
				"formatAndStandards": gin.H{"type": "keyword"},
				"datasetAliases": gin.H{
					"type":     "text",
					"analyzer": "medterms_index_analyzer",
					"fields": gin.H{
						"keyword": gin.H{"type": "keyword"},
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "dataset",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"datasets",
			fmt.Sprintf("dataset mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "datasets", "dataset mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefineToolSettings updates the settings of the tools index in elastic to use
// a custom similarity scoring algorithm.  The mappings of the tools index are
// updated so that the custom similarity algorithm is applied to the description
// field of the tools data.
func DefineToolSettings(c *gin.Context) {
	// Elastic requires the index to be closed before settings are updated
	closeIndexByName("tool")

	var buf bytes.Buffer
	elasticSettings := gin.H{
		"settings": gin.H{
			"index": gin.H{
				"similarity": gin.H{
					"custom_similarity": gin.H{
						"type": "BM25",
						"b":    0.1,
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticSettings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticSettings,
			err.Error()),
		)
	}

	request := esapi.IndicesPutSettingsRequest{
		Index: []string{"tool"},
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		pubSubAudit(
			"update settings",
			"tools",
			fmt.Sprintf("tool settings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	var mappings bytes.Buffer
	elasticMappings := gin.H{
		"properties": gin.H{
			"description": gin.H{
				"type":       "text",
				"similarity": "custom_similarity",
			},
		},
	}
	if err := json.NewEncoder(&mappings).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	mappingsRequest := esapi.IndicesPutMappingRequest{
		Index: []string{"tool"},
		Body:  &mappings,
	}
	mappingsResponse, err := mappingsRequest.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"tools",
			fmt.Sprintf("tool mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(mappingsResponse.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer mappingsResponse.Body.Close()
	mappingsBody, err := io.ReadAll(mappingsResponse.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var mappingResp map[string]interface{}
	json.Unmarshal(mappingsBody, &mappingResp)

	// Reopen the index
	openIndexByName("tool")

	pubSubAudit("update settings", "tools", "tool settings sucessfully updated")

	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

// DefineToolMappings initialises the tool index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefineToolMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"dataProvider":         gin.H{"type": "keyword"},
				"dataProviderColl":     gin.H{"type": "keyword"},
				"license":              gin.H{"type": "keyword"},
				"datasetTitles":        gin.H{"type": "keyword"},
				"programmingLanguages": gin.H{"type": "keyword"},
				"typeCategory":         gin.H{"type": "keyword"},
				"keywords":             gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "tool",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"tools",
			fmt.Sprintf("tool mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "tools", "tool mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefineCollectionSettings updates the settings of the collections index in elastic to use
// a custom similarity scoring algorithm.  The mappings of the collections index are
// updated so that the custom similarity algorithm is applied to the description
// field of the collection data.
func DefineCollectionSettings(c *gin.Context) {
	// Elastic requires the index to be closed before settings are updated
	closeIndexByName("collection")

	var buf bytes.Buffer
	elasticSettings := gin.H{
		"settings": gin.H{
			"index": gin.H{
				"similarity": gin.H{
					"custom_similarity": gin.H{
						"type": "BM25",
						"b":    0.1,
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticSettings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticSettings,
			err.Error()),
		)
	}

	request := esapi.IndicesPutSettingsRequest{
		Index: []string{"collection"},
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update settings",
			"collections",
			fmt.Sprintf("collection settings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	var mappings bytes.Buffer
	elasticMappings := gin.H{
		"properties": gin.H{
			"description": gin.H{
				"type":       "text",
				"similarity": "custom_similarity",
			},
		},
	}
	if err := json.NewEncoder(&mappings).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	mappingsRequest := esapi.IndicesPutMappingRequest{
		Index: []string{"collection"},
		Body:  &mappings,
	}
	mappingsResponse, err := mappingsRequest.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"collections",
			fmt.Sprintf("collection mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(mappingsResponse.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer mappingsResponse.Body.Close()
	mappingsBody, err := io.ReadAll(mappingsResponse.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var mappingResp map[string]interface{}
	json.Unmarshal(mappingsBody, &mappingResp)

	// Reopen the index
	openIndexByName("collection")

	pubSubAudit("update settings", "collections", "collection settings sucessfully updated")

	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

// DefineCollectionMappings initialises the collection index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefineCollectionMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"publisherName":    gin.H{"type": "keyword"},
				"dataProvider":     gin.H{"type": "keyword"},
				"dataProviderColl": gin.H{"type": "keyword"},
				"datasetTitles":    gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "collection",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"collections",
			fmt.Sprintf("collection mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "collections", "collection mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefineDataUseMappings initialises the datauseregister index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefineDataUseMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"publisherName":    gin.H{"type": "keyword"},
				"dataProvider":     gin.H{"type": "keyword"},
				"dataProviderColl": gin.H{"type": "keyword"},
				"sector":           gin.H{"type": "keyword"},
				"organisationName": gin.H{"type": "keyword"},
				"datasetTitles":    gin.H{"type": "keyword"},
				"collectionNames":  gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "datauseregister",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"data uses",
			fmt.Sprintf("data use mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "data uses", "data use mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefinePublicationMappings initialises the publication index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefinePublicationMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"publicationType":  gin.H{"type": "keyword"},
				"datasetTitles":    gin.H{"type": "keyword"},
				"datasetLinkTypes": gin.H{"type": "keyword"},
				"publicationDate":  gin.H{"type": "date"},
				"keywords":         gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "publication",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"publications",
			fmt.Sprintf("publication mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "publications", "publication mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefineDataProviderMappings initialises the dataprovider index and defines the custom
// mappings for specific fields which need to be used as filters.
// Mappings can only be defined BEFORE any data is indexed, updating mappings
// requires reindexing.
func DefineDataProviderMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"geographicLocation": gin.H{"type": "keyword"},
				"datasetTitles":      gin.H{"type": "keyword"},
				"dataType":           gin.H{"type": "keyword"},
				"dataProviderColl":   gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "dataprovider",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"data providers",
			fmt.Sprintf("data provider mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "data providers", "data provider mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// DefineDataCustodianNetworkSettings updates the settings of the dataCustodianNetworks index in elastic to use
// a custom similarity scoring algorithm. The mappings of the DataCustodianNetwork index are
// updated so that the custom similarity algorithm is applied to the description
// field of the DataCustodianNetwork data.
func DefineDataCustodianNetworkSettings(c *gin.Context) {
	// Elastic requires the index to be closed before settings are updated
	closeIndexByName("datacustodiannetwork")

	var buf bytes.Buffer
	elasticSettings := gin.H{
		"settings": gin.H{
			"index": gin.H{
				"similarity": gin.H{
					"custom_similarity": gin.H{
						"type": "BM25",
						"b":    0.1,
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticSettings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticSettings,
			err.Error()),
		)
	}

	request := esapi.IndicesPutSettingsRequest{
		Index: []string{"datacustodiannetwork"},
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update settings",
			"datacustodiannetwork",
			fmt.Sprintf("datacustodiannetwork settings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	var mappings bytes.Buffer
	elasticMappings := gin.H{
		"properties": gin.H{
			"description": gin.H{
				"type":       "text",
				"similarity": "custom_similarity",
			},
		},
	}
	if err := json.NewEncoder(&mappings).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	mappingsRequest := esapi.IndicesPutMappingRequest{
		Index: []string{"datacustodiannetwork"},
		Body:  &mappings,
	}
	mappingsResponse, err := mappingsRequest.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"datacustodiannetwork",
			fmt.Sprintf("datacustodiannetwork mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(mappingsResponse.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer mappingsResponse.Body.Close()
	mappingsBody, err := io.ReadAll(mappingsResponse.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var mappingResp map[string]interface{}
	json.Unmarshal(mappingsBody, &mappingResp)

	// Reopen the index
	openIndexByName("datacustodiannetwork")

	pubSubAudit("update settings", "datacustodiannetwork", "datacustodiannetwork settings sucessfully updated")

	c.JSON(http.StatusOK, gin.H{"acknowledged": true})
}

// DefineDataCustodianNetworkMappings initialises the DataCustodianNetwork index and defines the custom
// requires reindexing.
func DefineDataCustodianNetworkMappings(c *gin.Context) {
	var buf bytes.Buffer
	elasticMappings := gin.H{
		"mappings": gin.H{
			"properties": gin.H{
				"publisherNames":    gin.H{"type": "keyword"},
				"datasetTitles":     gin.H{"type": "keyword"},
				"durTitles":         gin.H{"type": "keyword"},
				"toolNames":         gin.H{"type": "keyword"},
				"publicationTitles": gin.H{"type": "keyword"},
				"collectionNames":   gin.H{"type": "keyword"},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(elasticMappings); err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to encode elastic query %s with %s",
			elasticMappings,
			err.Error()),
		)
	}

	request := esapi.IndicesCreateRequest{
		Index: "datacustodiannetwork",
		Body:  &buf,
	}
	response, err := request.Do(context.TODO(), ElasticClient)
	if err != nil {
		pubSubAudit(
			"update mappings",
			"datacustodiannetwork",
			fmt.Sprintf("datacustodiannetwork mappings failed to update with error: %s", err.Error()),
		)
		slog.Debug(fmt.Sprintf(
			"Failed to execute elastic query with %s",
			err.Error()),
		)
		c.JSON(response.StatusCode, gin.H{"message": err.Error()})
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Debug(fmt.Sprintf(
			"Failed to read elastic response with %s",
			err.Error()),
		)
	}
	var resp map[string]interface{}
	json.Unmarshal(body, &resp)

	pubSubAudit("update mappings", "datacustodiannetwork", "datacustodiannetwork mappings sucessfully updated")

	c.JSON(http.StatusOK, resp)
}

// closeIndexByName closes the elastic index matching the provided name.
func closeIndexByName(indexName string) {
	closeIndexRequest := esapi.IndicesCloseRequest{
		Index: []string{indexName},
	}
	closeResponse, err := closeIndexRequest.Do(context.TODO(), ElasticClient)
	if err != nil {
		fmt.Printf("Error closing %s index: %s", indexName, err)
	}
	defer closeResponse.Body.Close()
}

// openIndexByName opens the elastic index matching the provided name.
func openIndexByName(indexName string) {
	openIndexRequest := esapi.IndicesOpenRequest{
		Index: []string{indexName},
	}
	openResponse, err := openIndexRequest.Do(context.TODO(), ElasticClient)
	if err != nil {
		fmt.Printf("Error opening %s index: %s", indexName, err)
	}
	defer openResponse.Body.Close()
}

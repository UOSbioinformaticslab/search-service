package search

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"hdruk/search-service/utils/mocks"
)

func init() {
	ElasticClient = mocks.MockElasticClient()
	Client = &mocks.MockClient{}

	mocks.PostDoFunc = func(req *http.Request) (*http.Response, error) {
		r := io.NopCloser(bytes.NewReader([]byte(``)))
		return &http.Response{
			StatusCode: 200,
			Body:       r,
		}, nil
	}

	BQUpload = func(query Query, results SearchResponse, entityType string, searchUuid string) {}
}

func GetTestGinContext(w *httptest.ResponseRecorder) *gin.Context {
	log.SetOutput(io.Discard)
	gin.SetMode(gin.TestMode)

	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = &http.Request{
		Header: make(http.Header),
	}

	return ctx
}

func MockPostToSearch(c *gin.Context) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	bodyContent := gin.H{"query": "test query"}
	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		log.Fatal(err.Error())
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
}

func MockPostToSimilarSearch(c *gin.Context) {
	c.Request.Method = "POST"
	c.Request.Header.Set("Content-Type", "application/json")
	bodyContent := gin.H{"id": "1"}
	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		log.Fatal(err.Error())
	}
	c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
}

func TestSearchGeneric(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	SearchGeneric(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "dataset")
	assert.Contains(t, testResp, "tool")
	assert.Contains(t, testResp, "collection")
	assert.Contains(t, testResp, "dataUseRegister")
	assert.Contains(t, testResp, "publication")
	assert.Contains(t, testResp, "dataProvider")
	assert.Contains(t, testResp, "datacustodiannetwork")

	datasetResp := testResp["dataset"].(map[string]interface{})
	assert.EqualValues(t, 3, int(datasetResp["took"].(float64)))
}

func TestDatasetSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	DatasetSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestToolSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	ToolSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestCollectionSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	CollectionSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestDataCostodianNetworkSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	DataCustodianNetworkSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestDataUseSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	DataUseSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestPublicationSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	PublicationSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestDataProviderSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSearch(c)

	DataProviderSearch(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestSimilarDatasetSearch(t *testing.T) {
	w := httptest.NewRecorder()
	c := GetTestGinContext(w)
	MockPostToSimilarSearch(c)

	SearchSimilarDatasets(c)

	assert.EqualValues(t, http.StatusOK, w.Code)

	bodyBytes, err := io.ReadAll(w.Body)
	if err != nil {
		log.Fatal(err.Error())
	}

	var testResp map[string]interface{}
	json.Unmarshal(bodyBytes, &testResp)

	assert.Contains(t, testResp, "hits")
	assert.Contains(t, testResp, "took")
	assert.EqualValues(t, 3, int(testResp["took"].(float64)))
}

func TestDatasetElasticConfig(t *testing.T) {
	TestQuery := Query{
		QueryString: "search term test",
		Filters: map[string]map[string]interface{}{
			"dataset": {
				"publisherName": []interface{}{
					"publisher A",
					"publisher B",
				},
				"dataType": []interface{}{
					"data type A",
				},
				"dateRange": []interface{}{
					"2020",
					"2021",
				},
				"populationSize": map[string]interface{}{
					"includeUnreported": true,
					"from":              1000,
					"to":                10000,
				},
			},
		},
		Aggregations: []map[string]interface{}{
			{
				"type": "dataset",
				"keys": "publisherName",
			},
			{
				"type": "dataset",
				"keys": "dataType",
			},
			{
				"type": "dataset",
				"keys": "populationSize",
			},
		},
	}

	datasetConfig := datasetElasticConfig(TestQuery)

	// assert query clause exists and that it contains query term
	assert.Contains(t, datasetConfig, "query")
	queryJson, _ := json.Marshal(datasetConfig)
	queryStr := string(queryJson)
	assert.Contains(t, queryStr, "search term test")

	// assert filter clause exists and contains boolean query
	assert.Contains(t, datasetConfig, "post_filter")
	filterClause := datasetConfig["post_filter"].(gin.H)
	assert.Contains(t, filterClause, "bool")
	assert.Contains(t, filterClause["bool"], "must")

	// assert specific filter keys are included
	assert.Contains(t, queryStr, "\"publisherName\":\"publisher A\"")
	assert.Contains(t, queryStr, "\"publisherName\":\"publisher B\"")
	assert.Contains(t, queryStr, "\"dataType\":\"data type A\"")
	assert.Contains(t, queryStr, "\"lte\":\"2021\"")
	assert.Contains(t, queryStr, "\"gte\":\"2020\"")
	assert.Contains(t, queryStr, "\"lte\":10000")
	assert.Contains(t, queryStr, "\"gte\":1000")
	assert.Contains(t, queryStr, "\"populationSize\":-1")

	// assert aggregations clause exists and contains specific keys
	assert.Contains(t, datasetConfig, "aggs")
	aggsClause := datasetConfig["aggs"].(gin.H)
	assert.Contains(t, aggsClause, "publisherName")
	assert.Contains(t, aggsClause, "dataType")
	assert.Contains(t, aggsClause, "populationSize")

	// assert the aggregations clause has both aggs and filters inside
	// so facet numbers are updated according to filtering
	publisherAggsClause := aggsClause["publisherName"].(gin.H)
	assert.Contains(t, publisherAggsClause, "aggs")
	assert.Contains(t, publisherAggsClause, "filter")

	publisherInnerAgg, _ := json.Marshal(publisherAggsClause["aggs"])
	assert.Contains(t, string(publisherInnerAgg), "publisherName")

	publisherInnerFilter, _ := json.Marshal(publisherAggsClause["filter"])
	assert.Contains(t, string(publisherInnerFilter), "dataType")
	assert.Contains(t, string(publisherInnerFilter), "populationSize")
	assert.NotContains(t, string(publisherInnerFilter), "publisherName")
}

func TestDatasetElasticConfig_PartnerContextScopesToExactPartner(t *testing.T) {
	partner := "CRUK"
	TestQuery := Query{
		QueryString:    "search term test",
		PartnerContext: &partner,
	}

	datasetConfig := datasetElasticConfig(TestQuery)

	queryClause := datasetConfig["query"].(gin.H)
	assert.Contains(t, queryClause, "bool")
	boolClause := queryClause["bool"].(gin.H)
	assert.Contains(t, boolClause, "must")
	assert.Contains(t, boolClause, "filter")

	filterJson, _ := json.Marshal(boolClause["filter"])
	assert.Contains(t, string(filterJson), "\"term\":{\"partnerContext\":\"CRUK\"}")

	mustJson, _ := json.Marshal(boolClause["must"])
	assert.Contains(t, string(mustJson), "search term test")

	assert.Contains(t, datasetConfig, "post_filter")
}

func TestDatasetElasticConfig_HDRUKPartnerContextIncludesUnlabelledDatasets(t *testing.T) {
	partner := "HDRUK"
	TestQuery := Query{
		QueryString:    "search term test",
		PartnerContext: &partner,
	}

	datasetConfig := datasetElasticConfig(TestQuery)

	queryClause := datasetConfig["query"].(gin.H)
	boolClause := queryClause["bool"].(gin.H)
	filterJson, _ := json.Marshal(boolClause["filter"])
	filterStr := string(filterJson)

	// HDRUK scope must match documents labelled HDRUK...
	assert.Contains(t, filterStr, "\"term\":{\"partnerContext\":\"HDRUK\"}")
	// ...OR documents with no partnerContext label at all (legacy/unlabelled data)
	assert.Contains(t, filterStr, "\"must_not\"")
	assert.Contains(t, filterStr, "\"exists\":{\"field\":\"partnerContext\"}")
	assert.Contains(t, filterStr, "\"minimum_should_match\":1")

	// it must NOT be a plain exact-match term filter like the non-HDRUK case
	assert.NotEqual(t, "[{\"term\":{\"partnerContext\":\"HDRUK\"}}]", filterStr)
}

func TestDatasetElasticConfig_NoPartnerContextIsANoOp(t *testing.T) {
	withoutScope := datasetElasticConfig(Query{QueryString: "search term test"})
	withNilScope := datasetElasticConfig(Query{QueryString: "search term test", PartnerContext: nil})

	withoutScopeJson, _ := json.Marshal(withoutScope["query"])
	withNilScopeJson, _ := json.Marshal(withNilScope["query"])

	// no PartnerContext must produce the same query shape as
	// explicitly nil, and neither must reference partnerContext anywhere
	assert.Equal(t, string(withoutScopeJson), string(withNilScopeJson))
	assert.NotContains(t, string(withoutScopeJson), "partnerContext")

	queryClause := withoutScope["query"].(gin.H)
	boolClause := queryClause["bool"].(gin.H)
	assert.Contains(t, boolClause, "should")
	assert.NotContains(t, boolClause, "filter")
}

func TestDatasetElasticConfig_PartnerContextWithMatchAllQuery(t *testing.T) {
	partner := "CRUK"
	TestQuery := Query{
		QueryString:    "",
		PartnerContext: &partner,
	}

	datasetConfig := datasetElasticConfig(TestQuery)

	queryClause := datasetConfig["query"].(gin.H)
	assert.Contains(t, queryClause, "bool")
	boolClause := queryClause["bool"].(gin.H)
	assert.Contains(t, boolClause, "must")
	assert.Contains(t, boolClause, "filter")

	mustJson, _ := json.Marshal(boolClause["must"])
	assert.Contains(t, string(mustJson), "function_score")
	assert.Contains(t, string(mustJson), "match_all")

	filterJson, _ := json.Marshal(boolClause["filter"])
	assert.Contains(t, string(filterJson), "\"term\":{\"partnerContext\":\"CRUK\"}")
}

func TestCollectionElasticConfig(t *testing.T) {
	TestQuery := Query{
		QueryString: "search term test",
		Filters: map[string]map[string]interface{}{
			"collection": {
				"datasetTitles": []interface{}{
					"title A",
					"title B",
				},
			},
		},
		Aggregations: []map[string]interface{}{
			{
				"type": "collection",
				"keys": "datasetTitles",
			},
		},
	}

	collectionConfig := collectionsElasticConfig(TestQuery)

	// assert query clause exists and that it contains query term
	assert.Contains(t, collectionConfig, "query")
	queryJson, _ := json.Marshal(collectionConfig)
	queryStr := string(queryJson)
	assert.Contains(t, queryStr, "search term test")

	// assert filter clause exists and contains boolean query
	assert.Contains(t, collectionConfig, "post_filter")
	filterClause := collectionConfig["post_filter"].(gin.H)
	assert.Contains(t, filterClause, "bool")
	assert.Contains(t, filterClause["bool"], "must")

	// assert specific filter keys are included
	assert.Contains(t, queryStr, "\"datasetTitles\":\"title A\"")
	assert.Contains(t, queryStr, "\"datasetTitles\":\"title B\"")

	// assert aggregations clause exists and contains specific keys
	assert.Contains(t, collectionConfig, "aggs")
	aggsClause := collectionConfig["aggs"].(gin.H)
	assert.Contains(t, aggsClause, "datasetTitles")
}

func TestDataUseElasticConfig(t *testing.T) {
	TestQuery := Query{
		QueryString: "search term test",
		Filters: map[string]map[string]interface{}{
			"dataUseRegister": {
				"sector": []interface{}{
					"sector A",
					"sector B",
				},
				"organisationName": []interface{}{
					"organisation A",
				},
				"publisherName": []interface{}{
					"publisher A",
					"publisher A",
				},
				"datasetTitles": []interface{}{
					"Title A",
					"Title B",
				},
			},
		},
		Aggregations: []map[string]interface{}{
			{
				"type": "dataUseRegister",
				"keys": "sector",
			},
			{
				"type": "dataUseRegister",
				"keys": "organisationName",
			},
			{
				"type": "dataUseRegister",
				"keys": "publisherName",
			},
			{
				"type": "dataUseRegister",
				"keys": "datasetTitles",
			},
		},
	}

	durConfig := dataUseElasticConfig(TestQuery)

	// assert query clause exists and that it contains query term
	assert.Contains(t, durConfig, "query")
	queryJson, _ := json.Marshal(durConfig)
	queryStr := string(queryJson)
	assert.Contains(t, queryStr, "search term test")

	// assert filter clause exists and contains boolean query
	assert.Contains(t, durConfig, "post_filter")
	filterClause := durConfig["post_filter"].(gin.H)
	assert.Contains(t, filterClause, "bool")
	assert.Contains(t, filterClause["bool"], "must")

	// assert specific filter keys are included
	assert.Contains(t, queryStr, "\"sector\":\"sector A\"")

	// assert aggregations clause exists and contains specific keys
	assert.Contains(t, durConfig, "aggs")
	aggsClause := durConfig["aggs"].(gin.H)
	assert.Contains(t, aggsClause, "sector")
}

func TestPublicationElasticConfig(t *testing.T) {
	TestQuery := Query{
		QueryString: "search term test",
		Filters: map[string]map[string]interface{}{
			"paper": {
				"publicationType": []interface{}{
					"Type A",
					"Type B",
				},
				"publicationDate": []interface{}{
					"2020",
					"2021",
				},
				"datasetTitles": []interface{}{
					"Title A",
					"Title B",
				},
			},
		},
		Aggregations: []map[string]interface{}{
			{
				"type": "publication",
				"keys": "publicationType",
			},
			{
				"type": "publication",
				"keys": "publicationDate",
			},
			{
				"type": "publication",
				"keys": "datasetTitles",
			},
		},
	}

	pubConfig := publicationElasticConfig(TestQuery)

	// assert query clause exists and that it contains query term
	assert.Contains(t, pubConfig, "query")
	queryJson, _ := json.Marshal(pubConfig)
	queryStr := string(queryJson)
	assert.Contains(t, queryStr, "search term test")

	// assert filter clause exists and contains boolean query
	assert.Contains(t, pubConfig, "post_filter")
	filterClause := pubConfig["post_filter"].(gin.H)
	assert.Contains(t, filterClause, "bool")
	assert.Contains(t, filterClause["bool"], "must")

	// assert specific filter keys are included
	assert.Contains(t, queryStr, "\"publicationType\":\"Type A\"")
	assert.Contains(t, queryStr, "\"lte\":\"2021\"")
	assert.Contains(t, queryStr, "\"gte\":\"2020\"")

	// assert aggregations clause exists and contains specific keys
	assert.Contains(t, pubConfig, "aggs")
	aggsClause := pubConfig["aggs"].(gin.H)
	assert.Contains(t, aggsClause, "publicationType")
	assert.Contains(t, aggsClause, "publicationDate")

	pubDateClause, _ := json.Marshal(aggsClause["publicationDate"])
	assert.Contains(t, string(pubDateClause), "startDate")
	assert.Contains(t, string(pubDateClause), "endDate")
}

func TestDataProviderElasticConfig(t *testing.T) {
	TestQuery := Query{
		QueryString: "search term test",
		Filters: map[string]map[string]interface{}{
			"dataProvider": {
				"geographicLocation": []interface{}{
					"country A",
					"country B",
				},
				"datasetTitles": []interface{}{
					"Title A",
					"Title B",
				},
			},
		},
		Aggregations: []map[string]interface{}{
			{
				"type": "dataProvider",
				"keys": "geographicLocation",
			},
			{
				"type": "dataProvider",
				"keys": "datasetTitles",
			},
		},
	}

	durConfig := dataProviderElasticConfig(TestQuery)

	// assert query clause exists and that it contains query term
	assert.Contains(t, durConfig, "query")
	queryJson, _ := json.Marshal(durConfig)
	queryStr := string(queryJson)
	assert.Contains(t, queryStr, "search term test")

	// assert filter clause exists and contains boolean query
	assert.Contains(t, durConfig, "post_filter")
	filterClause := durConfig["post_filter"].(gin.H)
	assert.Contains(t, filterClause, "bool")
	assert.Contains(t, filterClause["bool"], "must")

	// assert specific filter keys are included
	assert.Contains(t, queryStr, "\"geographicLocation\":\"country A\"")

	// assert aggregations clause exists and contains specific keys
	assert.Contains(t, durConfig, "aggs")
	aggsClause := durConfig["aggs"].(gin.H)
	assert.Contains(t, aggsClause, "datasetTitles")
}

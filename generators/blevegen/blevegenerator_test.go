package blevegen

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BleveIndexer handles indexing of nested maps to a bleve index
// It can handle maps with up to two levels of nesting, e.g., map[string]map[string]string
type BleveIndexer struct {
	index bleve.Index
	path  string
}

// NewBleveIndexer creates a new indexer instance
// indexPath: path where the bleve index will be stored
func NewBleveIndexerMemOnly() (*BleveIndexer, error) {
	var index bleve.Index

	// Create a new index mapping
	// bleve will automatically handle nested maps and arrays
	// as it can index any struct or map that can be marshaled to JSON
	indexMapping := bleve.NewIndexMapping()

	documentMapping := bleve.NewDocumentMapping()
	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Name = "title"
	keywordFieldMapping.Store = false
	keywordFieldMapping.IncludeTermVectors = false
	keywordFieldMapping.IncludeInAll = false
	keywordFieldMapping.DocValues = false
	documentMapping.Fields = append(documentMapping.Fields, keywordFieldMapping)
	indexMapping.AddDocumentMapping("book", documentMapping)
	allDocumentMapping := bleve.NewDocumentMapping()
	allDocumentMapping.Enabled = false
	indexMapping.AddDocumentMapping("_all", allDocumentMapping)
	indexMapping.StoreDynamic = false
	// indexMapping.DocValuesDynamic = false
	// indexMapping.IndexDynamic = false

	// Create the index
	index, err := bleve.NewMemOnly(indexMapping)
	if err != nil {
		return nil, fmt.Errorf("error creating index: %v", err)
	}

	return &BleveIndexer{
		index: index,
	}, nil
}

// NewBleveIndexer creates a new indexer instance
// indexPath: path where the bleve index will be stored
func NewBleveIndexer(indexPath string) (*BleveIndexer, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		return nil, fmt.Errorf("error creating directory: %v", err)
	}

	var index bleve.Index

	// Check if index already exists
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		// Create a new index mapping
		// bleve will automatically handle nested maps and arrays
		// as it can index any struct or map that can be marshaled to JSON
		indexMapping := bleve.NewIndexMapping()

		// Create the index
		index, err = bleve.New(indexPath, indexMapping)
		if err != nil {
			return nil, fmt.Errorf("error creating index: %v", err)
		}
	} else {
		// Open existing index
		index, err = bleve.Open(indexPath)
		if err != nil {
			return nil, fmt.Errorf("error opening existing index: %v", err)
		}
	}

	return &BleveIndexer{
		index: index,
		path:  indexPath,
	}, nil
}

// Close closes the index
func (nmi *BleveIndexer) Close() error {
	return nmi.index.Close()
}

// IndexDocument indexes a single nested map
// id: unique identifier for the document
// data: the nested map to index (map[string]interface{})
func (nmi *BleveIndexer) IndexDocument(id string, data map[string]interface{}) error {
	// Validate input
	if id == "" {
		return fmt.Errorf("document ID cannot be empty")
	}
	if len(data) == 0 {
		return fmt.Errorf("data map cannot be empty")
	}

	// Index the data with the given ID
	// bleve automatically handles nested structures
	err := nmi.index.Index(id, data)
	if err != nil {
		return fmt.Errorf("error indexing data: %v", err)
	}
	return nil
}

// BatchIndexDocuments batch indexes multiple nested maps
// documents: a map of document IDs to data maps
func (nmi *BleveIndexer) BatchIndexDocuments(documents map[string]map[string]interface{}) error {
	// Create a new batch
	batch := nmi.index.NewBatch()

	// Add each document to the batch
	for id, data := range documents {
		if id == "" {
			return fmt.Errorf("document ID cannot be empty")
		}
		if len(data) == 0 {
			return fmt.Errorf("data map cannot be empty for document ID: %s", id)
		}

		err := batch.Index(id, data)
		if err != nil {
			return fmt.Errorf("error adding document to batch: %v", err)
		}
	}

	// Execute the batch
	err := nmi.index.Batch(batch)
	if err != nil {
		return fmt.Errorf("error executing batch: %v", err)
	}

	return nil
}

// SearchField searches a specific field with the given value
// field: the field name to search in
// value: the value to search for
func (nmi *BleveIndexer) SearchField(field, value string) (*bleve.SearchResult, error) {
	query := bleve.NewMatchQuery(value)
	query.SetField(field)
	searchRequest := bleve.NewSearchRequest(query)
	return nmi.index.Search(searchRequest)
}

// DeleteDocument removes a document from the index
// id: the document ID to delete
func (nmi *BleveIndexer) DeleteDocument(id string) error {
	if id == "" {
		return fmt.Errorf("document ID cannot be empty")
	}

	err := nmi.index.Delete(id)
	if err != nil {
		return fmt.Errorf("error deleting document: %v", err)
	}

	return nil
}

var batchMaps = map[string]map[string]interface{}{
	"doc1": {
		"_type": "book",
		"title": "Sample Document",
		"author": map[string]string{ // Nested map (level 2)
			"name":  "John Doe",
			"email": "john@example.com",
		},
		"tags": []string{"sample", "document", "nested"}, // Array
		"metadata": map[string]interface{}{ // Nested map with mixed types (level 2)
			"created": "2023-01-01",
			"updated": "2023-01-02",
			"views":   42,   // Numeric value
			"active":  true, // Boolean value
		},
	},
	"doc2": {
		"_type": "book",
		"title": "Another Document",
		"author": map[string]string{
			"name":  "Jane Smith",
			"email": "jane@example.com",
		},
		"tags": []string{"another", "document"},
	},
	"doc3": {
		"_type":   "book",
		"title":   "Third Document",
		"content": "This is the content of the third document",
		"metadata": map[string]interface{}{
			"created": "2023-01-03",
			"views":   10,
		},
	},
}

var bookSchema = schema{
	cols: map[string]value.ValueType{
		"title":    value.StringType,
		"author":   value.MapStringType,
		"metadata": value.MapValueType,
		"content":  value.StringType,
		"tags":     value.StringsType,
	},
}

func TestBleve(t *testing.T) {
	// Create a new indexer
	indexer, err := NewBleveIndexerMemOnly()
	require.NoError(t, err, "Failed to create indexer")
	t.Cleanup(func() { indexer.Close() })

	err = indexer.BatchIndexDocuments(batchMaps)
	require.NoError(t, err, "Failed to batch index documents")

	t.Log("Successfully batch indexed documents!")

	searchResults, err := indexer.SearchField("title", "document")
	require.NoError(t, err, "Failed to search index by field")

	t.Logf("Search results for 'document' in title field: %v hits\n", searchResults.Total)
	for _, hit := range searchResults.Hits {
		t.Logf("  Document ID: %s, Score: %f\n", hit.ID, hit.Score)
	}

	searchResults, err = indexer.SearchField("author.name", "John")
	require.NoError(t, err, "Failed to search nested filter")

	t.Logf("Search results for 'John' in author.name: %v hits\n", searchResults.Total)
	for _, hit := range searchResults.Hits {
		t.Logf("  Document ID: %s, Score: %f\n", hit.ID, hit.Score)
	}

	filterQlStr := `FILTER AND(EXISTS tags, title = "Sample Document", author.name LIKE "John %", metadata.views > 41, tags IN ("sample", "document")) `
	filter, err := rel.ParseFilterQL(filterQlStr)
	require.NoError(t, err, "Failed to parse filter")

	g := NewGenerator(time.Now(), nil, bookSchema)
	payload, err := g.Walk(filter)
	require.NoError(t, err, "Failed to walk filter")

	q := payload.Filter.(query.Query)
	searchRequest := bleve.NewSearchRequest(q)
	res, err := indexer.index.Search(searchRequest)
	require.NoError(t, err, "Failed to search bleve")

	assert.Equal(t, 1, int(res.Total), "Search results for `%s`: %v hits\n", filterQlStr, res.Total)

	// Delete a document
	err = indexer.DeleteDocument("doc2")
	require.NoError(t, err, "Failed to delete document")
}

type schema struct {
	cols map[string]value.ValueType
}

func (s schema) Column(f string) (value.ValueType, bool) {
	c, ok := s.cols[f]
	return c, ok
}

func (s schema) ColumnInfo(f string) (*gentypes.FieldType, bool) {
	c, ok := s.cols[f]
	if !ok {
		return nil, ok
	}
	return &gentypes.FieldType{
		Field:    f,
		Type:     c,
		TypeName: c.String(),
	}, true
}

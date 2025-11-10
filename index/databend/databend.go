package databend

import (
	"context"
	"errors"
	"fmt"
	"log"
	//"strconv"
	"strings"
	"sync"

	"database/sql"
	"github.com/RediSearch/RediSearchBenchmark/index"
	"github.com/RediSearch/RediSearchBenchmark/query"
	_ "github.com/datafuselabs/databend-go"
)

// IndexingOptions are flags passed to the the abstract Index call, which receives them as interface{}, allowing
// for implementation specific options
type IndexingOptions struct {
	// the language of the document, for stemmer analysis
	Language string
	// whether we should use stemming on the document. NOT SUPPORTED BY THE ENGINE YET!
	Stemming bool

	// If set, we will not save the documents contents, just index them, for fetching ids only
	NoSave bool

	NoFieldFlags bool

	NoScoreIndexes bool

	NoOffsetVectors bool

	Prefix string
}

var total int64 = 0
var mu sync.Mutex = sync.Mutex{}

// Index is an interface to databend's SQL connection
type Index struct {
	sync.Mutex
	hosts          []string
	password       string
	temporary      int
	md             *index.Metadata
	name           string
	commandPrefix  string
	db             *sql.DB
	cluster        bool
	withSuffixTrie bool
}

// NewIndex creates a new index connecting to the redis host, and using the given name as key prefix
func NewIndex(addrs []string, pass string, temporary int, name string, md *index.Metadata, withSuffixTrie bool) *Index {
	ret := &Index{
		hosts:          addrs,
		md:             md,
		password:       pass,
		temporary:      temporary,
		name:           name,
		cluster:        false,
		withSuffixTrie: withSuffixTrie,
	}

	if md != nil && md.Options != nil {
		if opts, ok := md.Options.(IndexingOptions); ok {
			if opts.Prefix != "" {
				ret.commandPrefix = md.Options.(IndexingOptions).Prefix
			}
		}
	}

	fmt.Println("0000000000-new")
	dsn := "databend://root:@0.0.0.0:48000/default?sslmode=disable"
	db, err := sql.Open("databend", dsn)
	if err != nil {
		fmt.Println("Error connecting to Databend:", err)
		//return err
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("Error pinging Databend:", err)
		//return err
	}

	ret.db = db

	return ret
}

func (i *Index) DocumentCount() (count int64) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", i.name)
	fmt.Println("query=", query)
	row := i.db.QueryRow(query)
	fmt.Println("Count row:", row)
	err := row.Scan(&count)
	if err != nil {
		log.Printf("Error getting document count: %v", err)
		return 0
	}
	return count
}
func docCountShard(ctx context.Context, db *sql.DB, tableName string) (count int64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	row := db.QueryRowContext(ctx, query)
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (i *Index) GetName() string {
	return i.name
}

// Create configues the index and creates it on redis
func (i *Index) Create() error {
	err2 := i.db.Ping()
	if err2 != nil {
		fmt.Println("---222--Error pinging Databend:", err2)
		dsn := "databend://root:@0.0.0.0:48000/default?sslmode=disable"
		db, err := sql.Open("databend", dsn)
		if err != nil {
			fmt.Println("Error connecting to Databend:", err)
			return err
		}
		i.db = db
	}

	// Create table with columns based on metadata fields
	createTableSQL := fmt.Sprintf("CREATE OR REPLACE TABLE %s (id VARCHAR, score FLOAT64, ", i.name)
	// Add columns based on metadata fields
	for _, f := range i.md.Fields {
		switch f.Type {
		case index.TextField:
			createTableSQL += fmt.Sprintf("%s VARCHAR, ", f.Name)
		case index.NumericField:
			createTableSQL += fmt.Sprintf("%s INT, ", f.Name)
		case index.NoIndexField:
			continue
		default:
			return fmt.Errorf("Unsupported field type %v", f.Type)
		}
	}

	// Add full-text index for text fields
	indexField := ""
	for _, f := range i.md.Fields {
		if f.Type == index.TextField {
			// Databend supports full-text search with INVERTED INDEX
			if len(indexField) > 0 {
				indexField += ", "
			}
			indexField += f.Name
		}
	}
	createTableSQL += fmt.Sprintf(" INVERTED INDEX idx(%s))", indexField)
	fmt.Println("Creating table SQL:", createTableSQL)

	// createTableSQL2 := "CREATE TABLE zzzzzz(i int, v float)"
	// Execute the create table statement
	_, err := i.db.Exec(createTableSQL)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	fmt.Printf("Created table %s successfully\n", i.name)
	return nil
}

// Index indexes multiple documents on the index, with optional IndexingOptions passed to options
func (i *Index) Index(docs []index.Document, options interface{}) error {
	if len(docs) == 0 {
		return nil
	}
	// Get all field names from the first document
	fields := []string{"id", "score"}
	for k := range docs[0].Properties {
		fields = append(fields, k)
	}
	// Create the INSERT statement
	placeholders := make([]string, len(docs))
	values := make([]interface{}, 0, len(docs)*len(fields))
	for i, doc := range docs {
		docPlaceholders := make([]string, len(fields))
		for j := 0; j < len(fields); j++ {
			docPlaceholders[j] = "?"
		}
		placeholders[i] = "(" + strings.Join(docPlaceholders, ", ") + ")"
		// Add values in the same order as fields
		values = append(values, doc.Id)
		values = append(values, doc.Score)
		for j := 2; j < len(fields); j++ { // Skip id and score which are already added
			fieldName := fields[j]
			if val, ok := doc.Properties[fieldName]; ok {
				values = append(values, val)
			} else {
				values = append(values, nil) // Use NULL for missing fields
			}
		}
	}
	// Build the SQL statement
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		i.name,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "))
	// Execute the query
	_, err := i.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("Error inserting documents: %v", err)

	}
	return nil
}

func (i *Index) SuffixQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

func (i *Index) ContainsQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

func (i *Index) PrefixQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

func (i *Index) WildCardQuery(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	return i.FullTextQuerySingleField(q, verbose)
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Index) FullTextQuerySingleField(q query.Query, verbose int) (docs []index.Document, total int, err error) {
	term := q.Term
	field := q.Field
	if field == "" {
		// If no specific field is provided, search in all text fields
		return nil, 0, errors.New("field must be specified for Databend search")
	}
	// Build the SQL query based on the query type
	var whereClause string
	if q.Flags&query.QueryTypePrefix != 0 {
		// Prefix search
		whereClause = fmt.Sprintf("%s QUERY('%s:%s*')", field, term)
	} else if q.Flags&query.QueryTypeSuffix != 0 {
		// Suffix search
		whereClause = fmt.Sprintf("%s QUERY('%s:*%s')", field, term)
	} else if q.Flags&query.QueryTypeWildcard != 0 {
		// Wildcard search
		whereClause = fmt.Sprintf("%s QUERY('%s:*%s*')'", field, term)
	} else {
		// Full-text search using MATCH
		whereClause = fmt.Sprintf("QUERY('%s:%s')", field, term)
	}
	// Count total matches
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", i.name, whereClause)
	row := i.db.QueryRow(countQuery)
	var count int
	err = row.Scan(&count)
	if err != nil {
		return nil, 0, fmt.Errorf("error counting search results: %v", err)
	}
	total = count
	// If verbose logging is enabled
	if verbose > 1 {
		log.Printf("Query: %s. %d hits", countQuery, total)
	}
	// If we need to return the actual documents
	if q.Paging.Num > 0 {
		// Get all fields to return
		fields := "id"
		for _, f := range i.md.Fields {
			fields += ", " + f.Name
		}

		// Build the query with pagination
		searchQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY score() DESC LIMIT %d OFFSET %d",
			fields, i.name, whereClause, q.Paging.Num, q.Paging.Offset)

		// Execute the query
		rows, err := i.db.Query(searchQuery)
		if err != nil {
			return nil, total, fmt.Errorf("error executing search query: %v", err)
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, total, fmt.Errorf("error getting column names: %v", err)
		}

		// Prepare result documents
		docs = []index.Document{}

		// Scan rows into documents
		for rows.Next() {
			// Create a slice of interface{} to hold the values
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// Scan the row into the slice of interface{}
			err = rows.Scan(valuePtrs...)
			if err != nil {
				return nil, total, fmt.Errorf("error scanning row: %v", err)
			}

			// Create a new document
			var id string
			var score float32

			// Convert id to string
			switch v := values[0].(type) {
			case string:
				id = v
			case []byte:
				id = string(v)
			default:
				id = fmt.Sprintf("%v", v)
			}

			// Convert the score to float32
			switch v := values[1].(type) {
			case float64:
				score = float32(v)
			case float32:
				score = v
			case int64:
				score = float32(v)
			default:
				score = 1.0 // Default score if conversion fails
			}

			doc := index.NewDocument(id, score)

			// Add properties
			for i := 2; i < len(columns); i++ {
				columnName := columns[i]
				if values[i] != nil {
					doc.Set(columnName, values[i])
				}
			}

			docs = append(docs, doc)
		}

		if err = rows.Err(); err != nil {
			return nil, total, fmt.Errorf("error iterating rows: %v", err)
		}
	}

	return docs, total, nil
}

func (i *Index) flush(ctx context.Context) error {
	//return client.FlushAll(ctx).Err()
	_, err := i.db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", i.name))
	return err

}

func (i *Index) Drop() (err error) {
	if i.db != nil {
		// Drop the table
		_, err = i.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", i.name))
		if err != nil {
			return fmt.Errorf("error dropping table: %v", err)
		}
		// Close the connection
		err = i.db.Close()
		if err != nil {
			return fmt.Errorf("error closing connection: %v", err)
		}
	}
	return nil
}

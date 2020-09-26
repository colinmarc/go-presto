package presto

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	initialRetry            = 50 * time.Millisecond
	maxRetry                = 800 * time.Millisecond
	ProgressUnknown float64 = -1.0
)

// Query represents an open query to presto.
type Query struct {
	host    string
	user    string
	source  string
	catalog string
	schema  string

	query   string
	closed  bool
	id      string
	columns []string

	bufferedRows [][]interface{}
	state        string
	progress     float64
	nextUri      string
}

type queryResult struct {
	ID               string          `json:"id"`
	InfoUri          string          `json:"infoUri"`
	NextUri          string          `json:"nextUri"`
	PartialCancelUri string          `json:"PartialCancelUri"`
	Data             [][]interface{} `json:"data"`
	Columns          []struct {
		Name string `json:"name"`
	} `json:"columns"`
	Error struct {
		ErrorCode   int `json:"errorCode"`
		FailureInfo struct {
			Message string `json:"message"`
		} `json:"failureInfo"`
	} `json:"error"`
	Stats struct {
		State           string `json:"state"`
		Scheduled       bool   `json:"scheduled"`
		CompletedSplits int    `json:"completedSplits"`
		TotalSplits     int    `json:"totalSplits"`
	} `json:"stats"`
}

func NewQuery(host, user, source, catalog, schema, query string) (*Query, error) {
	if user == "" {
		user = "anonymous"
	}

	if source == "" {
		source = userAgent
	}

	if catalog == "" {
		catalog = "default"
	}

	if schema == "" {
		schema = "default"
	}

	q := &Query{
		host:    host,
		user:    user,
		source:  source,
		catalog: catalog,
		schema:  schema,
		query:   query,
	}

	err := q.postQuery()
	if err != nil {
		return nil, err
	}

	// Immediately fetch one result set, to fill the column names.
	err = q.fetchNext()
	if err != nil {
		return nil, err
	}

	return q, nil
}

// Columns returns a list of the column names for the query.
func (q *Query) Columns() []string {
	return q.columns
}

// Progress returns the current progress of the query, which is the proportion
// of splits completed. Progress is only updated on calls to Next.
//
// If the total number of splits is not determined, Progress returns
// ProgressUnknown.
func (q *Query) Progress() float64 {
	return q.progress
}

// Id returns a execution id of the query.
func (q *Query) Id() string {
	return q.id
}

// Close closes the query, and cancels it if started.
func (q *Query) Close() error {
	if q.closed {
		return nil
	}

	q.closed = true
	req, _ := http.NewRequest("DELETE", q.nextUri, nil)
	resp, err := q.makeRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 204 {
		return fmt.Errorf("unexpected http status: %s", resp.Status)
	}

	return nil
}

// Next retrieves the next row from the dataset, fetching more if need be.
func (q *Query) Next() ([]interface{}, error) {
	retry := initialRetry
	for !q.closed && len(q.bufferedRows) == 0 {
		err := q.fetchNext()
		if err != nil {
			return nil, err
		}

		if len(q.bufferedRows) == 0 {
			time.Sleep(retry)
			retry *= 2
			if retry > maxRetry {
				retry = maxRetry
			}
		}
	}

	if len(q.bufferedRows) > 0 {
		row := q.bufferedRows[0]
		q.bufferedRows = q.bufferedRows[1:]
		return row, nil
	} else {
		return nil, nil
	}
}

func (q *Query) postQuery() error {
	queryUrl := fmt.Sprintf("%s/v1/statement", q.host)
	req, _ := http.NewRequest("POST", queryUrl, strings.NewReader(q.query))
	result, err := q.fetchResult(req)
	if err != nil {
		q.closed = true
		return err
	}

	q.id = result.ID
	q.nextUri = result.NextUri
	return nil
}

func (q *Query) fetchNext() error {
	req, _ := http.NewRequest("GET", q.nextUri, nil)
	result, err := q.fetchResult(req)
	if err != nil {
		q.closed = true
		return err
	}

	q.bufferedRows = result.Data
	q.state = result.Stats.State

	if q.columns == nil && len(result.Columns) > 0 {
		q.columns = make([]string, len(result.Columns))
		for i, col := range result.Columns {
			q.columns[i] = col.Name
		}
	}

	if result.Stats.Scheduled {
		q.progress = float64(result.Stats.CompletedSplits) / float64(result.Stats.TotalSplits)
	} else {
		q.progress = ProgressUnknown
	}

	q.nextUri = result.NextUri
	if result.NextUri == "" {
		q.closed = true
	}

	return nil
}

func (q *Query) fetchResult(req *http.Request) (*queryResult, error) {
	resp, err := q.makeRequest(req)
	if err != nil {
		return nil, err
	}

	result, err := q.readResult(resp)
	if err != nil {
		return result, err
	}

	if result.Error.FailureInfo.Message != "" {
		return result, fmt.Errorf("query failed: %s", result.Error.FailureInfo.Message)
	}

	return result, nil
}

func (q *Query) makeRequest(req *http.Request) (*http.Response, error) {
	req.Header.Add(userAgentHeader, userAgent)
	req.Header.Add(userHeader, q.user)
	req.Header.Add(catalogHeader, q.catalog)
	req.Header.Add(schemaHeader, q.schema)
	req.Header.Add(sourceHeader, q.source)

	// Sometimes presto returns a 503 to indicate that results aren't yet
	// available, and we should retry after waiting a bit.
	retry := initialRetry
	for {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == 200 {
			return resp, nil
		} else if resp.StatusCode != 503 {
			return nil, fmt.Errorf("unexpected http status: %s", resp.Status)
		}

		time.Sleep(retry)
		retry *= 2
		if retry > maxRetry {
			retry = maxRetry
		}
	}
}

func (q *Query) readResult(resp *http.Response) (*queryResult, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	result := queryResult{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("error decoding json response from presto: %s", err)
	}

	return &result, nil
}

/*
Package presto provides a standard database/sql driver for Facebook's Presto
query engine.
*/
package presto

const (
	version = "0.1.1"

	userHeader      = "X-Presto-User"
	sourceHeader    = "X-Presto-Source"
	catalogHeader   = "X-Presto-Catalog"
	schemaHeader    = "X-Presto-Schema"
	userAgentHeader = "User-Agent"
	userAgent       = "go-presto/" + version
)

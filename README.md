Presto for Go
=============

This is a tiny golang client for Facebook's [Presto][1].

```go
import (
  "fmt"
  "github.com/colinmarc/presto"
)

// Host, user, source, catalog, schema, query
query := "SELECT * FROM sys.node"
query, _ := presto.NewQuery("http//presto-coordinator:8080", "", "", "", query)

for row, _ := query.Next(); row != nil {
  fmt.Println(row...)
}
```

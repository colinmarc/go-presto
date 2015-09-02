Presto for Go
=============

This is a tiny golang client for Facebook's [Presto][1].

```go
import (
  "fmt"
  "github.com/colinmarc/go-presto"
)

// Host, user, source, catalog, schema, query
sql := "SELECT * FROM sys.node"
query, _ := presto.NewQuery("http://presto-coordinator:8080", "", "", "", "", sql)

if row, _ := query.Next(); row != nil {
  fmt.Println(row...)
}
```

[1]: https://github.com/facebook/presto

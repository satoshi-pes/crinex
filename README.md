# crinex
Golang package for compact RINEX (Hatanaka Format)

# Reader
crinex.NewReader returns a reader, and you can get extracted RINEX strings.  
Currently supports only CRINEX ver 3.0.

```
package main

import (
    "bufio"
    "fmt"
    "os"

    "github.com/satoshi-pes/crinex"
)

func main() {
    crx := "example.crx"

    f, err := os.Open(crx)
    if err != nil {
        panic(err.Error())
    }
    defer f.Close()

    // create crinex reader and scanner
    r, err := crinex.NewReader(f)
    if err != nil {
        panic(err)
    }
    s := bufio.NewScanner(r)

    // output to stdout
    w := bufio.NewWriter(os.Stdout)
    defer w.Flush()

    for s.Scan() {
        t := s.Text()
        fmt.Fprintln(w, t)
    }
}
```
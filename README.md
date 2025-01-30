# crinex
Golang package for decoding compact RINEX (Hatanaka RINEX Format)

# USAGE
## Scanner
crinex.NewScanner returns a scanner that provides sequential data decoding epoch by epoch.

Example:
```Go
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

    // create crinex scanner
    s, err := crinex.NewScanner(f)
    if err != nil {
        panic(err)
    }

    // output to stdout
    w := bufio.NewWriter(os.Stdout)
    defer w.Flush()

    // scan header contents
    s.ParseHeader()

    // output header as RINEX format
    fmt.Fprintf(w, "%s", string(s.Header()))

    // scan data epoch by epoch
    for s.ScanEpoch() {

        // output data as RINEX format
        fmt.Fprintf(w, "%s", s.EpochAsBytes()) // epoch record
        fmt.Fprintf(w, "%s", s.DataAsBytes())  // observables
    }
}
```

Decoded data can be retrieved by the following functions.
- `Header() -> []bytes`  // stores original header bytes
- `Epoch() -> time.Time`
- `SatList() -> []string`
- `ClockOffset() -> float64`
- `Data() -> []SatObss`  // stores all data for the epoch

```Go
// get time tag as time.Time
epoch := s.Epoch()
// -> time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)

// get satellite list for current epoch
satlist := s.SatList()
// -> []string{"G03", "G23", "G01", "G27", "G08", "G32", "G14", "G10", "G21", "G22"}

// get clock offset
clkoff := s.ClockOffset()
// -> 0.000000000

// get data as []SatObss
data := s.Data()

// print the observable of first obscode for the first satellite
i := 0  // index of the satellites (="G03")
j := 0  // index of the observation codes
fmt.Printf("Sat: %s\n", data[i].SatId)                  // Satellite 
fmt.Printf("Val: %f\n", data[i].ObsData[j].Data)        // observable
fmt.Printf("LLI: %s\n", string(data[i].ObsData[j].LLI)) // LLI (as byte)
fmt.Printf("SS : %s\n", string(data[i].ObsData[j].SS))  // SS (as byte)
// -> Sat: G03
// -> Val: 25065306.219000
// -> LLI: 
// -> SS : 5
```


## Reader
crinex.NewReader returns a reader, and you can get extracted RINEX strings line by line.  

```Go
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

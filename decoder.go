package crinex

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// strRecord stores the previous epoch record string.
//
// call strRecord.Update(newString) to update to the current epoch.
type strRecord struct {
	buf []byte
}

func (e *strRecord) Bytes() []byte {
	return e.buf
}

func (e *strRecord) String() string {
	return string(e.buf[:])
}

func (e *strRecord) StringRINEX() string {
	s := string(e.buf[:])
	if len(s) > 41 {
		s = s[:41]
		s = strings.TrimRight(s, " ")
	}
	return s
}

func (e *strRecord) StringRINEXV2(clk float64) string {
	var b []byte

	numSat, err := strconv.Atoi(string(bytes.TrimSpace(e.buf[29:32])))
	_ = err

	// first line
	if numSat > 12 {
		if math.IsNaN(clk) {
			// clock is missing
			b = append(b, fmt.Sprintf(" %67s\n", string(e.buf[1:68]))...)
		} else {
			b = append(b, fmt.Sprintf(" %67s%12.9f\n", string(e.buf[1:68]), clk)...)
		}
	} else {
		if math.IsNaN(clk) {
			// clock is missing
			b = append(b, fmt.Sprintf(" %s\n", e.buf[1:32+3*numSat])...)
		} else {
			b = append(b, fmt.Sprintf(" %-67s%12.9f\n", e.buf[1:32+3*numSat], clk)...)
		}
		return string(b)

	}

	// continuation lines
	for i := 1; numSat > 12*i; i++ {
		if numSat >= 12*(i+1) {
			b = append(b, fmt.Sprintf("%32s%-36.36s\n", "", e.buf[32+36*i:32+36*(i+1)])...)
		} else {
			b = append(b, fmt.Sprintf("%32s%-s\n", "", e.buf[32+36*i:32+36*i+3*(numSat%12)])...)
		}
	}

	return string(b)
}

func (e *strRecord) Decode(s string) error {
	if len(s) == 0 {
		// no update
		return nil
	}
	b := []byte(s)

	// update epoch record with a diff string
	if len(b) > len(e.buf) {
		e.buf = append(e.buf, make([]byte, len(b)-len(e.buf))...)
	}

	for i, c := range b {
		switch c {
		case ' ':
			continue
		case '&':
			e.buf[i] = ' '
		default:
			e.buf[i] = c
		}
	}

	return nil
}

type satDataRecord struct {
	obsCodes []string

	// differenced data
	data []diffRecord
	lli  []strRecord
	ss   []strRecord
}

// NewSatDataRecord returns a new satDataRecord initialized with obsCodes.
func NewSatDataRecord(obsCodes []string) satDataRecord {
	return satDataRecord{
		obsCodes: obsCodes,
		data:     make([]diffRecord, len(obsCodes)),
		lli:      make([]strRecord, len(obsCodes)),
		ss:       make([]strRecord, len(obsCodes)),
	}
}

// NewSatDataRecord returns a new satDataRecord initialized with obsCodes.
func NewSatDataRecordV1(obsCodes []string) satDataRecord {
	r := satDataRecord{
		obsCodes: obsCodes,
		data:     make([]diffRecord, len(obsCodes)),
		lli:      make([]strRecord, len(obsCodes)),
		ss:       make([]strRecord, len(obsCodes)),
	}

	// for crinex version 1
	// Initialize LLI and SS because no initialization identifier is defined
	// in the crinex version 1.
	for i := 0; i < len(obsCodes); i++ {
		r.lli[i].buf = []byte{' '}
		r.ss[i].buf = []byte{' '}
	}

	return r
}

// diffRecord stores differenced data.
// refData is kept to be the latest extracted value, and diffData stores
// differenced values for MaxDiff-orders.
type diffRecord struct {
	MaxDiff  int
	refData  int
	diffData []int
	missing  bool
}

func (r *diffRecord) Decode(b []byte) error {
	var v []byte
	if len(b) > 2 && b[1] == '&' {
		// case 1: initialize data
		diffOrder, e1 := strconv.Atoi(string(b[0]))
		ref, e2 := strconv.Atoi(string(b[2:]))

		if e1 != nil {
			return e1
		}
		if e2 != nil {
			return e2
		}

		// initialize
		r.refData = ref
		r.MaxDiff = diffOrder
		r.diffData = []int{}
		r.missing = false
	} else if len(b) > 0 {
		// case 2: update data
		v = b
		intNumber, err := strconv.Atoi(string(v))
		if err != nil {
			r.missing = true
			return err
		}

		// Note on the update algorithm
		//
		//         --> epoch
		//      0:  v1  v2   v3   v4    v5
		// diff 1:      d2   d3   d4    d5
		// diff 2:          dd3  dd4   dd5
		// diff 3:              ddd4  ddd5
		// diff 4:                   dddd5
		//
		// v1.. v5 are the original values at epochs 1.. 5., and d, dd, ddd, dddd
		// denote the 1st, 2nd, 3rd, and 4th orders of difference.
		// Here, v2 = v1 + d2, d3 = d2 + dd3 and so on.
		// Hatanaka RINEX stores v1, d2, dd3, ddd4, ddd5... for the file with maxdiff=3.
		//
		// Update algorithm to v5 from the previous data is as follows:
		// At the end of epoch4, r stores r.refData = v4, and r.diffData = [d2, dd3, ddd4].
		// When advancing to epoch5, the new value ddd5 is derived if maxdiff = 3.
		// (dddd5 will appear if maxdiff >= 4)
		//
		// Then d5 can be estimated by
		// d3  = d2 + dd3
		// dd4 = dd3 + ddd4
		// d4  = d3 + dd4
		// dd5 = dd4 + ddd5
		// d5  = d4 + dd5
		// and finally v5 can be estimated as v5 = v4 + d5.

		r.diffData = append(r.diffData, intNumber)

		// update diff data
		m := r.MaxDiff
		if len(r.diffData) > m {
			for i := m; i > 1; i-- {
				r.diffData[i-1] += r.diffData[i-2]
			}
			r.diffData = r.diffData[1:]
		}

		// Update refdata
		//
		// Firstly a single order difference is calculated from
		// the multi-order (diffOder) differences, then
		// new value is calculated by adding the single order
		// difference to the previous value.
		dv := make([]int, len(r.diffData))
		copy(dv, r.diffData)

		// Calculate a single order difference
		for len(dv) > 1 {
			dv = integ(dv)
		}
		r.refData += dv[0] // add to the previous value

		r.missing = false
	} else {
		// case 3: no data exists
		r.missing = true // missing data flag
	}

	return nil
}

func integ(d []int) []int {
	m := len(d)
	a := make([]int, m-1)
	for i := m - 1; i > 0; i-- {
		a[i-1] = d[i] + d[i-1]
	}

	return a
}

package crinex

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

var (
	ErrBadMagic            = errors.New("crxReader: Bad magic value ")
	ErrNotSupportedVersion = errors.New("crxReader: Not supported version ")
)

func NewReader(r io.Reader) (io.Reader, error) {
	var (
		epochStr string
		clockStr string
		buf      []byte

		epochRec strRecord
		data     map[string]satDataRecord
		clk      diffRecord
	)

	// setup new crxReader
	s, ver, err := setup(r)
	if err != nil {
		return r, err
	}

	_ = ver

	// parse obsTypes and get all header contents
	obsTypes, headers := scanHeader(s)
	buf = append(buf, headers...) // add header

	for s.Scan() {
		// update epoch record
		epochStr = s.Text()
		if strings.HasPrefix(epochStr, ">") {
			// crinex ver 3.0
			// check special event
			if (len(epochStr) >= 35) && (epochStr[31] > '1') {
				numSkip, err := strconv.Atoi(strings.TrimSpace(string(epochStr[32:35])))
				if err == nil {
					// special event found, skip numSkip lines
					buf = append(buf, epochStr...)
					buf = append(buf, '\n')
					for i := 0; i < numSkip; i++ {
						s.Scan()
						buf = append(buf, s.Text()...)
						buf = append(buf, '\n')
					}
					continue
				} else {
					// should be recover to the next epoch record that begins with '>'.
				}
			}

			// initialize epoch record
			epochRec.buf = []byte(epochStr)
			data = make(map[string]satDataRecord)
		} else if strings.HasPrefix(epochStr, "&") {
			// crinex ver 1.0
			// check special event
			if (len(epochStr) >= 32) && (epochStr[28] > '1') {
				numSkip, err := strconv.Atoi(strings.TrimSpace(string(epochStr[29:32])))
				if err == nil {
					// special event found, skip numSkip lines
					buf = append(buf, epochStr...)
					buf = append(buf, '\n')
					for i := 0; i < numSkip; i++ {
						s.Scan()
						buf = append(buf, s.Text()...)
						buf = append(buf, '\n')
					}
					continue
				} else {
					// should be recover to the next epoch record that begins with '>'.
				}
			}

			// initialize epoch record
			epochRec.buf = []byte(epochStr)
			data = make(map[string]satDataRecord)
		} else {
			epochRec.Update(epochStr)
		}

		// receiver clock
		s.Scan()
		clockStr = s.Text()
		clk.Update([]byte(clockStr))

		// get list of satellites
		var satList []string
		if ver == "3.0" {
			satList = getSatList(epochRec.Bytes())
		} else if ver == "1.0" {
			satList = getSatListV1(epochRec.Bytes())
		}

		// read data block
		for _, satId := range satList {
			satSys := satId[:1]
			obsCodes := obsTypes[satSys]

			s.Scan()
			t := s.Text()
			vals := strings.SplitN(t, " ", len(obsCodes)+1)

			// allocate for new sat
			if _, ok := data[satId]; !ok {
				if ver == "3.0" {
					data[satId] = NewSatDataRecord(obsCodes)
				} else if ver == "1.0" {
					data[satId] = NewSatDataRecordV1(obsCodes)
				}
			}

			// Update code and phase data
			for j := range obsCodes {
				// pointer to the current data
				dj := &data[satId].data[j]

				if len(vals)-1 < j {
					// case 3: missing data
					dj.missing = true
					continue
				}

				b := []byte(vals[j])
				dj.Update(b)

				// initialize arc
				if ver == "1.0" && len(b) > 1 && b[1] == '&' {
					data[satId].lli[j].buf[0] = ' '
					data[satId].ss[j].buf[0] = ' '
				}
			}

			// Update LLI and SS
			// LLI and SS is stored at the last element of vals
			if len(vals) == len(obsCodes)+1 {
				b := []byte(vals[len(obsCodes)]) // LLI and SS data

				// padding with spaces
				for j := len(b); j < len(obsCodes)*2; j++ {
					b = append(b, byte(' '))
				}

				// update
				for j := range obsCodes {
					data[satId].lli[j].Update(string(b[j*2]))
					data[satId].ss[j].Update(string(b[j*2+1]))
				}
			}
		}

		// ----- CRX to RINEX -----
		// buffer data in the RINEX format
		// epoch record

		if ver == "3.0" {
			// epoch record
			if clk.missing {
				buf = append(buf, fmt.Sprintf("%-35.35s\n", epochRec.StringRINEX())...)
			} else {
				buf = append(buf, fmt.Sprintf("%-35.35s      %15.12f\n", epochRec.StringRINEX(), float64(clk.refData)*0.000000000001)...)
			}

			// data block
			for _, satId := range satList {
				var bufs []byte
				bufs = append(bufs, fmt.Sprintf("%3.3s", satId)...)

				d := data[satId]
				for k, d1 := range d.data {
					if d1.missing {
						bufs = append(bufs, "                "...)
						continue
					}
					//bufs = append(bufs, fmt.Sprintf("%14.3f%1c%1c", float64(ref)*0.001, d.lli[k].buf[0], d.ss[k].buf[0])...)
					bufs = append(bufs, intToRinexDataBytes(d1.refData)...)
					bufs = append(bufs, d.lli[k].buf[0])
					bufs = append(bufs, d.ss[k].buf[0])
				}
				buf = append(buf, bytes.TrimRight(bufs, " ")...)
				buf = append(buf, '\n')
			}
		} else if ver == "1.0" {
			if clk.missing {
				buf = append(buf, epochRec.StringRINEXV2(math.NaN())...)
			} else {
				buf = append(buf, epochRec.StringRINEXV2(float64(clk.refData)*0.000000001)...)
			}

			// data block
			for _, satId := range satList {
				var bufs []byte

				d := data[satId]
				for k, d1 := range d.data {
					if d1.missing {
						bufs = append(bufs, "                "...)
					} else {
						bufs = append(bufs, intToRinexDataBytes(d1.refData)...)
						bufs = append(bufs, d.lli[k].buf[0])
						bufs = append(bufs, d.ss[k].buf[0])
					}

					// line feed
					if k == len(d.data)-1 || (k+1)%5 == 0 {
						buf = append(buf, bytes.TrimRight(bufs[:], " ")...)
						buf = append(buf, '\n')
						bufs = []byte{}
					}
				}
			}
		}

	}

	return bytes.NewReader(buf), nil
}

func integ(d []int) []int {
	m := len(d)
	a := make([]int, m-1)
	for i := m - 1; i > 0; i-- {
		a[i-1] = d[i] + d[i-1]
	}

	return a
}

// intToRinexDataBytes returns []byte that is equivalent to the output of
// fmt.Sprintf("%14.3f", float64(n)*0.001)...
func intToRinexDataBytes(n int) []byte {
	if n > 9999999999999 || n < -999999999999 {
		panic("overflow")
	}
	buf := [14]byte{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '0', '.', '0', '0', '0'}

	neg := n < 0
	if neg {
		n = -n
	}

	for i, pos := 0, len(buf); ; i++ {
		pos--
		buf[pos], n = '0'+byte(n%10), n/10
		if i == 2 {
			pos--
			//buf[pos] = '.'
		}
		if n == 0 {
			if neg {
				pos--

				if i < 3 {
					buf[8] = '-'
				} else {
					buf[pos] = '-'
				}
			}
			return buf[:14]
		}
	}
}

// getSatList returns a slice of satellite IDs
// b is a slice of byte contains epoch record (41 bytes) and satellite IDs (3bytes * n)
func getSatList(b []byte) []string {
	satList := []string{}
	for i := 41; i+3 <= len(b); i += 3 {
		satId := string(b[i : i+3])
		if satId != "   " {
			satList = append(satList, satId)
		}
	}
	return satList
}

func getSatListV1(b []byte) []string {
	satList := []string{}
	for i := 32; i+3 <= len(b); i += 3 {
		satId := string(b[i : i+3])
		if satId != "   " {
			satList = append(satList, satId)
		}
	}
	return satList
}

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

func (e *strRecord) Update(s string) error {
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

type diffRecord struct {
	MaxDiff  int
	refData  int
	diffData []int
	missing  bool
}

func (r *diffRecord) Update(b []byte) error {
	var v []byte
	if len(b) > 2 && b[1] == '&' {
		// case 1: initialize data
		diffOrder, _ := strconv.Atoi(string(b[0]))
		ref, _ := strconv.Atoi(string(b[2:]))

		// initialize
		r.refData = ref
		r.MaxDiff = diffOrder
		r.diffData = []int{}
		r.missing = false
	} else if len(b) > 0 {
		// case 2: update data
		v = b
		intNumber, _ := strconv.Atoi(string(v))

		// 0:  v1  v2   v3   v4    v5
		// 1:      d2   d3   d4    d5
		// 2:          dd3  dd4   dd5
		// 3:              ddd4  ddd5
		// 4:                   dddd5
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

func setup(r io.Reader) (s *bufio.Scanner, ver string, err error) {
	s = bufio.NewScanner(r)
	if err = s.Err(); err != nil {
		return s, ver, err
	}

	// check first line: "CRINEX VERS   / TYPE"
	// "3.0                 COMPACT RINEX FORMAT"
	s.Scan()
	t := s.Text()

	// check header
	if len(t) < 40 {
		return s, ver, ErrBadMagic
	}

	ver = strings.TrimSpace(t[:20])
	magic := t[20:40]

	//3.0                 COMPACT RINEX FORMAT                    CRINEX VERS   / TYPE
	if magic != "COMPACT RINEX FORMAT" {
		return s, ver, ErrBadMagic
	}
	if ver != "3.0" && ver != "1.0" {
		return s, ver, ErrNotSupportedVersion
	}

	// skip second line: "CRINEX PROG / DATE"
	s.Scan()

	return s, ver, nil
}

func scanHeader(s *bufio.Scanner) (obsTypes map[string][]string, h []byte) {
	var obsTypesStrings []string
	var obsTypesStringsV2 []string
	var rinexVer byte

	for s.Scan() {
		buf := s.Text()
		h = append(h, []byte(buf)...)
		h = append(h, byte('\n'))

		if strings.HasPrefix(buf[60:], "RINEX VERSION / TYPE") {
			rinexVer = strings.TrimSpace(buf[:20])[0] // '2', '3', or '4'
		}
		if strings.HasPrefix(buf[60:], "SYS / # / OBS TYPES") {
			obsTypesStrings = append(obsTypesStrings, buf)
		}
		if strings.HasPrefix(buf[60:], "# / TYPES OF OBSERV") {
			obsTypesStringsV2 = append(obsTypesStringsV2, buf)
		}
		if strings.HasPrefix(buf[60:], "END OF HEADER") {
			break
		}
	}

	// currently errors are ignored
	if rinexVer >= '3' {
		obsTypes, _ = parseObsTypes(obsTypesStrings)
	} else if rinexVer >= '2' {
		obsTypes, _ = parseObsTypesV2(obsTypesStringsV2)
	} else {
		// not supported
	}

	return
}

func parseObsTypes(buf []string) (obsTypes map[string][]string, err error) {
	var (
		s, satSys string
		numCodes  int
	)
	obsTypes = make(map[string][]string)

	if len(buf) == 0 {
		return
	}

	for k := 0; k < len(buf); k++ {
		s = buf[k]

		if len(s) < 6 {
			err = fmt.Errorf("too short msg, s='%s'", s)
			return
		}

		// parse satsys code
		satSys = s[:1] // "G", "R", "J", "E", "C"
		numCodes, err = strconv.Atoi(strings.TrimSpace(s[3:6]))
		if err != nil {
			err = fmt.Errorf("cannot parse numCodes, err=%w", err)
			return
		}

		n := 0   // number of codes in the current line
		idx := 7 // index of the string
		for i := 0; i < numCodes; i++ {
			if len(s) < idx+3 {
				err = fmt.Errorf("too short msg, s='%s'", s)
				return
			}
			obsTypes[satSys] = append(obsTypes[satSys], s[idx:idx+3])

			n++
			idx += 4
			if n == 13 && i+1 < numCodes {
				// move to the new line
				k++
				s = buf[k]
				n, idx = 0, 7
			}
		}
	}
	return
}

func parseObsTypesV2(buf []string) (obsTypes map[string][]string, err error) {
	var (
		s        string
		numCodes int
	)
	obsTypes = make(map[string][]string)

	if len(buf) == 0 {
		err = fmt.Errorf("failed to parse obsTypes, no data found")
		return
	}

	s = buf[0]
	sep := strings.Fields(s)
	sep = sep[1:] // remove the first element that indicates the numCodes

	// parse number of obsCodes
	numCodes, err = strconv.Atoi(strings.TrimSpace(s[:6]))
	if err != nil {
		err = fmt.Errorf("failed to parse numCodes, s='%s', err=%v", s[:6], err)
		return
	}
	obsCodes := make([]string, numCodes)

	for k := 0; k < len(buf); k++ {
		// s = buf[k]

		// if len(s) < 6 {
		// 	err = fmt.Errorf("too short msg, s='%s'", s)
		// 	return
		// }

		n := 0    // number of codes in the current line
		idx := 10 // index of the string
		for i := 0; i < numCodes; i++ {
			if len(sep[n]) >= 2 {
				obsCodes[i] = sep[n][:2]
			} else {
				err = fmt.Errorf("failed to parse obsCode, s='%s'", sep[n])
				return
			}

			n++
			idx += 6
			if n == 9 && i+1 < numCodes {
				// move to the new line
				k++
				s = buf[k]
				n, idx = 0, 10

				// workaround for invalid format
				if len(s) > 60 {
					sep = strings.Fields(s[:60])
				} else {
					sep = strings.Fields(s)
				}
			}
		}
	}

	for _, satSys := range []string{"G", "R", "J", "E", "C", "I", "S"} {
		obsTypes[satSys] = obsCodes
	}

	return
}

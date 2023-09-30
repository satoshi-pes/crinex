package crinex

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrBadMagic            = errors.New("crxReader: Bad magic value ")
	ErrNotSupportedVersion = errors.New("crxReader: Not supported version ")
	ErrInvalidEpochStr     = errors.New("crxReader: Invalid EpochStr found ")
	ErrRecovered           = errors.New("crxReader: Invalid record found and recovered ")
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
	obsTypes, headers, err := scanHeader(s)
	if err != nil {
		return bytes.NewReader(buf), err
	}

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
			epochRec.Decode(epochStr)
		}

		// receiver clock
		s.Scan()
		clockStr = s.Text()
		clk.Decode([]byte(clockStr))

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
				dj.Decode(b)

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
					data[satId].lli[j].Decode(string(b[j*2]))
					data[satId].ss[j].Decode(string(b[j*2+1]))
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

// setup parses the first two lines of the Hatanaka RINEX and returns
// scanner and version. The first two lines contain Hatanaka RINEX header.
// The file position will be advanced 2 lines after the call.
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

// scanHeader parses the header, stores header contents and obstypes to
// s.header and s.obsTypes, and advance reader position to the head of
// the first data block.
func scanHeader(s *bufio.Scanner) (obsTypes map[string][]string, h []byte, err error) {
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
		err = ErrNotSupportedVersion
		return
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

// ----------------------------------------------------------------------------
// utility functions
// ----------------------------------------------------------------------------

// epochRecBytestoTime converts epochRec.bytes() to time.Time
func epochRecBytestoTime(b []byte, ver string) (t time.Time, err error) {
	if ver == "3.0" {
		dtLayout := "2006  1  2 15  4  5" // YYYY mm dd HH MM SS

		// date
		t, err = time.Parse(dtLayout, string(b[2:29]))
		if err != nil {
			return t, ErrInvalidEpochStr
		}
		return t, nil
	} else if ver == "1.0" {
		var (
			yy, mm, dd, HH, MM, ss, ns int
			errs                       [7]error
		)
		yy, errs[0] = strconv.Atoi(string(bytes.TrimSpace(b[1:3])))
		mm, errs[1] = strconv.Atoi(string(bytes.TrimSpace(b[4:6])))
		dd, errs[2] = strconv.Atoi(string(bytes.TrimSpace(b[7:9])))
		HH, errs[3] = strconv.Atoi(string(bytes.TrimSpace(b[10:12])))
		MM, errs[4] = strconv.Atoi(string(bytes.TrimSpace(b[13:15])))
		ss, errs[5] = strconv.Atoi(string(bytes.TrimSpace(b[16:18])))
		ns_bytes := append(bytes.TrimLeft(b[19:25], "0"), b[25]) // nano seconds
		ns, errs[6] = strconv.Atoi(string(ns_bytes))

		if yy >= 80 {
			yy += 1900
		} else {
			yy += 2000
		}

		for _, e := range errs {
			if e != nil {
				return t, ErrInvalidEpochStr
			}
		}
		t = time.Date(yy, time.Month(mm), dd, HH, MM, ss, ns*100, time.UTC)

		return t, nil
	}

	return t, ErrNotSupportedVersion
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

// intToRinexDataBytes returns []byte that is equivalent to the output of
// fmt.Sprintf("%14.3f", float64(n)*0.001)...
func intToRinexDataBytes(n int64) []byte {
	if n > 9999999999999 || n < -999999999999 {
		fmt.Fprintf(os.Stderr, "intToRinexDataBytes: value overflow: v='%d'\n", n)

		if n > 0 {
			return []byte("9999999999.999")
		} else {
			return []byte("-999999999.999")
		}
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

package crinex

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// crinex logger
var logger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)

var (
	ErrBadMagic            = errors.New("crinex: Bad magic value")
	ErrNotSupportedVersion = errors.New("crinex: Not supported version")
	ErrInvalidHeader       = errors.New("crinex: Invalid Header")
	ErrInvalidEpochStr     = errors.New("crinex: Invalid EpochStr found")
	ErrInvalidData         = errors.New("crinex: Invalid record found")
	ErrInvalidMaxDiff      = errors.New("crinex: Invalid maxdiff found")
	ErrInvalidSatList      = errors.New("crinex: Invalid satellite list found")
	ErrRecovered           = errors.New("crinex: Invalid record found and recovered")
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
	s, ver, _, err := setup(r)
	if err != nil {
		return r, err
	}

	_ = ver

	// parse obsTypes and get all header contents
	obsTypes, headers, _, warns, err := scanHeader(s)
	if err != nil {
		return bytes.NewReader(buf), err
	}
	for _, w := range warns {
		logger.Printf("[warning] line=%d: %s\n", w.Pos, w.Msg)
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
		satList, warns, err := getSatListWithCorrection(epochRec.Bytes(), ver, -1)
		if err != nil {
			return bytes.NewReader(buf), err
		}
		for _, w := range warns {
			logger.Printf("[warning] line=%d: %s\n", w.Pos, w.Msg)
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
				data[satId] = NewSatDataRecord(obsCodes)
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
		switch ver {
		case "3.0", "3.1":
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
		case "1.0":
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
func setup(r io.Reader) (s *bufio.Scanner, ver string, lines int, err error) {
	s = bufio.NewScanner(r)
	if err = s.Err(); err != nil {
		return s, ver, lines, err
	}

	// check first line: "CRINEX VERS   / TYPE"
	// "3.0                 COMPACT RINEX FORMAT"
	s.Scan()
	lines++
	t := s.Text()

	// check header
	if len(t) < 40 {
		return s, ver, lines, ErrBadMagic
	}

	ver = strings.TrimSpace(t[:20])
	magic := t[20:40]

	//3.0                 COMPACT RINEX FORMAT                    CRINEX VERS   / TYPE
	if magic != "COMPACT RINEX FORMAT" {
		return s, ver, lines, ErrBadMagic
	}
	if ver != "3.1" && ver != "3.0" && ver != "1.0" {
		return s, ver, lines, ErrNotSupportedVersion
	}

	// skip second line: "CRINEX PROG / DATE"
	s.Scan()
	lines++

	return s, ver, lines, nil
}

// scanHeader parses the header, stores header contents and obstypes to
// s.header and s.obsTypes, and advance reader position to the head of
// the first data block.
func scanHeader(s *bufio.Scanner) (obsTypes map[string][]string, h []byte, lines int, warnings WarningList, err error) {
	var (
		obsTypesStrings   []string
		obsTypesStringsV2 []string
		rinexVer          byte

		// flags for validation of Header
		RinexVerIsOk    bool
		endOfHeaderIsOk bool
	)

	for s.Scan() {
		lines++

		buf := s.Text()
		if len(buf) < 61 {
			// no header label found, and read as a comment
			warnings.Add(lines, fmt.Sprintf("no header label found: s='%s'", buf))
			buf = fmt.Sprintf("%-60sCOMMENT", buf)
		}

		h = append(h, []byte(buf)...)
		h = append(h, byte('\n'))

		if strings.HasPrefix(buf[60:], "RINEX VERSION / TYPE") {
			rinexVer = strings.TrimSpace(buf[:20])[0] // '2', '3', or '4'
			RinexVerIsOk = true
		}
		if strings.HasPrefix(buf[60:], "SYS / # / OBS TYPES") {
			obsTypesStrings = append(obsTypesStrings, buf)
		}
		if strings.HasPrefix(buf[60:], "# / TYPES OF OBSERV") {
			obsTypesStringsV2 = append(obsTypesStringsV2, buf)
		}
		if strings.HasPrefix(buf[60:], "END OF HEADER") {
			endOfHeaderIsOk = true
			break
		}
	}

	// check if header is ok
	switch {
	case !RinexVerIsOk:
		err = fmt.Errorf("%w: RINEX version not found", ErrInvalidHeader)
		return
	case !endOfHeaderIsOk:
		err = fmt.Errorf("%w: END OF HEADER not found", ErrInvalidHeader)
		return
	}

	// currently errors are ignored
	var e error
	if rinexVer >= '3' {
		obsTypes, e = parseObsTypes(obsTypesStrings)
		if e != nil {
			// obstypes header is not correct, but only show a warning
			// because the number of observation types could be inferred from
			// the first initialization line.
			warnings.Add(lines, fmt.Sprintf("failed to parse obstypes: %v", e))
		}
	} else if rinexVer >= '2' {
		obsTypes, e = parseObsTypesV2(obsTypesStringsV2)
		if e != nil {
			// obstypes header is not correct, but only show a warning
			// because the number of observation types could be inferred from
			// the first initialization line.
			warnings.Add(lines, fmt.Sprintf("failed to parse obstypes: %v", e))
		}
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
			err = fmt.Errorf("too short obstypes, s='%s'", s)
			return
		}

		// parse satsys code
		satSys = s[:1] // "G", "R", "J", "E", "C"
		numCodes, err = strconv.Atoi(strings.TrimSpace(s[3:6]))
		if err != nil {
			err = fmt.Errorf("failed to parse numCodes, err=%v", err)
			return
		}
		obsTypes[satSys] = make([]string, numCodes)

		n := 0   // number of codes in the current line
		idx := 7 // index of the string
		for i := 0; i < numCodes; i++ {
			if len(s) < idx+3 {
				err = fmt.Errorf("too short obstypes, s='%s'", s)
				return
			}
			obsTypes[satSys][i] = s[idx : idx+3]

			n++
			idx += 4
			if n == 13 && i+1 < numCodes {
				// move to the new line
				k++
				if k >= len(buf) {
					err = fmt.Errorf("obstypes header is missing")
					return
				}
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
	sep := strings.Fields(s[:60])
	sep = sep[1:] // remove the first element that indicates the numCodes

	// parse number of obsCodes

	// This is a workaround to parse invalid number of obstypes, e.g., tow21810.99d:
	// "    x5    C1    L1    L2    P2    P1                        # / TYPES OF OBSERV"
	strNumObs := replaceNonNumericToSpace(s[:6])
	numCodes, err = strconv.Atoi(strings.TrimSpace(strNumObs))
	if err != nil {
		err = fmt.Errorf("failed to parse numCodes, s='%s', err=%v", s[:6], err)
		return
	}
	obsCodes := make([]string, numCodes)

	// store same obsCodes for all satellite system before return.
	// note that " " denotes "G".
	defer func() {
		for _, satSys := range VALID_SATSYS {
			obsTypes[satSys] = obsCodes
		}
	}()

	for k := 0; k < len(buf); k++ {
		n := 0    // number of codes in the current line
		idx := 10 // index of the string

		for i := 0; i < numCodes; i++ {
			// check number of obscodes exist
			if len(sep) <= n {
				err = fmt.Errorf("not enough obsCodes, numCodes='%d', s='%v'", numCodes, sep)
				return
			}

			// check code length
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
				if k >= len(buf) {
					err = fmt.Errorf("obstypes header is missing")
					return
				}
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

	return
}

// ----------------------------------------------------------------------------
// utility functions
// ----------------------------------------------------------------------------

// epochRecBytestoTime converts epochRec.bytes() to time.Time
func epochRecBytestoTime(b []byte, ver string) (t time.Time, err error) {
	switch ver {
	case "3.0", "3.1":
		dtLayout := "2006  1  2 15  4  5" // YYYY mm dd HH MM SS

		if len(b) < 29 {
			// too short string
			return t, ErrInvalidEpochStr
		}

		// date
		t, err = time.Parse(dtLayout, string(b[2:29]))
		if err != nil {
			return t, ErrInvalidEpochStr
		}
		return t, nil
	case "1.0":
		var (
			yy, mm, dd, HH, MM, ss, ns int
			errs                       [7]error
		)

		if len(b) < 25 {
			// too short string
			return t, ErrInvalidEpochStr
		}

		// check if spaces are correctly placed
		if (b[0] != ' ' && b[0] != '&') || b[3] != ' ' || b[6] != ' ' || b[9] != ' ' || b[12] != ' ' || b[15] != ' ' {
			return t, ErrInvalidEpochStr
		}

		yy, errs[0] = strconv.Atoi(string(bytes.TrimSpace(b[1:3])))
		mm, errs[1] = strconv.Atoi(string(bytes.TrimSpace(b[4:6])))
		dd, errs[2] = strconv.Atoi(string(bytes.TrimSpace(b[7:9])))
		HH, errs[3] = strconv.Atoi(string(bytes.TrimSpace(b[10:12])))
		MM, errs[4] = strconv.Atoi(string(bytes.TrimSpace(b[13:15])))

		// In case of blank, it is considered to be 0 seconds.
		if strings.TrimSpace(string(b[16:18])) == "" {
			ss = 0
		} else {
			ss, errs[5] = strconv.Atoi(string(bytes.TrimSpace(b[16:18])))
		}

		ns_bytes := append(bytes.TrimLeft(b[19:25], "0"), b[25]) // nano seconds
		// In case of blank, it is considered to be 0 nano seconds.
		if strings.TrimSpace(string(ns_bytes)) == "" {
			ns = 0
		} else {
			ns, errs[6] = strconv.Atoi(string(ns_bytes))
		}

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
	s := bytes.TrimRight(b, " ")
	for i := OFFSET_SATLST_V3; i+3 <= len(s); i += 3 {
		satId := string(s[i : i+3])
		satList = append(satList, satId)
	}
	return satList
}

func getSatListV1(b []byte) []string {
	satList := []string{}
	s := bytes.TrimRight(b, " ")
	for i := OFFSET_SATLST_V1; i+3 <= len(s); i += 3 {
		satId := string(s[i : i+3])
		satList = append(satList, satId)
	}
	return satList
}

// getSatListWithCorrection returns a slice of satellite IDs.
// b is a slice of byte contains epoch record and ver is the crinex version (1.0 or 3.0).
// If invalid satellite id is found, this func attempts to repair it.
func getSatListWithCorrection(b []byte, ver string, lineNum int) (satList []string, warns WarningList, err error) {
	var (
		offsetNumSat  int
		offsetSatList int
	)

	switch ver {
	case "3.0", "3.1":
		offsetNumSat, offsetSatList = OFFSET_NUMSAT_V3, OFFSET_SATLST_V3
	case "1.0":
		offsetNumSat, offsetSatList = OFFSET_NUMSAT_V1, OFFSET_SATLST_V1
	default:
		return satList, WarningList{}, ErrNotSupportedVersion
	}

	// no satellite list found
	if len(b) < offsetSatList {
		err = fmt.Errorf("%w: b='%s'", ErrInvalidSatList, b)
		return satList, WarningList{}, err
	}

	// get number of satellites
	n, e := strconv.Atoi(string(bytes.TrimSpace(b[offsetNumSat : offsetNumSat+3])))
	if e != nil {
		err = fmt.Errorf("%w: err=%v", ErrInvalidSatList, e)
		return satList, WarningList{}, err
	}

	// repair invalid epoch record
	if len(bytes.TrimRight(b, " ")) != offsetSatList+3*n {
		warns.Add(lineNum, fmt.Sprintf("length of epoch record is wrong: b='%s'", b))

		switch {
		case len(bytes.TrimRight(b, " ")) < offsetSatList+3*n:
			// case1:
			// There is a wrong epoch record at line 281 in jab11630.99d:
			// `              4 &                            4&19&2 &15&`,
			// this line represents
			// ` 99  6 12  0 14  0.0000000  0  8 18 14 27 16 4 19 22 15`.
			//
			// However, satID ' 4' should be '  4' correctly.
			// So here the satellite list is corrected by separating b with a space.
			// The same issue found in jab11630.99d, jab11640.99d, jab11660.99d,
			// jab11670.99d, maw10360.99d and maw10860.99d.

			if bb := bytes.Fields(bytes.Trim(b[offsetSatList:], " ")); len(bb) == n {
				warns.Add(lineNum, "modify to be the correct 3 bytes sat IDs.")

				// rearrange epoch record to be the correct 3 bytes satellite IDs.
				ss := string(b[:offsetSatList])
				for _, b1 := range bb {
					ss += fmt.Sprintf("%3.3s", b1)
				}
				b = []byte(ss)
			}

		case len(bytes.TrimRight(b, " ")) == offsetSatList+3*n+1 && b[offsetSatList] == ' ':
			// case2:
			// There is a wrong epoch record at line 433 in jab12250.99d:
			// `                3                &07&27&18&04&10&02&19&13`,
			// this line represents
			// ` 99  8 13  0 20 30.0000000  0  8  07 27 18 04 10 02 19 13`.
			//
			// However, there is an extra space before the satellite list.
			// Correctly, this line would look like:
			// `                3                07&27&18&04&10&02&19&13` and
			// ` 99  8 13  0 20 30.0000000  0  8 07 27 18 04 10 02 19 13`.
			//
			// So here the extra space is checked and it will be removed.
			// The same issue found in jab12280.99d, jab12420.99d, jab12370.99d,
			// jab12830.99d, jab12250.99d, and jab12390.99d.
			warns.Add(lineNum, "delete an extra space found at the begining of the satellite list.")

			// delete the extra space, not modifying the original slice.
			r := make([]byte, len(b))
			copy(r, b)
			b = slices.Delete(r, offsetSatList, offsetSatList+1)
		}
	}

	switch ver {
	case "3.0", "3.1":
		satList = getSatList(b)
	case "1.0":
		satList = getSatListV1(b)
	}

	// check for consistency between numsat and len of satList
	lens := len(satList)
	if lens != n {
		warns.Add(lineNum, fmt.Sprintf("mismatch between number of satellites: ns='%d', satList='%+v'", n, satList))

		// the last index where the satellites list were correctly parsed
		i := offsetSatList + lens*3

		// try to repair the invalid satID
		if len(b) > i+2 {
			bb := b[i : i+2]
			if satId, ok := repairInvalidSatID(bb); ok {
				satList = append(satList, satId)
				warns.Add(lineNum, fmt.Sprintf("modified invalid satellite '%s '->'%s'", string(bb), satId))
			}
		}
	}

	return satList, warns, nil
}

// repairInvalidSatID returns correct satID and ok on success.
func repairInvalidSatID(b []byte) (s string, ok bool) {
	if len(b) < 2 {
		return "", false
	}

	// Workarounds for invalid satellite IDs
	switch {
	// case1 satellite ID "X9 " to "X 9", e.g. line 1653 of alic2520.98d
	case len(bytes.TrimRight(b, " ")) == 2 && slices.Contains(VALID_SATSYS, string(b[0])) && isNumeric(b[1]):
		// case1 invalid satellite ID found at line 1653 of alic2520.98d:
		// "                3              2 &4 9&  & &&  & &&  &  &"
		// This case the second satellite is " 9 " but correctly it is "  9".
		return string([]byte{b[0], ' ', b[1]}), true
	}

	return
}

// intToRinexDataBytes returns []byte that is equivalent to the output of
// fmt.Sprintf("%14.3f", float64(n)*0.001)...
func intToRinexDataBytes(n int64) []byte {
	if n > 9999999999999 || n < -999999999999 {
		logger.Printf("intToRinexDataBytes: value overflow: v='%d'\n", n)

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

// replaceNonNumericToSpace replaces non numeric characters to spaces.
func replaceNonNumericToSpace(s string) string {
	ss := []byte(s)
	for i := range ss {
		if !isNumeric(ss[i]) {
			ss[i] = ' '
		}
	}
	return string(ss)
}

// allBytesAreNumeric reports whether all the passed bytes are numeric characters.
// Returns false if the length of b == 0.
func allBytesAreNumeric(b []byte) bool {
	if len(b) == 0 {
		return false
	}

	for _, s := range b {
		if !isNumeric(s) {
			return false
		}
	}
	return true
}

// isNumeric reports whether the byte is a numeric character.
func isNumeric(s byte) bool {
	return '0' <= s && s <= '9'
}

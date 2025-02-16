package crinex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------
// Hatanaka RINEX scanner
// ---------------------------------------------------

type Scanner struct {
	// file information
	ver      string
	header   []byte              // header bytes
	obsTypes map[string][]string // obstypes

	// decorded and differenced data updated every epoch
	epochRec strRecord                // epoch record
	data     map[string]satDataRecord // data
	clk      diffRecord               // clock
	picoSec  strRecord                // pico-second part of the epoch (CRINEX>=3.1, RINEX>=4.02)

	// real values for easier access
	epoch   time.Time
	satList []string // list of satellites in the current epoch

	// file reader and scanner
	r *io.Reader
	s *bufio.Scanner

	// line number
	epochLineNum int // line number of the current epoch record
	clockLineNum int // line number of the current clock record
	lineNum      int // line number of the current position

	// error and warnings
	err      error
	Warnings WarningList
}

type SatObss struct {
	SatId   string
	ObsData []SatObsData
}

func (o *SatObss) StringRINEX() (s string) {
	var buf []string
	buf = append(buf, fmt.Sprintf("%3s", o.SatId))

	for _, d := range o.ObsData {
		buf = append(buf, d.StringRINEX())
	}
	return strings.TrimRight(strings.Join(buf, ""), " ")
}

type SatObsData struct {
	Data float64 // math.NaN represents the missing data
	LLI  byte
	SS   byte
}

func (d *SatObsData) StringRINEX() (s string) {
	if math.IsNaN(d.Data) {
		// missing data
		return "              " + string(d.LLI) + string(d.SS)
	}
	return fmt.Sprintf("%14.3f%c%c", d.Data, d.LLI, d.SS)
}

func NewScanner(r io.Reader) (*Scanner, error) {
	var (
		s     Scanner
		err   error
		lines int
	)

	// setup scanner and get the version of Hatanaka RINEX
	// Note: RINEX header contents have not parsed at this point
	s.r = &r
	s.s, s.ver, lines, err = setup(r)
	s.lineNum += lines // first two lines were scanned in setup

	s.obsTypes = make(map[string][]string)

	return &s, err
}

func (s *Scanner) Scan() bool {
	ok := s.s.Scan()
	if ok {
		s.lineNum++
	}
	return ok
}

// ParseHeader parses the header, stores header contents and obstypes to
// s.header and s.obsTypes, and advance reader position to the head of
// the first data block.
func (s *Scanner) ParseHeader() (err error) {
	var (
		lines int
		warns WarningList
	)

	s.obsTypes, s.header, lines, warns, err = scanHeader(s.s)
	s.lineNum += lines
	s.Warnings = append(s.Warnings, warns...)

	return err
}

// ScanEpoch reads Hatanaka compressed data for an epoch and
// set decoded values. Returns true if scan is successful.
// In the case the scan failed, the error is stored in s.err.
// Returns true for io.EOF.
func (s *Scanner) ScanEpoch() bool {
	// The header must be scanned header before the data block is scanned
	if s.header == nil {
		if err := s.ParseHeader(); err != nil {
			s.err = fmt.Errorf("failed to parse header: %w", err)
			return false
		}
	}

	// scan next data block and update data
	if ok := s.Scan(); !ok {
		s.err = s.s.Err()
		return false
	}
	epochStr := s.s.Text()

RETRY_SCAN_EPOCH:
	// read a set of data for an epoch.
	// s will be updated in place.
	err := s.scanEpoch(epochStr)
	if err == io.EOF {
		return true
	}

	if err != nil {
		s.Warnings.Add(s.lineNum, fmt.Sprintf("failed to scan epoch: %v", err))

		// seek new epoch record identifier to recover

		// Check for an initialization flag '>' exists in the middle of the line.
		// This only supports ver 3.0 because the initialization flag '&' for ver 1.0 is
		// indistinguishable from the the differentiation flag.
		if i := strings.Index(epochStr, ">"); i > 0 {
			// found an initialization flag
			s.Warnings.Add(s.lineNum, fmt.Sprintf("epochrec modified: '%s'", epochStr))

			epochStr = epochStr[i:]
			goto RETRY_SCAN_EPOCH
		}

		// Search for the next initialization flag
		for s.Scan() {
			epochStr = s.s.Text()
			if strings.HasPrefix(epochStr, ">") || strings.HasPrefix(epochStr, "&") {
				// found initialization flag
				goto RETRY_SCAN_EPOCH
			}
		}

		// recover failed
		s.err = err
		return false
	}

	return true
}

// Header returns the header contents for the file
func (s *Scanner) Header() []byte {
	return s.header
}

// ObsTypes returns the observation types defined for the file
func (s *Scanner) ObsTypes() map[string][]string {
	return s.obsTypes
}

// SatList reuturns the list of satellites for current epoch
func (s *Scanner) SatList() []string {
	return s.satList
}

// Epoch returns the time tag for current epoch as time.Time
func (s *Scanner) Epoch() time.Time {
	return s.epoch
}

func (s *Scanner) EpochAsBytes() []byte {
	switch s.ver {
	case "3.0":
		if s.clk.missing {
			return []byte(fmt.Sprintf("%-35.35s\n", s.epochRec.StringRINEX()))
		}
		return []byte(fmt.Sprintf("%-35.35s      %15.12f\n", s.epochRec.StringRINEX(), s.ClockOffset()))

	case "3.1":
		// CRINEX 3.1 can include pico-second records
		hasClock := !s.clk.missing
		picoSecBytes, hasPicoSec := s.PicoSecondsBytes()

		switch {
		case !hasClock && !hasPicoSec:
			return []byte(fmt.Sprintf("%-35.35s\n", s.epochRec.StringRINEX()))
		case hasClock && !hasPicoSec:
			return []byte(fmt.Sprintf("%-35.35s      %15.12f\n", s.epochRec.StringRINEX(), s.ClockOffset()))
		case !hasClock && hasPicoSec:
			return []byte(fmt.Sprintf("%-35.35s                      %5.5s\n", s.epochRec.StringRINEX(), picoSecBytes))
		default:
			return []byte(fmt.Sprintf("%-35.35s      %15.12f %5.5s\n", s.epochRec.StringRINEX(), s.ClockOffset(), picoSecBytes))
		}
	case "1.0":
		return []byte(s.epochRec.StringRINEXV2(s.ClockOffset()))
	}

	return []byte{}
}

// ClockOffset returns clock offset as float64 value.
// Returns math.NaN() if the clock offset record is missing or unexpected error found.
func (s *Scanner) ClockOffset() (clkoff float64) {
	clkoff = math.NaN()
	if s.clk.missing {
		return // nan
	}

	switch s.ver {
	case "3.0", "3.1":
		return float64(s.clk.refData) * 0.000000000001
	case "1.0":
		return float64(s.clk.refData) * 0.000000001
	}

	return // nan
}

// PicoSeconds returns pico-second part of the epoch as an int value.
// Returns -1 if the pico-second is missing or unexpected error found.
// pico-second record has been introduced from RINEX>=4.02 (CRINEX>=3.1) as an
// optional record.
//
// Note that negative values and non-numeric entries in the pico-second record
// will be considered format violations.
func (s *Scanner) PicoSeconds() int {
	missingVal := -1

	switch {
	case len(s.picoSec.Bytes()) == 0:
		return missingVal

	// non-numeric entries are format violation
	case !allBytesAreNumeric(s.picoSec.Bytes()):
		// warning
		s.Warnings.Add(s.clockLineNum, fmt.Sprintf("non-numeric entries found in the pico-second record: picoSec='%s'", s.picoSec.String()))

		return missingVal
	}

	picoSec, err := strconv.Atoi(s.picoSec.String())
	if err != nil {
		return missingVal // -1
	}

	return picoSec
}

// PicoSeconds returns pico-second part of the epoch as [5]byte and a bool
// indicating success.
//
// Note:
// pico-second record has been introduced from RINEX>=4.02 (CRINEX>=3.1) as an
// optional record. Negative values and non-numeric entries in the pico-second
// record will be considered format violations.
func (s *Scanner) PicoSecondsBytes() (bytes [5]byte, ok bool) {
	picoSecBytes := s.picoSec.Bytes()
	if len(picoSecBytes) != 5 {
		return bytes, false
	}

	for i, b := range picoSecBytes {
		if !isNumeric(b) {
			// warning
			s.Warnings.Add(s.clockLineNum, fmt.Sprintf("non-numeric entries found in the pico-second record: picoSec='%s'", picoSecBytes))

			return bytes, false
		}
		bytes[i] = b
	}

	// ok
	return bytes, true
}

// Data returns decompressed RINEX data
func (s *Scanner) Data() (obs []SatObss) {
	obs = make([]SatObss, len(s.satList))

	switch s.ver {
	case "1.0", "3.0", "3.1":
		// data block
		for i, satId := range s.satList {
			obs[i].SatId = satId
			obs[i].ObsData = make([]SatObsData, len(s.data[satId].obsCodes))

			d := s.data[satId]
			for j, d1 := range d.data {
				if d1.missing {
					obs[i].ObsData[j].Data = math.NaN()
					obs[i].ObsData[j].LLI = ' '
					obs[i].ObsData[j].SS = ' '
					continue
				}
				obs[i].ObsData[j].Data = float64(d1.refData) * 0.001
				obs[i].ObsData[j].LLI = d.lli[j].buf[0]
				obs[i].ObsData[j].SS = d.ss[j].buf[0]
			}
		}
	}
	return
}

// Data returns decompressed RINEX data as RINEX bytes
func (s *Scanner) DataAsBytes() (buf []byte) {
	switch s.ver {
	case "3.0", "3.1":
		// data block
		for _, satId := range s.satList {
			var bufs []byte
			bufs = append(bufs, fmt.Sprintf("%3.3s", satId)...)

			d := s.data[satId]
			for k, d1 := range d.data {
				if d1.missing {
					bufs = append(bufs, "                "...)
					continue
				}

				// intToRinexDataByptes is optimized and faster than a fmt.Sprintf call.
				// this outputs the same text as follows:
				//     bufs = append(bufs, fmt.Sprintf("%14.3f%1c%1c", float64(ref)*0.001, d.lli[k].buf[0], d.ss[k].buf[0])...)
				bufs = append(bufs, intToRinexDataBytes(d1.refData)...)
				bufs = append(bufs, d.lli[k].buf[0])
				bufs = append(bufs, d.ss[k].buf[0])
			}
			buf = append(buf, bytes.TrimRight(bufs, " ")...)
			buf = append(buf, '\n')
		}
	case "1.0":
		// data block
		for _, satId := range s.satList {
			var bufs []byte

			d := s.data[satId]
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
	return
}

// checkInitialized parse epoch record string and returns followings:
//   - initialized: initialization flag '>' or '&' was found
//   - numSkip    : number of lines to skip
//     numSkip = 0: no special event
//     numSkip > 0: special records follow
func checkInitialized(epochStr string) (initialized bool, specialEventFound bool, numSkip int, err error) {

	switch {
	case strings.HasPrefix(epochStr, ">"):
		// found initialization flag for crinex ver 3.0
		initialized = true

		// error check
		if len(epochStr) < 35 {
			err = fmt.Errorf("%w: too short initialization epochstr '%s'", ErrInvalidEpochStr, epochStr)
			return
		}

		epochFlag := epochStr[31]

		// check special event
		if epochFlag > '1' {
			numSkip, err = strconv.Atoi(strings.TrimSpace(string(epochStr[32:35])))
			specialEventFound = true
			if err != nil {
				err = fmt.Errorf("%w: failed to parse numSkip '%s': %s", ErrInvalidEpochStr, epochStr[32:35], err.Error())
				return
			}
		}

		return
	case strings.HasPrefix(epochStr, "&"):
		// found initialization flag for crinex ver 1.0
		initialized = true

		// error check
		if len(epochStr) < 32 {
			err = fmt.Errorf("%w: too short initialization epochstr '%s'", ErrInvalidEpochStr, epochStr)
			return
		}

		epochFlag := epochStr[28]

		// check special event
		if epochFlag > '1' {
			numSkip, err = strconv.Atoi(strings.TrimSpace(string(epochStr[29:32])))
			specialEventFound = true
			if err != nil {
				if len(epochStr) < 35 {
					err = fmt.Errorf("%w: failed to parse numSkip: numSkip not found: %s", ErrInvalidEpochStr, err.Error())
					return
				}

				err = fmt.Errorf("%w: failed to parse numSkip '%s': %s", ErrInvalidEpochStr, epochStr[32:35], err.Error())
				return
			}
		}

		return
	}

	// no initalization flag found
	return false, false, 0, nil
}

// updateEpochRec parses the epochStr and update s.epochRec.
// Special events are skipped until a new initialization flag found.
// If any error is found, the record is skipped to next epoch header that is correctly formatted.
func (s *Scanner) updateEpochRec(epochStr string) error {
	var (
		initFlagFound     bool
		specialEventFound bool
		numSkip           int
		err               error
	)

	// check epochStr, and skip invalid epochs or special event
	for {
		initFlagFound, specialEventFound, numSkip, err = checkInitialized(epochStr)

		if err != nil {
			// invalid epoch record found, and try to recover to the correct epoch head
			return fmt.Errorf("%w: %s", ErrInvalidEpochStr, err.Error())
		}

		if specialEventFound {
			// special event found, skip numSkip lines
			for i := 0; i < numSkip; i++ {
				if ok := s.Scan(); !ok {
					err = s.s.Err()
					if err != nil {
						return err
					}
					return io.EOF
				}
			}

			// get new epochStr, and continue to check epochStr
			if ok := s.Scan(); !ok {
				err = s.s.Err()
				if err != nil {
					return err
				}
				return io.EOF
			}
			epochStr = s.s.Text()

			// check initialized that is required after a special event
			if !strings.HasPrefix(epochStr, "&") && !strings.HasPrefix(epochStr, ">") {
				return fmt.Errorf("%w: %s", ErrInvalidEpochStr, "not initialized after a special event")
			}
		} else {
			// correct epoch record and break
			break
		}
	}

	// update epochRec
	if initFlagFound {
		// initalization flag found and initialize differenciated data
		s.epochRec.buf = []byte(epochStr)
		s.data = make(map[string]satDataRecord)
	} else {
		// no initialization flag
		if err = s.epochRec.Decode(epochStr); err != nil {
			return err
		}
	}

	// epochRec was updated successfully, and update timetag
	if s.epoch, err = epochRecBytestoTime(s.epochRec.Bytes(), s.ver); err != nil {
		return err
	}

	return nil
}

// scanEpoch reads crinex data for an epoch,
// and updates p.epochRec, p.clk, p.satList and p.data.
// The s.scanner must be at the epoch record before the call.
//
// The epoch record string is required as the argument
// because the first line may be corrected if error is encountered.
func (s *Scanner) scanEpoch(epochStr string) error {
	var (
		clockBytes   []byte
		picoSecBytes []byte
		scanOK       bool
	)

	ver := s.ver           // version of hatanakaRINEX (not RINEX)
	obsTypes := s.obsTypes // obstypes is identical in a file

	// In Hatanaka RINEX format, data for each epoch consists of three parts:
	// (1) epoch record
	// (2) clock offset
	// (3) observation data
	//
	// The Hatanaka compressed file contains differenced string for (1) epoch
	// record, and differential values for (2) clock offset and (3) observation
	// data. Those data are stored in *Scanner struct and updated every epoch.
	//
	// If the epoch record starts with a initialization flag ('>' for
	// ver3.0, '&' for ver1.0), the stored data are initialized.

	// Update of (1) epoch record
	if err := s.updateEpochRec(epochStr); err != nil {
		return err
	}
	s.epochLineNum = s.lineNum

	// Update of (2) clock offset (reference and differenced values) & pico-second part of the epoch (stored as string)
	if scanOK = s.Scan(); !scanOK {
		err := s.s.Err()
		if err != nil {
			return err
		}
		return io.EOF
	}
	s.clockLineNum = s.lineNum

	sep := []byte{' '}                        // separator " " (1 space)
	vals := bytes.SplitN(s.s.Bytes(), sep, 2) // receiver clock offset & pico-second part of the epoch

	clockBytes = vals[0]
	if err := s.clk.Decode(clockBytes); err != nil {
		return err
	}

	// if Hatanaka RINEX version >= 3.1, decode the optional pico-second record.
	if ver >= "3.1" && len(vals) >= 2 {
		picoSecBytes = vals[1]
		if err := s.picoSec.Decode(string(picoSecBytes)); err != nil {
			return err
		}
	}

	// Update of (3) observation data
	// Get and update the satellite list for current epoch
	satList, warns, err := getSatListWithCorrection(s.epochRec.Bytes(), ver, s.epochLineNum)
	if err != nil {
		return err
	}

	s.satList = satList
	if warns.Len() > 0 {
		s.Warnings = append(s.Warnings, warns...)
	}

	// read data block
	var numValidSat int
	//for i, satId := range s.satList {
	for _, satId := range s.satList {
		satSys := satId[:1]

		// check if satId is valid
		if slices.Contains(VALID_SATSYS, satSys) && !strings.HasSuffix(satId, " ") {
			// valid satellite
			numValidSat++
		} else {
			s.Warnings.Add(s.epochLineNum, fmt.Sprintf("ignored invalid satellite: sat='%s'", satId))
			continue
		}

		// scan one line
		if scanOK = s.Scan(); !scanOK {
			err := s.s.Err()
			if err != nil {
				return err
			}

			// in the case of EOF, finalize the stored data
			if numValidSat > 0 {
				s.changeNumSatellites(numValidSat)
			}
			return io.EOF
		}
		t := s.s.Text()

		if _, ok := obsTypes[satSys]; !ok {
			switch ver {
			case "3.0", "3.1":
				// A mismatch between obstypes in header and data found.
				// This may not be a reliable method, but infers the number of observation
				// types from the number of fields in the line. Dummy obsCodes is
				// also stored in s.obsTypes here.
				//
				// Note that this method can only be used for the initialization line for crinex >= 3.0,
				// which is usually the case for the first satellite data found in
				// the file.
				s.Warnings.Add(s.lineNum, fmt.Sprintf("satsys not included in obstypes found: sat='%s'", satSys))

				n := strings.Count(strings.TrimRight(t, " "), " ") // number of data = number of spaces in the initialization line
				s.obsTypes[satSys] = make([]string, n)
				obsTypes = s.obsTypes
			default:
				// There is no way to recover.
				return fmt.Errorf("unknown satellite found: line='%d', sat='%s'", s.lineNum, satSys)
			}
		}
		obsCodes := obsTypes[satSys]
		vals := strings.SplitN(t, " ", len(obsCodes)+1)

		// allocate for new sat
		if _, ok := s.data[satId]; !ok {
			s.data[satId] = NewSatDataRecord(obsCodes)
		}

		// Update code and phase data
		for j := range obsCodes {
			// get pointer to the current data for convenience
			dj := &s.data[satId].data[j]

			if len(vals)-1 < j {
				// case 3: missing data
				dj.missing = true
				continue
			}

			// update one value
			b := []byte(vals[j])
			if err := dj.Decode(b); err != nil {
				return err
			}

			// initialize arc
			if ver == "1.0" && len(b) > 1 && b[1] == '&' {
				s.data[satId].lli[j].buf[0] = ' '
				s.data[satId].ss[j].buf[0] = ' '
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

			// update LLI and SS
			for j := range obsCodes {
				s.data[satId].lli[j].Decode(string(b[j*2]))
				s.data[satId].ss[j].Decode(string(b[j*2+1]))
			}
		}
	}

	return nil
}

// changeNumSatellites modifies the number of satellites.
// This function should only be used to shutdown scanner
// when the record is interuppted.
func (s *Scanner) changeNumSatellites(i int) {
	if i > 999 || i < 0 {
		return
	}

	if len(s.satList) > i {
		s.satList = s.satList[:i]
	}

	num := []byte(fmt.Sprintf("%3d", i))

	switch s.ver {
	case "3.0", "3.1":
		if len(s.epochRec.buf) < 35 {
			return
		}
		s.epochRec.buf[32], s.epochRec.buf[33], s.epochRec.buf[34] = num[0], num[1], num[2]
	case "1.0":
		if len(s.epochRec.buf) < 32 {
			return
		}
		s.epochRec.buf[29], s.epochRec.buf[30], s.epochRec.buf[31] = num[0], num[1], num[2]

		// trim satellite list
		if len(s.epochRec.buf) > 32+3*i {
			s.epochRec.buf = s.epochRec.buf[:32+3*i]
		}
	}
}

func (s *Scanner) Err() error {
	return s.err
}

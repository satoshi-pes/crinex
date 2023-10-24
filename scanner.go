package crinex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
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

	// real values for easier access
	epoch   time.Time
	satList []string // list of satellites in the current epoch

	// file reader and scanner
	r *io.Reader
	s *bufio.Scanner

	// error
	err error
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
	var s Scanner
	var err error

	// setup scanner and get the version of Hatanaka RINEX
	// Note: RINEX header contents have not parsed at this point
	s.r = &r
	s.s, s.ver, err = setup(r)

	s.obsTypes = make(map[string][]string)

	return &s, err
}

// ParseHeader parses the header, stores header contents and obstypes to
// s.header and s.obsTypes, and advance reader position to the head of
// the first data block.
func (s *Scanner) ParseHeader() (err error) {
	s.obsTypes, s.header, err = scanHeader(s.s)
	return err
}

// ScanEpoch reads Hatanaka compressed data for an epoch and
// set decoded values. Returns true if scan is successful.
// In the case the scan failed, the error is stored in s.err.
// Returns true for io.EOF.
func (s *Scanner) ScanEpoch() bool {
	if s.header == nil {
		var err error
		s.obsTypes, s.header, err = scanHeader(s.s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: failed to parse header. %s\n", err.Error())
			return false
		}
	}

	// scan next data block and update data
	if ok := s.s.Scan(); !ok {
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
		// seek new epoch record identifier to recover

		// Check for an initialization flag '>' exists in the middle of the line.
		// This only supports ver 3.0 because the initialization flag '&' for ver 1.0 is
		// indistinguishable from the the differentiation flag.
		if i := strings.Index(epochStr, ">"); i > 0 {
			// found an initialization flag
			fmt.Fprintf(os.Stderr, "epochrec modified: '%s'\n", epochStr)

			epochStr = epochStr[i:]
			goto RETRY_SCAN_EPOCH
		}

		// Search for the next initialization flag
		for s.s.Scan() {
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
	if s.ver == "3.0" {
		if s.clk.missing {
			return []byte(fmt.Sprintf("%-35.35s\n", s.epochRec.StringRINEX()))
		} else {
			return []byte(fmt.Sprintf("%-35.35s      %15.12f\n", s.epochRec.StringRINEX(), s.ClockOffset()))
		}
	} else if s.ver == "1.0" {
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

	if s.ver == "3.0" {
		return float64(s.clk.refData) * 0.000000000001
	} else if s.ver == "1.0" {
		return float64(s.clk.refData) * 0.000000001
	}

	return // nan
}

// Data returns decompressed RINEX data
func (s *Scanner) Data() (obs []SatObss) {
	obs = make([]SatObss, len(s.satList))

	switch s.ver {
	case "1.0", "3.0":
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
	if s.ver == "3.0" {
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
	} else if s.ver == "1.0" {
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
func checkInitialized(epochStr string) (initialized bool, numSkip int, err error) {

	if strings.HasPrefix(epochStr, ">") {
		// found initialization flag for crinex ver 3.0
		initialized = true

		// error check
		if len(epochStr) < 35 {
			err = fmt.Errorf("%w: too short initialization epochstr '%s'", ErrInvalidEpochStr, epochStr)
			return
		}

		// check special event
		if epochStr[31] > '1' {
			numSkip, err = strconv.Atoi(strings.TrimSpace(string(epochStr[32:35])))
			if err != nil {
				err = fmt.Errorf("%w: failed to parse numSkip '%s': %s", ErrInvalidEpochStr, epochStr[32:35], err.Error())
				return
			}
		}

		return
	} else if strings.HasPrefix(epochStr, "&") {
		// found initialization flag for crinex ver 1.0
		initialized = true

		// error check
		if len(epochStr) < 32 {
			err = fmt.Errorf("%w: too short initialization epochstr '%s'", ErrInvalidEpochStr, epochStr)
			return
		}

		// check special event
		if epochStr[28] > '1' {
			numSkip, err = strconv.Atoi(strings.TrimSpace(string(epochStr[29:32])))
			if err != nil {
				err = fmt.Errorf("%w: failed to parse numSkip '%s': %s", ErrInvalidEpochStr, epochStr[32:35], err.Error())
				return
			}
		}

		return
	}

	// no initalization flag found
	return false, 0, nil
}

// updateEpochRec parses the epochStr and update s.epochRec.
// Special events are skipped until a new initialization flag found.
// If any error is found, the record is skipped to next epoch header that is correctly formatted.
func (s *Scanner) updateEpochRec(epochStr string) error {
	var (
		initFlagFound bool
		numSkip       int
		err           error
	)

	// check epochStr, and skip invalid epochs or special event
	for {
		initFlagFound, numSkip, err = checkInitialized(epochStr)

		if err != nil {
			// invalid epoch record found, and try to recover to the correct epoch head
			return fmt.Errorf("%w: %s", ErrInvalidEpochStr, err.Error())
		} else if numSkip > 0 {
			// special event found, skip numSkip lines
			for i := 0; i < numSkip; i++ {
				if ok := s.s.Scan(); !ok {
					err = s.s.Err()
					if err != nil {
						return err
					}
					return io.EOF
				}
			}

			// get new epochStr, and continue to check epochStr
			if ok := s.s.Scan(); !ok {
				err = s.s.Err()
				if err != nil {
					return err
				}
				return io.EOF
			}
			epochStr = s.s.Text()
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
		clockBytes []byte
		scanOK     bool
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

	// Update of (2) clock offset (reference and differenced values)
	if scanOK = s.s.Scan(); !scanOK {
		err := s.s.Err()
		if err != nil {
			return err
		}
		return io.EOF
	}
	clockBytes = s.s.Bytes()
	if err := s.clk.Decode([]byte(clockBytes)); err != nil {
		return err
	}

	// Update of (3) observation data
	// Get and update the satellite list for current epoch
	if ver == "3.0" {
		s.satList = getSatList(s.epochRec.Bytes())
	} else if ver == "1.0" {
		s.satList = getSatListV1(s.epochRec.Bytes())
	}

	// read data block
	//for _, satId := range s.satList {
	for i, satId := range s.satList {
		satSys := satId[:1]
		obsCodes := obsTypes[satSys]

		// scan one line
		if scanOK = s.s.Scan(); !scanOK {
			err := s.s.Err()
			if err != nil {
				return err
			}

			// in the case of EOF, finalize the stored data
			if i > 0 {
				s.changeNumSatellites(i)
			}
			return io.EOF
		}
		t := s.s.Text()

		vals := strings.SplitN(t, " ", len(obsCodes)+1)

		// allocate for new sat
		if _, ok := s.data[satId]; !ok {
			if ver == "3.0" {
				s.data[satId] = NewSatDataRecord(obsCodes)
			} else if ver == "1.0" {
				s.data[satId] = NewSatDataRecordV1(obsCodes)
			}
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

	if s.ver == "3.0" {
		if len(s.epochRec.buf) < 35 {
			return
		}
		s.epochRec.buf[32], s.epochRec.buf[33], s.epochRec.buf[34] = num[0], num[1], num[2]
	} else if s.ver == "1.0" {
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

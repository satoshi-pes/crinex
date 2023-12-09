package crinex

// valid satellite systems (" " denotes GPS)
var VALID_SATSYS = []string{" ", "G", "R", "E", "J", "C", "I", "S"}

const (
	OFFSET_NUMSAT_V3 int = 32 // offset bytes to number of satellite (crx v3.0)
	OFFSET_SATLST_V3 int = 41 // offset bytes to satellite list (crx v3.0)
	OFFSET_NUMSAT_V1 int = 29 // offset bytes to number of satellite (crx v1.0)
	OFFSET_SATLST_V1 int = 32 // offset bytes to satellite list (crx v1.0)
)

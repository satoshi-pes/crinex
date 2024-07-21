package crinex

import "fmt"

// Warning stores a warning message and the line number where the warning raised.
type Warning struct {
	Pos int
	Msg string
}

func (w Warning) String() string {
	if w.Pos > 0 {
		return fmt.Sprintf("pos:%d, msg:%s", w.Pos, w.Msg)
	}
	return w.Msg
}

// WarningList is a list of *Warning.
type WarningList []*Warning

// Add adds an [Error] with given position and error message to an [ErrorList].
func (p *WarningList) Add(pos int, msg string) {
	*p = append(*p, &Warning{pos, msg})
}

// Reset resets the WarningList to empty.
func (p *WarningList) Reset() { *p = (*p)[0:0] }

// Len returns the number of the Warnings.
func (p *WarningList) Len() int { return len(*p) }

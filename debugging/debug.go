package debugging

import (
	"log"
	"os"
)

type Debugging bool

// flip to false to deactivate debug output
var Debug Debugging

func (d Debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf("DEBUG >>  "+format, args...)
	}
}

func init() {
	hfDbg := os.Getenv("DEBUG")
	if hfDbg != "" {
		// the env var DEBUG is actually defined
		Debug = true
	}
}

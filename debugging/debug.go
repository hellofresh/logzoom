package debugging

import (
	"log"
	"os"

	"github.com/fatih/color"
)

type Debugging bool

var Debug Debugging

// Colored DEBUG output for easy spotting.
// Reference: https://github.com/fatih/color#insert-into-noncolor-strings-sprintfunc
var yellowBold func(a ...interface{}) string

func (d Debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(yellowBold("DEBUG >>  ")+format, args...)
	}
}

// Activate debug output based on `DEBUG` environment variable.
func init() {
	hfDbg := os.Getenv("DEBUG")
	if hfDbg != "" {
		// the env var DEBUG is actually defined
		Debug = true
		yellowBold = color.New(color.Bold, color.FgYellow).SprintFunc()
	}
}

package filebeat

import (
	"github.com/hellofresh/logzoom/input"
)

func init() {
	input.Register("filebeat", New)
}

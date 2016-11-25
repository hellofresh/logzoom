package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hellofresh/elastic"
	"github.com/hellofresh/logzoom/buffer"
	. "github.com/hellofresh/logzoom/debugging"
	"github.com/hellofresh/logzoom/output"
	"github.com/hellofresh/logzoom/route"
	"github.com/paulbellamy/ratecounter"
	"github.com/peterbourgon/g2s"
	"gopkg.in/yaml.v2"
)

const (
	defaultHost        = "127.0.0.1"
	defaultIndexPrefix = "logstash"
	esFlushInterval    = 5
	esMaxConns         = 20
	esRecvBuffer       = 100
	esSendBuffer       = 100
)

type Indexer struct {
	bulkService       *elastic.BulkService
	indexPrefix       string
	indexType         string
	RateCounter       *ratecounter.RateCounter
	lastDisplayUpdate time.Time
}

type Config struct {
	Hosts           []string `yaml:"hosts"`
	IndexPrefix     string   `yaml:"index"`
	IndexType       string   `yaml:"index_type"`
	Timeout         int      `yaml:"timeout"`
	GzipEnabled     bool     `yaml:"gzip_enabled"`
	InfoLogEnabled  bool     `yaml:"info_log_enabled"`
	ErrorLogEnabled bool     `yaml:"error_log_enabled"`
}

type ESServer struct {
	name   string
	fields map[string]string
	config Config
	host   string
	hosts  []string
	b      buffer.Sender
	term   chan bool
}

// metrics publishing
var statPub *StatsDCntPublisher
var configuredESHost string

type StatsDCntPublisher struct {
	Client  *g2s.Statsd
	enabled bool
	prefix  string
}

// Depending on the value of 'addr' - which is taken from the environment variable "STATSD_ADDRESS" - we create
// a functional or "dummy" instance of StatsDCntPublisher.
func NewStatsDCntPublisher(proto, addr, prefix string) *StatsDCntPublisher {
	sdm := StatsDCntPublisher{
		Client:  nil,
		enabled: false,
		prefix:  prefix,
	}
	switch addr {
	case "unset":
		return &sdm
	default:
		statsdClnt, err := g2s.Dial(proto, addr)
		if err != nil {
			log.Println("Unable to connect to StatsD at defined address '%s'.\n", addr)
		}
		log.Printf("Successfully connected to StatsD at defined address '%s'.\n", addr)
		sdm.Client = statsdClnt
		sdm.enabled = true
	}
	return &sdm
}

func (p *StatsDCntPublisher) Cntr(name string, val int) {
	if p.enabled {
		p.Client.Counter(1.0, p.prefix+name, val)
	}
}

func init() {
	output.Register("elasticsearch", New)

	// metrics publishing
	statsDAddrFromEnv := os.Getenv("STATSD_ADDRESS")
	if statsDAddrFromEnv == "" {
		statPub = NewStatsDCntPublisher("udp", "unset", "logcollector.")
	} else {
		statPub = NewStatsDCntPublisher("udp", statsDAddrFromEnv, "logcollector.")
	}

}

func New() output.Output {
	return &ESServer{
		host: fmt.Sprintf("%s:%d", defaultHost, time.Now().Unix()),
		term: make(chan bool, 1),
	}
}

// Dummy discard, satisfies io.Writer without importing io or os.
type DevNull struct{}

func (DevNull) Write(p []byte) (int, error) {
	return len(p), nil
}

func indexName(idx string) string {
	if len(idx) == 0 {
		idx = defaultIndexPrefix
	}

	return fmt.Sprintf("%s-%s", idx, time.Now().Format("2006.01.02"))
}

// Helper function to extract ElasticSearch host for metrics publishing.
func extractESHostPrfx(esHosts []string) string {
	fstEsHost := esHosts[0]
	inpWoSchema := strings.Split(fstEsHost, "//")[1]
	fqdn := strings.Split(inpWoSchema, ":")[0]
	return strings.Split(fqdn, ".")[0]
}

// Helper function to print bulk responses for debugging purposes.
func printBulkResponse(r *elastic.BulkResponse) {
	Debug.Printf("------------------------- bulk response ::BEGIN -------------------------")
	Debug.Printf("blkRsp:\n")
	Debug.Printf("...  Took: %d\n", r.Took)
	Debug.Printf("...  Errors: %t\n", r.Errors)
	Debug.Printf("...  Num of items: %d\n", len(r.Items))
	Debug.Printf("...  Failed items   : %d\n", len(r.Failed()))
	Debug.Printf("...  Succeeded items: %d\n", len(r.Succeeded()))

	// explode `Items  []map[string]*BulkResponseItem`
	for i, elm := range r.Items {
		Debug.Printf("......  item no %d\n", i)
		for key, val := range elm {
			Debug.Printf("......  key: '%s'\n", key)
			Debug.Printf("......  val: %#v\n", val)
		}
	}
	Debug.Printf("------------------------- bulk response ::END   -------------------------\n\n")
}

// Send metrics about messages saved in ElasticSearch (total/failed).
func publishESMsgMetrics(esHost string, total, lost int) {
	statPub.Cntr(fmt.Sprintf("%s.messages.total", esHost), total)
	statPub.Cntr(fmt.Sprintf("%s.messages.lost", esHost), lost)
}

func (i *Indexer) flush() error {
	numEvents := i.bulkService.NumberOfActions()

	if numEvents > 0 {
		if time.Now().Sub(i.lastDisplayUpdate) >= time.Duration(1*time.Second) {
			log.Printf("Flushing %d event(s) to Elasticsearch, current rate: %d/s", numEvents, i.RateCounter.Rate())
			i.lastDisplayUpdate = time.Now()
		}

		blkRsp, err := i.bulkService.Do()
		// debug output
		printBulkResponse(blkRsp)
		// metrics publishing
		publishESMsgMetrics(configuredESHost, len(blkRsp.Items), len(blkRsp.Failed()))

		if err != nil {
			log.Printf("Unable to flush events: %s", err)
		}

		return err
	}

	return nil
}

func (i *Indexer) index(ev *buffer.Event) error {
	doc := *ev.Text
	idx := indexName(i.indexPrefix)
	typ := i.indexType

	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkService.Add(request)
	i.RateCounter.Incr(1)

	numEvents := i.bulkService.NumberOfActions()

	if numEvents < esSendBuffer {
		return nil
	}

	return i.flush()
}

func (e *ESServer) ValidateConfig(config *Config) error {
	if len(config.Hosts) == 0 {
		return errors.New("Missing hosts")
	}

	if len(config.IndexPrefix) == 0 {
		return errors.New("Missing index prefix (e.g. logstash)")
	}

	if len(config.IndexType) == 0 {
		return errors.New("Missing index type (e.g. logstash)")
	}

	return nil
}

func (e *ESServer) Init(name string, config yaml.MapSlice, b buffer.Sender, route route.Route) error {
	var esConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &esConfig); err != nil {
		return fmt.Errorf("Error parsing elasticsearch config: %v", err)
	}

	e.name = name
	e.fields = route.Fields
	e.config = *esConfig
	e.hosts = esConfig.Hosts
	e.b = b

	// hack to expose ElasticSearch hosts for metrics publishing
	configuredESHost = extractESHostPrfx(e.hosts)

	return nil
}

func readInputChannel(idx *Indexer, receiveChan chan *buffer.Event) {
	// Drain the channel only if we have room
	if idx.bulkService.NumberOfActions() < esSendBuffer {
		select {
		case ev := <-receiveChan:
			idx.index(ev)
		}
	} else {
		log.Printf("Internal Elasticsearch buffer is full, waiting")
		time.Sleep(1 * time.Second)
	}
}

func (es *ESServer) insertIndexTemplate(client *elastic.Client) error {
	var template map[string]interface{}
	err := json.Unmarshal([]byte(IndexTemplate), &template)

	if err != nil {
		return err
	}

	template["template"] = es.config.IndexPrefix + "-*"

	inserter := elastic.NewIndicesPutTemplateService(client)
	inserter.Name(es.config.IndexPrefix)
	inserter.Create(true)
	inserter.BodyJson(template)

	response, err := inserter.Do()

	if response != nil {
		log.Println("Inserted template response:", response.Acknowledged)
	}

	return err
}

func (es *ESServer) Start() error {
	if es.b == nil {
		log.Printf("[%s] No Route is specified for this output", es.name)
		return nil
	}
	var client *elastic.Client
	var err error

	for {
		httpClient := http.DefaultClient
		timeout := 60 * time.Second

		if es.config.Timeout > 0 {
			timeout = time.Duration(es.config.Timeout) * time.Second
		}

		log.Println("Setting HTTP timeout to", timeout)
		log.Println("Setting GZIP enabled:", es.config.GzipEnabled)

		httpClient.Timeout = timeout

		var infoLogger, errorLogger *log.Logger

		if es.config.InfoLogEnabled {
			infoLogger = log.New(os.Stdout, "", log.LstdFlags)
		} else {
			infoLogger = log.New(new(DevNull), "", log.LstdFlags)
		}

		if es.config.ErrorLogEnabled {
			errorLogger = log.New(os.Stderr, "", log.LstdFlags)
		} else {
			errorLogger = log.New(new(DevNull), "", log.LstdFlags)
		}

		client, err = elastic.NewClient(elastic.SetURL(es.hosts...),
			elastic.SetHttpClient(httpClient),
			elastic.SetGzip(es.config.GzipEnabled),
			elastic.SetInfoLog(infoLogger),
			elastic.SetErrorLog(errorLogger))

		if err != nil {
			log.Printf("Error starting Elasticsearch: %s, will retry", err)
			time.Sleep(2 * time.Second)
			continue
		}

		es.insertIndexTemplate(client)

		break
	}

	log.Printf("Connected to Elasticsearch")

	service := elastic.NewBulkService(client)

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	// Create indexer
	idx := &Indexer{service, es.config.IndexPrefix, es.config.IndexType, rateCounter, time.Now()}

	// Loop events and publish to elasticsearch
	tick := time.NewTicker(time.Duration(esFlushInterval) * time.Second)

	for {
		readInputChannel(idx, receiveChan)

		if len(tick.C) > 0 || len(es.term) > 0 {
			select {
			case <-tick.C:
				idx.flush()
			case <-es.term:
				tick.Stop()
				log.Println("Elasticsearch received term signal")
				break
			}
		}
	}

	return nil
}

func (es *ESServer) Stop() error {
	es.term <- true
	return nil
}

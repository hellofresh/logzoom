package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	//	"golang.org/x/net/context"

	"strings"

	"github.com/hellofresh/elastic"
	"github.com/hellofresh/logzoom/buffer"
	. "github.com/hellofresh/logzoom/debugging"
	"github.com/hellofresh/logzoom/output"
	"github.com/hellofresh/logzoom/route"
	"github.com/paulbellamy/ratecounter"
	"github.com/peterbourgon/g2s"
	"gopkg.in/yaml.v2"
)

// StatsD metrics publishing.
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

// Convenience function for metrics publishing.
func (p *StatsDCntPublisher) Cntr(name string, val int) {
	if p.enabled {
		p.Client.Counter(1.0, p.prefix+name, val)
	}
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
			// Do we have a non-nil elastic.ErrorDetails as part of elastic.BulkResponseItem?
			errDtls := val.Error
			if errDtls != nil {
				Debug.Printf("......  error details:\n")
				Debug.Printf(".........  CausedBy     : %v", errDtls.CausedBy)
				Debug.Printf(".........  FailedShards : %v", errDtls.FailedShards)
				Debug.Printf(".........  Grouped      : %t", errDtls.Grouped)
				Debug.Printf(".........  Index        : %s", errDtls.Index)
				Debug.Printf(".........  Phase        : %s", errDtls.Phase)
				Debug.Printf(".........  Reason       : %s", errDtls.Reason)
				Debug.Printf(".........  ResourceId   : %s", errDtls.ResourceId)
				Debug.Printf(".........  ResourceType : %s", errDtls.ResourceType)
				Debug.Printf(".........  RootCause    : %v", errDtls.RootCause)
				Debug.Printf(".........  Type         : %s", errDtls.Type)
			}
		}
	}
	Debug.Printf("------------------------- bulk response ::END   -------------------------\n\n")
}

// Send metrics about messages saved in ElasticSearch (total/failed).
func publishESMsgMetrics(esHost string, blkRsp *elastic.BulkResponse) {
	statPub.Cntr(fmt.Sprintf("%s.messages.succeeded", esHost), len(blkRsp.Succeeded()))
	// Publish each failed message as it's own error type metric as indicated by ElasticSearch.
	for _, fItmVal := range blkRsp.Failed() {
		statPub.Cntr(fmt.Sprintf("%s.messages.failed.%s", esHost, fItmVal.Error.Type), 1)
	}

}

const (
	defaultHost        = "127.0.0.1"
	defaultIndexPrefix = "logstash"
	esFlushInterval    = 10
	esRecvBuffer       = 10000
	esSendBuffer       = 10000
	esWorker           = 20
	esBulkLimit        = 10000
)

type Indexer struct {
	bulkProcessor     *elastic.BulkProcessor
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
	idx    *Indexer
}

func init() {
	output.Register("elasticsearch", New)

	// StatsD metrics publishing.
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

func (i *Indexer) index(ev *buffer.Event) error {
	doc := *ev.Text
	idx := indexName(i.indexPrefix)
	typ := i.indexType

	request := elastic.NewBulkIndexRequest().Index(idx).Type(typ).Doc(doc)
	i.bulkProcessor.Add(request)
	i.RateCounter.Incr(1)

	return nil
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

	// Hack to expose ElasticSearch hosts for metrics publishing.
	configuredESHost = extractESHostPrfx(e.hosts)

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

	// Hack to expose the ElasticSearch host for metrics publishing.
	configuredESHost = extractESHostPrfx(e.hosts)

	return nil
}

func readInputChannel(idx *Indexer, receiveChan chan *buffer.Event) {
	select {
	case ev := <-receiveChan:
		idx.index(ev)
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

	//	response, err := inserter.Do(context.Background())
	response, err := inserter.Do()

	if response != nil {
		log.Println("Inserted template response:", response.Acknowledged)
	}

	return err
}

func (es *ESServer) afterCommit(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if es.idx.RateCounter.Rate() > 0 {
		log.Printf("Flushed events to Elasticsearch, current rate: %d/s", es.idx.RateCounter.Rate())
	}
	if response != nil {
		// Display debug output if DEBUG env var is set.
		printBulkResponse(response)
		// Publish metrics if STATSD_ADDRESS env var is set.
		publishESMsgMetrics(configuredESHost, response)
	}
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

	// Add the client as a subscriber
	receiveChan := make(chan *buffer.Event, esRecvBuffer)
	es.b.AddSubscriber(es.host, receiveChan)
	defer es.b.DelSubscriber(es.host)

	rateCounter := ratecounter.NewRateCounter(1 * time.Second)

	// Create bulk processor
	bulkProcessor, err := client.BulkProcessor().
		After(es.afterCommit).                        // Function to call after commit
		Workers(esWorker).                            // # of workers
		BulkActions(esBulkLimit).                     // # of queued requests before committed
		BulkSize(7 * 1024 * 1024).                    // Limit Bulk requests to 7 MB (AWS ES request size limit is 10MB)
		FlushInterval(esFlushInterval * time.Second). // autocommit every # seconds
		Stats(true).                                  // gather statistics
		Do()

	if err != nil {
		log.Println(err)
	}

	idx := &Indexer{bulkProcessor, es.config.IndexPrefix, es.config.IndexType, rateCounter, time.Now()}
	es.idx = idx

	for {
		readInputChannel(idx, receiveChan)

		if len(es.term) > 0 {
			select {
			case <-es.term:
				log.Println("Elasticsearch received term signal")
				break
			}
		}
	}

	log.Println("Shutting down. Flushing existing events.")
	defer bulkProcessor.Close()
	return nil
}

func (es *ESServer) Stop() error {
	es.term <- true
	return nil
}

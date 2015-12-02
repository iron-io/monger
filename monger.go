package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/beevik/ntp"
	"github.com/influxdb/influxdb/client"
	"github.com/iron-io/iron_go3/api"
	"github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	body string

	qname    = flag.String("basename", "test", "enter the base name for a queue, to be appended w/ #, i.e. queue0, queue1, ...")
	bodyLen  = flag.Int("body-size", 1024, "body size in bytes")
	messages = flag.Int("messages", 1024, "number of messages to enqueue")
	threads  = flag.Int("threads", 1, "number of threads in aggregate")
	nq       = flag.Int("queues", 1, "number of queues to run (best result: divisible by #threads)")
	n        = flag.Int("n", 1, "batching factor")
	wat      = flag.String("wat", "prod", `what to do, pick one: { "prod", "cons" }`)

	// for config
	project = flag.String("p", "", "project_id to use")
	token   = flag.String("t", "", "what your token be")
	host    = flag.String("host", "", "host to shoot things at")
	scheme  = flag.String("scheme", "http", "how secure should your shots be")
	port    = flag.Int("port", 80, "really, where is it")

	influxHost     = flag.String("influx-host", "", "influx to send stuffs to")
	influxTag      = flag.String("influx-tag", "", "cluster tag to report to influx with")
	influxInterval = flag.Float64("influx-interval", 1, "how often in seconds to report to influx (float)")
	influxTimeout  = flag.Float64("influx-timeout", 10, "how long for influx to retort")
	influxDB       = flag.String("influx-db", "", "db name to report to")
	influxUsername = flag.String("influx-username", "", "username for influx db")
	influxPassword = flag.String("influx-password", "", "password for username for influx db")

	ntpdelta   time.Duration // b/c clock sync
	conf       *config.Settings
	influxConf *InfluxConfig
)

func now() time.Time { return time.Now().Add(-ntpdelta) }

func init() {
	err := errors.New("bogo")
	var ntpnow time.Time
	for err != nil {
		ntpnow, err = ntp.Time("pool.ntp.org")
		log15.Crit("sorry dude, late for work", "err", err)
	}
	ntpdelta = time.Now().Sub(ntpnow)

	flag.Parse()
	if payload := os.Getenv("PAYLOAD_FILE"); payload != "" { // running on iw
		jason := struct {
			Basename *string `json:"basename"`
			BodySize *int    `json:"body_size"`
			Messages *int    `json:"messages"`
			Threads  *int    `json:"threads"`
			Queues   *int    `json:"queues"`
			N        *int    `json:"n"` // batch size
			Wat      *string `json:"wat"`
			// iron stuff
			Project *string `json:"project_id"`
			Token   *string `json:"token"`
			Host    *string `json:"host"`
			Scheme  *string `json:"scheme"`
			Port    *int    `json:"port"`
			// influx stuff
			*InfluxConfig
		}{
			Basename:     qname,
			BodySize:     bodyLen,
			Messages:     messages,
			Threads:      threads,
			Queues:       nq,
			Project:      project,
			Token:        token,
			Host:         host,
			Scheme:       scheme,
			Port:         port,
			N:            n,
			Wat:          wat,
			InfluxConfig: influxConf,
		}

		f, err := os.Open(payload)
		if err != nil {
			log15.Error("you're screwed", "err", err)
			os.Exit(1)
		}
		if err := json.NewDecoder(f).Decode(&jason); err != nil {
			log15.Error("you're done for", "err", err)
			os.Exit(1)
		}
		influxConf = jason.InfluxConfig
	}

	if *influxHost != "" {
		influxConf = &InfluxConfig{
			InfluxHost: *influxHost,
			Tag:        *influxTag,
			Interval:   *influxInterval,
			Timeout:    *influxTimeout,
			Database:   *influxDB,
			Username:   *influxUsername,
			Password:   *influxPassword,
		}
	}

	if *project == "" { // this is a good sign anyway
		c := config.Config("iron_mq")
		conf = &c
	} else {
		conf = &config.Settings{
			Token:     *token,
			ProjectId: *project,
			Host:      *host,
			Scheme:    *scheme,
			Port:      uint16(*port),
		}
	}

	bod := make([]byte, *bodyLen)
	rand.Read(bod)
	body = string(bod)

	api.HttpClient.Timeout = 30 * time.Second
}

func main() {
	var reporter Reporter = LogReporter{}
	fmt.Println("starting monger")
	if influxConf != nil {
		reporter = NewInfluxReporter(influxConf)
	}

	msgs := make(chan struct{})
	go func() {
		for i := 0; i < *messages/(*n); i++ {
			msgs <- struct{}{}
		}
		close(msgs)
	}()

	f := func(q mq.Queue) { prod(reporter, msgs, q) }
	if *wat == "cons" {
		f = func(q mq.Queue) { cons(reporter, msgs, q) }
	}

	var wait sync.WaitGroup
	wait.Add(*threads)
	for i := 0; i < *threads; i++ {
		name := *qname
		if *nq > 1 {
			name = name + strconv.Itoa(i%*nq)
		}
		go func(name string) {
			f(mq.ConfigNew(name, conf))
			wait.Done()
		}(name)
	}
	wait.Wait()
}

func cons(reporter Reporter, msgs <-chan struct{}, q mq.Queue) {
	for _ = range msgs {
		start := now()
		messages, err := q.LongPoll(*n, 0, 0, false)
		fin := now()

		var bytes int
		for _, m := range messages {
			bytes += len(m.Body)
		}
		reporter.Add(q.Name, conf.ProjectId, "deq", len(messages), bytes, fin.Sub(start), fin)
		if err != nil {
			log15.Error("stats", "op", "deq", "queue", q.Name, "start", start, "fin", fin, "err", err)
		}
		if len(messages) == 0 {
			continue
		}

		start = now()
		err = q.DeleteReservedMessages(messages)
		fin = now()
		numdel := len(messages) // show failed deletes as a request with 0 msgs deleted
		if err != nil {
			log15.Error("error pushing messages", "op", "del", "queue", q.Name, "start", start, "fin", fin, "err", err)
			numdel = 0
		}
		reporter.Add(q.Name, conf.ProjectId, "del", numdel, 0, fin.Sub(start), fin)
	}
}

func prod(reporter Reporter, msgs <-chan struct{}, q mq.Queue) {
	// just for counting, don't want to copy string every time
	messages := make([]mq.Message, *n)
	for i := range messages {
		messages[i].Body = body
	}

	for range msgs {
		start := now()
		ids, err := q.PushMessages(messages...)
		fin := now()

		if err != nil {
			log15.Error("error pushing messages", "op", "enq", "queue", q.Name, "err", err)
			time.Sleep(500 * time.Millisecond)
		}
		reporter.Add(q.Name, conf.ProjectId, "enq", len(ids), len(ids)*len(body), fin.Sub(start), fin)
	}
}

type InfluxConfig struct {
	InfluxHost string  `json:"influx_host"`
	Tag        string  `json:"influx_tag"`
	Interval   float64 `json:"influx_interval"`
	Timeout    float64 `json:"influx_timeout"`
	Database   string  `json:"influx_db"`
	Username   string  `json:"influx_username"`
	Password   string  `json:"influx_password"`
}

type LogReporter struct{}

func (LogReporter) Add(q, pid, op string, messages, bytes int, latency time.Duration, now time.Time) {
	log15.Info("stats", "op", op, "queue", q, "project_id", pid, "latency_ms", latency)
}

type Reporter interface {
	Add(q, pid, op string, messages, bytes int, latency time.Duration, now time.Time)
}

type InfluxReporter struct {
	sync.Mutex // protects points
	points     []client.Point

	client  *client.Client
	tag, db string
}

// localhost:8086
func NewInfluxReporter(config *InfluxConfig) *InfluxReporter {
	h, err := url.Parse("http://" + config.InfluxHost)
	if err != nil {
		log15.Crit("error parsing influx url", "err", err)
		os.Exit(1)
	}

	interval := time.Duration(config.Interval * float64(time.Second))
	if interval == 0 {
		interval = 1 * time.Second
	}
	timeout := time.Duration(config.Timeout * float64(time.Second))

	con, err := client.NewClient(client.Config{URL: *h, Timeout: timeout, Username: config.Username, Password: config.Password})
	if err != nil {
		log15.Crit("error talking to influx", "err", err)
		os.Exit(1)
	}

	if config.Database == "" {
		log15.Crit("must specify an influx db to talk to", "err", err)
		os.Exit(1)
	}

	// we should make sure this is an 'already exists' err, but ya know, patience..
	// won't overwrite if it does exist. if theres any other error then influx just won't work
	err = createDB(con, config.Database)
	if err != nil {
		log15.Warn("error creating db -- it's ok if it already exists", "err", err)
	}

	ir := &InfluxReporter{
		client: con,
		tag:    config.Tag,
		db:     config.Database,
	}
	go func() {
		for range time.Tick(interval) {
			ir.report()
		}
	}()
	return ir
}

// include 'now' to avoid scheduling conflicts, so we can call Add in a goroutine to
// sit and wait on our little mutex, but still have an accurate 'now'

// queue adder
func (ir *InfluxReporter) Add(q, pid, op string, messages, bytes int, latency time.Duration, now time.Time) {
	pt := client.Point{ // TODO dear god make the map allocs stop
		Measurement: "queues",
		Tags: map[string]string{
			"cluster":    ir.tag,
			"op":         op,
			"project_id": pid,
			"queue":      q,
		},
		Fields:    make(map[string]interface{}),
		Time:      now,
		Precision: "n", // ms maybe?
	}
	if latency > 0 {
		pt.Fields["latency"] = float64(latency) / float64(time.Millisecond) // micros also seems reasonable
	}
	if bytes > 0 {
		pt.Fields["bytes"] = bytes
	}
	if messages > 0 {
		pt.Fields["messages"] = messages
	}
	ir.Lock()
	ir.points = append(ir.points, pt)
	ir.Unlock()
}

func (ir *InfluxReporter) report() {
	// take a slice of our slice, hand back lock before talking on net (append only)
	ir.Lock()
	pts := ir.points
	ir.Unlock()

	if len(pts) == 0 {
		return
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        ir.db,
		RetentionPolicy: "default",
	}
	_, err := ir.client.Write(bps)
	if err != nil {
		// TODO do we want an upper bound on buffer size before we start throwing things away?
		log15.Warn("could not reach influx, trying again next interval", "err", err, "buf_size", len(pts))
	} else { // woot
		ir.Lock()
		// it may be possible we're leaking a slice here; we do want to reuse, but might want an upper bound
		ir.points = ir.points[len(pts):]
		ir.Unlock()
	}
}

// queryDB convenience function to query the database
func queryDB(con *client.Client, db, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Database: db,
		Command:  cmd,
	}
	if response, err := con.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	}
	return
}

func createDB(con *client.Client, dbname string) error {
	_, err := queryDB(con, dbname, fmt.Sprintf("create database %s", dbname))
	return err
}

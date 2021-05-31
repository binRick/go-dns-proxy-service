package server

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/miekg/dns"
)

type DNSProxyServer struct {
	Running bool
	Started time.Time

	Port  int
	Proto string

	Upstreams []string

	Questions   uint64
	Answers     uint64
	Rejections  uint64
	Shutdowns   uint64
	Startups    uint64
	Tests       uint64
	PassedTests uint64
	FailedTests uint64

	TestDomains []string

	mutex sync.RWMutex

	Server *dns.Server

	DebugMode bool
	Shutdown  chan bool
}

var (
	TEST_DOMAINS                = []string{`amazon.com`, `google.com`, `playboy.com`, `deloitte.com`, `yahoo.com`, `ups.com`, `espn.com`}
	MAX_WAIT_DURATION           = 100 * time.Millisecond
	UPSTREAM_TIMEOUT            = 150 * time.Millisecond
	TEST_WHILE_RUNNING_INTERVAL = 10 * time.Second
	DNS_PROXY_HOST              = `127.0.0.1`
)

const (
	DEBUG_MODE = true
)

func init() {
	rand.Seed(time.Now().Unix())
}

func (s *DNSProxyServer) UpstreamsQty() int {
	return len(s.Upstreams)
}

func (s *DNSProxyServer) New() *DNSProxyServer {
	if len(s.Upstreams) == 0 {
		log.Fatal("No Upstreams Specified")
	}

	dns_server := &dns.Server{
		Addr: fmt.Sprintf(`%s:%d`, ``, s.Port),
		Net:  s.Proto,
	}

	server := DNSProxyServer{
		Running:     false,
		Questions:   0,
		Answers:     0,
		Server:      dns_server,
		Port:        s.Port,
		Proto:       s.Proto,
		TestDomains: TEST_DOMAINS,
		Upstreams:   s.Upstreams,
		Shutdown:    s.Shutdown,
	}
	go server.MonitorSignals()
	go server.MonitorShutdown()
	go s.TestWhileRunning()

	return &server
}

func (s *DNSProxyServer) Stop() {
	go func() {
		s.Shutdown <- true
	}()
}

func (s *DNSProxyServer) StartServer() {
	go func() {
		log.Debugf("Starting %v\n", s.Server.Addr)
		if err := s.Server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
}

func (s *DNSProxyServer) Stats() string {

	st := `===========================
	 DNS Proxy
===========================

Running: 	{{.Running}}
Started: 	{{.Started}}
Port: 	 	{{.Port}}
Proto: 		{{.Proto}}
Queries:	{{.Questions}} Questions | {{.Answers}} Answers    | {{.Rejections}} Rejections
Activity:	{{.Startups}} Startups   | {{.Shutdowns}} Shutdowns | 
Tests:		{{.PassedTests}} Passed  | {{.FailedTests}} Failed | {{.Tests}} Total | 
Test Domains:		{{.TestDomains}}
Upstream Resolvers: {{.Upstreams}}


`
	var buf bytes.Buffer
	tpl, _ := template.New("stats").Parse(st)
	s.mutex.Lock()
	tpl.Execute(&buf, s)
	s.mutex.Unlock()
	ret := fmt.Sprintf(`%s`, &buf)
	log.Debugf("%s\n", ret)
	return ret

}

func (s *DNSProxyServer) Address() string {
	return fmt.Sprintf(`%s:%d`, DNS_PROXY_HOST, s.Port)
}

func (s *DNSProxyServer) WaitForPort() (time.Duration, error) {
	started := time.Now()
	for {
		_, err := dns.DialTimeout(s.Proto, s.Address(), 10*time.Millisecond)
		if err != nil {
			if time.Since(started) > MAX_WAIT_DURATION {
				return time.Since(started), err
			} else {
				log.Debugf("Not Failing yet, only %dms\n", time.Since(started).Milliseconds())
			}
		} else {
			start_time := time.Since(started)
			log.Debugf("Waited for port OK => %s\n", s.Address())
			s.Running = true
			return start_time, nil
		}
	}

}

func (s *DNSProxyServer) ResolveName(dns_name string) ([]string, bool, error) {
	started := time.Now()
	ips := []string{}
	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(dns_name), dns.TypeA)
	m.RecursionDesired = true
	r, _, err := c.Exchange(m, s.Address())
	if r == nil {
		log.Errorf("[ResolveName Failed]   name: %s | error: %s\n", dns_name, err.Error())
		return ips, false, err
	}

	if r.Rcode != dns.RcodeSuccess {
		msg := errors.New(fmt.Sprintf(" *** unsuccessfull query  name=%s | r.Rcode=%v, \n", dns_name, r.Rcode))
		log.Errorf(`%s`, msg)
		return ips, false, msg
	}

	for _, a := range r.Answer {
		if IP, ok := a.(*dns.A); ok {
			atomic.AddUint64(&s.Answers, 1)
			ips = append(ips, IP.A.String())
			log.Debugf("[%dms] %s @%s (Type %d) => %s \n",
				time.Since(started).Milliseconds(),
				strings.ToLower(r.Question[0].Name),
				s.Address(),
				dns.TypeA,
				IP.A.String(),
			)
		}
	}
	return ips, true, nil

}

func (s *DNSProxyServer) TestWhileRunning() {
	for {
		s.Test()
		if !s.Running {
			break
		}
		time.Sleep(TEST_WHILE_RUNNING_INTERVAL)
	}
}

func (s *DNSProxyServer) Test() bool {
	for _, d := range s.TestDomains {
		if s.Running {
			ips, ok, err := s.ResolveName(d)
			atomic.AddUint64(&s.Tests, 1)
			if !ok || err != nil || len(ips) == 0 {
				atomic.AddUint64(&s.FailedTests, 1)
			} else {
				atomic.AddUint64(&s.PassedTests, 1)
			}
		}
	}
	return true
}

func (s *DNSProxyServer) Start() error {
	log.Debugf("[DNSProxyServer Start]Starting DNS Proxy\n")
	started := time.Now()
	//	s.mutex.Lock()
	//	defer s.mutex.Unlock()

	dns.HandleFunc(".", s.route)
	s.Server.ReusePort = true
	s.StartServer()

	log.Debugf("Waiting for DNS Port to open........\n")
	time.Sleep(10 * time.Millisecond)

	dur, err := s.WaitForPort()
	if err != nil {
		log.Fatal(err)
	}
	log.Debugf("Port Opened in %dms\n", dur.Milliseconds())
	s.Started = time.Now()

	log.Debugf("Testing DNS Proxy\n")
	//	s.mutex.Unlock()
	st := time.Now()
	s.Test()
	tdur := time.Since(st)
	totaldur := time.Since(started)
	time.Sleep(5 * time.Millisecond)
	log.Debugf("Test Completed in %dms | Startup completed in %dms\n", tdur.Milliseconds(), totaldur.Milliseconds())

	return nil
}

func (s *DNSProxyServer) MonitorShutdown() {
	for {
		log.Debugf("[DNSProxyServer] Waiting for Controller Signal.")
		wait_Start := time.Now()
		shutdown_signal := <-s.Shutdown
		wait_dur := time.Since(wait_Start)
		log.Debugf("           Signal Recieved after %dms! => %v | \n", wait_dur.Milliseconds(), shutdown_signal)
		if shutdown_signal {
			stopped_start := time.Now()
			s.Running = false
			atomic.AddUint64(&s.Shutdowns, 1)
			s.Server.Shutdown()
			log.Debugf("Shutdown Completed in %dms\n", time.Since(stopped_start).Milliseconds())
		} else {
			started_start := time.Now()
			atomic.AddUint64(&s.Startups, 1)
			start_err := s.Start()
			if start_err != nil {
				log.Errorf("Startup Failed to Start in %dms\n", time.Since(started_start).Milliseconds())
			} else {
				log.Debugf("Startup Completed in %dms\n", time.Since(started_start).Milliseconds())
			}
		}
	}
}
func (s *DNSProxyServer) MonitorSignals() {

	// Wait for SIGINT or SIGTERM
	sigs := make(chan os.Signal, 1)
	for {
		log.Debugf("MonitorSignals Started.\n")
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		log.Debugf("Shutting down dns proxies.........\n")
		s.Running = false
		started := time.Now()
		s.Server.Shutdown()
		log.Debugf("DNS Proxies %v Shutdown in %dms", s.Server.Addr, time.Since(started).Milliseconds())
		os.Exit(0)
	}
}

func (s *DNSProxyServer) RandomUpstream() string {
	random_upstream_id := rand.Intn(len(s.Upstreams)-0) + 0
	return s.Upstreams[random_upstream_id]

}

func (s *DNSProxyServer) route(w dns.ResponseWriter, req *dns.Msg) {
	if len(req.Question) == 0 || !allowed(w, req) {
		dns.HandleFailed(w, req)
		atomic.AddUint64(&s.Rejections, 1)
		return
	}
	atomic.AddUint64(&s.Questions, 1)
	s.proxy(w, req)
}

func isTransfer(req *dns.Msg) bool {
	for _, q := range req.Question {
		switch q.Qtype {
		case dns.TypeIXFR, dns.TypeAXFR:
			return true
		}
	}
	return false
}

func allowed(w dns.ResponseWriter, req *dns.Msg) bool {
	if !isTransfer(req) {
		return true
	}
	return false
}

func (s *DNSProxyServer) proxy(w dns.ResponseWriter, req *dns.Msg) {
	if isTransfer(req) {
		return
	}
	c := &dns.Client{Net: s.Proto, Timeout: UPSTREAM_TIMEOUT}
	resp, _, err := c.Exchange(req, s.RandomUpstream())
	if err != nil {
		dns.HandleFailed(w, req)
		return
	}
	w.WriteMsg(resp)
}

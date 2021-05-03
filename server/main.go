package server

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/template"
	"time"

	"github.com/miekg/dns"
)

type DNSProxyServer struct {
	Running        bool
	xxxxxxxxxxxxxx bool
	Started        time.Time

	Port  int
	Proto string

	Upstreams []string

	Questions  uint64
	Answers    uint64
	Rejections uint64
	Shutdowns  uint64
	Startups   uint64

	mutex sync.RWMutex

	Server *dns.Server

	DebugMode bool
	Shutdown  <-chan bool
}

var (
	TEST_DOMAINS                = []string{`amazon.com`, `google.com`, `playboy.com`, `deloitte.com`, `yahoo.com`, `ups.com`, `espn.com`}
	MAX_WAIT_DURATION           = 100 * time.Millisecond
	UPSTREAM_TIMEOUT            = 150 * time.Millisecond
	TEST_WHILE_RUNNING_INTERVAL = 10 * time.Second
	DNS_PROXY_HOST              = `127.0.0.1`
)

func init() {
	rand.Seed(time.Now().Unix())
	//	pp.Print(resolvconf.Path())
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
		Running:   false,
		Questions: 0,
		Answers:   0,
		Server:    dns_server,
		Port:      s.Port,
		Proto:     s.Proto,
		Upstreams: s.Upstreams,
		Shutdown:  s.Shutdown,
	}
	go server.MonitorSignals()
	go server.MonitorShutdown()
	go s.TestWhileRunning()

	return &server
}

func (s *DNSProxyServer) StartServer() {
	go func() {
		fmt.Printf("Starting %v\n", s.Server.Addr)
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
Activity:	{{.Startups}} Startups   | {{.Shutdowns}} Shutdowns
Running: 	{{.Running}}

`
	var buf bytes.Buffer
	tpl, _ := template.New("stats").Parse(st)
	tpl.Execute(&buf, s)
	ret := fmt.Sprintf(`%s`, &buf)
	fmt.Printf("%s\n", ret)
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
				fmt.Printf("Not Failing yet, only %dms\n", time.Since(started).Milliseconds())
			}
		} else {
			start_time := time.Since(started)
			fmt.Printf("Waited for port OK => %s\n", s.Address())
			s.Running = true
			return start_time, nil
		}
	}

}

func (s *DNSProxyServer) ResolveName(dns_name string) ([]string, bool, error) {
	ips := []string{}
	started := time.Now()
	c := new(dns.Client)
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(dns_name), dns.TypeA)
	m.RecursionDesired = true
	r, _, err := c.Exchange(m, s.Address())
	if r == nil {
		fmt.Printf("[ResolveName Failed]   name: %s | error: %s\n", dns_name, err.Error())
		return ips, false, err
	}

	if r.Rcode != dns.RcodeSuccess {
		msg := errors.New(fmt.Sprintf(" *** unsuccessfull query  name=%s | r.Rcode=%v, \n", dns_name, r.Rcode))
		fmt.Print(msg)
		return ips, false, msg
	}

	for _, a := range r.Answer {
		if IP, ok := a.(*dns.A); ok {
			atomic.AddUint64(&s.Answers, 1)
			ips = append(ips, IP.A.String())
			if false {
				fmt.Printf("[%dms] %s @%s (Type %d) => %s \n",
					time.Since(started).Milliseconds(),
					strings.ToLower(r.Question[0].Name),
					s.Address(),
					dns.TypeA,
					IP.A.String(),
				)
			}
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
		//s.Stats()
	}
}

func (s *DNSProxyServer) Test() bool {
	for _, d := range TEST_DOMAINS {
		if s.Running {
			s.ResolveName(d)
		}
	}
	return true
}

func (s *DNSProxyServer) Start() error {
	fmt.Printf("[DNSProxyServer Start]Starting DNS Proxy\n")
	started := time.Now()

	//	s.mutex.Lock()

	dns.HandleFunc(".", s.route)
	s.Server.ReusePort = true
	s.StartServer()

	fmt.Printf("Waiting for DNS Port to open........\n")
	time.Sleep(10 * time.Millisecond)

	dur, err := s.WaitForPort()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Port Opened in %dms\n", dur.Milliseconds())
	s.Started = time.Now()

	fmt.Printf("Testing DNS Proxy\n")
	//	s.mutex.Unlock()
	st := time.Now()
	s.Test()
	tdur := time.Since(st)
	totaldur := time.Since(started)
	time.Sleep(5 * time.Millisecond)
	fmt.Printf("Test Completed in %dms | Startup completed in %dms\n", tdur.Milliseconds(), totaldur.Milliseconds())

	return nil
}

func (s *DNSProxyServer) MonitorShutdown() {
	for {
		fmt.Printf("MonitorShutdown Started. Waiting for shutdown signal.............\n")
		shutdown_signal := <-s.Shutdown
		fmt.Printf("MonitorShutdown Recieved! => %v | \n", shutdown_signal)
		if shutdown_signal {
			s.Running = false
			atomic.AddUint64(&s.Shutdowns, 1)
			s.Server.Shutdown()
		} else {
			atomic.AddUint64(&s.Startups, 1)
			start_err := s.Start()
			if start_err != nil {
				fmt.Printf("MonitorShutdown Start Failed: %s\n", start_err.Error())
			} else {
				fmt.Printf("MonitorShutdown Start Completed! OK!\n")
			}
		}
	}
}
func (s *DNSProxyServer) MonitorSignals() {

	// Wait for SIGINT or SIGTERM
	sigs := make(chan os.Signal, 1)
	for {
		fmt.Printf("MonitorSignals Started.\n")
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		fmt.Printf("Shutting down dns proxies.........\n")
		s.Running = false
		s.Server.Shutdown()
		fmt.Printf("Shutdown %v\n", s.Server.Addr)
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

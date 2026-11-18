package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:30610", "local address; do not use 30591-30595")
	upstream := flag.String("upstream", "", "origin to forward to, such as https://aiplatform.googleapis.com")
	delay := flag.Duration("delay", defaultDelay, "hold an upstream 429 before returning it")
	delayCap := flag.Duration("delay-cap", defaultDelayCap, "maximum hold, including Retry-After")
	firstValid := flag.Duration("first-valid", defaultFirstWait, "end a 200 stream with no nonempty data field after this long")
	idle := flag.Duration("idle", defaultIdle, "end a 200 stream when only comments follow the last nonempty data field")
	flag.Parse()
	if strings.TrimSpace(*upstream) == "" {
		log.Fatal("upstream is required")
	}
	_, port, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatal("listen address must be host:port")
	}
	switch port {
	case "30591", "30592", "30593", "30595":
		log.Fatal("refusing listen port " + port)
	}
	server := &http.Server{
		Addr:              *listen,
		Handler:           NewHandler(Config{Upstream: *upstream, Delay: *delay, DelayCap: *delayCap, FirstValid: *firstValid, Idle: *idle, Logf: log.Printf}),
		ReadHeaderTimeout: 30 * time.Second,
	}
	log.Printf("listening on %s", *listen)
	log.Fatal(server.ListenAndServe())
}

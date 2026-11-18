package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const helperVersion = "0.1.0"

const defaultListenAddr = "127.0.0.1:30610"

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("gemini-egress-helper", flag.ContinueOnError)
	flags.SetOutput(stderr)
	listen := flags.String("listen", defaultListenAddr, "loopback address only; 30591, 30592, 30593, and 30595 are refused")
	upstream := flags.String("upstream", "", "fixed origin, such as https://aiplatform.googleapis.com, with no path or userinfo")
	delay := flags.Duration("delay", defaultDelay, "hold an upstream 429 before returning it")
	delayCap := flags.Duration("delay-cap", defaultDelayCap, "maximum hold, including Retry-After")
	firstValid := flags.Duration("first-valid", defaultFirstWait, "end a committed 200 stream that has no nonempty data field")
	idle := flags.Duration("idle", defaultIdle, "end a committed 200 stream when only comments follow the last nonempty data field")
	headerTimeout := flags.Duration("header-timeout", defaultHeaderTimeout, "bound the wait for upstream response headers")
	version := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *version {
		fmt.Fprintf(stderr, "gemini-egress-helper %s\n", helperVersion)
		return 0
	}
	if err := validateListen(*listen); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return 2
	}
	if err := validateUpstream(*upstream); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return 2
	}
	server := &http.Server{
		Addr:              *listen,
		Handler:           NewHandler(Config{Upstream: *upstream, Delay: *delay, DelayCap: *delayCap, FirstValid: *firstValid, Idle: *idle, HeaderTimeout: *headerTimeout, Logf: log.Printf}),
		ReadHeaderTimeout: 30 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	log.Printf("gemini-egress-helper %s listening on %s", helperVersion, *listen)
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return 1
	}
	return 0
}

func validateListen(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address must be host:port")
	}
	switch port {
	case "30591", "30592", "30593", "30595":
		return fmt.Errorf("refusing listen port %s", port)
	}
	if host == "" {
		return fmt.Errorf("listen host must be loopback")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen host must be loopback")
	}
	return nil
}

func validateUpstream(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("upstream must be an absolute origin")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("upstream scheme must be http or https")
	}
	if parsed.User != nil {
		return fmt.Errorf("upstream must not contain userinfo")
	}
	if (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("upstream must not contain a path, query, or fragment")
	}
	return nil
}

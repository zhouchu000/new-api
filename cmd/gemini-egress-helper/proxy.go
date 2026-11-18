// Command gemini-egress-helper is an optional pass-through in front of one
// upstream origin. It does not replace the live gateway and it does not retry.
//
// The gateway waits for the upstream HTTP response before it uses its own
// retry budget. Holding a 429 here spaces that existing failover. A helper in
// front of the gateway only sees the final 429, after that failover has
// already happened, and a helper-level retry would issue another full set of
// gateway attempts.
package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDelay     = 2 * time.Second
	defaultDelayCap  = 30 * time.Second
	defaultFirstWait = 300 * time.Second
	defaultIdle      = 300 * time.Second
	maxErrorBody     = 1 << 20
)

// Config is the helper's behavior. Zero durations use the defaults above.
// Upstream is the only origin requests are forwarded to.
type Config struct {
	Upstream   string
	Delay      time.Duration
	DelayCap   time.Duration
	FirstValid time.Duration
	Idle       time.Duration
	Logf       func(string, ...any)
}

type handler struct {
	cfg    Config
	client *http.Client
}

// NewHandler forwards each request to cfg.Upstream once.
func NewHandler(cfg Config) http.Handler {
	if cfg.Delay <= 0 {
		cfg.Delay = defaultDelay
	}
	if cfg.DelayCap <= 0 {
		cfg.DelayCap = defaultDelayCap
	}
	if cfg.Delay > cfg.DelayCap {
		cfg.Delay = cfg.DelayCap
	}
	if cfg.FirstValid <= 0 {
		cfg.FirstValid = defaultFirstWait
	}
	if cfg.Idle <= 0 {
		cfg.Idle = defaultIdle
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &handler{
		cfg: cfg,
		client: &http.Client{
			// A stream can outlive any fixed client timeout. Header wait is
			// bounded separately; the body is bounded by the SSE deadlines.
			Timeout: 0,
			Transport: &http.Transport{
				Proxy:                 nil,
				DisableCompression:    true,
				ResponseHeaderTimeout: 2 * time.Minute,
				IdleConnTimeout:       90 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upstreamURL := strings.TrimRight(h.cfg.Upstream, "/") + r.URL.RequestURI()
	out, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, r.Body)
	if err != nil {
		h.cfg.Logf("reject request")
		writeFailure(w)
		return
	}
	if parsed, err := http.NewRequest(http.MethodGet, h.cfg.Upstream, nil); err == nil {
		out.Host = parsed.Host
	}
	out.ContentLength = r.ContentLength
	copyHeaders(out.Header, r.Header)

	resp, err := h.client.Do(out)
	if err != nil {
		h.cfg.Logf("upstream dial failed")
		if r.Context().Err() == nil {
			writeFailure(w)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		h.finishTooMany(w, r, resp)
		return
	}
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if isEventStream(resp.Header.Get("Content-Type")) {
		// Status is already committed. The pump may only close this one
		// response. It does not dial again.
		err = pumpSSE(r.Context(), w, resp.Body, h.cfg.FirstValid, h.cfg.Idle)
		if err != nil && r.Context().Err() == nil && err != io.EOF {
			h.cfg.Logf("stream ended")
		}
		return
	}
	_, _ = io.Copy(w, resp.Body)
}

func (h *handler) finishTooMany(w http.ResponseWriter, r *http.Request, resp *http.Response) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	wait := holdDuration(resp.Header.Get("Retry-After"), time.Now(), h.cfg.Delay, h.cfg.DelayCap)
	h.cfg.Logf("hold status=429 wait=%s", wait)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return
	case <-timer.C:
	}
	copyHeaders(w.Header(), resp.Header)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(body)
}

func writeFailure(w http.ResponseWriter) {
	http.Error(w, "upstream request failed", http.StatusBadGateway)
}

func holdDuration(retryAfter string, now time.Time, initial, cap time.Duration) time.Duration {
	wait := initial
	if parsed, ok := parseRetryAfter(retryAfter, now); ok {
		wait = parsed
	}
	if wait < 0 {
		wait = 0
	}
	if wait > cap {
		return cap
	}
	return wait
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(now)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func isEventStream(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(contentType), "text/event-stream")
}

// pumpSSE writes each line as it is read. A nonempty data field is progress.
// Comments, event lines, blank lines, and empty data fields are not. Hitting
// the deadline closes this body and returns. The caller must not dial again.
func pumpSSE(ctx context.Context, dst io.Writer, src io.ReadCloser, firstValid, idle time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := watchDeadline(ctx, src, firstValid, idle)
	defer close(stop)
	reader := bufio.NewReaderSize(src, 32<<10)
	flusher, _ := dst.(interface{ Flush() })
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := dst.Write(line); werr != nil {
				return werr
			}
			if flusher != nil {
				flusher.Flush()
			}
			if nonemptyDataLine(line) {
				select {
				case stop <- struct{}{}:
				default:
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func nonemptyDataLine(line []byte) bool {
	text := strings.TrimSpace(string(line))
	payload, ok := strings.CutPrefix(text, "data:")
	if !ok {
		return false
	}
	return strings.TrimSpace(payload) != ""
}

// watchDeadline closes src when no nonempty data field arrives in time.
// A value on the returned channel marks one such field and restarts the idle wait.
func watchDeadline(ctx context.Context, src io.Closer, firstValid, idle time.Duration) chan struct{} {
	payloads := make(chan struct{}, 1)
	go func() {
		timer := time.NewTimer(firstValid)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-payloads:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(idle)
			case <-timer.C:
				_ = src.Close()
				return
			}
		}
	}()
	return payloads
}

func copyHeaders(dst, src http.Header) {
	hop := map[string]struct{}{
		"connection":          {},
		"keep-alive":          {},
		"proxy-connection":    {},
		"proxy-authenticate":  {},
		"proxy-authorization": {},
		"te":                  {},
		"trailer":             {},
		"transfer-encoding":   {},
		"upgrade":             {},
	}
	for _, item := range src.Values("Connection") {
		for part := range strings.SplitSeq(item, ",") {
			hop[strings.ToLower(strings.TrimSpace(part))] = struct{}{}
		}
	}
	for key, values := range src {
		if _, skip := hop[strings.ToLower(key)]; skip {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

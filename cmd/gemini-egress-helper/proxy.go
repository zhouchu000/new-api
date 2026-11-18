// Command gemini-egress-helper is an optional pass-through in front of one
// upstream origin. It does not replace the live gateway.
//
// By default it dials that origin once. Holding a 429 here spaces the
// gateway's existing failover. A helper in front of the gateway only sees the
// final 429, after that failover has already happened.
//
// -region-failover stays off unless the operator also sets an allowlist of
// regions (us, eu) and exact models. It may then make one extra dial after an
// explicit HTTP 429, before any downstream byte, rewriting only the paired
// location and host. It does not replay an HTTP 200, a committed stream, or a
// transport failure. A non-2xx alternate response is not returned as success;
// the client still sees the original 429. Switching region does not add quota.
//
// maxExtraRegionAttempts is 1. With the gateway's RetryTimes=1, two helper
// dials times two channels is four upstream calls, plus one capped backoff on
// each helper request that sees a 429.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDelay          = 2 * time.Second
	defaultDelayCap       = 30 * time.Second
	defaultFirstWait      = 300 * time.Second
	defaultIdle           = 300 * time.Second
	defaultHeaderTimeout  = 2 * time.Minute
	defaultRegionBody     = 1 << 20
	maxRegionBody         = 8 << 20
	defaultRegionDeadline = 45 * time.Second
	maxErrorBody          = 1 << 20
	maxRequestTarget      = 8 << 10
	// maxExtraRegionAttempts is the whole failover budget. Do not loop the
	// allowlist. One extra dial, then stop.
	maxExtraRegionAttempts = 1
)

var errRejectedHost = errors.New("rejected host")

// Config is the helper's behavior. Zero durations use the defaults above.
// Upstream is the only origin requests are forwarded to unless region
// failover is explicitly enabled. Region targets are region codes, never hosts.
// testRegionHosts, when non-nil, replaces the official host table. The CLI
// does not set it. A missing code does not fall through to the official hosts.
type Config struct {
	Upstream         string
	Delay            time.Duration
	DelayCap         time.Duration
	FirstValid       time.Duration
	Idle             time.Duration
	HeaderTimeout    time.Duration
	RegionFailover   bool
	RegionTargets    []string
	RegionModels     []string
	RegionBodyBudget int64
	RegionDeadline   time.Duration
	Logf             func(string, ...any)
	testRegionHosts  map[string]string
}

type handler struct {
	cfg      Config
	client   *http.Client
	upstream *urlUpstream
}

// urlUpstream is the parsed fixed origin. It is not taken from the request.
type urlUpstream struct {
	scheme string
	host   string
}

// NewHandler forwards each request to cfg.Upstream once unless region
// failover is enabled with both allowlists. An empty allowlist keeps the
// single dial even if RegionFailover is set.
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
	if cfg.HeaderTimeout <= 0 {
		cfg.HeaderTimeout = defaultHeaderTimeout
	}
	if cfg.RegionBodyBudget <= 0 {
		cfg.RegionBodyBudget = defaultRegionBody
	}
	if cfg.RegionBodyBudget > maxRegionBody {
		cfg.RegionBodyBudget = maxRegionBody
	}
	if cfg.RegionDeadline <= 0 {
		cfg.RegionDeadline = defaultRegionDeadline
	}
	if cfg.RegionFailover && (len(cfg.RegionTargets) == 0 || len(cfg.RegionModels) == 0) {
		cfg.RegionFailover = false
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	parsed, _ := parseFixedOrigin(cfg.Upstream)
	return &handler{
		cfg:      cfg,
		upstream: parsed,
		client: &http.Client{
			// A stream can outlive any fixed client timeout. Header wait is
			// bounded separately; the body is bounded by the SSE deadlines.
			Timeout: 0,
			Transport: &http.Transport{
				Proxy:                 nil,
				DisableCompression:    true,
				ResponseHeaderTimeout: cfg.HeaderTimeout,
				IdleConnTimeout:       90 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target, ok := originRequestTarget(r)
	if !ok {
		http.Error(w, "invalid request target", http.StatusBadRequest)
		return
	}
	if h.upstream == nil {
		h.cfg.Logf("reject request")
		writeFailure(w)
		return
	}
	path, rawQuery, _ := strings.Cut(target, "?")
	plan := h.planRegion(r, path, rawQuery)
	body, length, replay, err := h.prepareReplay(r, plan.eligible)
	if err != nil {
		h.cfg.Logf("reject request")
		if r.Context().Err() == nil {
			writeFailure(w)
		}
		return
	}
	resp, err := h.roundTrip(r.Context(), r, h.upstream.scheme+"://"+h.upstream.host+target, h.upstream.host, body, length)
	if err != nil {
		h.cfg.Logf("upstream dial failed")
		if r.Context().Err() == nil {
			writeFailure(w)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		h.finishTooMany(w, r, resp, replay, plan)
		return
	}
	h.writeUpstream(w, r, resp)
}

func (h *handler) writeUpstream(w http.ResponseWriter, r *http.Request, resp *http.Response) {
	copyHeaders(w.Header(), resp.Header)
	if isEventStream(resp.Header.Get("Content-Type")) {
		// The body is copied unchanged, including CRLF, but a deadline may
		// close it early. A Content-Length copied from upstream would promise
		// bytes that will not be sent.
		w.Header().Del("Content-Length")
	}
	w.WriteHeader(resp.StatusCode)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if isEventStream(resp.Header.Get("Content-Type")) {
		// Status is already committed. The pump may only close this one
		// response. It does not dial again.
		err := pumpSSE(r.Context(), w, resp.Body, h.cfg.FirstValid, h.cfg.Idle)
		if err != nil && r.Context().Err() == nil && err != io.EOF {
			h.cfg.Logf("stream ended")
		}
		return
	}
	_, _ = io.Copy(w, resp.Body)
}

type saved429 struct {
	header     http.Header
	body       []byte
	retryAfter string
}

func (h *handler) finishTooMany(w http.ResponseWriter, r *http.Request, resp *http.Response, replay *replayBody, plan regionPlan) {
	saved := saved429{
		header:     resp.Header.Clone(),
		retryAfter: resp.Header.Get("Retry-After"),
	}
	saved.body, _ = io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if h.tryRegion(w, r, saved, replay, plan) {
		return
	}
	h.writeSaved429(w, r, saved, true)
}

// tryRegion makes at most one extra dial. The hold before that dial is the
// same capped wait the helper uses when failover is off. The original 429 is
// not held a second time. A non-2xx alternate result is discarded; the client
// still receives the saved 429 so a regional 403 or 404 is not presented as a
// healthy response and the quota diagnostic is preserved.
func (h *handler) tryRegion(w http.ResponseWriter, r *http.Request, saved saved429, replay *replayBody, plan regionPlan) bool {
	if !plan.eligible || replay == nil || maxExtraRegionAttempts < 1 {
		return false
	}
	wait := holdDuration(saved.retryAfter, time.Now(), h.cfg.Delay, h.cfg.DelayCap)
	h.cfg.Logf("hold status=429 wait=%s", wait)
	h.cfg.Logf("region attempt region=%s", plan.region)
	attemptCtx, cancel := context.WithTimeout(r.Context(), h.cfg.RegionDeadline)
	defer cancel()
	if err := sleepContext(attemptCtx, wait); err != nil {
		if r.Context().Err() != nil {
			return true
		}
		h.cfg.Logf("region deadline")
		h.writeSaved429(w, r, saved, false)
		return true
	}
	resp, err := h.roundTrip(attemptCtx, r, plan.rawURL, plan.host, bytes.NewReader(replay.data), int64(len(replay.data)))
	if err != nil {
		h.cfg.Logf("region transport failed")
		if r.Context().Err() != nil {
			return true
		}
		h.writeSaved429(w, r, saved, false)
		return true
	}
	defer resp.Body.Close()
	h.cfg.Logf("region result region=%s status=%d", plan.region, resp.StatusCode)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		h.writeUpstream(w, r, resp)
		return true
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	h.writeSaved429(w, r, saved, false)
	return true
}

func (h *handler) writeSaved429(w http.ResponseWriter, r *http.Request, saved saved429, hold bool) {
	if hold {
		wait := holdDuration(saved.retryAfter, time.Now(), h.cfg.Delay, h.cfg.DelayCap)
		h.cfg.Logf("hold status=429 wait=%s", wait)
		if err := sleepContext(r.Context(), wait); err != nil {
			return
		}
	}
	copyHeaders(w.Header(), saved.header)
	w.Header().Set("Content-Length", strconv.Itoa(len(saved.body)))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(saved.body)
}

func sleepContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type replayBody struct {
	data []byte
}

// prepareReplay buffers only a request that is allowed to fail over and whose
// size is already known to fit the budget. Anything else is streamed once.
func (h *handler) prepareReplay(r *http.Request, eligible bool) (io.Reader, int64, *replayBody, error) {
	if !eligible || r.ContentLength < 0 || r.ContentLength > h.cfg.RegionBodyBudget {
		return r.Body, r.ContentLength, nil, nil
	}
	buf := make([]byte, r.ContentLength)
	if _, err := io.ReadFull(r.Body, buf); err != nil {
		return nil, 0, nil, err
	}
	return bytes.NewReader(buf), int64(len(buf)), &replayBody{data: buf}, nil
}

func (h *handler) roundTrip(ctx context.Context, r *http.Request, rawURL, host string, body io.Reader, length int64) (*http.Response, error) {
	if !h.allowHost(host) {
		return nil, errRejectedHost
	}
	out, err := http.NewRequestWithContext(ctx, r.Method, rawURL, body)
	if err != nil {
		return nil, err
	}
	// bytes.Reader would install GetBody, and the transport can then replay a
	// POST on a recycled connection. This helper dials explicitly or not at all.
	out.GetBody = nil
	out.Host = host
	out.ContentLength = length
	copyHeaders(out.Header, r.Header)
	return h.client.Do(out)
}

// originRequestTarget accepts only an origin-form path on this process's
// fixed upstream. An absolute-form request would be a forward proxy.
func originRequestTarget(r *http.Request) (string, bool) {
	if r == nil || r.URL == nil || r.URL.IsAbs() || r.URL.Host != "" {
		return "", false
	}
	target := r.URL.RequestURI()
	if len(target) == 0 || len(target) > maxRequestTarget {
		return "", false
	}
	if !strings.HasPrefix(target, "/") || strings.HasPrefix(target, "//") {
		return "", false
	}
	return target, true
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

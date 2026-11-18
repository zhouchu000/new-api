package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOne429IsOneUpstreamCallAndHoldsBeforeReturn(t *testing.T) {
	var hits atomic.Int32
	var gotAuth, gotKey, gotProxy, gotBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("X-Goog-Api-Key")
		gotProxy = r.Header.Get("Proxy-Authorization")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = string(body)
		assert.Equal(t, "/v1/projects/p/locations/global/publishers/google/models/gemini-4-argon:generateContent", r.URL.Path)
		w.Header().Set("Retry-After", "120")
		w.Header().Set("Connection", "close")
		w.Header().Set("X-Usage-Meta", "kept")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
	}))
	t.Cleanup(upstream.Close)

	logs := &bytes.Buffer{}
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: time.Second, DelayCap: 60 * time.Millisecond,
		FirstValid: time.Second, Idle: time.Second,
		Logf: func(format string, args ...any) { fmt.Fprintf(logs, format, args...) },
	}))
	t.Cleanup(helper.Close)

	body := `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`
	req, err := http.NewRequest(http.MethodPost, helper.URL+"/v1/projects/p/locations/global/publishers/google/models/gemini-4-argon:generateContent", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer leak-token-xyz")
	req.Header.Set("X-Goog-Api-Key", "secret-key-xyz")
	req.Header.Set("Proxy-Authorization", "Basic c2VjcmV0")
	started := time.Now()
	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(started)
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, int32(1), hits.Load())
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
	assert.Equal(t, "Bearer leak-token-xyz", gotAuth)
	assert.Equal(t, "secret-key-xyz", gotKey)
	assert.Empty(t, gotProxy)
	assert.Equal(t, body, gotBody)
	assert.Equal(t, "120", resp.Header.Get("Retry-After"))
	assert.Equal(t, "kept", resp.Header.Get("X-Usage-Meta"))
	assert.Empty(t, resp.Header.Get("Connection"))
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
	assert.NotContains(t, logs.String(), "leak-token-xyz")
	assert.NotContains(t, logs.String(), "secret-key-xyz")
	assert.NotContains(t, string(got), "leak-token-xyz")
}

func TestSerial429sStayOnTheGatewayBudget(t *testing.T) {
	var mu sync.Mutex
	var hits []time.Time
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, "busy")
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: 40 * time.Millisecond, DelayCap: time.Second,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	for range 2 {
		resp, err := http.Post(helper.URL+"/v1/chat", "application/json", strings.NewReader(`{"model":"gemini-4-argon"}`))
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, hits, 2)
	assert.GreaterOrEqual(t, hits[1].Sub(hits[0]), 30*time.Millisecond)
}

func TestCancelDuring429HoldDoesNotDialAgain(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: 5 * time.Second, DelayCap: 5 * time.Second,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, helper.URL+"/v1/chat", strings.NewReader("{}"))
	require.NoError(t, err)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	_, err = http.DefaultClient.Do(req)
	assert.Error(t, err)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, int32(1), hits.Load())
}

func TestThoughtAndUsagePassThroughOnce(t *testing.T) {
	const payload = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"step\"}]}}]}\n\ndata: {\"usageMetadata\":{\"totalTokenCount\":4}}\n\n"
	var hits atomic.Int32
	var gotBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, payload)
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: time.Second, DelayCap: time.Second,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	reqBody := `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"xhigh"}}}`
	resp, err := http.Post(helper.URL+"/v1beta/models/gemini-4-hydrogen:streamGenerateContent?alt=sse", "application/json", strings.NewReader(reqBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, payload, string(got))
	assert.Equal(t, reqBody, gotBody)
	assert.Equal(t, int32(1), hits.Load())
}

func TestCommentOnlyStreamEndsWithoutAnotherDial(t *testing.T) {
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: time.Second, DelayCap: time.Second,
		FirstValid: 80 * time.Millisecond, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	started := time.Now()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(helper.URL + "/stream")
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(got), ": keepalive")
	assert.NotContains(t, string(got), "data:")
	assert.Less(t, time.Since(started), 2*time.Second)
	assert.Equal(t, int32(1), hits.Load())
}

func TestStreamForwardsALineBeforeTheUpstreamFinishes(t *testing.T) {
	var hits atomic.Int32
	firstSeen := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"usageMetadata\":{\"totalTokenCount\":1}}\n\n")
		flusher.Flush()
		close(firstSeen)
		<-release
		_, _ = io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"next\"}]}}]}\n\n")
		flusher.Flush()
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: time.Second, DelayCap: time.Second,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(helper.URL + "/stream")
	require.NoError(t, err)
	defer resp.Body.Close()
	buf := make([]byte, 128)
	n, err := resp.Body.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "usageMetadata")
	select {
	case <-firstSeen:
	case <-time.After(time.Second):
		t.Fatal("first payload was not forwarded before the upstream continued")
	}
	close(release)
	rest, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(rest), `"text":"next"`)
	assert.Equal(t, int32(1), hits.Load())
}

func TestOtherStatusesPassThroughAndRedirectIsNotFollowed(t *testing.T) {
	var evilHits atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(evil.Close)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", evil.URL+"/stolen")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(upstream.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: upstream.URL, Delay: 300 * time.Millisecond, DelayCap: time.Second,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	started := time.Now()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get(helper.URL + "/v1/chat")
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, evil.URL+"/stolen", resp.Header.Get("Location"))
	assert.Less(t, time.Since(started), 150*time.Millisecond)
	assert.Equal(t, int32(0), evilHits.Load())
}

func TestDialFailureHidesHostAndCredentials(t *testing.T) {
	logs := &bytes.Buffer{}
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: "http://127.0.0.1:1", Delay: time.Second, DelayCap: time.Second,
		FirstValid: time.Second, Idle: time.Second,
		Logf: func(format string, args ...any) { fmt.Fprintf(logs, format, args...) },
	}))
	t.Cleanup(helper.Close)
	req, err := http.NewRequest(http.MethodPost, helper.URL+"/v1/chat", strings.NewReader(`{"model":"gemini-4-argon"}`))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer leak-token-xyz")
	req.Header.Set("X-Goog-Api-Key", "secret-key-xyz")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.NotContains(t, string(got), "leak-token-xyz")
	assert.NotContains(t, string(got), "secret-key-xyz")
	assert.NotContains(t, string(got), "127.0.0.1:1")
	assert.NotContains(t, logs.String(), "leak-token-xyz")
	assert.NotContains(t, logs.String(), "secret-key-xyz")
	assert.NotContains(t, logs.String(), "127.0.0.1:1")
}

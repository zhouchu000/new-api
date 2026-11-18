package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const regionToken = "Bearer ya29.wire-test-token"

func TestRegionFailoverStaysOneDialWhenDisabled(t *testing.T) {
	global, us, eu := newRegionServers(t, http.StatusTooManyRequests, `{"error":"RESOURCE_EXHAUSTED"}`, http.StatusOK, `{"ok":true}`, http.StatusOK, `{"eu":true}`)
	cfg := regionTestConfig(global.server.URL, us.server.URL, eu.server.URL)
	cfg.RegionFailover = false
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", "alt=sse"), `{"n":1}`, regionToken)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
	assert.Equal(t, 1, global.count())
	assert.Equal(t, 0, us.count())
	assert.Equal(t, 0, eu.count())

	cfg.RegionFailover = true
	cfg.RegionModels = nil
	helper = httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)
	resp = postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, 2, global.count())
	assert.Equal(t, 0, us.count())
}

func TestGlobal429RewritesOnceToApprovedRegion(t *testing.T) {
	const (
		reqBody  = `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`
		payload  = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"thought\":true,\"text\":\"step\"}]}}]}\r\n\r\ndata: {\"usageMetadata\":{\"totalTokenCount\":4}}\r\n\r\n"
		original = `{"error":"RESOURCE_EXHAUSTED"}`
	)
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.Header().Set("Retry-After", "120")
		w.Header().Set("X-Usage-Meta", "kept")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, original)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "identity")
		w.Header().Set("Content-Length", "9999")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, payload)
	})
	eu := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"eu":true}`)
	})
	var evilHits atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(evil.Close)

	logs := &bytes.Buffer{}
	cfg := regionTestConfig(global.server.URL, us.server.URL, eu.server.URL)
	cfg.Delay = 40 * time.Millisecond
	cfg.DelayCap = 50 * time.Millisecond
	cfg.Logf = func(format string, args ...any) {
		fmt.Fprintf(logs, format, args...)
	}
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	started := time.Now()
	req := regionRequest(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", "alt=sse"), reqBody)
	req.Header.Set("X-Goog-User-Project", "vertex-project")
	req.Header.Set("Proxy-Authorization", "Basic c2VjcmV0")
	req.Host = "evil.example"
	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(started)
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, payload, string(got))
	assert.Equal(t, "identity", resp.Header.Get("Content-Encoding"))
	assert.Empty(t, resp.Header.Get("Content-Length"))
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
	assert.Equal(t, int32(0), evilHits.Load())
	assert.Equal(t, 0, eu.count())

	require.Len(t, global.hits(), 1)
	require.Len(t, us.hits(), 1)
	g, u := global.hits()[0], us.hits()[0]
	assert.Equal(t, reqBody, g.body)
	assert.Equal(t, reqBody, u.body)
	assert.Equal(t, regionToken, g.auth)
	assert.Equal(t, regionToken, u.auth)
	assert.Equal(t, "vertex-project", g.project)
	assert.Equal(t, "vertex-project", u.project)
	assert.Empty(t, g.apiKey)
	assert.Empty(t, u.apiKey)
	assert.Empty(t, g.proxyAuth)
	assert.Empty(t, u.proxyAuth)
	assert.Equal(t, "/v1/projects/vertex-project/locations/global/publishers/google/models/gemini-4-argon:streamGenerateContent", g.path)
	assert.Equal(t, "/v1/projects/vertex-project/locations/us/publishers/google/models/gemini-4-argon:streamGenerateContent", u.path)
	assert.Equal(t, "alt=sse", g.rawQuery)
	assert.Equal(t, "alt=sse", u.rawQuery)
	assert.Contains(t, g.requestURI, ":streamGenerateContent")
	assert.Contains(t, u.requestURI, ":streamGenerateContent")
	assert.NotContains(t, g.rawPath, "%3A")
	assert.NotContains(t, u.rawPath, "%3A")
	assert.Equal(t, mustHost(t, global.server.URL), g.host)
	assert.Equal(t, mustHost(t, us.server.URL), u.host)
	assert.NotEqual(t, "evil.example", g.host)
	assert.NotEqual(t, "evil.example", u.host)
	assert.NotContains(t, logs.String(), "ya29.wire-test-token")
	assert.NotContains(t, logs.String(), "thinkingLevel")
	assert.NotContains(t, logs.String(), "alt=sse")
	assert.NotContains(t, logs.String(), "127.0.0.1")
	assert.NotContains(t, logs.String(), "evil.example")
}

func TestRepeated429StopsAfterOneAlternate(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"region-us-denied-body"}`)
	})
	eu := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"eu":true}`)
	})
	cfg := regionTestConfig(global.server.URL, us.server.URL, eu.server.URL)
	cfg.Delay = 40 * time.Millisecond
	cfg.DelayCap = 40 * time.Millisecond
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	started := time.Now()
	for range 2 {
		resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-hydrogen", ""), `{"n":1}`, regionToken)
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
		assert.NotContains(t, string(got), "region-us-denied-body")
	}
	elapsed := time.Since(started)
	assert.Equal(t, 2, global.count())
	assert.Equal(t, 2, us.count())
	assert.Equal(t, 0, eu.count())
	assert.GreaterOrEqual(t, us.hits()[0].when.Sub(global.hits()[0].when), 30*time.Millisecond)
	assert.GreaterOrEqual(t, elapsed, 60*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
	assert.Equal(t, global.hits()[0].body, us.hits()[0].body)
	assert.Equal(t, global.hits()[0].auth, us.hits()[0].auth)
}

func TestRegionDenialPreservesOriginal429(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusTooManyRequests, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			logs := &bytes.Buffer{}
			global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
				w.Header().Set("Retry-After", "1")
				w.Header().Set("X-Usage-Meta", "kept")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
			})
			us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
				w.Header().Set("X-Region-Denied", "yes")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":"denied-region-body"}`)
			})
			cfg := regionTestConfig(global.server.URL, us.server.URL, "")
			cfg.Delay = time.Millisecond
			cfg.DelayCap = 20 * time.Millisecond
			cfg.Logf = func(format string, args ...any) { fmt.Fprintf(logs, format, args...) }
			helper := httptest.NewServer(NewHandler(cfg))
			t.Cleanup(helper.Close)

			resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-carbon", ""), `{"n":1}`, regionToken)
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
			assert.Equal(t, "kept", resp.Header.Get("X-Usage-Meta"))
			assert.Equal(t, "1", resp.Header.Get("Retry-After"))
			assert.Empty(t, resp.Header.Get("X-Region-Denied"))
			assert.NotContains(t, string(got), "denied-region-body")
			assert.Equal(t, 1, global.count())
			assert.Equal(t, 1, us.count())
			assert.Contains(t, logs.String(), fmt.Sprintf("status=%d", status))
			assert.NotContains(t, logs.String(), "denied-region-body")
			assert.NotContains(t, logs.String(), "RESOURCE_EXHAUSTED")
			assert.NotContains(t, logs.String(), "ya29.wire-test-token")
		})
	}
}

func TestRegionRedirectIsNotFollowed(t *testing.T) {
	var evilHits atomic.Int32
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(evil.Close)
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.Header().Set("Location", evil.URL+"/stolen")
		w.WriteHeader(http.StatusFound)
	})
	cfg := regionTestConfig(global.server.URL, us.server.URL, "")
	cfg.Delay = time.Millisecond
	cfg.DelayCap = 20 * time.Millisecond
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
	assert.NotContains(t, resp.Header.Get("Location"), "stolen")
	assert.Equal(t, int32(0), evilHits.Load())
	assert.Equal(t, 1, us.count())
}

func TestCancelDuringRegionBackoffDoesNotDialAgain(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	cfg := regionTestConfig(global.server.URL, us.server.URL, "")
	cfg.Delay = 5 * time.Second
	cfg.DelayCap = 5 * time.Second
	cfg.RegionDeadline = 5 * time.Second
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req := regionRequest(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`)
	req = req.WithContext(ctx)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	_, err := http.DefaultClient.Do(req)
	assert.Error(t, err)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, 1, global.count())
	assert.Equal(t, 0, us.count())
}

func TestRetryAfterIsCappedBeforeTheRegionDial(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	})
	cfg := regionTestConfig(global.server.URL, us.server.URL, "")
	cfg.Delay = time.Second
	cfg.DelayCap = 50 * time.Millisecond
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	started := time.Now()
	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	elapsed := time.Since(started)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `{"ok":true}`, string(got))
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
	assert.Less(t, elapsed, time.Second)
	assert.Equal(t, 1, global.count())
	assert.Equal(t, 1, us.count())
	assert.GreaterOrEqual(t, us.hits()[0].when.Sub(global.hits()[0].when), 40*time.Millisecond)
}

func TestRegionDeadlineSkipsTheSecondDial(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	logs := &bytes.Buffer{}
	cfg := regionTestConfig(global.server.URL, us.server.URL, "")
	cfg.Delay = 5 * time.Second
	cfg.DelayCap = 5 * time.Second
	cfg.RegionDeadline = 40 * time.Millisecond
	cfg.Logf = func(format string, args ...any) { fmt.Fprintf(logs, format, args...) }
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)

	started := time.Now()
	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	elapsed := time.Since(started)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, `{"error":"RESOURCE_EXHAUSTED"}`, string(got))
	assert.Less(t, elapsed, time.Second)
	assert.Equal(t, 1, global.count())
	assert.Equal(t, 0, us.count())
	assert.Contains(t, logs.String(), "region deadline")
	assert.NotContains(t, logs.String(), "ya29.wire-test-token")
}

func TestSuccessfulStreamIsNotReplayedWhenFailoverIsOn(t *testing.T) {
	const reqBody = `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`
	t.Run("comment only", func(t *testing.T) {
		var hits atomic.Int32
		var bodyMu sync.Mutex
		var gotBody string
		global := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			body, _ := io.ReadAll(r.Body)
			bodyMu.Lock()
			gotBody = string(body)
			bodyMu.Unlock()
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
					if _, err := io.WriteString(w, ": keepalive\r\n\r\n"); err != nil {
						return
					}
					flusher.Flush()
				}
			}
		}))
		t.Cleanup(global.Close)
		us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
			w.WriteHeader(http.StatusOK)
		})
		cfg := regionTestConfig(global.URL, us.server.URL, "")
		cfg.Delay = time.Second
		cfg.FirstValid = 80 * time.Millisecond
		cfg.Idle = time.Second
		helper := httptest.NewServer(NewHandler(cfg))
		t.Cleanup(helper.Close)

		resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", "alt=sse"), reqBody, regionToken)
		defer resp.Body.Close()
		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, string(got), ": keepalive\r\n\r\n")
		bodyMu.Lock()
		assert.Equal(t, reqBody, gotBody)
		bodyMu.Unlock()
		assert.Equal(t, int32(1), hits.Load())
		assert.Equal(t, 0, us.count())
	})

	t.Run("idle after data", func(t *testing.T) {
		const line = "data: {\"usageMetadata\":{\"totalTokenCount\":4}}\r\n\r\n"
		var hits atomic.Int32
		global := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			_, _ = io.WriteString(w, line)
			flusher.Flush()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-ticker.C:
					if _, err := io.WriteString(w, ": idle\r\n\r\n"); err != nil {
						return
					}
					flusher.Flush()
				}
			}
		}))
		t.Cleanup(global.Close)
		us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
			w.WriteHeader(http.StatusOK)
		})
		cfg := regionTestConfig(global.URL, us.server.URL, "")
		cfg.FirstValid = time.Second
		cfg.Idle = 80 * time.Millisecond
		helper := httptest.NewServer(NewHandler(cfg))
		t.Cleanup(helper.Close)

		resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", "alt=sse"), reqBody, regionToken)
		defer resp.Body.Close()
		got, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, string(got), line)
		assert.Equal(t, int32(1), hits.Load())
		assert.Equal(t, 0, us.count())
	})
}

func TestUnsupportedOrOversizedRequestsStayOneDial(t *testing.T) {
	const secret = "super-secret-key"
	cases := []struct {
		name   string
		path   string
		header bool
		budget int64
		body   string
		chunk  bool
	}{
		{name: "other model", path: vertexRequestPath("global", "gemini-2.5-flash", ""), body: `{"n":1}`},
		{name: "express path", path: "/v1/publishers/google/models/gemini-4-argon:generateContent", body: `{"n":1}`},
		{name: "single region", path: "/v1/projects/vertex-project/locations/us-central1/publishers/google/models/gemini-4-argon:generateContent", body: `{"n":1}`},
		{name: "pt region", path: "/v1/projects/vertex-project/locations/europe-west2/publishers/google/models/gemini-4-argon:generateContent", body: `{"n":1}`},
		{name: "api key query", path: vertexRequestPath("global", "gemini-4-argon", "key="+secret), body: `{"n":1}`},
		{name: "api key with sse", path: vertexRequestPath("global", "gemini-4-argon", "alt=sse&key="+secret), body: `{"n":1}`},
		{name: "api key header", path: vertexRequestPath("global", "gemini-4-argon", "alt=sse"), header: true, body: `{"n":1}`},
		{name: "over budget", path: vertexRequestPath("global", "gemini-4-argon", ""), budget: 16, body: strings.Repeat("a", 64)},
		{name: "chunked", path: vertexRequestPath("global", "gemini-4-argon", ""), chunk: true, body: `{"n":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &bytes.Buffer{}
			global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":"RESOURCE_EXHAUSTED"}`)
			})
			us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
				w.WriteHeader(http.StatusOK)
			})
			cfg := regionTestConfig(global.server.URL, us.server.URL, "")
			cfg.Delay = time.Millisecond
			cfg.DelayCap = 15 * time.Millisecond
			cfg.Logf = func(format string, args ...any) { fmt.Fprintf(logs, format, args...) }
			if tc.budget > 0 {
				cfg.RegionBodyBudget = tc.budget
			}
			helper := httptest.NewServer(NewHandler(cfg))
			t.Cleanup(helper.Close)

			req := regionRequest(t, helper.URL, tc.path, tc.body)
			if tc.chunk {
				req = chunkedRequest(t, helper.URL, tc.path, tc.body)
			}
			if tc.header {
				req.Header.Set("X-Goog-Api-Key", secret)
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			require.Equal(t, 1, global.count())
			assert.Equal(t, 0, us.count())
			assert.Equal(t, tc.body, global.hits()[0].body)
			assert.NotContains(t, logs.String(), secret)
			assert.NotContains(t, logs.String(), "ya29.wire-test-token")
		})
	}
}

func TestShortBodyDoesNotDialWhenLengthOverruns(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	handler := NewHandler(regionTestConfig(global.server.URL, "http://127.0.0.1:1", ""))
	path := vertexRequestPath("global", "gemini-4-argon", "")
	req := httptest.NewRequest(http.MethodPost, "http://example.com"+path, strings.NewReader("ab"))
	req.URL.Scheme = ""
	req.URL.Host = ""
	req.Host = ""
	req.RequestURI = path
	req.ContentLength = 20
	req.Header.Set("Authorization", regionToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusBadGateway, recorder.Code)
	assert.Equal(t, 0, global.count())
	assert.NotContains(t, recorder.Body.String(), "ya29.wire-test-token")
}

func TestTransportErrorDoesNotChangeRegion(t *testing.T) {
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	logs := &bytes.Buffer{}
	cfg := regionTestConfig("http://127.0.0.1:1", us.server.URL, "")
	cfg.Logf = func(format string, args ...any) { fmt.Fprintf(logs, format, args...) }
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)
	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, 0, us.count())
	assert.NotContains(t, string(got), "ya29.wire-test-token")
	assert.NotContains(t, logs.String(), "ya29.wire-test-token")
	assert.NotContains(t, logs.String(), "127.0.0.1")
}

func TestNon429DoesNotFailOver(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"backend"}`)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	cfg := regionTestConfig(global.server.URL, us.server.URL, "")
	helper := httptest.NewServer(NewHandler(cfg))
	t.Cleanup(helper.Close)
	resp := postRegion(t, helper.URL, vertexRequestPath("global", "gemini-4-argon", ""), `{"n":1}`, regionToken)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, `{"error":"backend"}`, string(got))
	assert.Equal(t, 1, global.count())
	assert.Equal(t, 0, us.count())
}

func TestAbsoluteFormStaysClosedWhenFailoverIsOn(t *testing.T) {
	global := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	us := newCaptureServer(t, func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
		w.WriteHeader(http.StatusOK)
	})
	handler := NewHandler(regionTestConfig(global.server.URL, us.server.URL, ""))
	req := httptest.NewRequest(http.MethodPost, "http://evil.example/steal", strings.NewReader(`{"n":1}`))
	req.RequestURI = "http://evil.example/steal"
	req.URL.Host = "evil.example"
	req.Header.Set("Authorization", regionToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, 0, global.count())
	assert.Equal(t, 0, us.count())
	assert.NotContains(t, recorder.Body.String(), "ya29.wire-test-token")
}

func TestOfficialRegionHostsStayPaired(t *testing.T) {
	handler := NewHandler(Config{
		Upstream:       "https://aiplatform.googleapis.com",
		Delay:          time.Millisecond,
		DelayCap:       time.Millisecond,
		FirstValid:     time.Second,
		Idle:           time.Second,
		RegionFailover: true,
		RegionTargets:  []string{"eu", "us"},
		RegionModels:   []string{"gemini-4-argon", "gemini-4-carbon", "gemini-4-hydrogen"},
	}).(*handler)
	req := httptest.NewRequest(http.MethodPost, "http://helper"+vertexRequestPath("global", "gemini-4-argon", "alt=sse"), strings.NewReader("{}"))
	plan := handler.planRegion(req, req.URL.Path, req.URL.RawQuery)
	require.True(t, plan.eligible)
	assert.Equal(t, "eu", plan.region)
	assert.Equal(t, regionHostEU, plan.host)
	assert.Equal(t, "https://aiplatform.eu.rep.googleapis.com/v1/projects/vertex-project/locations/eu/publishers/google/models/gemini-4-argon:streamGenerateContent?alt=sse", plan.rawURL)

	req = httptest.NewRequest(http.MethodPost, "http://helper"+vertexRequestPath("eu", "gemini-4-hydrogen", ""), strings.NewReader("{}"))
	plan = handler.planRegion(req, req.URL.Path, req.URL.RawQuery)
	require.True(t, plan.eligible)
	assert.Equal(t, "us", plan.region)
	assert.Equal(t, regionHostUS, plan.host)
	assert.Contains(t, plan.rawURL, "/locations/us/publishers/google/models/gemini-4-hydrogen:generateContent")

	req = httptest.NewRequest(http.MethodPost, "http://helper"+vertexRequestPath("global", "gemini-2.5-flash", ""), strings.NewReader("{}"))
	assert.False(t, handler.planRegion(req, req.URL.Path, req.URL.RawQuery).eligible)

	blocked := "/v1/projects/vertex-project/locations/europe-west2/publishers/google/models/gemini-4-argon:generateContent"
	req = httptest.NewRequest(http.MethodPost, "http://helper"+blocked, strings.NewReader("{}"))
	assert.False(t, handler.planRegion(req, req.URL.Path, req.URL.RawQuery).eligible)
	blocked = "/v1/projects/vertex-project/locations/asia-southeast1/publishers/google/models/gemini-4-argon:generateContent"
	req = httptest.NewRequest(http.MethodPost, "http://helper"+blocked, strings.NewReader("{}"))
	assert.False(t, handler.planRegion(req, req.URL.Path, req.URL.RawQuery).eligible)
	blocked = "/v1/projects/vertex-project/locations/us-central1/publishers/google/models/gemini-4-argon:generateContent"
	req = httptest.NewRequest(http.MethodPost, "http://helper"+blocked, strings.NewReader("{}"))
	assert.False(t, handler.planRegion(req, req.URL.Path, req.URL.RawQuery).eligible)

	handler.cfg.testRegionHosts = map[string]string{}
	req = httptest.NewRequest(http.MethodPost, "http://helper"+vertexRequestPath("global", "gemini-4-argon", ""), strings.NewReader("{}"))
	assert.False(t, handler.planRegion(req, req.URL.Path, req.URL.RawQuery).eligible)
}

func TestRegionFlagsRejectUnapprovedTargets(t *testing.T) {
	targets, err := parseRegionTargets("us, eu")
	require.NoError(t, err)
	assert.Equal(t, []string{"us", "eu"}, targets)
	models, err := parseRegionModels("gemini-4-hydrogen, gemini-4-carbon, gemini-4-argon")
	require.NoError(t, err)
	assert.Equal(t, []string{"gemini-4-hydrogen", "gemini-4-carbon", "gemini-4-argon"}, models)

	base := []string{"-listen", "127.0.0.1:30610", "-upstream", "https://aiplatform.googleapis.com"}
	rejected := [][]string{
		{"-region-failover", "-region-targets", "us-central1", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "europe-west2", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "asia-southeast1", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "global", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "aiplatform.us.rep.googleapis.com", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "us"},
		{"-region-failover", "-region-models", "gemini-4-argon"},
		{"-region-failover", "-region-targets", "us,eu", "-region-models", "gemini-4-argon", "-region-body-budget", "0"},
		{"-region-failover", "-region-targets", "us", "-region-models", "../secret"},
		{"-region-targets", "us-central1"},
	}
	for _, extra := range rejected {
		var buf bytes.Buffer
		args := append(append([]string{}, base...), extra...)
		assert.Equal(t, 2, run(args, &buf), buf.String())
	}
}

type regionHit struct {
	host       string
	path       string
	rawPath    string
	rawQuery   string
	requestURI string
	auth       string
	project    string
	apiKey     string
	proxyAuth  string
	body       string
	when       time.Time
}

type captureServer struct {
	server *httptest.Server
	mu     sync.Mutex
	seen   []regionHit
}

func newCaptureServer(t *testing.T, respond func(http.ResponseWriter, *http.Request, *regionHit)) *captureServer {
	t.Helper()
	cs := &captureServer{}
	cs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		hit := regionHit{
			host:       r.Host,
			path:       r.URL.Path,
			rawPath:    r.URL.RawPath,
			rawQuery:   r.URL.RawQuery,
			requestURI: r.RequestURI,
			auth:       r.Header.Get("Authorization"),
			project:    r.Header.Get("X-Goog-User-Project"),
			apiKey:     r.Header.Get("X-Goog-Api-Key"),
			proxyAuth:  r.Header.Get("Proxy-Authorization"),
			body:       string(body),
			when:       time.Now(),
		}
		cs.mu.Lock()
		cs.seen = append(cs.seen, hit)
		cs.mu.Unlock()
		respond(w, r, &hit)
	}))
	t.Cleanup(cs.server.Close)
	return cs
}

func (c *captureServer) hits() []regionHit {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]regionHit, len(c.seen))
	copy(out, c.seen)
	return out
}

func (c *captureServer) count() int {
	return len(c.hits())
}

func newRegionServers(t *testing.T, globalStatus int, globalBody string, usStatus int, usBody string, euStatus int, euBody string) (*captureServer, *captureServer, *captureServer) {
	t.Helper()
	write := func(status int, body string) func(http.ResponseWriter, *http.Request, *regionHit) {
		return func(w http.ResponseWriter, r *http.Request, hit *regionHit) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
	return newCaptureServer(t, write(globalStatus, globalBody)),
		newCaptureServer(t, write(usStatus, usBody)),
		newCaptureServer(t, write(euStatus, euBody))
}

func regionTestConfig(globalURL, usURL, euURL string) Config {
	hosts := map[string]string{}
	targets := make([]string, 0, 2)
	if usURL != "" {
		hosts["us"] = mustHost(nil, usURL)
		targets = append(targets, "us")
	}
	if euURL != "" {
		hosts["eu"] = mustHost(nil, euURL)
		targets = append(targets, "eu")
	}
	return Config{
		Upstream:         globalURL,
		Delay:            20 * time.Millisecond,
		DelayCap:         20 * time.Millisecond,
		FirstValid:       time.Second,
		Idle:             time.Second,
		HeaderTimeout:    2 * time.Second,
		RegionFailover:   true,
		RegionTargets:    targets,
		RegionModels:     []string{"gemini-4-argon", "gemini-4-carbon", "gemini-4-hydrogen"},
		RegionBodyBudget: 1 << 20,
		RegionDeadline:   2 * time.Second,
		testRegionHosts:  hosts,
	}
}

func mustHost(t *testing.T, raw string) string {
	if t != nil {
		t.Helper()
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return parsed.Host
}

func vertexRequestPath(location, model, query string) string {
	action := "generateContent"
	if strings.Contains(query, "alt=sse") {
		action = "streamGenerateContent"
	}
	path := "/v1/projects/vertex-project/locations/" + location + "/publishers/google/models/" + model + ":" + action
	if query != "" {
		path += "?" + query
	}
	return path
}

func regionRequest(t *testing.T, helperURL, path, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, helperURL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", regionToken)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func chunkedRequest(t *testing.T, helperURL, path, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, helperURL+path, unsizedReader{strings.NewReader(body)})
	require.NoError(t, err)
	req.Header.Set("Authorization", regionToken)
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = -1
	return req
}

type unsizedReader struct{ r io.Reader }

func (u unsizedReader) Read(p []byte) (int, error) { return u.r.Read(p) }

func postRegion(t *testing.T, helperURL, path, body, token string) *http.Response {
	t.Helper()
	req := regionRequest(t, helperURL, path, body)
	if token == "" {
		req.Header.Del("Authorization")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

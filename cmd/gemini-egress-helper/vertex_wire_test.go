package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relay/channel/vertex"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVertexGlobalServiceAccountUsesBaseURLAndTwoDials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service.InitHttpClient()
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("ALL_PROXY", "")

	const (
		projectID = "vertex-project"
		token     = "ya29.wire-test-token"
		channelID = 910041
	)
	payload := `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`
	var mu sync.Mutex
	type hit struct {
		at       time.Time
		host     string
		path     string
		rawQuery string
		auth     string
		project  string
		body     string
	}
	var hits []hit
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		hits = append(hits, hit{
			at: time.Now(), host: r.Host, path: r.URL.Path, rawQuery: r.URL.RawQuery,
			auth: r.Header.Get("Authorization"), project: r.Header.Get("X-Goog-User-Project"), body: string(body),
		})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"status":"RESOURCE_EXHAUSTED"}}`)
	}))
	t.Cleanup(fake.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: fake.URL, Delay: defaultDelay, DelayCap: defaultDelayCap,
		FirstValid: time.Second, Idle: time.Second,
	}))
	t.Cleanup(helper.Close)

	info := vertexRelayInfo(t, channelID, projectID, helper.URL, false)
	vertex.Cache.SetDefault("access-token-910041", token)
	cached, err := vertex.Cache.Get("access-token-910041")
	require.NoError(t, err)
	require.Equal(t, token, cached)
	adaptor := &vertex.Adaptor{}
	adaptor.Init(info)

	direct := *info
	direct.ChannelBaseUrl = ""
	directURL, err := adaptor.GetRequestURL(&direct)
	require.NoError(t, err)
	assert.Equal(t, "https://aiplatform.googleapis.com/v1/projects/vertex-project/locations/global/publishers/google/models/gemini-4-argon:generateContent", directURL)

	info.ChannelBaseUrl = helper.URL
	gotURL, err := adaptor.GetRequestURL(info)
	require.NoError(t, err)
	assert.Equal(t, helper.URL+"/v1/projects/vertex-project/locations/global/publishers/google/models/gemini-4-argon:generateContent", gotURL)

	const retryTimes = 1
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request.Header.Set("Content-Type", "application/json")
	started := time.Now()
	for retry := 0; retry <= retryTimes; retry++ {
		c.Request.Body = io.NopCloser(strings.NewReader(payload))
		c.Request.ContentLength = int64(len(payload))
		raw, err := adaptor.DoRequest(c, info, strings.NewReader(payload))
		require.NoError(t, err)
		resp, ok := raw.(*http.Response)
		require.True(t, ok)
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		_, _ = io.Copy(io.Discard, resp.Body)
		require.NoError(t, resp.Body.Close())
		if retry == retryTimes {
			break
		}
	}
	elapsed := time.Since(started)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, hits, 2)
	assert.GreaterOrEqual(t, hits[1].at.Sub(hits[0].at), 2*time.Second)
	assert.Less(t, hits[1].at.Sub(hits[0].at), 3500*time.Millisecond)
	assert.GreaterOrEqual(t, elapsed, 4*time.Second, "the final 429 is held as well")
	assert.Less(t, elapsed, 6*time.Second)
	fakeHost := strings.TrimPrefix(fake.URL, "http://")
	for _, item := range hits {
		assert.Equal(t, fakeHost, item.host)
		assert.NotEqual(t, strings.TrimPrefix(helper.URL, "http://"), item.host)
		assert.Equal(t, "/v1/projects/vertex-project/locations/global/publishers/google/models/gemini-4-argon:generateContent", item.path)
		assert.Empty(t, item.rawQuery)
		assert.Equal(t, "Bearer "+token, item.auth)
		assert.Equal(t, projectID, item.project)
		assert.Equal(t, payload, item.body)
	}
}

func TestVertexStreamCloseAfter200DoesNotDialAgain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service.InitHttpClient()
	if constant.StreamingTimeout <= 0 {
		constant.StreamingTimeout = 30
		t.Cleanup(func() { constant.StreamingTimeout = 0 })
	}
	const channelID = 910042
	var hits int
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
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
	t.Cleanup(fake.Close)
	helper := httptest.NewServer(NewHandler(Config{
		Upstream: fake.URL, Delay: time.Second, DelayCap: time.Second,
		FirstValid: 120 * time.Millisecond, Idle: 120 * time.Millisecond,
	}))
	t.Cleanup(helper.Close)
	vertex.Cache.SetDefault("access-token-910042", "ya29.wire-test-token")

	info := vertexRelayInfo(t, channelID, "vertex-project", helper.URL, true)
	adaptor := &vertex.Adaptor{}
	adaptor.Init(info)
	streamURL, err := adaptor.GetRequestURL(info)
	require.NoError(t, err)
	assert.Equal(t, helper.URL+"/v1/projects/vertex-project/locations/global/publishers/google/models/gemini-4-argon:streamGenerateContent?alt=sse", streamURL)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"contents":[]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	info.DisablePing = true
	raw, err := adaptor.DoRequest(c, info, strings.NewReader(`{"contents":[]}`))
	require.NoError(t, err)
	resp, ok := raw.(*http.Response)
	require.True(t, ok)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, apiErr := adaptor.DoResponse(c, resp, info)
	assert.Nil(t, apiErr)
	assert.Equal(t, 1, hits)
}

func vertexRelayInfo(t *testing.T, channelID int, projectID, baseURL string, stream bool) *relaycommon.RelayInfo {
	t.Helper()
	return &relaycommon.RelayInfo{
		IsStream:        stream,
		OriginModelName: "gemini-4-argon",
		DisablePing:     true,
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       constant.ChannelTypeVertexAi,
			ChannelId:         channelID,
			ChannelBaseUrl:    baseURL,
			ApiVersion:        `{"default":"global"}`,
			ApiKey:            `{"project_id":"` + projectID + `","client_email":"sa@example.iam.gserviceaccount.com","private_key":"unused"}`,
			UpstreamModelName: "gemini-4-argon",
			ChannelOtherSettings: dto.ChannelOtherSettings{
				VertexKeyType: dto.VertexKeyTypeJSON,
			},
		},
	}
}

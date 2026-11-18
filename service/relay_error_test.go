package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	kitdto "github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/system_setting"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestShouldRetryRelayErrorHonorsChannelPinOnChannelError(t *testing.T) {
	err := types.NewError(errors.New("channel failed"), types.ErrorCodeChannelNoAvailableKey)
	for _, test := range []struct {
		name      string
		pin       *dto.ChannelPin
		wantRetry bool
	}{
		{name: "unrestricted channel error", wantRetry: true},
		{
			name: "single attempt pin suppresses channel error retry",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt,
			},
			wantRetry: false,
		},
		{
			name: "origin task pin permits retry on the same channel",
			pin: &dto.ChannelPin{
				ChannelId: 1, Source: dto.PinSourceOriginTask, Rank: dto.PinRankOriginTask, RetryMode: dto.PinRetrySameChannel,
			},
			wantRetry: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if test.pin != nil {
				GetChannelConstraints(c).AddPin(*test.pin)
			}
			assert.Equal(t, test.wantRetry, ShouldRetryRelayError(c, err, 1))
		})
	}
}

func TestProcessChannelErrorMasksDisableReasonAndNotification(t *testing.T) {
	previousDB, previousType := model.DB, common.MainDatabaseType()
	previousCache, previousRedis := common.MemoryCacheEnabled, common.RedisEnabled
	previousAutoDisable, previousErrorLog := common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled
	previousNotifyLimit := constant.NotifyLimitCount
	previousClient, previousWorker := httpClient, system_setting.WorkerUrl
	fetch := system_setting.GetFetchSetting()
	previousFetch := *fetch
	t.Cleanup(func() {
		model.DB = previousDB
		common.SetMainDatabaseType(previousType)
		common.MemoryCacheEnabled, common.RedisEnabled = previousCache, previousRedis
		common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = previousAutoDisable, previousErrorLog
		constant.NotifyLimitCount = previousNotifyLimit
		httpClient, system_setting.WorkerUrl = previousClient, previousWorker
		*fetch = previousFetch
	})
	database, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := database.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, database.AutoMigrate(&model.Channel{}, &model.Ability{}, &model.User{}))
	model.DB = database
	common.SetMainDatabaseType(common.DatabaseTypeSQLite)
	common.MemoryCacheEnabled, common.RedisEnabled = false, false
	common.AutomaticDisableChannelEnabled, constant.ErrorLogEnabled = true, false
	constant.NotifyLimitCount = 10
	channel := &model.Channel{Name: "relay-review", Key: "fixture-key", Type: 1, Status: common.ChannelStatusEnabled, Group: "default", Models: "test-model"}
	require.NoError(t, channel.Insert())
	notifications := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		notifications <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	httpClient, system_setting.WorkerUrl = server.Client(), ""
	fetch.EnableSSRFProtection = false
	settings, err := common.Marshal(kitdto.UserSetting{NotifyType: kitdto.NotifyTypeWebhook, WebhookUrl: server.URL})
	require.NoError(t, err)
	root := &model.User{Username: "notification-test-root", Role: common.RoleRootUser, Status: common.UserStatusEnabled, Setting: string(settings)}
	require.NoError(t, database.Create(root).Error)
	notifyKey := fmt.Sprintf("%d:%s:%s", root.Id, formatNotifyType(channel.Id, common.ChannelStatusAutoDisabled), time.Now().Format("2006010215"))
	notifyLimitStore.Delete(notifyKey)
	t.Cleanup(func() { notifyLimitStore.Delete(notifyKey) })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, "https://private.example.com")
	apiErr := types.NewErrorWithStatusCode(errors.New("upstream https://private.example.com/path?token=review-token api_key:review-secret"), types.ErrorCodeChannelNoAvailableKey, http.StatusUnauthorized)
	ProcessChannelError(c, types.ChannelError{ChannelId: channel.Id, ChannelName: channel.Name, AutoBan: true}, apiErr, nil)
	var notification WebhookPayload
	select {
	case payload := <-notifications:
		require.NoError(t, common.Unmarshal(payload, &notification))
	case <-time.After(5 * time.Second):
		t.Fatal("automatic channel-disable notification was not delivered")
	}
	loaded, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	assert.Equal(t, common.ChannelStatusAutoDisabled, loaded.Status)
	wantReason := "status_code=401, upstream https://***.com/***?token=*** api_key:***"
	assert.Equal(t, wantReason, loaded.GetOtherInfo()["status_reason"])
	assert.Contains(t, notification.Content, wantReason)
	assert.NotContains(t, notification.Content, "review-token")
	assert.NotContains(t, notification.Content, "review-secret")
	assert.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
}

// Only addresses the admin configured are masked: a channel on the base URL
// new-api presets reaches a public official endpoint, and so do adaptors that
// build official endpoints themselves.
func TestUpstreamAddressesMaskOnlyConfiguredAddresses(t *testing.T) {
	for _, tc := range []struct {
		name        string
		channelType int
		baseURL     string
		origin      string
		proxyEnv    string
		message     string
		want        string
	}{
		{
			name:        "preset base URL stays readable",
			channelType: constant.ChannelTypeAnthropic,
			baseURL:     "https://api.anthropic.com",
			origin:      "https://api.anthropic.com",
			message:     "request to https://api.anthropic.com/v1/messages failed, see https://platform.claude.com/docs/en/build-with-claude/task-budgets",
			want:        "request to https://api.anthropic.com/v1/messages failed, see https://platform.claude.com/docs/en/build-with-claude/task-budgets",
		},
		{
			name:        "official endpoint built by the adaptor stays readable",
			channelType: constant.ChannelTypeVertexAi,
			origin:      "https://us-central1-aiplatform.googleapis.com",
			message:     "request to https://us-central1-aiplatform.googleapis.com/v1/projects failed",
			want:        "request to https://us-central1-aiplatform.googleapis.com/v1/projects failed",
		},
		{
			name:        "configured base URL is masked",
			channelType: constant.ChannelTypeOpenAI,
			baseURL:     "https://relay.private.example",
			origin:      "https://relay.private.example",
			message:     "request to https://relay.private.example/v1/chat/completions failed",
			want:        "request to https://***.example/***/***/*** failed",
		},
		{
			name:        "absolute Advanced Custom route is masked",
			channelType: constant.ChannelTypeAdvancedCustom,
			origin:      "https://route.private.example",
			message:     "dial tcp: lookup route.private.example: no such host",
			want:        "dial tcp: lookup ***.***.example: no such host",
		},
		{
			name:        "environment proxy is masked on a preset channel",
			channelType: constant.ChannelTypeAnthropic,
			baseURL:     "https://api.anthropic.com",
			proxyEnv:    "http://egress.private.invalid:8080",
			message:     "proxyconnect tcp: dial tcp: lookup egress.private.invalid: no such host",
			want:        "proxyconnect tcp: dial tcp: lookup ***.***.invalid: no such host",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HTTPS_PROXY", tc.proxyEnv)
			t.Setenv("HTTP_PROXY", "")
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			common.SetContextKey(c, constant.ContextKeyChannelType, tc.channelType)
			common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, tc.baseURL)
			common.SetContextKey(c, constant.ContextKeyUpstreamRequestOrigin, tc.origin)
			apiErr := types.NewErrorWithStatusCode(errors.New(tc.message), types.ErrorCodeBadResponseStatusCode, http.StatusBadGateway)

			assert.Equal(t, tc.want, apiErr.ToOpenAIError(common.UpstreamAddresses(c)...).Message)
		})
	}
}

func TestDecideRelayRetryReasons(t *testing.T) {
	upstream := func(status int) *types.NewAPIError {
		return types.NewOpenAIError(errors.New("upstream"), types.ErrorCodeBadResponseStatusCode, status)
	}
	for _, tc := range []struct {
		name    string
		err     *types.NewAPIError
		retries int
		setup   func(*gin.Context)
		want    PolicyDecision
	}{
		{name: "retry status matched", err: upstream(http.StatusTooManyRequests), retries: 1, want: PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}},
		{name: "status outside retry rules", err: upstream(http.StatusBadRequest), retries: 1, want: PolicyDecision{Action: "stop", Reason: "status_not_retryable", Source: "global"}},
		{name: "attempt budget exhausted", err: upstream(http.StatusTooManyRequests), retries: 0, want: PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}},
		{name: "always skipped status", err: upstream(http.StatusGatewayTimeout), retries: 1, want: PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}},
		{name: "success status never retries", err: upstream(http.StatusOK), retries: 1, want: PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}},
		{name: "skip retry error", err: types.NewErrorWithStatusCode(errors.New("local"), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry()), retries: 1, want: PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}},
		{name: "channel error retries without budget", err: types.NewError(errors.New("no key"), types.ErrorCodeChannelNoAvailableKey), retries: 0, want: PolicyDecision{Action: "retry", Reason: "channel_error", Source: "system"}},
		{name: "single attempt pin", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			GetChannelConstraints(c).AddPin(dto.ChannelPin{ChannelId: 1, Source: dto.PinSourceToken, Rank: dto.PinRankToken, RetryMode: dto.PinRetrySingleAttempt})
		}, want: PolicyDecision{Action: "stop", Reason: "pinned_channel", Source: "channel_constraint"}},
		{name: "strict session", err: upstream(http.StatusTooManyRequests), retries: 1, setup: func(c *gin.Context) {
			c.Set(ginKeyChannelAffinitySkipRetry, true)
			RequestPolicy(c).SessionModeSource = "global"
		}, want: PolicyDecision{Action: "stop", Reason: "strict_session", Source: "global"}},
		{name: "nil error", retries: 1, want: PolicyDecision{Action: "stop", Reason: "request_completed", Source: "system"}},
		{name: "upstream stream already relayed", err: upstream(http.StatusInternalServerError), retries: 1, setup: func(c *gin.Context) {
			MarkRelayStreamStarted(c)
		}, want: PolicyDecision{Action: "stop", Reason: "response_committed", Source: "system"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if tc.setup != nil {
				tc.setup(c)
			}
			decision := DecideRelayRetry(c, tc.err, tc.retries)
			assert.Equal(t, tc.want, decision)
			assert.Equal(t, tc.want.Action == "retry", ShouldRetryRelayError(c, tc.err, tc.retries))
		})
	}
}

func TestRelayRetryDelayStaysInsideGemini429Scope(t *testing.T) {
	previousInitial, previousCap := geminiRetryBackoffInitial, geminiRetryBackoffCap
	geminiRetryBackoffInitial = 2 * time.Second
	geminiRetryBackoffCap = 30 * time.Second
	t.Cleanup(func() {
		geminiRetryBackoffInitial = previousInitial
		geminiRetryBackoffCap = previousCap
	})
	newContext := func() *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		return c
	}
	withRetryAfter := func(delay time.Duration, present bool) *types.NewAPIError {
		apiErr := types.NewOpenAIError(errors.New("RESOURCE_EXHAUSTED"), types.ErrorCodeBadResponseStatusCode, http.StatusTooManyRequests)
		if present {
			apiErr.SetRetryAfter(delay)
		}
		return apiErr
	}
	c := newContext()
	assert.Equal(t, 2*time.Second, RelayRetryDelay(c, constant.ChannelTypeGemini, withRetryAfter(0, false)))
	assert.Equal(t, 2*time.Second, RelayRetryDelay(c, constant.ChannelTypeVertexAi, withRetryAfter(0, false)))
	assert.Equal(t, time.Duration(0), RelayRetryDelay(c, constant.ChannelTypeOpenAI, withRetryAfter(9*time.Second, true)))
	assert.Equal(t, time.Duration(0), RelayRetryDelay(c, constant.ChannelTypeGemini, types.NewOpenAIError(errors.New("upstream"), types.ErrorCodeBadResponseStatusCode, http.StatusInternalServerError)))
	assert.Equal(t, time.Second, RelayRetryDelay(c, constant.ChannelTypeVertexAi, withRetryAfter(time.Second, true)))
	assert.Equal(t, time.Duration(0), RelayRetryDelay(c, constant.ChannelTypeGemini, withRetryAfter(0, true)))
	assert.Equal(t, 30*time.Second, RelayRetryDelay(c, constant.ChannelTypeGemini, withRetryAfter(2*time.Minute, true)))

	configured := newContext()
	setChannelAffinityContext(configured, channelAffinityMeta{RetryBackoffSeconds: 5, RetryBackoffCapSeconds: 10})
	assert.Equal(t, 5*time.Second, RelayRetryDelay(configured, constant.ChannelTypeOpenAI, withRetryAfter(0, false)))
	assert.Equal(t, 10*time.Second, RelayRetryDelay(configured, constant.ChannelTypeGemini, withRetryAfter(50*time.Second, true)))
	disabled := newContext()
	setChannelAffinityContext(disabled, channelAffinityMeta{RetryBackoffDisabled: true})
	assert.Equal(t, time.Duration(0), RelayRetryDelay(disabled, constant.ChannelTypeVertexAi, withRetryAfter(2*time.Minute, true)))

	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header:     http.Header{"Retry-After": []string{"4"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"RESOURCE_EXHAUSTED","type":"rate_limit"}}`)),
	}
	parsed := RelayErrorHandler(context.Background(), resp, false)
	delay, ok := parsed.RetryAfter()
	require.True(t, ok)
	assert.Equal(t, 4*time.Second, delay)
	assert.Equal(t, 4*time.Second, RelayRetryDelay(c, constant.ChannelTypeGemini, parsed))
	assert.Equal(t, time.Duration(0), RelayRetryDelay(c, constant.ChannelTypeOpenAI, parsed))

	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	dateDelay, dateOK := parseRetryAfter(now.Add(5*time.Second).UTC().Format(http.TimeFormat), now)
	require.True(t, dateOK)
	assert.Equal(t, 5*time.Second, dateDelay)
	pastDelay, pastOK := parseRetryAfter(now.Add(-5*time.Second).UTC().Format(http.TimeFormat), now)
	require.True(t, pastOK)
	assert.Equal(t, time.Duration(0), pastDelay)
	_, badOK := parseRetryAfter("soon", now)
	assert.False(t, badOK)
}

func TestGemini429WaitsOnceThenAllowsOneFallback(t *testing.T) {
	previousInitial, previousCap := geminiRetryBackoffInitial, geminiRetryBackoffCap
	geminiRetryBackoffInitial = 40 * time.Millisecond
	geminiRetryBackoffCap = 30 * time.Second
	t.Cleanup(func() {
		geminiRetryBackoffInitial = previousInitial
		geminiRetryBackoffCap = previousCap
	})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	apiErr := types.NewOpenAIError(errors.New("RESOURCE_EXHAUSTED"), types.ErrorCodeBadResponseStatusCode, http.StatusTooManyRequests)
	const retryTimes = 1
	attempts := 0
	waits := 0
	for retry := 0; retry <= retryTimes; retry++ {
		attempts++
		decision := DecideRelayRetry(c, apiErr, retryTimes-retry)
		waited, err := WaitBeforeRelayRetry(c, constant.ChannelTypeVertexAi, apiErr, decision)
		require.NoError(t, err)
		if waited > 0 {
			waits++
		}
		if decision.Action != "retry" {
			assert.Equal(t, time.Duration(0), waited)
			break
		}
	}
	assert.Equal(t, 2, attempts, "RetryTimes=1 is the original attempt plus one fallback")
	assert.Equal(t, 1, waits)
}

func TestSkipRetryAndCancellationDoNotSleepThroughBackoff(t *testing.T) {
	previousInitial := geminiRetryBackoffInitial
	geminiRetryBackoffInitial = 5 * time.Second
	t.Cleanup(func() { geminiRetryBackoffInitial = previousInitial })
	apiErr := types.NewOpenAIError(errors.New("RESOURCE_EXHAUSTED"), types.ErrorCodeBadResponseStatusCode, http.StatusTooManyRequests)

	skipped, _ := gin.CreateTestContext(httptest.NewRecorder())
	skipped.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	skipped.Set(ginKeyChannelAffinitySkipRetry, true)
	RequestPolicy(skipped).SessionModeSource = "session_rule"
	decision := DecideRelayRetry(skipped, apiErr, 1)
	require.Equal(t, "stop", decision.Action)
	started := time.Now()
	waited, err := WaitBeforeRelayRetry(skipped, constant.ChannelTypeGemini, apiErr, decision)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), waited)
	assert.Less(t, time.Since(started), 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancelled, _ := gin.CreateTestContext(httptest.NewRecorder())
	cancelled.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	started = time.Now()
	waited, err = WaitBeforeRelayRetry(cancelled, constant.ChannelTypeGemini, apiErr, PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 5*time.Second, waited)
	assert.Less(t, time.Since(started), time.Second)

	recorder := httptest.NewRecorder()
	keepalive, _ := gin.CreateTestContext(recorder)
	keepalive.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, writeErr := keepalive.Writer.Write([]byte(": PING\n\n"))
	require.NoError(t, writeErr)
	require.True(t, keepalive.Writer.Written())
	require.True(t, DownstreamResponseCommitted(keepalive))
	assert.Equal(t, PolicyDecision{Action: "stop", Reason: "response_committed", Source: "system"}, DecideRelayRetry(keepalive, apiErr, 1))
	waited, err = WaitBeforeRelayRetry(keepalive, constant.ChannelTypeGemini, apiErr, DecideRelayRetry(keepalive, apiErr, 1))
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), waited)

	upgraded, _ := gin.CreateTestContext(httptest.NewRecorder())
	upgraded.Request = httptest.NewRequest(http.MethodGet, "/v1/realtime", nil)
	upgraded.Writer.WriteHeader(http.StatusSwitchingProtocols)
	upgraded.Writer.WriteHeaderNow()
	require.True(t, upgraded.Writer.Written())
	require.Equal(t, http.StatusSwitchingProtocols, upgraded.Writer.Status())
	require.False(t, DownstreamResponseCommitted(upgraded))
	assert.Equal(t, "retry", DecideRelayRetry(upgraded, apiErr, 1).Action)
}

func TestMatchedAffinityRuleCarriesRetryBackoff(t *testing.T) {
	setting := operation_setting.GetChannelAffinitySetting()
	previous := *setting
	t.Cleanup(func() { *setting = previous })
	setting.Enabled = true
	setting.Rules = []operation_setting.ChannelAffinityRule{{
		Name:                   "gemini argon",
		ModelRegex:             []string{"^gemini-4-argon$"},
		KeySources:             []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session"}},
		RetryBackoffSeconds:    5,
		RetryBackoffCapSeconds: 10,
	}}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request.Header.Set("X-Session", "session-a")
	_, _ = GetPreferredChannelByAffinity(c, "gemini-4-argon", "default")
	*setting = previous
	meta, ok := getChannelAffinityMeta(c)
	require.True(t, ok)
	assert.Equal(t, 5, meta.RetryBackoffSeconds)
	assert.Equal(t, 10, meta.RetryBackoffCapSeconds)
	assert.False(t, meta.RetryBackoffDisabled)
	apiErr := types.NewOpenAIError(errors.New("RESOURCE_EXHAUSTED"), types.ErrorCodeBadResponseStatusCode, http.StatusTooManyRequests)
	apiErr.SetRetryAfter(50 * time.Second)
	assert.Equal(t, 10*time.Second, RelayRetryDelay(c, constant.ChannelTypeOpenAI, apiErr))
}

func TestRequestPolicyEventsReachLogAdminInfo(t *testing.T) {
	previousAutoDisable := common.AutomaticDisableChannelEnabled
	common.AutomaticDisableChannelEnabled = true
	t.Cleanup(func() { common.AutomaticDisableChannelEnabled = previousAutoDisable })
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Set("auto_ban", true)
	c.Set("channel_id", 7)
	state := RequestPolicy(c)
	state.BeginAttempt(&model.Channel{Id: 7}, "default")
	apiErr := types.NewOpenAIError(errors.New("invalid credential"), types.ErrorCodeBadResponseStatusCode, http.StatusUnauthorized)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))

	failed := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, failed)
	events, ok := failed.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a failed relay exposes its decision events to administrators")
	require.Len(t, events, 3)
	assert.Equal(t, PolicyDecision{Action: "attempt", Reason: "channel_selected", Source: "routing"}, events[0].Decision)
	assert.Equal(t, "default", events[0].Group)
	assert.Equal(t, PolicyDecision{Action: "failure", Reason: "upstream_failure", Source: "upstream"}, events[1].Decision)
	assert.Equal(t, http.StatusUnauthorized, events[1].Status)
	assert.Equal(t, PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}, events[2].Decision)
	assert.Equal(t, "channel_disable_requested", events[2].Health, "the health entry follows the automatic disable rules")
	common.SetContextKey(c, constant.ContextKeyChannelIsMultiKey, true)
	RecordPolicyFailure(c, 7, apiErr, DecideRelayRetry(c, apiErr, 0))
	assert.Equal(t, "key_disable_requested", state.Events()[4].Health)

	state.BeginAttempt(&model.Channel{Id: 8}, "default")
	c.Set("channel_id", 8)
	MarkRequestPolicySuccess(c, nil)
	MarkRequestPolicySuccess(c, nil)
	succeeded := model.NewLogOther()
	AppendRelayLogAdminInfo(c, nil, succeeded)
	events, ok = succeeded.Snapshot()["admin_info"].(map[string]any)["request_policy"].([]PolicyEvent)
	require.True(t, ok, "a successful relay exposes its decision events to administrators")
	require.Len(t, events, 7, "the outcome is recorded once")
	assert.Equal(t, PolicyDecision{Action: "success", Reason: "request_completed", Source: "upstream"}, events[6].Decision)
	assert.Equal(t, 8, events[6].ChannelID)
	assert.Equal(t, 2, events[6].Attempt)
	assert.True(t, state.Successful)

	untouched, _ := gin.CreateTestContext(httptest.NewRecorder())
	other := model.NewLogOther()
	AppendRelayLogAdminInfo(untouched, nil, other)
	assert.NotContains(t, other.Snapshot()["admin_info"], "request_policy", "requests without decisions do not carry an empty record")
}

package service

import (
	"context"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
)

const ginKeyRelayStreamStarted = "relay_stream_started"

// Gemini and Vertex channels in this deployment share one upstream project, so
// an immediate failover hits the same exhausted window. Two seconds is enough
// to leave a short rate window and still returns the single fallback inside a
// normal client timeout. It is not a daily-quota recovery. Thirty seconds is
// the longest a Retry-After hint or an affinity override can hold the request;
// a longer hint is clamped rather than ignored or followed unbounded.
var (
	geminiRetryBackoffInitial = 2 * time.Second
	geminiRetryBackoffCap     = 30 * time.Second
)

// MarkRelayStreamStarted records that an upstream success stream is being
// relayed. A keepalive comment does not call this; a flushed ping is still a
// committed HTTP response and is reported by DownstreamResponseCommitted.
func MarkRelayStreamStarted(c *gin.Context) {
	if c == nil {
		return
	}
	c.Set(ginKeyRelayStreamStarted, true)
}

// RelayStreamStarted reports whether this request already began relaying an
// upstream success stream. A later upstream call would be a second provider
// request for a response the client has already been given.
func RelayStreamStarted(c *gin.Context) bool {
	if c == nil {
		return false
	}
	started, ok := c.Get(ginKeyRelayStreamStarted)
	if !ok {
		return false
	}
	value, ok := started.(bool)
	return ok && value
}

// DownstreamResponseCommitted reports that the client has already been sent
// an HTTP response. Header values staged for an event stream are not a
// commit; the first Write is, including a keepalive comment and no model
// payload. A websocket upgrade is status 101 and is not this commit, so a
// failed dial can still use the remaining attempt budget.
func DownstreamResponseCommitted(c *gin.Context) bool {
	if c == nil || c.Writer == nil || !c.Writer.Written() {
		return false
	}
	return c.Writer.Status() != http.StatusSwitchingProtocols
}

// DecideRelayRetry is the single retry decision for relay attempts. The reason
// is recorded in the request policy decision events of the log details.
func DecideRelayRetry(c *gin.Context, err *types.NewAPIError, retryTimes int) PolicyDecision {
	if err == nil {
		return PolicyDecision{Action: "stop", Reason: "request_completed", Source: "system"}
	}
	if RelayStreamStarted(c) || DownstreamResponseCommitted(c) {
		return PolicyDecision{Action: "stop", Reason: "response_committed", Source: "system"}
	}
	if ShouldSkipRetryAfterChannelAffinityFailure(c) {
		source := RequestPolicy(c).SessionModeSource
		if source == "" {
			source = "session_rule"
		}
		return PolicyDecision{Action: "stop", Reason: "strict_session", Source: source}
	}
	if GetChannelConstraints(c).SuppressesRetry() {
		return PolicyDecision{Action: "stop", Reason: "pinned_channel", Source: "channel_constraint"}
	}
	if types.IsChannelError(err) {
		return PolicyDecision{Action: "retry", Reason: "channel_error", Source: "system"}
	}
	if types.IsSkipRetryError(err) {
		return PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"}
	}
	if retryTimes <= 0 {
		return PolicyDecision{Action: "stop", Reason: "attempt_budget_exhausted", Source: "global"}
	}
	code := err.StatusCode
	if code >= 200 && code < 300 {
		return PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}
	}
	if code < 100 || code > 599 {
		return PolicyDecision{Action: "retry", Reason: "unrecognized_status", Source: "system"}
	}
	if operation_setting.IsAlwaysSkipRetryCode(err.GetErrorCode()) || operation_setting.IsAlwaysSkipRetryStatusCode(code) {
		return PolicyDecision{Action: "stop", Reason: "system_retry_exclusion", Source: "system"}
	}
	if operation_setting.ShouldRetryByStatusCode(code) {
		return PolicyDecision{Action: "retry", Reason: "retry_status_matched", Source: "global"}
	}
	return PolicyDecision{Action: "stop", Reason: "status_not_retryable", Source: "global"}
}

func ShouldRetryRelayError(c *gin.Context, openaiErr *types.NewAPIError, retryTimes int) bool {
	return DecideRelayRetry(c, openaiErr, retryTimes).Action == "retry"
}

func geminiScopedChannel(channelType int) bool {
	return channelType == constant.ChannelTypeGemini || channelType == constant.ChannelTypeVertexAi
}

// RelayRetryDelay is the wait before one more attempt after a retryable 429.
// Gemini and Vertex use the built-in initial delay. A matched affinity rule
// can replace that initial delay, lower the cap, or disable the wait. Other
// channels wait only when a matched rule sets its own delay. A present
// Retry-After replaces the initial delay and is still clamped to the cap.
// The attempt budget is unchanged.
func RelayRetryDelay(c *gin.Context, channelType int, apiErr *types.NewAPIError) time.Duration {
	if apiErr == nil || apiErr.StatusCode != http.StatusTooManyRequests {
		return 0
	}
	cap := geminiRetryBackoffCap
	if cap <= 0 {
		cap = 30 * time.Second
	}
	initial := time.Duration(0)
	applies := geminiScopedChannel(channelType)
	if applies {
		initial = geminiRetryBackoffInitial
		if initial < 0 {
			initial = 0
		}
	}
	if meta, ok := getChannelAffinityMeta(c); ok {
		if meta.RetryBackoffDisabled {
			return 0
		}
		if meta.RetryBackoffCapSeconds > 0 {
			configured := time.Duration(meta.RetryBackoffCapSeconds) * time.Second
			if configured < cap {
				cap = configured
			}
		}
		if meta.RetryBackoffSeconds > 0 {
			applies = true
			initial = time.Duration(meta.RetryBackoffSeconds) * time.Second
		}
	}
	if !applies {
		return 0
	}
	if initial > cap {
		initial = cap
	}
	wait := initial
	if retryAfter, ok := apiErr.RetryAfter(); ok {
		if retryAfter < 0 {
			retryAfter = 0
		}
		wait = retryAfter
	}
	if wait > cap {
		return cap
	}
	return wait
}

// WaitBeforeRelayRetry waits for RelayRetryDelay when the decision is to
// retry. A decision that stops, including skip_retry_on_failure, returns
// immediately. Cancelling the request context interrupts the wait and skips
// the fallback attempt.
func WaitBeforeRelayRetry(c *gin.Context, channelType int, apiErr *types.NewAPIError, decision PolicyDecision) (time.Duration, error) {
	if decision.Action != "retry" {
		return 0, nil
	}
	delay := RelayRetryDelay(c, channelType, apiErr)
	if delay <= 0 {
		return 0, nil
	}
	status := 0
	if apiErr != nil {
		status = apiErr.StatusCode
	}
	logger.LogInfo(c, common.LogText("retry backoff before fallback: channel_type=%d status=%d wait=%s", channelType, status, delay))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	select {
	case <-ctx.Done():
		return delay, ctx.Err()
	case <-timer.C:
		return delay, nil
	}
}

func ProcessChannelError(c *gin.Context, channelError types.ChannelError, err *types.NewAPIError, relayInfo *relaycommon.RelayInfo) {
	if err == nil {
		return
	}
	upstreamURLs := common.UpstreamAddresses(c)
	logger.LogError(c, common.LogText("channel error (channel #%d, status code: %d): %s", channelError.ChannelId, err.StatusCode, common.LocalLogPreview(err.MaskSensitiveErrorWithStatusCode(upstreamURLs...))))
	if ShouldDisableChannel(err) && channelError.AutoBan {
		reason := err.MaskSensitiveErrorWithStatusCode(upstreamURLs...)
		gopool.Go(func() {
			DisableChannel(channelError, reason)
		})
	}

	if constant.ErrorLogEnabled && types.IsRecordErrorLog(err) {
		userId := c.GetInt("id")
		tokenName := c.GetString("token_name")
		modelName := c.GetString("original_model")
		tokenId := c.GetInt("token_id")
		userGroup := c.GetString("group")
		other := model.NewLogOther()
		if c.Request != nil && c.Request.URL != nil {
			other.SetPublic("request_path", c.Request.URL.Path)
		}
		other.SetPublic("error_type", err.GetErrorType())
		other.SetPublic("error_code", err.GetErrorCode())
		other.SetPublic("status_code", err.StatusCode)
		AppendRelayLogAdminInfo(c, relayInfo, other)
		AppendResponseModelLogInfo(relayInfo, other)
		AppendTaskPluginContextAuditInfo(c, other)
		startTime := common.GetContextKeyTime(c, constant.ContextKeyRequestStartTime)
		if startTime.IsZero() {
			startTime = time.Now()
		}
		useTimeSeconds := int(time.Since(startTime).Seconds())
		model.RecordErrorLog(c, userId, channelError.ChannelId, modelName, tokenName, err.MaskSensitiveErrorWithStatusCode(upstreamURLs...), tokenId, useTimeSeconds, common.GetContextKeyBool(c, constant.ContextKeyIsStream), userGroup, other)
	}
}

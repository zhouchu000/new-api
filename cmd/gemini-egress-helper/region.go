package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// Official Standard PayGo multi-region hosts. Single-region hosts such as
// us-central1 and the Argon ML-processing regions are intentionally absent.
const (
	regionHostUS = "aiplatform.us.rep.googleapis.com"
	regionHostEU = "aiplatform.eu.rep.googleapis.com"
)

type regionPlan struct {
	eligible bool
	region   string
	host     string
	rawURL   string
}

type vertexLocation struct {
	project  string
	location string
	model    string
	action   string
}

var vertexLocationPath = regexp.MustCompile(`^/v1/projects/([A-Za-z0-9._-]+)/locations/(global|us|eu)/publishers/google/models/([A-Za-z0-9._-]+):([A-Za-z0-9]+)$`)

var modelIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func parseFixedOrigin(raw string) (*urlUpstream, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errRejectedHost
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return nil, errRejectedHost
	}
	return &urlUpstream{scheme: parsed.Scheme, host: parsed.Host}, nil
}

func (h *handler) planRegion(r *http.Request, path, rawQuery string) regionPlan {
	if h == nil || !h.cfg.RegionFailover || h.upstream == nil {
		return regionPlan{}
	}
	// Channel 25's API key, in a query or header, is unverified on regional
	// hosts. Fail closed instead of sending it there.
	if r.Header.Get("X-Goog-Api-Key") != "" || queryCarriesAPIKey(rawQuery) {
		return regionPlan{}
	}
	parts, ok := parseVertexLocation(path)
	if !ok || !slices.Contains(h.cfg.RegionModels, parts.model) {
		return regionPlan{}
	}
	region, host, ok := h.nextRegion(parts.location)
	if !ok || !h.allowHost(host) {
		return regionPlan{}
	}
	rewritten := "/v1/projects/" + parts.project + "/locations/" + region + "/publishers/google/models/" + parts.model + ":" + parts.action
	rawURL := h.upstream.scheme + "://" + host + rewritten
	if rawQuery != "" {
		rawURL += "?" + rawQuery
	}
	return regionPlan{eligible: true, region: region, host: host, rawURL: rawURL}
}

func parseVertexLocation(path string) (vertexLocation, bool) {
	match := vertexLocationPath.FindStringSubmatch(path)
	if match == nil {
		return vertexLocation{}, false
	}
	return vertexLocation{project: match[1], location: match[2], model: match[3], action: match[4]}, true
}

func (h *handler) nextRegion(current string) (string, string, bool) {
	for _, code := range h.cfg.RegionTargets {
		if code == current || (code != "us" && code != "eu") {
			continue
		}
		host, ok := h.regionHost(code)
		if !ok || !safeDialHost(host) {
			continue
		}
		return code, host, true
	}
	return "", "", false
}

func (h *handler) regionHost(code string) (string, bool) {
	if h.cfg.testRegionHosts != nil {
		host, ok := h.cfg.testRegionHosts[code]
		return host, ok && host != ""
	}
	switch code {
	case "us":
		return regionHostUS, true
	case "eu":
		return regionHostEU, true
	default:
		return "", false
	}
}

func (h *handler) allowHost(host string) bool {
	if !safeDialHost(host) || h.upstream == nil {
		return false
	}
	if host == h.upstream.host {
		return true
	}
	if h.cfg.testRegionHosts != nil {
		for _, allowed := range h.cfg.testRegionHosts {
			if host == allowed {
				return true
			}
		}
		return false
	}
	return host == regionHostUS || host == regionHostEU
}

func safeDialHost(host string) bool {
	if host == "" || strings.ContainsAny(host, "/?#@ \t\r\n") {
		return false
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return true
	}
	return !strings.Contains(host, ":")
}

func queryCarriesAPIKey(rawQuery string) bool {
	if rawQuery == "" {
		return false
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return true
	}
	for name := range values {
		if strings.EqualFold(name, "key") {
			return true
		}
	}
	return false
}

func parseRegionTargets(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []string
	seen := map[string]bool{}
	for part := range strings.SplitSeq(raw, ",") {
		code := strings.TrimSpace(part)
		if code == "" {
			continue
		}
		if code != "us" && code != "eu" {
			return nil, fmt.Errorf("region %q is not an approved failover target", code)
		}
		if seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("region targets are empty")
	}
	return out, nil
}

func parseRegionModels(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []string
	seen := map[string]bool{}
	for part := range strings.SplitSeq(raw, ",") {
		model := strings.TrimSpace(part)
		if model == "" {
			continue
		}
		if len(model) > 128 || !modelIDPattern.MatchString(model) {
			return nil, fmt.Errorf("model %q is not an exact model id", model)
		}
		if seen[model] {
			continue
		}
		seen[model] = true
		out = append(out, model)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("region models are empty")
	}
	if len(out) > 16 {
		return nil, fmt.Errorf("too many region models")
	}
	return out, nil
}

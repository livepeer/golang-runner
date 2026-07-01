package liverunner

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

type createChannelsRequest struct {
	Channels []ChannelRequest `json:"channels"`
}

type createChannelsResponse struct {
	Channels []Channel `json:"channels"`
}

type deleteChannelsRequest struct {
	Channels []string `json:"channels"`
}

type deleteChannelsResponse struct {
	Deleted []string `json:"deleted"`
}

func (r *Runner) CreateTrickleChannels(ctx context.Context, req *http.Request, channels []ChannelRequest) ([]Channel, error) {
	return CreateTrickleChannels(ctx, r.client, r.OrchestratorURL(), r.RunnerID(), req, channels)
}

func (r *Runner) DeleteTrickleChannels(ctx context.Context, req *http.Request, channels []string) ([]string, error) {
	return DeleteTrickleChannels(ctx, r.client, r.OrchestratorURL(), r.RunnerID(), req, channels)
}

func CreateTrickleChannels(ctx context.Context, client HTTPDoer, orchestratorURL, runnerID string, req *http.Request, channels []ChannelRequest) ([]Channel, error) {
	for _, ch := range channels {
		if strings.TrimSpace(ch.Name) == "" || strings.TrimSpace(ch.MimeType) == "" {
			return nil, errors.New("trickle channel name and mime_type are required")
		}
	}
	endpoint, token, err := channelsEndpoint(orchestratorURL, runnerID, req)
	if err != nil {
		return nil, err
	}
	var resp createChannelsResponse
	if err := requestJSON(ctx, client, http.MethodPost, endpoint, createChannelsRequest{Channels: channels}, map[string]string{HeaderSessionToken: token}, defaultTimeout, &resp); err != nil {
		return nil, err
	}
	if resp.Channels == nil {
		return nil, errors.New("trickle channel create response missing channels")
	}
	return resp.Channels, nil
}

func DeleteTrickleChannels(ctx context.Context, client HTTPDoer, orchestratorURL, runnerID string, req *http.Request, channels []string) ([]string, error) {
	endpoint, token, err := channelsEndpoint(orchestratorURL, runnerID, req)
	if err != nil {
		return nil, err
	}
	var resp deleteChannelsResponse
	if err := requestJSON(ctx, client, http.MethodDelete, endpoint, deleteChannelsRequest{Channels: channels}, map[string]string{HeaderSessionToken: token}, defaultTimeout, &resp); err != nil {
		return nil, err
	}
	if resp.Deleted == nil {
		return nil, errors.New("trickle channel delete response missing deleted")
	}
	return resp.Deleted, nil
}

func channelsEndpoint(orchestratorURL, runnerID string, req *http.Request) (string, string, error) {
	if req == nil {
		return "", "", errors.New("request is required")
	}
	sessionID := strings.TrimSpace(req.Header.Get(HeaderSessionID))
	token := strings.TrimSpace(SessionToken(req))
	controlURL := strings.TrimSpace(SessionControl(req))
	if sessionID == "" {
		return "", "", errors.New("session_id is required")
	}
	if token == "" {
		return "", "", errors.New("session_token is required")
	}
	if controlURL != "" {
		endpoint, err := joinEndpoint(controlURL, "channels")
		return endpoint, token, err
	}
	if runnerID == "" {
		runnerID = strings.TrimSpace(req.Header.Get(HeaderRunnerRoute))
	}
	if runnerID == "" {
		return "", "", errors.New("runner_id is required")
	}
	endpoint, err := joinEndpoint(orchestratorURL, "/runner/"+url.PathEscape(runnerID)+"/session/"+url.PathEscape(sessionID)+"/channels")
	return endpoint, token, err
}

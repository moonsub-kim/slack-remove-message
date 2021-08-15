package main

import (
	"strings"
	"time"

	"github.com/slack-go/slack"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

type API struct {
	logger    *zap.Logger
	client    *slack.Client
	rateLimit int
}

func (api API) Delete(
	query string,
	msgHook func(m slack.SearchMessage) error,
	fileHook func(m slack.File) error,
) error {
	limiter := ratelimit.New(api.rateLimit)
	page := 0
	api.logger.Info("query", zap.Any("query", query))
	for {
		msgs, files, err := api.client.Search(
			query,
			slack.SearchParameters{
				Sort:          "timestamp",
				SortDirection: "asc",
				Highlight:     slack.DEFAULT_SEARCH_HIGHLIGHT,
				Count:         api.rateLimit,
				Page:          page,
			},
		)
		if err != nil {
			return err
		} else if msgs.Count == 0 && msgs.Paging.Pages < page {
			api.logger.Info(
				"end",
				zap.Any("pages", msgs.Paging.Pages),
				zap.Any("page", page),
			)
			return nil
		}

		api.logger.Info(
			"search_result",
			zap.Any("messages", msgs.Count),
			zap.Any("files", files.Count),
		)
		page += 1

		for _, m := range msgs.Matches {
			limiter.Take()

			err := msgHook(m)
			if err != nil {
				return err
			}

			_, _, err = api.client.DeleteMessage(m.Channel.ID, m.Timestamp)
			err = api.errHook(err)
			if err != nil {
				return err
			}
		}

		for _, f := range files.Matches {
			limiter.Take()

			err := fileHook(f)
			if err != nil {
				return err
			}

			err = api.client.DeleteFile(f.ID)
			err = api.errHook(err)
			if err != nil {
				return err
			}
		}
	}
}

func (api API) errHook(err error) error {
	if err != nil && strings.Contains(err.Error(), "slack rate limit exceeded") {
		api.logger.Info("rate_limited")
		time.Sleep(time.Second)
		return nil
	} else if err != nil && strings.Contains(err.Error(), "not_found") {
		api.logger.Info("not_found")
		return nil
	} else if err != nil {
		return err
	}

	return nil
}

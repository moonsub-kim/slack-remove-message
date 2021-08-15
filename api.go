package main

import (
	"errors"
	"strings"
	"time"

	"github.com/slack-go/slack"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

type API struct {
	logger  *zap.Logger
	client  *slack.Client
	limiter ratelimit.Limiter
}

func (api API) Delete(
	query string,
	msgHook func(m slack.SearchMessage) error,
	fileHook func(m slack.File) error,
) error {
	page := 0
	api.logger.Info("query", zap.Any("query", query))
	for {
		var msgs *slack.SearchMessages
		var files *slack.SearchFiles

		search := func() error {
			var err error
			msgs, files, err = api.client.Search(
				query,
				slack.SearchParameters{
					Sort:          "timestamp",
					SortDirection: "asc",
					Highlight:     slack.DEFAULT_SEARCH_HIGHLIGHT,
					Count:         100,
					Page:          page,
				},
			)
			return err
		}
		err := api.callAPIWithRate(search)
		if err != nil {
			return err
		} else if msgs.Paging.Pages < page {
			api.logger.Info("end")
			return nil
		}

		api.logger.Info(
			"search_result",
			zap.Any("messages", msgs.Count),
			zap.Any("files", files.Count),
			zap.Any("paging", msgs.Paging),
			zap.Any("pagination", msgs.Pagination),
		)
		page += 1

		for _, m := range msgs.Matches {
			err := msgHook(m)
			if err != nil {
				return err
			}

			err = api.callAPIWithRate(
				func() error {
					_, _, err := api.client.DeleteMessage(m.Channel.ID, m.Timestamp)
					return err
				},
			)
			if err != nil && strings.Contains(err.Error(), "not_found") {
				api.logger.Info("already_removed", zap.Any("message", m.Text))
				continue
			} else if err != nil && strings.Contains(err.Error(), "cant_delete_message") {
				api.logger.Info("unremovable_message", zap.Any("message", m.Text))
				continue
			} else if err != nil {
				return err
			}
		}

		for _, f := range files.Matches {
			err := fileHook(f)
			if err != nil {
				return err
			}

			err = api.callAPIWithRate(
				func() error {
					return api.client.DeleteFile(f.ID)
				},
			)
			if err != nil && strings.Contains(err.Error(), "not_found") {
				api.logger.Info("already_removed", zap.Any("name", f.Name))
				continue
			} else if err != nil {
				return err
			}
		}
	}
}

func (api API) callAPIWithRate(f func() error) error {
	var rateLimitederr *slack.RateLimitedError
	for {
		api.limiter.Take()
		err := f()
		if errors.As(err, &rateLimitederr) {
			api.logger.Info("rate_limited", zap.Any("sleep", rateLimitederr.RetryAfter))
			time.Sleep(rateLimitederr.RetryAfter)
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
}

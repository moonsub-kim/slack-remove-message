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
	logger    *zap.Logger
	client    *slack.Client
	limiter   ratelimit.Limiter
	nonDryRun bool
}

func (api API) DeleteFiles(
	channel string,
	user string,
	fileHook func(m slack.File) error,
) error {
	api.logger.Info("arg", zap.Any("channel", user), zap.Any("channel", user))
	params := &slack.ListFilesParameters{
		Limit:   100,
		User:    user,
		Channel: channel,
		Cursor:  "",
	}
	for {
		var files []slack.File

		search := func() error {
			var err error
			files, params, err = api.client.ListFiles(*params)
			return err
		}
		err := api.callAPIWithRate(search)
		if err != nil {
			return err
		} else if len(files) == 0 {
			api.logger.Info("end")
			return nil
		}

		api.logger.Info(
			"file_list_result",
			zap.Any("files", len(files)),
			zap.Any("cursor", params.Cursor),
		)

		for _, f := range files {
			if fileHook != nil {
				err := fileHook(f)
				if err != nil {
					return err
				}
			}
			if !api.nonDryRun {
				continue
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
				api.logger.Info("err", zap.Any("name", f.Name), zap.Any("id", f.ID), zap.Any("user", f.User), zap.Error(err))
			}
		}

		if params.Cursor == "" {
			return nil
		}
	}
}

func (api API) Delete(
	query string,
	msgHook func(m slack.SearchMessage) error,
	fileHook func(m slack.File) error,
) error {
	page := 1
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
			if !api.nonDryRun {
				continue
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
			if !api.nonDryRun {
				continue
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

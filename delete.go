package main

import (
	"fmt"
	"reflect"

	"github.com/slack-go/slack"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

var token string
var query string
var rateLimit int

var deleteCommand = &cli.Command{
	Name: "delete",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "query",
			Destination: &query,
		},
		&cli.StringFlag{
			Name:        "token",
			EnvVars:     []string{"SLACK_USER_TOKEN"},
			Destination: &token,
		},
		&cli.IntFlag{
			Name:        "rate_limit",
			DefaultText: "30",
			Destination: &rateLimit,
		},
	},
	Action: delete,
}

func delete(ctx *cli.Context) error {
	logger := zapLogger()
	api := API{
		logger:    logger,
		client:    slack.New(token),
		rateLimit: rateLimit,
	}

	err := api.Delete(
		fmt.Sprintf("%s from:me", query),
		messageLog(logger),
		fileLog(logger),
	)
	if err != nil {
		logger.Info("err", zap.Error(err), zap.Any("type", reflect.TypeOf(err)))
		return err
	}

	return nil
}

func messageLog(logger *zap.Logger) func(m slack.SearchMessage) error {
	return func(m slack.SearchMessage) error {
		logger.Info("messsage_deleted", zap.Any("message", m))
		return nil
	}
}

func fileLog(logger *zap.Logger) func(f slack.File) error {
	return func(f slack.File) error {
		logger.Info("file_deleted", zap.Any("file", f))
		return nil
	}
}

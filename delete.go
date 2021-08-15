package main

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/slack-go/slack"
	"github.com/urfave/cli/v2"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

var token string
var query string

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
	},
	Action: delete,
}

func delete(ctx *cli.Context) error {
	logger := zapLogger()
	api := API{
		logger:  logger,
		client:  slack.New(token),
		limiter: ratelimit.New(30),
	}

	err := api.Delete(
		fmt.Sprintf("%s from:me", query),
		messageLog(logger),
		fileLog(logger),
	)
	if err != nil {
		logger.Info("err", zap.Error(err), zap.Any("type", reflect.TypeOf(err)), zap.Stack("stack"))
		return err
	}

	return nil
}

func messageLog(logger *zap.Logger) func(m slack.SearchMessage) error {
	return func(m slack.SearchMessage) error {
		ts, err := strconv.ParseFloat(m.Timestamp, 32)
		if err != nil {
			return err
		}
		logger.Info(
			"messsage_deleted",
			zap.Time("timestamp", time.Unix(int64(ts), int64(ts-float64(int64(ts)))*1000000000)),
			zap.String("timestamp_raw", m.Timestamp),
			zap.String("text", m.Text),
			zap.String("channel", m.Channel.Name),
			zap.String("channel_id", m.Channel.ID),
		)
		return nil
	}
}

func fileLog(logger *zap.Logger) func(f slack.File) error {
	return func(f slack.File) error {
		logger.Info("file_deleted", zap.Any("file", f))
		return nil
	}
}

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
var nonDryRun bool

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
		&cli.BoolFlag{
			Name:        "non_dry_run",
			DefaultText: "false",
			Destination: &nonDryRun,
		},
	},
	Action: delete,
}

var channel string
var user string

var deleteFilesCommand = &cli.Command{
	Name: "delete-file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "channel",
			Destination: &channel,
		},
		&cli.StringFlag{
			Name:        "user",
			Destination: &user,
		},
		&cli.StringFlag{
			Name:        "token",
			EnvVars:     []string{"SLACK_USER_TOKEN"},
			Destination: &token,
		},
		&cli.BoolFlag{
			Name:        "non_dry_run",
			DefaultText: "false",
			Destination: &nonDryRun,
		},
	},
	Action: deleteFile,
}

func delete(ctx *cli.Context) error {
	logger := zapLogger()
	logger.Info(
		"options",
		zap.Any("token", token),
		zap.Any("query", query),
		zap.Any("non_dry_run", nonDryRun),
	)

	api := API{
		logger:    logger,
		client:    slack.New(token),
		limiter:   ratelimit.New(30),
		nonDryRun: nonDryRun,
	}

	times := 1
	if nonDryRun {
		times = 10 // Run 10 times to remove missing messages
	}
	for i := 0; i < times; i++ {
		err := api.Delete(
			fmt.Sprintf("%s from:me", query),
			messageLog(logger),
			fileLog(logger),
		)
		if err != nil {
			logger.Info("err", zap.Error(err), zap.Any("type", reflect.TypeOf(err)), zap.Stack("stack"))
			return err
		}
	}

	return nil
}

func deleteFile(ctx *cli.Context) error {
	logger := zapLogger()
	logger.Info(
		"deletefiles",
		zap.Any("channel", channel),
		zap.Any("user", user),
		zap.Any("non_dry_run", nonDryRun),
	)

	api := API{
		logger:    logger,
		client:    slack.New(token),
		limiter:   ratelimit.New(30),
		nonDryRun: nonDryRun,
	}

	times := 1
	if nonDryRun {
		times = 10 // Run 10 times to remove missing messages
	}
	for i := 0; i < times; i++ {
		err := api.DeleteFiles(
			channel,
			user,
			nil,
		)
		if err != nil {
			logger.Info("err", zap.Error(err), zap.Any("type", reflect.TypeOf(err)), zap.Stack("stack"))
			return err
		}
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

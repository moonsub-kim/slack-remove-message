package main

import (
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			deleteCommand,
		},
	}
	app.Run(os.Args)
}

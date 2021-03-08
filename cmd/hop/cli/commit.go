package cli

import (
	"context"
	"errors"
	"strings"

	"github.com/myelnet/go-hop-exchange/node"
	"github.com/peterbourgon/ff/v2/ffcli"
)

var commitCmd = &ffcli.Command{
	Name:      "commit",
	ShortHelp: "Commit the current index into a single dag",
	LongHelp: strings.TrimSpace(`

The 'hop commit' command creates a single DAG with the current index of staged DAGs. It optionally
archives it into a CAR file for storage.

`),
	Exec: runCommit,
}

func runCommit(ctx context.Context, args []string) error {
	c, cc, ctx, cancel := connect(ctx)
	defer cancel()

	crc := make(chan *node.CommitResult, 1)
	cc.SetNotifyCallback(func(n node.Notify) {
		if cr := n.CommitResult; cr != nil {
			crc <- cr
		}
	})
	go receive(ctx, cc, c)

	cc.Commit(&node.CommitArgs{})
	select {
	case cr := <-crc:
		if cr.Err != "" {
			return errors.New(cr.Err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

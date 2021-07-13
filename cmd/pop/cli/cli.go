package cli

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/myelnet/pop/build"
	"github.com/myelnet/pop/node"
	"github.com/peterbourgon/ff/v3"
	"github.com/peterbourgon/ff/v3/ffcli"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var loggingLevels = map[string]zerolog.Level{
	zerolog.TraceLevel.String(): zerolog.TraceLevel, // trace
	zerolog.DebugLevel.String(): zerolog.DebugLevel, // debug
	zerolog.InfoLevel.String():  zerolog.InfoLevel,  // info (default)
}

// LoggerHook displays the file & line the log comes from
type LoggerHook struct{}

func (h LoggerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if _, file, line, ok := runtime.Caller(3); ok {
		e.Str("file", path.Base(file)).Int("line", line)
	}
}

// Run runs the CLI. The args do not include the binary name.
func Run(args []string) error {
	if len(args) == 1 && (args[0] == "-V" || args[0] == "--version" || args[0] == "version") {
		fmt.Println(build.Version)
		return nil
	}

	rootfs := flag.NewFlagSet("pop", flag.ExitOnError)
	logLevel := rootfs.String("log", zerolog.InfoLevel.String(), "Set logging mode")

	// env vars can be used as program args, i.e : ENV LOG=debug go run . start
	err := ff.Parse(rootfs, args, ff.WithEnvVarNoPrefix())
	if err != nil {
		return err
	}

	loggingLevel, ok := loggingLevels[*logLevel]
	if !ok {
		return fmt.Errorf("logging level [%s] does not exist", *logLevel)
	}

	zerolog.SetGlobalLevel(loggingLevel)

	if loggingLevel < zerolog.InfoLevel {
		output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
		output.FormatLevel = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
		}
		output.FormatMessage = func(i interface{}) string {
			return fmt.Sprintf("%s |", i)
		}
		output.FormatFieldName = func(i interface{}) string {
			return fmt.Sprintf("%s:", i)
		}
		output.FormatFieldValue = func(i interface{}) string {
			return strings.ToUpper(fmt.Sprintf("%s", i))
		}

		log.Logger = log.Hook(LoggerHook{}).Output(output)
		log.Info().Msg(fmt.Sprintf("Running in %s mode", *logLevel))

	} else {
		output := zerolog.ConsoleWriter{Out: os.Stderr}
		log.Logger = log.Output(output)
	}

	rootCmd := &ffcli.Command{
		Name:       "pop",
		ShortUsage: "pop subcommand [flags]",
		ShortHelp:  "Manage your Myel point of presence from the command line",
		LongHelp: strings.TrimSpace(`
This CLI is still under active development. Commands and flags will
change until a first stable release. To get started run 'pop start'.
`),
		Subcommands: []*ffcli.Command{
			startCmd,
			offCmd,
			pingCmd,
			putCmd,
			statusCmd,
			commCmd,
			storeCmd,
			getCmd,
			listCmd,
			walletCmd,
		},
		FlagSet: rootfs,
		Exec:    func(context.Context, []string) error { return flag.ErrHelp },
	}

	if err := rootCmd.Parse(args); err != nil {
		return err
	}

	err = rootCmd.Run(context.Background())
	if err == flag.ErrHelp {
		return nil
	}
	return err
}

func connect(ctx context.Context) (net.Conn, *node.CommandClient, context.Context, context.CancelFunc) {
	c, err := net.Dial("tcp", "127.0.0.1:2001")
	if err != nil {
		log.Fatal().Msg("Unable to connect")
	}

	clientToServer := func(b []byte) {
		node.WriteMsg(c, b)
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		<-interrupt
		c.Close()
		cancel()
	}()

	cc := node.NewCommandClient(clientToServer)
	return c, cc, ctx, cancel
}

// receive backend messages on conn and push them into cc.
func receive(ctx context.Context, cc *node.CommandClient, conn net.Conn) {
	defer conn.Close()
	for ctx.Err() == nil {
		msg, err := node.ReadMsg(conn)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error().Err(err).Msg("ReadMsg")
			break
		}
		cc.GotNotifyMsg(msg)
	}
}

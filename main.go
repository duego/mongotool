package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"
)

var usageTemplate = `Mongotool is a tool for working with MongoDB databases.

   	Usage:

   		mongotool command [arguments]

   	The commands are:
   	{{range .}}
   	    {{.Name | printf "%-11s"}} {{.Short}}{{end}}

   	Use "mongotool help [command]" for more information about a command.

`

var helpTemplate = `usage: mongotool {{.UsageLine}}

{{.Long | trim}}
`

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace, "capitalize": capitalize})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToTitle(r)) + s[n:]
}

func printUsage(w io.Writer) {
	tmpl(w, usageTemplate, commands)
}

func usage() {
	printUsage(os.Stderr)
	os.Exit(2)
}

// help implements the 'help' command.
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: mongotool help command\n\nToo many arguments given.\n")
		os.Exit(2)
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q.  Run 'mongotool help'.\n", arg)
	os.Exit(2)
}

var commands = []*Command{
	cmdDump,
	cmdRestore,
}

func main() {
	flag.Usage = usage
	flag.Parse()
	log.SetFlags(0)

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	if args[0] == "help" {
		help(args[1:])
		return
	}

	for _, cmd := range commands {
		if cmd.Name() == args[0] && cmd.Run != nil {
			cmd.Flag.Usage = func() { cmd.Usage() }
			if cmd.CustomFlags {
				args = args[1:]
			} else {
				cmd.Flag.Parse(args[1:])
				args = cmd.Flag.Args()
			}
			cmd.Run(cmd, args)
			os.Exit(exitStatus)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "mongotool: unknown subcommand %q\nRun 'mongotool help' for usage.\n", args[0])
	os.Exit(2)
}

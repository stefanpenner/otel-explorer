// Package cwdflag makes tools safe under `bazel run` and PATH wrappers.
//
// Call Apply() at the start of main, before flag parsing.
//
// Precedence:
//  1. Last --cwd DIR or --cwd=DIR on the command line (stripped from os.Args)
//  2. Else BUILD_WORKING_DIRECTORY (set by `bazel run` to the caller's cwd)
//
// After Apply, the process cwd is the intended working directory and os.Args
// no longer contains --cwd flags.
package cwdflag

import (
	"fmt"
	"os"
	"strings"
)

// Apply chdirs per --cwd / BUILD_WORKING_DIRECTORY and rewrites os.Args.
func Apply() error {
	rest, dir, err := consume(os.Args)
	if err != nil {
		return err
	}
	if dir == "" {
		dir = os.Getenv("BUILD_WORKING_DIRECTORY")
	}
	if dir != "" {
		if err := os.Chdir(dir); err != nil {
			return fmt.Errorf("cwdflag: chdir %q: %w", dir, err)
		}
	}
	os.Args = rest
	return nil
}

// Consume is like Apply but returns the new argv without mutating os.Args or chdir'ing.
// Useful in tests. dir is empty if no --cwd was present.
func Consume(args []string) (rest []string, dir string, err error) {
	return consume(args)
}

func consume(args []string) ([]string, string, error) {
	if len(args) == 0 {
		return args, "", nil
	}
	out := make([]string, 0, len(args))
	out = append(out, args[0])
	var dir string
	for i := 1; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--cwd":
			if i+1 >= len(args) {
				return nil, "", fmt.Errorf("cwdflag: --cwd requires a path")
			}
			dir = args[i+1]
			i++
		case strings.HasPrefix(a, "--cwd="):
			dir = strings.TrimPrefix(a, "--cwd=")
			if dir == "" {
				return nil, "", fmt.Errorf("cwdflag: --cwd= requires a path")
			}
		default:
			out = append(out, a)
		}
	}
	return out, dir, nil
}

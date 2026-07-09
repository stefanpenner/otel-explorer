// specgen — generate a Go decision module from a TLA+ spec.
//
// The generated module mirrors the spec's state machine as pure functions:
// State struct, Init(), Can<Action>() guards, <Action>() transitions,
// EnabledActions() dispatcher, and trace helpers for conformance checking.
//
// Usage:
//
//	specgen [flags] <spec.tla>
//
// Flags:
//
//	-o, --output <dir>    output directory (default: <spec>_gen)
//	-p, --package <name>  Go package name (default: <spec>spec)
//	-h, --help
package main

import (
	"flag"
	"fmt"
	"github.com/stefanpenner/otel-explorer/tools/cwdflag"
	"go/format"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/stefanpenner/otel-explorer/tools/specgen/tla"
)

const usage = `specgen — generate a Go decision module from a TLA+ spec

Usage:
  specgen [flags] <spec.tla>

Flags:
  -o, --output <dir>    output directory (default: <spec>_gen)
  -p, --package <name>  Go package name (default: <spec>spec)
  -const Name=Value     bind a CONSTANT to a literal before codegen
                        (repeatable). TRUE/FALSE -> bool, else an integer
                        if parseable, else a string. A CONSTANT referenced
                        inside a Step-dispatch spec's step bodies that is
                        NOT bound this way becomes an extra bool Step
                        parameter instead (it must be used only in boolean
                        position — e.g. IF HasSel THEN ...).
  -h, --help

Examples:
  specgen Toggle.tla                          # → Toggle_gen/spec.go
  specgen -o ./out Toggle.tla                 # → ./out/spec.go
  specgen -p cache Toggle.tla                 # → Toggle_gen/spec.go (package cache)
  specgen -const Guarded=TRUE Picker.tla       # bind Guarded; HasSel (unbound)
                                               # becomes Step's extra bool param
`

// constFlags accumulates repeated -const Name=Value flags.
type constFlags []string

func (c *constFlags) String() string     { return strings.Join(*c, ",") }
func (c *constFlags) Set(v string) error { *c = append(*c, v); return nil }

func main() {
	if err := cwdflag.Apply(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var (
		outputDir string
		pkgName   string
		help      bool
		consts    constFlags
	)
	fs := flag.NewFlagSet("specgen", flag.ExitOnError)
	fs.StringVar(&outputDir, "o", "", "output directory")
	fs.StringVar(&outputDir, "output", "", "output directory")
	fs.StringVar(&pkgName, "p", "", "Go package name")
	fs.StringVar(&pkgName, "package", "", "Go package name")
	fs.Var(&consts, "const", "bind a CONSTANT to a literal (Name=Value, repeatable)")
	fs.BoolVar(&help, "h", false, "show help")
	fs.BoolVar(&help, "help", false, "show help")
	fs.Usage = func() { fmt.Print(usage) }

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if help || len(fs.Args()) == 0 {
		fmt.Print(usage)
		os.Exit(0)
	}

	bindings, err := parseConstFlags(consts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "specgen: %v\n", err)
		os.Exit(2)
	}

	specPath := fs.Arg(0)
	if err := run(specPath, outputDir, pkgName, bindings, []string(consts)); err != nil {
		fmt.Fprintf(os.Stderr, "specgen: %v\n", err)
		os.Exit(1)
	}
}

// parseConstFlags parses "Name=Value" strings into TLA+ literals: TRUE/FALSE
// become BoolLit, else a parseable integer becomes IntLit, else the raw
// string becomes StrLit.
func parseConstFlags(flags []string) (map[string]tla.Expr, error) {
	if len(flags) == 0 {
		return nil, nil
	}
	out := make(map[string]tla.Expr, len(flags))
	for _, f := range flags {
		name, val, ok := strings.Cut(f, "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("-const %q: expected Name=Value", f)
		}
		switch val {
		case "TRUE":
			out[name] = tla.BoolLit{Val: true}
		case "FALSE":
			out[name] = tla.BoolLit{Val: false}
		default:
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				out[name] = tla.IntLit{Val: n}
			} else {
				out[name] = tla.StrLit{Val: val}
			}
		}
	}
	return out, nil
}

// formatOrRaw gofmt's generated source so callers never need a manual gofmt
// pass. If the generator ever produces something gofmt rejects, that's a
// real codegen bug — surface the raw (broken) output as-is rather than
// hiding it, so the resulting compile error points at the actual problem.
func formatOrRaw(src string) []byte {
	out, err := format.Source([]byte(src))
	if err != nil {
		return []byte(src)
	}
	return out
}

// findSkipped returns the skip reason for a def name, or "" if it was not
// skipped.
func findSkipped(spec *Spec, name string) string {
	for _, u := range spec.Unsupported {
		if u.Name == name {
			return u.Reason
		}
	}
	return ""
}

func run(specPath, outputDir, pkgName string, constBindings map[string]tla.Expr, constFlags []string) error {
	src, err := os.ReadFile(specPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", specPath, err)
	}

	spec, err := Parse(string(src))
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	spec.ConstBindings = constBindings

	// Report what fell outside the supported subset, so the gap is visible
	// rather than silent. These defs (TypeOK, temporal properties, ...) are not
	// needed for codegen; a load-bearing one being here surfaces below.
	// Hint only for shapes that belong in full TLC models (records, CHOOSE),
	// not for routine skips like temporal Spec (every decision core has one).
	hintDecisionCore := false
	for _, u := range spec.Unsupported {
		fmt.Fprintf(os.Stderr, "specgen: skipped %s (%s)\n", u.Name, u.Reason)
		if strings.Contains(u.Reason, "[...]") ||
			strings.Contains(u.Reason, "record") ||
			strings.Contains(u.Reason, "CHOOSE") {
			hintDecisionCore = true
		}
	}
	// Keep the pure decision-module core: multi-object / record TLC models
	// stay model-checkers. Point users at the scalar decision-core pattern.
	if hintDecisionCore {
		fmt.Fprintln(os.Stderr, "specgen: hint: records/CHOOSE/multi-object interleavings stay TLC-only;")
		fmt.Fprintln(os.Stderr, "  extract a scalar decision core (bool/int/string vars, named actions)")
		fmt.Fprintln(os.Stderr, "  and generate that — see TOOL.md \"Supported TLA+ subset\".")
	}

	// Next is either the \E-dispatch shape (PATH B: one Step function) or an
	// ordinary named-action disjunction (PATH A). DetectStepDispatch decides
	// which; an error here means Next LOOKED dispatch-shaped but is broken,
	// which deserves a specific message rather than PATH A's generic
	// "no Next definition" / "references undefined action <expr>".
	dispatch, err := tla.DetectStepDispatch(spec)
	if err != nil {
		return fmt.Errorf("Next looks like a dispatch (\\E ...) but is malformed: %w", err)
	}

	var cg *CodeGen
	if dispatch != nil {
		freeVars, err := tla.CollectFreeVars(spec, dispatch)
		if err != nil {
			return fmt.Errorf("dispatch: %w", err)
		}
		cg = &CodeGen{Spec: spec, Dispatch: dispatch, FreeVars: freeVars}
	} else if refs, refErr := tla.ExtractActionRefs(spec); refErr == nil && tla.HasQuantifiedAction(refs) {
		// PATH C: Next is a guarded-transition library (`\E x \in D : A(x) \/
		// ...`). Extract each action WITH its \E-bound parameter so codegen
		// emits Can<A>(x)/<A>(x) + EnabledSteps/ApplyStep. DetectStepDispatch
		// declined above (no branch is `var = v /\ Op(arg)`-shaped), and PATH
		// A's ExtractActions can't carry a parameter, so this is the only path
		// that models it.
		actions, err := tla.ExtractParamActions(spec)
		if err != nil {
			return fmt.Errorf("extract parameterised actions: %w", err)
		}
		cg = &CodeGen{Spec: spec, Actions: actions}
	} else {
		actions, err := ExtractActions(spec)
		if err != nil {
			if skipped := findSkipped(spec, "Next"); skipped != "" {
				return fmt.Errorf("Next is outside the supported subset (%s); nothing to generate", skipped)
			}
			return fmt.Errorf("extract actions: %w", err)
		}
		cg = &CodeGen{Spec: spec, Actions: actions}
	}

	if outputDir == "" {
		base := strings.TrimSuffix(filepath.Base(specPath), ".tla")
		outputDir = base + "_gen"
	}
	if pkgName == "" {
		pkgName = strings.ToLower(spec.Name) + "spec"
	}
	cg.PackageName = pkgName
	// Provenance for the generated header: where it came from and how to remake
	// it. constFlags is the raw -const list, echoed back verbatim.
	cg.SpecPath = specPath
	cg.OutputDir = outputDir
	cg.ConstFlags = constFlags

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	specFile := filepath.Join(outputDir, "spec.go")
	if err := os.WriteFile(specFile, formatOrRaw(cg.Generate()), 0644); err != nil {
		return fmt.Errorf("write spec.go: %w", err)
	}

	testFile := filepath.Join(outputDir, "spec_test.go")
	if err := os.WriteFile(testFile, formatOrRaw(cg.GenerateTests()), 0644); err != nil {
		return fmt.Errorf("write spec_test.go: %w", err)
	}

	fmt.Printf("generated %s\n", specFile)
	fmt.Printf("generated %s\n", testFile)
	return nil
}

package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stefanpenner/otel-explorer/tools/specgen/tla"
)

// CodeGen generates Go source from a parsed TLA+ spec.
//
// Two mutually exclusive codegen paths:
//   - Actions set (PATH A): Next is a plain disjunction of independently-
//     named actions. Each becomes Can<Name>()/<Name>() with a guard split
//     from the action body.
//   - Dispatch set (PATH B): Next is the \E-dispatch shape (see
//     tla.DetectStepDispatch) — an input-driven decision machine. Generates
//     a single Step(key string, ...extra bool params) State that mode-
//     dispatches into each per-mode operator's inlined CASE/IF body.
type CodeGen struct {
	Spec        *Spec
	Actions     []Action          // PATH A
	Dispatch    *tla.StepDispatch // PATH B
	FreeVars    []string          // PATH B: CONSTANTS needing extra Step params
	PackageName string

	// Provenance for the generated header — where this came from and how to
	// remake it. Empty when codegen is driven directly from a test.
	SpecPath   string   // path to the source .tla passed on the command line
	OutputDir  string   // -o target, so the regen command is copy-paste exact
	ConstFlags []string // raw -const Name=Value strings, in the order given

	atExpr string // current Go expr for `@` inside an EXCEPT clause (see exprToGo)

	setConsts map[string]bool // memoized: CONSTANTS used in a set/domain position
}

// Generate produces the full Go decision module source.
func (cg *CodeGen) Generate() string {
	// Body is built BEFORE the header so writeHeader can decide whether the
	// funcEq helper (the one production-file helper that needs an import,
	// "reflect") actually got emitted — writeHelpers' own gating is a
	// string-search over the body, so it isn't known until the body exists.
	var body strings.Builder
	cg.writeState(&body)
	cg.writeInit(&body)
	switch {
	case cg.Dispatch != nil:
		cg.writeStep(&body)
	case cg.isParamLib():
		cg.writeParamActions(&body)
		cg.writeStepLib(&body)
		cg.writeStateSerializer(&body)
	default:
		cg.writeActions(&body)
		cg.writeEnabledActions(&body)
		cg.writePurePredicates(&body)
	}
	cg.writeTrace(&body)
	cg.writeRecursiveFuncs(&body)
	cg.writeHelpers(&body)

	var b strings.Builder
	cg.writeHeader(&b, strings.Contains(body.String(), "funcEq("))
	b.WriteString(body.String())
	return b.String()
}

// recFuncName is the Go name for a RECURSIVE TLA+ operator: lower-cased first
// rune so the func is unexported (an internal detail of the decision module).
func recFuncName(name string) string {
	if name == "" {
		return name
	}
	r := []rune(name)
	r[0] = []rune(strings.ToLower(string(r[0])))[0]
	return string(r)
}

// writeRecursiveFuncs emits a standalone Go func for each RECURSIVE operator
// that is actually called somewhere in the already-written body (same body-scan
// gating as writeHelpers — writeRecursiveFuncs runs late, after every action).
//
// The body is emitted STATEMENT-oriented with a lazy `if`: `if cond { return
// then }; return else`. It must NOT use the eager specgenIf helper — that
// evaluates BOTH branches, so the self-call in the ELSE would recurse forever
// regardless of the guard. R2 supports scalar int64 recursion (SessAttach's
// set-typed Closure is a later slice); a set-typed body emits a compile-
// breaking marker rather than silently-wrong code.
func (cg *CodeGen) writeRecursiveFuncs(b *strings.Builder) {
	if cg.Spec == nil {
		return
	}
	body := b.String()
	// Deterministic order: follow Defs, not the map, so output is stable.
	for _, d := range cg.Spec.Defs {
		if !cg.Spec.Recursive[d.Name] {
			continue
		}
		fn := recFuncName(d.Name)
		if !strings.Contains(body, fn+"(") {
			continue // never called — don't emit a dead func
		}
		inlined, err := tla.Inline(cg.Spec, d.Body)
		if err != nil {
			b.WriteString(fmt.Sprintf("\n/* recursive operator %s: inline failed: %v */\n", d.Name, err))
			continue
		}
		paramTypes, retType, ok := recFuncTypes(d.Params, inlined)
		if !ok {
			// Out of the currently-supported scope (e.g. tuple/edge-keyed sets —
			// R3b). Loud failure: breaks the build rather than emitting wrong code.
			b.WriteString(fmt.Sprintf("\n/* recursive operator %s not yet supported (R3b: mixed/tuple-keyed sets) */\n", d.Name))
			continue
		}
		locals := make(map[string]string, len(d.Params))
		var params []string
		for i, p := range d.Params {
			locals[p] = p
			params = append(params, p+" "+paramTypes[i])
		}
		b.WriteString(fmt.Sprintf("\nfunc %s(%s) %s {\n", fn, strings.Join(params, ", "), retType))
		b.WriteString(cg.recBodyToGo(inlined, locals))
		b.WriteString("}\n")
	}
}

// recBodyToGo emits a recursive func body as lazy if/return statements. A
// top-level IF becomes `if cond { <then-stmts> }` followed by the else body, so
// the ELSE (and its self-call) is only reached when the guard is false. Any
// non-IF expression is the base case: `return <expr>`.
func (cg *CodeGen) recBodyToGo(e tla.Expr, locals map[string]string) string {
	if ife, ok := e.(tla.IfExpr); ok {
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("\tif %s {\n\t", cg.exprToGo(ife.Cond, locals, "")))
		sb.WriteString(cg.recBodyToGo(ife.Then, locals))
		sb.WriteString("\t}\n\t")
		sb.WriteString(cg.recBodyToGo(ife.Else, locals))
		return sb.String()
	}
	return fmt.Sprintf("return %s\n", cg.exprToGo(e, locals, ""))
}

// setTypedExpr reports whether an expression is (or builds) a TLA+ set — the
// signal that a recursive operator is set-valued rather than scalar-int64.
// Conservative: any set literal, comprehension, image, range, or set-algebra
// operator counts.
func setTypedExpr(e tla.Expr) bool {
	found := false
	tla.Walk(e, func(n tla.Expr) bool {
		switch v := n.(type) {
		case tla.SetLit, tla.SetCompr, tla.SetMap, tla.RangeLit:
			found = true
		case tla.BinOp:
			if v.Op == "\\cup" || v.Op == "\\cap" || v.Op == "\\" {
				found = true
			}
		}
		return true
	})
	return found
}

// hasTupleExpr reports whether an expression mentions a tuple anywhere — the
// signal that a recursive operator touches tuple/edge-keyed sets (map[[N]string]
// bool), which is R3b's scope, not R3a's string-keyed spine.
func hasTupleExpr(e tla.Expr) bool {
	found := false
	tla.Walk(e, func(n tla.Expr) bool {
		if _, ok := n.(tla.TupleLit); ok {
			found = true
		}
		return true
	})
	return found
}

// elemKeyType returns the Go map-key type for a value used as a set ELEMENT: an
// N-tuple keys a set as [N]string (an edge set), anything else as string.
func elemKeyType(e tla.Expr) string {
	if t, ok := e.(tla.TupleLit); ok {
		return fmt.Sprintf("[%d]string", len(t.Elems))
	}
	return "string"
}

// setLitKey returns the element key type of a set LITERAL operand, or "" if e is
// not a `{...}` literal (a comprehension or var carries no local key evidence).
func setLitKey(e tla.Expr) string {
	if sl, ok := e.(tla.SetLit); ok {
		return setLitKeyType(sl.Elems)
	}
	return ""
}

// paramSetType infers a recursive-operator parameter's Go type from how the
// (inlined) body uses it, resolving the KEY type per param so an edge/tuple set
// (map[[2]string]bool) and a node/string set (map[string]bool) in the SAME func
// are typed independently. A param is a set iff it appears as an operand of set
// algebra (\cup/\cap/\), the RHS of membership/subset, or a fold domain. The key
// type comes from a concrete element (membership LHS) or literal operand; folds
// and comprehensions give no local key, so those default to string.
func paramSetType(param string, body tla.Expr) (string, bool) {
	isP := func(e tla.Expr) bool {
		vr, ok := e.(tla.VarRef)
		return ok && vr.Name == param
	}
	key, isSet := "", false
	mark := func(k string) {
		isSet = true
		if k != "" && key == "" {
			key = k
		}
	}
	tla.Walk(body, func(n tla.Expr) bool {
		switch v := n.(type) {
		case tla.BinOp:
			switch v.Op {
			case "\\cup", "\\cap", "\\":
				if isP(v.Lhs) {
					mark(setLitKey(v.Rhs))
				}
				if isP(v.Rhs) {
					mark(setLitKey(v.Lhs))
				}
			case "\\in", "\\notin":
				if isP(v.Rhs) {
					mark(elemKeyType(v.Lhs))
				}
			case "\\subseteq", "\\subset":
				if isP(v.Rhs) {
					mark(setLitKey(v.Lhs))
				}
			}
		case tla.ForallExpr:
			if isP(v.Domain) {
				mark("")
			}
		case tla.ExistsExpr:
			if isP(v.Domain) {
				mark("")
			}
		case tla.SetCompr:
			if isP(v.Domain) {
				mark("")
			}
		}
		return true
	})
	if !isSet {
		return "", false
	}
	if key == "" {
		key = "string"
	}
	return "map[" + key + "]bool", true
}

// recRetType infers a recursive func's Go return type from its (inlined) body by
// following the accumulator through the branch structure: the non-recursive
// branch of an IF, either operand of set algebra, or a literal/comprehension —
// resolving to a param type via ptypes. Self-recursive Calls carry no type
// (that IS the type we are inferring) so the other branch decides. Falls back to
// string-keyed set if the body is set-valued, else int64.
func recRetType(body tla.Expr, ptypes map[string]string) string {
	var resolve func(e tla.Expr) string
	resolve = func(e tla.Expr) string {
		switch v := e.(type) {
		case tla.IfExpr:
			if t := resolve(v.Then); t != "" {
				return t
			}
			return resolve(v.Else)
		case tla.VarRef:
			return ptypes[v.Name]
		case tla.BinOp:
			if v.Op == "\\cup" || v.Op == "\\cap" || v.Op == "\\" {
				if t := resolve(v.Lhs); t != "" {
					return t
				}
				return resolve(v.Rhs)
			}
		case tla.SetLit:
			return "map[" + setLitKeyType(v.Elems) + "]bool"
		case tla.SetCompr, tla.SetMap:
			return "map[string]bool"
		}
		return ""
	}
	if t := resolve(body); t != "" {
		return t
	}
	if setTypedExpr(body) {
		return "map[string]bool"
	}
	return "int64"
}

// recFuncTypes infers the Go parameter and return types for a recursive
// operator's standalone func from its (inlined) body. Each set param's key type
// is resolved independently (mixed edge/node sets — SessAttach's Closure(E, S)),
// scalars stay int64. ok is currently always true; the flag is retained so
// genuinely unsupported shapes can still emit a compile-breaking marker rather
// than silently-wrong code.
func recFuncTypes(params []string, body tla.Expr) (paramTypes []string, retType string, ok bool) {
	ptypes := map[string]string{}
	for _, p := range params {
		if t, isSet := paramSetType(p, body); isSet {
			paramTypes = append(paramTypes, t)
			ptypes[p] = t
		} else {
			paramTypes = append(paramTypes, "int64")
			ptypes[p] = "int64"
		}
	}
	return paramTypes, recRetType(body, ptypes), true
}

// isParamLib reports whether this is a PATH C guarded-transition library: Next
// is `\E x \in D : A(x) \/ ...`, so at least one action takes a \E-bound
// parameter. Such a spec has no single dispatcher — the caller picks which
// enabled (action, arg) pair to fire — so it gets per-arg Can/Do methods and
// an EnabledSteps/ApplyStep driver instead of PATH A's nullary surface.
func (cg *CodeGen) isParamLib() bool {
	return cg.Dispatch == nil && tla.AnyParameterised(cg.Actions)
}

// writeHelpers emits the shared runtime helpers the translated guard/update
// expressions call — but ONLY the ones this file actually uses. Dead helpers
// are bloat that hurts the generated code's readability and token cost, so
// each is gated on its identifier appearing in the body already written to b
// (writeHelpers runs last). The helpers are mutually independent — none calls
// another — so a single flat presence check is exact; if that ever changes,
// this must grow into a fixpoint over the selected set.
func (cg *CodeGen) writeHelpers(b *strings.Builder) {
	body := b.String()
	for _, h := range runtimeHelpers {
		if strings.Contains(body, h.name+"(") {
			b.WriteString(h.code)
		}
	}
}

// runtimeHelper is one gated helper: its call identifier and its full
// definition (leading blank line included so spacing survives gofmt).
type runtimeHelper struct {
	name string
	code string
}

// runtimeHelpers are emitted in this order when referenced. Keep each `code`
// self-contained (no cross-helper calls) so the presence check in
// writeHelpers stays exact.
var runtimeHelpers = []runtimeHelper{
	{"specgenIf", `
// specgenIf evaluates a TLA+ IF/THEN/ELSE used as a value.
func specgenIf[T any](cond bool, then, els T) T {
	if cond {
		return then
	}
	return els
}
`},
	{"cloneMap", `
// cloneMap returns a FRESH copy of m. A Go struct copy is shallow — an
// UNCHANGED map field, or one an IF/CASE branch resolved straight back to the
// receiver's own field, would otherwise still share m's backing store — so
// this is the copy-on-write that keeps every returned State's map fields
// independent of the receiver's, even when the field itself didn't change.
func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
`},
	{"setUnion", `
// setUnion returns a FRESH map holding every key of a or b — never mutating an
// input, so State keeps value semantics: s.Held = setUnion(pre.Held, ...)
// leaves the caller's pre-state set untouched. Generic over the element type so
// it serves both scalar sets (map[string]bool) and sets of tuples
// (map[[2]string]bool, an attach-graph edge set).
func setUnion[K comparable](a, b map[K]bool) map[K]bool {
	out := make(map[K]bool, len(a)+len(b))
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}
`},
	{"setInter", `
// setInter returns a FRESH map of the keys common to a and b.
func setInter[K comparable](a, b map[K]bool) map[K]bool {
	out := map[K]bool{}
	for k := range a {
		if b[k] {
			out[k] = true
		}
	}
	return out
}
`},
	{"setDiff", `
// setDiff returns a FRESH map of the keys in a but not b.
func setDiff[K comparable](a, b map[K]bool) map[K]bool {
	out := map[K]bool{}
	for k := range a {
		if !b[k] {
			out[k] = true
		}
	}
	return out
}
`},
	{"setEq", `
// setEq reports whether two sets hold the same elements. TLA+ set (in)equality
// (a = b, a # b) can't compile to Go's == / != — maps are comparable only to
// nil — so it routes through here instead.
func setEq[K comparable](a, b map[K]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
`},
	{"funcEq", `
// funcEq reports whether two total functions hold equal values at every key.
// TLA+ function (in)equality (f = g, f # g) can't compile to Go's == / !=
// when the value type is itself non-comparable (a func-of-func's value is
// map[string]V) — maps compare only to nil — so it routes through
// reflect.DeepEqual instead, which recurses through nested maps correctly.
func funcEq[K comparable, V any](a, b map[K]V) bool {
	return reflect.DeepEqual(a, b)
}
`},
	{"setFilter", `
// setFilter is TLA+ set comprehension {x \in dom : p(x)}: a FRESH set of the
// domain elements that satisfy p. It builds the one-hop / reachability sets an
// attach-graph guard folds over (Step, Reach).
func setFilter(dom []string, p func(string) bool) map[string]bool {
	out := map[string]bool{}
	for _, x := range dom {
		if p(x) {
			out[x] = true
		}
	}
	return out
}
`},
	{"funcGet", `
// funcGet reads a TLA+ total function stored as a lazy map: an absent key
// resolves to def (the function's Init default), so the map can start empty
// over an abstract CONSTANT domain.
func funcGet[V any](f map[string]V, k string, def V) V {
	if v, ok := f[k]; ok {
		return v
	}
	return def
}
`},
	{"funcSet", `
// funcSet is TLA+ [f EXCEPT ![k] = v]: it returns a FRESH map with one key
// updated, never mutating f, so State keeps value semantics.
func funcSet[V any](f map[string]V, k string, v V) map[string]V {
	out := make(map[string]V, len(f)+1)
	for kk, vv := range f {
		out[kk] = vv
	}
	out[k] = v
	return out
}
`},
	{"forAll", `
// forAll is TLA+ universal quantification \A x \in dom : p(x): true iff p holds
// for every element, short-circuiting on the first counterexample. Over the
// empty domain it is vacuously true, matching TLA+.
func forAll(dom []string, p func(string) bool) bool {
	for _, x := range dom {
		if !p(x) {
			return false
		}
	}
	return true
}
`},
	{"exists", `
// exists is TLA+ existential quantification \E x \in dom : p(x): true iff p
// holds for some element, short-circuiting on the first witness. Over the empty
// domain it is false, matching TLA+.
func exists(dom []string, p func(string) bool) bool {
	for _, x := range dom {
		if p(x) {
			return true
		}
	}
	return false
}
`},
	{"keysOf", `
// keysOf returns the live keys of a set stored as map[string]bool, so a
// quantifier can range over a set-typed state variable (\A x \in held : ...).
func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k, in := range m {
		if in {
			out = append(out, k)
		}
	}
	return out
}
`},
}

// GenerateTests produces a Go test file that verifies the generated module:
// Init correctness, and a reachability walk (BFS for PATH A, exhaustive
// Domain x FreeVars exploration for PATH B).
func (cg *CodeGen) GenerateTests() string {
	var b strings.Builder
	cg.writeTestHeader(&b)
	cg.writeTestInit(&b)
	switch {
	case cg.Dispatch != nil:
		cg.writeTestStepWalk(&b)
	case cg.isParamLib():
		cg.writeTestStepLibWalk(&b)
		cg.writeTestImmutabilityGuard(&b)
	default:
		cg.writeTestBFS(&b)
		cg.writeTestPurePredicates(&b)
	}
	return b.String()
}

// writeTestPurePredicates is the generated dual-stub for pure gates:
// exact name set from Decision.tla + no-panic on Init.
// Production duals only need semantic prod↔decision checks (not registry smoke).
func (cg *CodeGen) writeTestPurePredicates(b *strings.Builder) {
	preds := cg.purePredicates()
	if len(preds) == 0 {
		return
	}
	b.WriteString(`// TestPurePredicates is the generated pure-gate dual stub.
// Names come from Decision.tla (SSOT); dual package tests cover production wiring.
func TestPurePredicates(t *testing.T) {
	want := []string{
`)
	for _, p := range preds {
		fmt.Fprintf(b, "\t\t%q,\n", p.name)
	}
	b.WriteString(`	}
	s := Init()
	preds := PurePredicates()
	if len(preds) != len(want) {
		t.Fatalf("PurePredicates len=%d want %d", len(preds), len(want))
	}
	got := make(map[string]bool, len(preds))
	for _, p := range preds {
		if p.Name == "" {
			t.Fatal("empty PurePredicate.Name")
		}
		if got[p.Name] {
			t.Fatalf("duplicate pure name %q", p.Name)
		}
		got[p.Name] = true
		t.Run(p.Name, func(t *testing.T) {
			_ = p.Check(s) // must not panic
		})
	}
	for _, name := range want {
		if !got[name] {
			t.Fatalf("missing pure %q in PurePredicates()", name)
		}
	}
}

`)
}

// mapFields returns the Go field names of every map-typed (set or function)
// State variable, in Spec.Variables order.
func (cg *CodeGen) mapFields() []string {
	var fields []string
	for _, v := range cg.Spec.Variables {
		if strings.HasPrefix(cg.inferType(v), "map[") {
			fields = append(fields, goField(v))
		}
	}
	return fields
}

func (cg *CodeGen) writeTestHeader(b *strings.Builder) {
	fmt.Fprintln(b, "// Code generated by specgen — DO NOT EDIT.")
	fmt.Fprintln(b)
	fmt.Fprintf(b, "package %s\n\n", cg.pkgName())
	// Only the BFS / Step-dispatch walks use fmt (for the seen-set key); the
	// PATH C smoke test does not, and Go rejects an unused import. The
	// multi-param immutability guard is the one PATH C test that needs fmt
	// too — its Step.Args is a slice, so the failure label can't just +
	// concatenate it the way the single-Arg string case does.
	if cg.isParamLib() {
		// assertMapUnchanged compares map VALUES with reflect.DeepEqual (not
		// ==) so it works for any value type, including a func-of-func field
		// whose value type (map[string]V) isn't comparable — see
		// writeTestImmutabilityGuard. "reflect" is only needed when that guard
		// is actually emitted (len(mapFields()) > 0).
		var extra []string
		if cg.anyMultiParam() && len(cg.mapFields()) > 0 {
			extra = append(extra, "\"fmt\"")
		}
		if len(cg.mapFields()) > 0 {
			extra = append(extra, "\"reflect\"")
		}
		if len(extra) == 0 {
			b.WriteString("import \"testing\"\n\n")
		} else {
			b.WriteString("import (\n\t" + strings.Join(extra, "\n\t") + "\n\t\"testing\"\n)\n\n")
		}
		return
	}
	b.WriteString(`import (
	"fmt"
	"testing"
)

`)
}

func (cg *CodeGen) pkgName() string {
	if cg.PackageName != "" {
		return cg.PackageName
	}
	return strings.ToLower(cg.Spec.Name) + "spec"
}

func (cg *CodeGen) writeTestInit(b *strings.Builder) {
	fmt.Fprintln(b, "func TestInit(t *testing.T) {")
	fmt.Fprintln(b, "\ts := Init()")
	initBody := cg.Spec.FindDef("Init")
	for _, v := range cg.Spec.Variables {
		val := findInitVal(v, initBody)
		if val != nil {
			goType := cg.inferType(v)
			if strings.HasPrefix(goType, "map[") {
				// Maps aren't != comparable, so compare by length + per-key
				// rather than assuming the initial map is always empty — a
				// SetLit or concrete-domain FuncLit Init value (e.g.
				// `[k \in {"k1","k2"} |-> "none"]`) is legitimately non-empty.
				// updateValToGo already renders the correct literal (or a
				// typed nil for the empty/abstract-domain case), so `want`'s
				// type conversion covers both uniformly.
				// `want` is qualified per-field: a spec with 2+ map-typed
				// Init vars would otherwise redeclare a bare `want` on the
				// second iteration ("no new variables on left side of :=").
				wantVar := "want" + goField(v)
				wantGo := cg.updateValToGo(val)
				fmt.Fprintf(b, "\t%s := %s(%s)\n", wantVar, goType, wantGo)
				fmt.Fprintf(b, "\tif len(s.%s) != len(%s) {\n", goField(v), wantVar)
				fmt.Fprintf(b, "\t\tt.Errorf(\"len(%s) = %%d, want %%d\", len(s.%s), len(%s))\n", goField(v), goField(v), wantVar)
				fmt.Fprintln(b, "\t}")
				if !strings.Contains(goType, "]map[") {
					// A nested func-of-func's value type is itself a map, and
					// maps aren't != comparable at all — the length check
					// above is as far as that shape can go. Every other value
					// type (bool, string, int64, tuple) is comparable, so
					// those get the fuller per-key check too.
					fmt.Fprintf(b, "\tfor k, ev := range %s {\n", wantVar)
					fmt.Fprintf(b, "\t\tif s.%s[k] != ev {\n", goField(v))
					fmt.Fprintf(b, "\t\t\tt.Errorf(\"%s[%%v] = %%v, want %%v\", k, s.%s[k], ev)\n", goField(v), goField(v))
					fmt.Fprintln(b, "\t\t}")
					fmt.Fprintln(b, "\t}")
				}
				continue
			}
			// litToGo only covers scalar literals — a VarRef Init value (a
			// model-value/CONSTANT reference like `owner = NoOne`) has no
			// case there and silently defaults to "nil", which no longer
			// matches writeInit's own (correct) rendering via updateValToGo.
			// Delegate here too so the test's expectation and the actual
			// Init() output are built by the same lowering.
			wantGo := cg.updateValToGo(val)
			fmt.Fprintf(b, "\tif s.%s != %s(%s) {\n", goField(v), goType, wantGo)
			fmt.Fprintf(b, "\t\tt.Errorf(\"%s = %%v, want %%v\", s.%s, %s(%s))\n",
				goField(v), goField(v), goType, wantGo)
			fmt.Fprintln(b, "\t}")
		}
	}
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

func (cg *CodeGen) writeTestBFS(b *strings.Builder) {
	b.WriteString(`// TestBFSReachability walks the state space from Init, applying every
// enabled action. Verifies no transition panics and all states are reachable.
func TestBFSReachability(t *testing.T) {
	seen := map[string]bool{}
	queue := []State{Init()}
	maxStates := 10000

	for len(queue) > 0 && len(seen) < maxStates {
		s := queue[0]
		queue = queue[1:]
		key := fmt.Sprintf("%+v", s)
		if seen[key] {
			continue
		}
		seen[key] = true

		for _, action := range s.EnabledActions() {
			next := s.ApplyAction(action)
			queue = append(queue, next)
		}
	}

	if len(seen) == 0 {
		t.Fatal("no reachable states")
	}
	t.Logf("explored %d reachable states", len(seen))
}
`)
}

// writeTestStepLibWalk generates a smoke test for PATH C. The \E domain is a
// CONSTANT (e.g. Items) with no codegen-time extent, so — unlike PATH B, whose
// domain is a literal set — a full reachability walk can't be enumerated here.
// The caller (conform, a hand-written test) supplies the domain at runtime.
// This test just exercises the generated surface: Init, EnabledSteps on an
// empty domain, and that no step panics.
func (cg *CodeGen) writeTestStepLibWalk(b *strings.Builder) {
	if cg.anyMultiParam() {
		cg.writeTestStepLibWalkMulti(b)
		return
	}
	b.WriteString(`// TestStepLibSmoke exercises the guarded-transition library. A full
// reachability walk needs the \E domain (a CONSTANT), supplied at runtime by
// the caller — so this only checks the surface compiles and behaves at the
// empty domain.
func TestStepLibSmoke(t *testing.T) {
	s := Init()
	// Over the empty domain no parameterised action has an argument to fire on,
	// so every enabled step must be nullary (Arg == ""). A nullary action whose
	// guard holds at Init (or is vacuously true — \A over the empty domain) may
	// legitimately appear.
	for _, st := range s.EnabledSteps(nil) {
		if st.Arg != "" {
			t.Fatalf("EnabledSteps(nil) returned a parameterised step %v; empty domain has no args", st)
		}
	}
	// A non-empty domain must not panic and every returned step must apply.
	for _, st := range s.EnabledSteps([]string{"probe"}) {
		_ = s.ApplyStep(st)
	}
}
`)
}

// paramDomainConsts returns every CONSTANT name a parameterised action's \E
// domain bottoms out on (recursing through \cup, the one composed shape
// domainExtentGo supports), deduplicated and sorted — the keys a caller's
// domains map must supply for every action to have something to range over.
func (cg *CodeGen) paramDomainConsts() []string {
	seen := map[string]bool{}
	var add func(d tla.Expr)
	add = func(d tla.Expr) {
		switch v := cg.resolveDomain(d).(type) {
		case tla.VarRef:
			seen[v.Name] = true
		case tla.BinOp:
			if v.Op == "\\cup" {
				add(v.Lhs)
				add(v.Rhs)
			}
		}
	}
	for _, act := range cg.Actions {
		for _, d := range act.ParamDomains {
			add(d)
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// writeTestStepLibWalkMulti is writeTestStepLibWalk's multi-param counterpart:
// the same empty-domain/non-empty-domain smoke test, but over the
// map[string][]string domains and Step{Args []string} surface writeStepLibMulti
// actually emits (see anyMultiParam), rather than the single-Arg []string form.
func (cg *CodeGen) writeTestStepLibWalkMulti(b *strings.Builder) {
	b.WriteString(`// TestStepLibSmoke exercises the guarded-transition library. A full
// reachability walk needs the \E domains (CONSTANTs), supplied at runtime by
// the caller — so this only checks the surface compiles and behaves at the
// empty domain.
func TestStepLibSmoke(t *testing.T) {
	s := Init()
	// Over the empty domain no parameterised action has an argument to fire on,
	// so every enabled step must be nullary (Args empty). A nullary action
	// whose guard holds at Init (or is vacuously true — \A over the empty
	// domain) may legitimately appear.
	for _, st := range s.EnabledSteps(nil) {
		if len(st.Args) != 0 {
			t.Fatalf("EnabledSteps(nil) returned a parameterised step %v; empty domain has no args", st)
		}
	}
`)
	consts := cg.paramDomainConsts()
	fmt.Fprintln(b, "\t// A non-empty domain must not panic and every returned step must apply.")
	fmt.Fprintln(b, "\tdomains := map[string][]string{")
	for _, name := range consts {
		fmt.Fprintf(b, "\t\t%q: {\"probe\"},\n", name)
	}
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "\tfor _, st := range s.EnabledSteps(domains) {")
	fmt.Fprintln(b, "\t\t_ = s.ApplyStep(st)")
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
}

// writeTestImmutabilityGuard emits TestApplyStepDoesNotAliasReceiver for PATH C
// modules with map-typed (set or function) state. A Go struct copy is
// shallow: without the cloneMap discipline in writeParamActions, ApplyStep
// could hand back a State whose map fields are the SAME underlying map as the
// receiver's. If they were, mutating the post-state (a perfectly normal thing
// for a caller to do) would silently corrupt the pre-state too — a bug the
// differential oracle (BFS==TLC) can't catch, because two states with equal
// keys still compare equal even if they alias the same backing map. Emitted
// only when the module HAS map state; a scalar-only module has nothing to
// alias.
func (cg *CodeGen) writeTestImmutabilityGuard(b *strings.Builder) {
	fields := cg.mapFields()
	if len(fields) == 0 {
		return
	}

	b.WriteString(`
// snapshotMap copies m's live entries into a fresh map, so it survives later
// mutation of m. V is "any" (not "comparable") because a func-of-func field's
// value type is itself a map — Go maps aren't comparable, but snapshotMap
// never compares V, only copies it.
func snapshotMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// tamperMap destructively empties m — the "teeth" that would corrupt any map
// sharing the same backing store as m.
func tamperMap[K comparable, V any](m map[K]V) {
	for k := range m {
		delete(m, k)
	}
}

// assertMapUnchanged fails t if got no longer matches want (same keys, same
// values, nothing extra). Values compare via reflect.DeepEqual rather than
// == so V can be any type Go can't natively compare (e.g. a func-of-func
// field's map[string]V value).
func assertMapUnchanged[K comparable, V any](t *testing.T, label string, want, got map[K]V) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: len changed: was %d, now %d (got %v, want %v)", label, len(want), len(got), got, want)
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok || !reflect.DeepEqual(gv, v) {
			t.Fatalf("%s: key %v changed: was %v, now %v", label, k, v, got[k])
		}
	}
}
`)

	fmt.Fprintln(b)
	fmt.Fprintln(b, "// TestApplyStepDoesNotAliasReceiver guards against a shared-backing-map bug:")
	fmt.Fprintln(b, "// ApplyStep (and the guarded action methods it dispatches to) must never")
	fmt.Fprintf(b, "// return a State whose %s map(s) are the SAME underlying map as the\n", strings.Join(fields, "/"))
	fmt.Fprintln(b, "// receiver's. See writeParamActions' cloneMap discipline.")
	fmt.Fprintln(b, "func TestApplyStepDoesNotAliasReceiver(t *testing.T) {")
	if cg.anyMultiParam() {
		fmt.Fprintln(b, "\tdomains := map[string][]string{")
		for _, name := range cg.paramDomainConsts() {
			fmt.Fprintf(b, "\t\t%q: {\"probe1\", \"probe2\"},\n", name)
		}
		fmt.Fprintln(b, "\t}")
	} else {
		fmt.Fprintln(b, "\tdomain := []string{\"probe1\", \"probe2\"}")
	}
	fmt.Fprintln(b, "\tpre := Init()")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "\tfor depth := 0; depth < 6; depth++ {")
	if cg.anyMultiParam() {
		fmt.Fprintln(b, "\t\tsteps := pre.EnabledSteps(domains)")
	} else {
		fmt.Fprintln(b, "\t\tsteps := pre.EnabledSteps(domain)")
	}
	fmt.Fprintln(b, "\t\tif len(steps) == 0 {")
	fmt.Fprintln(b, "\t\t\tbreak")
	fmt.Fprintln(b, "\t\t}")
	fmt.Fprintln(b, "\t\tfor _, step := range steps {")
	fmt.Fprintln(b, "\t\t\tpost := pre.ApplyStep(step)")
	fmt.Fprintln(b)
	for _, f := range fields {
		fmt.Fprintf(b, "\t\t\twant%s := snapshotMap(pre.%s)\n", f, f)
		fmt.Fprintf(b, "\t\t\ttamperMap(post.%s)\n", f)
		if cg.anyMultiParam() {
			fmt.Fprintf(b, "\t\t\tassertMapUnchanged(t, fmt.Sprintf(\"pre.%s after ApplyStep(%%s,%%v)\", step.Action, step.Args), want%s, pre.%s)\n", f, f, f)
		} else {
			fmt.Fprintf(b, "\t\t\tassertMapUnchanged(t, \"pre.%s after ApplyStep(\"+step.Action+\",\"+step.Arg+\")\", want%s, pre.%s)\n", f, f, f)
		}
	}
	fmt.Fprintln(b)
	fmt.Fprintln(b, "\t\t}")
	fmt.Fprintln(b, "\t\t// advance the walk using the first enabled step so subsequent depths")
	fmt.Fprintln(b, "\t\t// exercise a state with (hopefully) more populated map fields.")
	fmt.Fprintln(b, "\t\tpre = pre.ApplyStep(steps[0])")
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
}

// writeTestStepWalk generates a reachability test for PATH B: explore every
// (state, domain-value, free-var-combination) reachable from Init by calling
// Step, the way TestBFSReachability explores PATH A's EnabledActions/
// ApplyAction. Requires the \E domain to be a literal SetLit — the only
// domain shape real specs use for a key/mode dispatch.
func (cg *CodeGen) writeTestStepWalk(b *strings.Builder) {
	sd := cg.Dispatch
	domain, ok := sd.Domain.(tla.SetLit)
	if !ok {
		fmt.Fprintf(b, "// specgen: cannot generate a reachability walk — \\E domain is %T, not a literal set\n", sd.Domain)
		return
	}

	fmt.Fprintln(b, "// TestStepReachability walks the state space from Init, calling Step with")
	fmt.Fprintln(b, "// every domain value (and every combination of free-var settings, if any).")
	fmt.Fprintln(b, "// Verifies no transition panics and all states are reachable.")
	fmt.Fprintln(b, "func TestStepReachability(t *testing.T) {")
	fmt.Fprintf(b, "\tdomain := []string{")
	for i, el := range domain.Elems {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(b, "%s", litToGo(el))
	}
	fmt.Fprintln(b, "}")

	if len(cg.FreeVars) > 0 {
		fmt.Fprintln(b, "\tfreeVarSettings := [][]bool{}")
		fmt.Fprintf(b, "\tfor i := 0; i < 1<<%d; i++ {\n", len(cg.FreeVars))
		fmt.Fprintf(b, "\t\tvar combo []bool\n")
		fmt.Fprintf(b, "\t\tfor j := 0; j < %d; j++ {\n", len(cg.FreeVars))
		fmt.Fprintln(b, "\t\t\tcombo = append(combo, i&(1<<uint(j)) != 0)")
		fmt.Fprintln(b, "\t\t}")
		fmt.Fprintln(b, "\t\tfreeVarSettings = append(freeVarSettings, combo)")
		fmt.Fprintln(b, "\t}")
	}

	b.WriteString(`
	seen := map[string]bool{}
	queue := []State{Init()}
	maxStates := 10000

	for len(queue) > 0 && len(seen) < maxStates {
		s := queue[0]
		queue = queue[1:]
		key := fmt.Sprintf("%+v", s)
		if seen[key] {
			continue
		}
		seen[key] = true

`)
	if len(cg.FreeVars) > 0 {
		fmt.Fprintln(b, "\t\tfor _, d := range domain {")
		fmt.Fprintln(b, "\t\t\tfor _, combo := range freeVarSettings {")
		fmt.Fprintf(b, "\t\t\t\tnext := s.Step(d")
		for i := range cg.FreeVars {
			fmt.Fprintf(b, ", combo[%d]", i)
		}
		fmt.Fprintln(b, ")")
		fmt.Fprintln(b, "\t\t\t\tqueue = append(queue, next)")
		fmt.Fprintln(b, "\t\t\t}")
		fmt.Fprintln(b, "\t\t}")
	} else {
		fmt.Fprintln(b, "\t\tfor _, d := range domain {")
		fmt.Fprintln(b, "\t\t\tnext := s.Step(d)")
		fmt.Fprintln(b, "\t\t\tqueue = append(queue, next)")
		fmt.Fprintln(b, "\t\t}")
	}
	b.WriteString(`	}

	if len(seen) == 0 {
		t.Fatal("no reachable states")
	}
	t.Logf("explored %d reachable states", len(seen))
}
`)
}

func (cg *CodeGen) writeHeader(b *strings.Builder, needsReflect bool) {
	pkg := cg.PackageName
	if pkg == "" {
		pkg = strings.ToLower(cg.Spec.Name) + "spec"
	}
	source := cg.SpecPath
	if source == "" {
		source = cg.Spec.Name + ".tla"
	}
	fmt.Fprintf(b, "// Code generated by specgen from %s — DO NOT EDIT.\n", source)
	fmt.Fprintln(b, "//")
	fmt.Fprintf(b, "// Source:     %s\n", source)
	fmt.Fprintf(b, "// Regenerate: %s\n", cg.regenCommand(pkg))
	fmt.Fprintln(b, "//")
	fmt.Fprintln(b, "// Edit the spec, not this file: change the .tla above and re-run the command.")
	fmt.Fprintln(b)
	fmt.Fprintf(b, "package %s\n\n", pkg)
	if needsReflect {
		// funcEq (whole-func equality, e.g. `tbl[k] = [b \in Bs |-> ...]`) is
		// the one production-file helper that needs an import.
		fmt.Fprintln(b, `import "reflect"`)
		fmt.Fprintln(b)
	}
}

// purePredicate is a nullary pure def (name + already-inlined body expr as Go).
type purePredicate struct {
	name   string
	goExpr string
}

// purePredicates lists nullary TLA+ operators that are pure (no primed vars),
// not actions, not baits, and lower to a Go bool expression.
func (cg *CodeGen) purePredicates() []purePredicate {
	if cg.Spec == nil {
		return nil
	}
	actionNames := make(map[string]bool, len(cg.Actions))
	for _, a := range cg.Actions {
		actionNames[a.Name] = true
	}
	reserved := map[string]bool{
		"Init": true, "Next": true, "Spec": true,
		"TypeOK": true, "TypeOk": true, "vars": true,
	}
	var out []purePredicate
	for _, d := range cg.Spec.Defs {
		if reserved[d.Name] || actionNames[d.Name] || len(d.Params) > 0 {
			continue
		}
		// Bait* operators exist only to force TLC exploration (must FAIL).
		// They are not production decision API — skip them.
		if strings.HasPrefix(d.Name, "Bait") {
			continue
		}
		if cg.Spec.Recursive != nil && cg.Spec.Recursive[d.Name] {
			continue
		}
		if d.Body == nil || tla.HasPrimed(d.Body) {
			continue
		}
		inlined, err := tla.Inline(cg.Spec, d.Body)
		if err != nil {
			continue
		}
		goExpr := cg.guardToGo(inlined, "s")
		if goExpr == "" || strings.Contains(goExpr, "/* unsupported") {
			continue
		}
		out = append(out, purePredicate{name: d.Name, goExpr: goExpr})
	}
	return out
}

// writePurePredicates emits State methods + a PurePredicates() registry for
// dual/CI enumeration. Example: WaitNeeded == remaining = 0 /\ ... becomes
//
//	func (s State) WaitNeeded() bool { return s.Remaining == 0 && ... }
//
// Production duals call these by name instead of re-deriving the formula.
// Keeps the decision-module core: only scalar pure predicates, no records.
func (cg *CodeGen) writePurePredicates(b *strings.Builder) {
	preds := cg.purePredicates()
	if len(preds) == 0 {
		return
	}
	b.WriteString("// --- Pure operators (no primed vars; not actions) ---\n\n")
	for _, p := range preds {
		fmt.Fprintf(b, "// %s is the pure TLA+ operator %s.\n", p.name, p.name)
		fmt.Fprintf(b, "func (s State) %s() bool {\n", p.name)
		fmt.Fprintf(b, "\treturn %s\n", p.goExpr)
		fmt.Fprintln(b, "}")
		fmt.Fprintln(b)
	}
	// Registry so duals/CI can walk every pure gate without hand lists.
	b.WriteString("// PurePredicate is a named pure decision gate (no state change).\n")
	b.WriteString("type PurePredicate struct {\n")
	b.WriteString("\tName  string\n")
	b.WriteString("\tCheck func(State) bool\n")
	b.WriteString("}\n\n")
	b.WriteString("// PurePredicates returns every pure operator emitted for this module.\n")
	b.WriteString("func PurePredicates() []PurePredicate {\n")
	b.WriteString("\treturn []PurePredicate{\n")
	for _, p := range preds {
		fmt.Fprintf(b, "\t\t{Name: %q, Check: State.%s},\n", p.name, p.name)
	}
	b.WriteString("\t}\n}\n\n")
}

// regenCommand reconstructs the exact specgen invocation that (re)produces this
// module, so the DO-NOT-EDIT header is actionable. Flags are ordered to match
// the CLI help: -const (in the order given), then -o, -p, then the spec path.
// Paths are cwd-relative when possible so temp absolute -o dirs do not poison
// checked-in headers (regen no-diff).
func (cg *CodeGen) regenCommand(pkg string) string {
	parts := []string{"specgen"}
	for _, c := range cg.ConstFlags {
		parts = append(parts, "-const", c)
	}
	if cg.OutputDir != "" {
		parts = append(parts, "-o", stablePath(cg.OutputDir))
	}
	parts = append(parts, "-p", pkg)
	source := cg.SpecPath
	if source == "" {
		source = cg.Spec.Name + ".tla"
	}
	parts = append(parts, stablePath(source))
	return strings.Join(parts, " ")
}

// stablePath returns a cwd-relative path when p is under the working
// directory; otherwise the original string.
func stablePath(p string) string {
	if p == "" || !filepath.IsAbs(p) {
		return p
	}
	cwd, err := filepath.Abs(".")
	if err != nil {
		return p
	}
	rel, err := filepath.Rel(cwd, p)
	if err != nil || strings.HasPrefix(rel, "..") {
		return p
	}
	return rel
}

func (cg *CodeGen) writeState(b *strings.Builder) {
	fmt.Fprintln(b, "// State mirrors the TLA+ VARIABLES.")
	fmt.Fprintln(b, "type State struct {")
	for _, v := range cg.Spec.Variables {
		goType := cg.inferType(v)
		fmt.Fprintf(b, "\t%s %s\n", goField(v), goType)
	}
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

func (cg *CodeGen) writeInit(b *strings.Builder) {
	fmt.Fprintln(b, "// Init returns the initial state from the TLA+ Init predicate.")
	fmt.Fprintln(b, "func Init() State {")
	fmt.Fprintln(b, "\ts := State{")
	initBody := cg.Spec.FindDef("Init")
	for _, v := range cg.Spec.Variables {
		val := findInitVal(v, initBody)
		if val == nil {
			continue
		}
		// A set literal's own rendering can't know the element type of the empty
		// set `{}`, so a tuple-set field (map[[2]string]bool) would get a
		// map[string]bool value. Emit it against the field's inferred type, which
		// TypeOK fixed authoritatively.
		if sl, ok := val.(tla.SetLit); ok {
			goType := cg.inferType(v)
			if strings.HasPrefix(goType, "map[") && strings.HasSuffix(goType, "]bool") {
				var elems []string
				for _, el := range sl.Elems {
					elems = append(elems, cg.exprToGo(el, nil, "s")+": true")
				}
				fmt.Fprintf(b, "\t\t%s: %s{%s},\n", goField(v), goType, strings.Join(elems, ", "))
				continue
			}
		}
		// Anything besides the bool-set literal special-cased above (a scalar
		// literal, or a func-literal `[k \in D |-> body]` with a concrete or
		// abstract domain) delegates to updateValToGo, the fully general
		// value-position lowering — litToGo only ever covered scalars and
		// SetLit, silently rendering a func literal's Init value as untyped
		// "nil" (never actually populating it) rather than the map it stood
		// for.
		fmt.Fprintf(b, "\t\t%s: %s,\n", goField(v), cg.updateValToGo(val))
	}
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "\treturn s")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

func (cg *CodeGen) writeActions(b *strings.Builder) {
	for _, act := range cg.Actions {
		// Guard
		fmt.Fprintf(b, "// Can%s is the guard for the %s action.\n", act.Name, act.Name)
		fmt.Fprintf(b, "func (s State) Can%s() bool {\n", act.Name)
		fmt.Fprintf(b, "\treturn %s\n", cg.guardToGo(act.Guard, "s"))
		fmt.Fprintln(b, "}")
		fmt.Fprintln(b)

		// Transition. pre freezes the receiver as of entry: TLA+ primed
		// assignments within one action are simultaneous, not sequential, so
		// an update like `y' = x /\ x' = y` (or Commit-style `actMode' =
		// mode` alongside `mode' = ...`) must read the PRE-transition value
		// even after an earlier line in this same method already wrote a new
		// value to that field. Only the assignment target (s.Field = ...)
		// touches the mutating receiver. pre is declared only if some update
		// actually reads it — a literal-only action (e.g. mode' = "Search")
		// never references pre, and Go rejects an unused local.
		//
		// Every map-typed field is additionally wrapped in cloneMap (or, if
		// UNCHANGED/self-assigned, assigned cloneMap(pre.Field) outright) —
		// see writeParamActions' cloneMap discipline: pre := s is a shallow
		// struct copy, so a map field this action doesn't touch, or assigns
		// from an expression resolving to pre's own map, would otherwise
		// still alias the receiver's backing map.
		touched := map[string]bool{}
		var lines strings.Builder
		for _, u := range act.Update {
			if isSelfAssign(u) {
				continue // x' = x → no change; cloned below like any UNCHANGED field
			}
			touched[u.Var] = true
			val := cg.updateValToGo(u.Val)
			if strings.HasPrefix(cg.inferType(u.Var), "map[") {
				val = fmt.Sprintf("cloneMap(%s)", val)
			}
			fmt.Fprintf(&lines, "\ts.%s = %s\n", goField(u.Var), val)
		}
		for _, v := range cg.Spec.Variables {
			if touched[v] || !strings.HasPrefix(cg.inferType(v), "map[") {
				continue
			}
			fmt.Fprintf(&lines, "\ts.%s = cloneMap(pre.%s)\n", goField(v), goField(v))
		}
		fmt.Fprintf(b, "// %s applies the %s action, returning a new state.\n", act.Name, act.Name)
		fmt.Fprintf(b, "func (s State) %s() State {\n", act.Name)
		if strings.Contains(lines.String(), "pre.") {
			fmt.Fprintln(b, "\tpre := s")
		}
		b.WriteString(lines.String())
		fmt.Fprintln(b, "\treturn s")
		fmt.Fprintln(b, "}")
		fmt.Fprintln(b)
	}
}

// actionParamsSig renders an action's \E-bound params as a Go signature
// suffix. Set elements are model values, so each param is a string:
// `(x string)`, or `()` for a nullary action.
func actionParamsSig(act Action) string {
	var parts []string
	for _, p := range act.Params {
		parts = append(parts, p+" string")
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// actionLocals maps an action's \E-bound param names to the Go identifiers
// that hold them (their own function parameters, by construction), so
// exprToGo resolves `x` to the parameter rather than a state field.
func actionLocals(act Action) map[string]string {
	locals := map[string]string{}
	for _, p := range act.Params {
		locals[p] = p
	}
	return locals
}

// resolveDomain unwraps `DOMAIN f` back to f's declared domain expression, so
// callers that already special-case a SetLit/VarRef domain (quantDomainGo,
// inToGo, inToGoCtx, guardNeedsDomain) need no separate DOMAIN case of their
// own. This is deliberately NOT a runtime helper reading a function's live map
// keys: a function declared over an abstract CONSTANT domain is stored as a
// lazy map (see funcDefault) that may hold far fewer keys than its logical
// domain, so "keys of the map" would silently undercount. Resolving back to
// the ORIGINAL domain expression keeps DOMAIN precise in both the concrete and
// abstract cases, by reusing the same machinery a bare `\in`/quantifier domain
// already goes through.
func (cg *CodeGen) resolveDomain(e tla.Expr) tla.Expr {
	de, ok := e.(tla.DomainExpr)
	if !ok {
		return e
	}
	vr, ok := de.Of.(tla.VarRef)
	if !ok {
		return e // can't resolve further; leave as-is
	}
	initBody := cg.Spec.FindDef("Init")
	if fl, ok := findInitVal(vr.Name, initBody).(tla.FuncLit); ok {
		return fl.Domain
	}
	return e
}

// guardNeedsDomain reports whether a guard quantifies over the abstract \E
// domain (the CONSTANT set, unknown at codegen time — the same set EnabledSteps
// walks). Its Can method then takes an extra `domain []string` to fold over. A
// guard that only quantifies over set-typed state variables reads keysOf and
// needs no domain, matching quantDomainGo's split.
func (cg *CodeGen) guardNeedsDomain(guard tla.Expr) bool {
	needs := false
	tla.Walk(guard, func(n tla.Expr) bool {
		var dom tla.Expr
		switch q := n.(type) {
		case tla.ForallExpr:
			dom = q.Domain
		case tla.ExistsExpr:
			dom = q.Domain
		case tla.SetCompr:
			// `{h \in S : ...}` folds over S just like \A/\E: if S is the
			// abstract CONSTANT (not a set var), the Can method needs `domain`.
			dom = q.Domain
		default:
			return true
		}
		if vr, ok := cg.resolveDomain(dom).(tla.VarRef); ok && !cg.isSetVar(vr.Name) {
			needs = true
		}
		return true
	})
	return needs
}

// guardSig is the Can<A> signature: the action's \E-bound params plus a
// trailing `domain []string` when the guard folds over the abstract domain
// (see guardNeedsDomain). The transition method keeps the plain
// actionParamsSig — only guards quantify.
func (cg *CodeGen) guardSig(act Action) string {
	var parts []string
	for _, p := range act.Params {
		parts = append(parts, p+" string")
	}
	if cg.guardNeedsDomain(act.Guard) {
		parts = append(parts, "domain []string")
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// writeParamActions emits the PATH C guarded-transition library: for every
// action (parameterised or nullary) a Can<A>(args)/<A>(args) pair. The guard
// reads live state ("s"); the transition freezes state as "pre" (TLA+ primed
// assignments in one action are simultaneous) and rebuilds each assigned set/
// function field from pre via copy-on-write helpers, so value semantics hold —
// s.Field = setUnion(pre.Field, ...) never mutates the caller's map.
//
// Every map-typed field is additionally wrapped in cloneMap (or, if UNCHANGED
// / a no-op x' = x, assigned a cloneMap of pre's field outright). A Go struct
// copy (pre := s) is shallow: a map field an action doesn't touch is still the
// SAME backing map as the receiver's, and an assigned value can itself resolve
// to the receiver's own map (e.g. a constant-folded IF branch, `s.Open =
// specgenIf(true, pre.Open, ...)`, picks the bare pre.Open branch). Either way,
// without cloneMap the returned State would alias the receiver's map — so a
// caller mutating the post-state's map silently corrupts the pre-state too.
func (cg *CodeGen) writeParamActions(b *strings.Builder) {
	for _, act := range cg.Actions {
		sig := actionParamsSig(act)
		locals := actionLocals(act)

		fmt.Fprintf(b, "// Can%s is the guard for the %s action.\n", act.Name, act.Name)
		fmt.Fprintf(b, "func (s State) Can%s%s bool {\n", act.Name, cg.guardSig(act))
		fmt.Fprintf(b, "\treturn %s\n", cg.exprToGo(act.Guard, locals, "s"))
		fmt.Fprintln(b, "}")
		fmt.Fprintln(b)

		touched := map[string]bool{}
		var lines strings.Builder
		for _, u := range act.Update {
			if isSelfAssign(u) {
				continue
			}
			touched[u.Var] = true
			val := cg.exprToGo(u.Val, locals, "pre")
			if strings.HasPrefix(cg.inferType(u.Var), "map[") {
				val = fmt.Sprintf("cloneMap(%s)", val)
			}
			fmt.Fprintf(&lines, "\ts.%s = %s\n", goField(u.Var), val)
		}
		// UNCHANGED (or self-assigned) map/function fields: never assigned
		// above, so they're still the receiver's own map. Clone explicitly.
		for _, v := range cg.Spec.Variables {
			if touched[v] || !strings.HasPrefix(cg.inferType(v), "map[") {
				continue
			}
			fmt.Fprintf(&lines, "\ts.%s = cloneMap(pre.%s)\n", goField(v), goField(v))
		}
		fmt.Fprintf(b, "// %s applies the %s action, returning a new state.\n", act.Name, act.Name)
		fmt.Fprintf(b, "func (s State) %s%s State {\n", act.Name, sig)
		if strings.Contains(lines.String(), "pre.") {
			fmt.Fprintln(b, "\tpre := s")
		}
		b.WriteString(lines.String())
		fmt.Fprintln(b, "\treturn s")
		fmt.Fprintln(b, "}")
		fmt.Fprintln(b)
	}
}

// writeStepLib emits the PATH C driver: a Step descriptor plus EnabledSteps
// (enumerate enabled (action, arg) pairs over a caller-supplied domain) and
// ApplyStep (fire one). The domain is the spec's \E set — a CONSTANT, unknown
// at codegen time — so the caller (conform's walk, or a test) passes its
// extent in. Nullary actions ignore the domain and appear once.
// anyMultiParam reports whether any action binds more than one \E parameter —
// the signal to emit the heterogeneous-domain Step surface (Args []string +
// EnabledSteps(domains map[string][]string)) rather than the single-arg form.
// Single-param specs (Hub, setbox, pty) keep the compact single-Arg surface.
func (cg *CodeGen) anyMultiParam() bool {
	for _, act := range cg.Actions {
		if len(act.Params) > 1 {
			return true
		}
	}
	return false
}

// domainConst returns the CONSTANT name a param ranges over — the key the caller
// uses in the domains map. ParamDomains carry the inlined \E domain, a VarRef to
// the CONSTANT set for every case codegen supports here.
func domainConst(d tla.Expr) string {
	if vr, ok := d.(tla.VarRef); ok {
		return vr.Name
	}
	return ""
}

// domainExtentGo lowers a param's \E domain to a Go []string expression — the
// runtime extent to range over. A plain set CONSTANT (VarRef) reads its extent
// from the domains map; a model-value singleton {Term} is a literal slice; a
// union `A \cup B` (SessAttach's `Hosts == Sessions \cup {Term}`) concatenates
// both extents. Unions compose, so nested \cup nests naturally.
func (cg *CodeGen) domainExtentGo(d tla.Expr) string {
	switch v := cg.resolveDomain(d).(type) {
	case tla.VarRef:
		return fmt.Sprintf("domains[%q]", v.Name)
	case tla.SetLit:
		elems := make([]string, len(v.Elems))
		for i, el := range v.Elems {
			elems[i] = cg.exprToGo(el, nil, "")
		}
		return "[]string{" + strings.Join(elems, ", ") + "}"
	case tla.BinOp:
		if v.Op == "\\cup" {
			return fmt.Sprintf("append(append([]string{}, %s...), %s...)",
				cg.domainExtentGo(v.Lhs), cg.domainExtentGo(v.Rhs))
		}
	}
	return fmt.Sprintf("domains[%q] /* unhandled domain %T */", domainConst(d), d)
}

func (cg *CodeGen) writeStepLib(b *strings.Builder) {
	if cg.anyMultiParam() {
		cg.writeStepLibMulti(b)
		return
	}
	b.WriteString(`// Step names one enabled transition: an action and its argument.
// Arg is "" for a nullary action.
type Step struct {
	Action string ` + "`json:\"action\"`" + `
	Arg    string ` + "`json:\"arg,omitempty\"`" + `
}

`)
	fmt.Fprintln(b, "// EnabledSteps returns every enabled (action, arg) pair. Parameterised")
	fmt.Fprintln(b, "// actions are tried against each value of domain (the spec's \\E set).")
	fmt.Fprintln(b, "func (s State) EnabledSteps(domain []string) []Step {")
	fmt.Fprintln(b, "\tvar out []Step")
	hasParam := false
	for _, act := range cg.Actions {
		if len(act.Params) > 0 {
			hasParam = true
		}
	}
	if hasParam {
		fmt.Fprintln(b, "\tfor _, x := range domain {")
		for _, act := range cg.Actions {
			if len(act.Params) == 0 {
				continue
			}
			guardArgs := "x"
			if cg.guardNeedsDomain(act.Guard) {
				guardArgs = "x, domain"
			}
			fmt.Fprintf(b, "\t\tif s.Can%s(%s) {\n", act.Name, guardArgs)
			fmt.Fprintf(b, "\t\t\tout = append(out, Step{Action: %q, Arg: x})\n", act.Name)
			fmt.Fprintln(b, "\t\t}")
		}
		fmt.Fprintln(b, "\t}")
	}
	for _, act := range cg.Actions {
		if len(act.Params) > 0 {
			continue
		}
		guardArgs := ""
		if cg.guardNeedsDomain(act.Guard) {
			guardArgs = "domain"
		}
		fmt.Fprintf(b, "\tif s.Can%s(%s) {\n", act.Name, guardArgs)
		fmt.Fprintf(b, "\t\tout = append(out, Step{Action: %q})\n", act.Name)
		fmt.Fprintln(b, "\t}")
	}
	fmt.Fprintln(b, "\treturn out")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)

	fmt.Fprintln(b, "// ApplyStep fires one step, returning the new state. Panics on an unknown")
	fmt.Fprintln(b, "// action name.")
	fmt.Fprintln(b, "func (s State) ApplyStep(st Step) State {")
	fmt.Fprintln(b, "\tswitch st.Action {")
	for _, act := range cg.Actions {
		fmt.Fprintf(b, "\tcase %q:\n", act.Name)
		if len(act.Params) > 0 {
			fmt.Fprintf(b, "\t\treturn s.%s(st.Arg)\n", act.Name)
		} else {
			fmt.Fprintf(b, "\t\treturn s.%s()\n", act.Name)
		}
	}
	fmt.Fprintln(b, "\tdefault:")
	fmt.Fprintln(b, "\t\tpanic(\"unknown action: \" + st.Action)")
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

// writeStepLibMulti emits the heterogeneous-domain PATH C driver: a Step whose
// Args holds one chosen value per \E-bound parameter, and an EnabledSteps that
// takes the per-CONSTANT extents as a map (each param may range over a different
// set — c over Clients, h over Hosts, t over Sessions). For each action the
// enabled set is the cartesian product of its params' domains; nullary actions
// appear once. This is SessAttach's Check(c,h,t) shape.
func (cg *CodeGen) writeStepLibMulti(b *strings.Builder) {
	b.WriteString(`// Step names one enabled transition: an action and one chosen value per
// \E-bound parameter (Args, in parameter order). Args is empty for a nullary action.
type Step struct {
	Action string ` + "`json:\"action\"`" + `
	Args   []string ` + "`json:\"args,omitempty\"`" + `
}

`)
	fmt.Fprintln(b, "// EnabledSteps returns every enabled (action, args) tuple. Each parameter is")
	fmt.Fprintln(b, "// tried against domains[<CONSTANT>] — the extent of the set it ranges over.")
	fmt.Fprintln(b, "func (s State) EnabledSteps(domains map[string][]string) []Step {")
	fmt.Fprintln(b, "\tvar out []Step")
	for _, act := range cg.Actions {
		if len(act.Params) == 0 {
			guardArgs := ""
			if cg.guardNeedsDomain(act.Guard) {
				guardArgs = "nil"
			}
			fmt.Fprintf(b, "\tif s.Can%s(%s) {\n", act.Name, guardArgs)
			fmt.Fprintf(b, "\t\tout = append(out, Step{Action: %q})\n", act.Name)
			fmt.Fprintln(b, "\t}")
			continue
		}
		indent := "\t"
		for i, p := range act.Params {
			ext := cg.domainExtentGo(act.ParamDomains[i])
			fmt.Fprintf(b, "%sfor _, %s := range %s {\n", indent, p, ext)
			indent += "\t"
		}
		fmt.Fprintf(b, "%sif s.Can%s(%s) {\n", indent, act.Name, strings.Join(act.Params, ", "))
		quoted := make([]string, len(act.Params))
		for i, p := range act.Params {
			quoted[i] = p
		}
		fmt.Fprintf(b, "%s\tout = append(out, Step{Action: %q, Args: []string{%s}})\n",
			indent, act.Name, strings.Join(quoted, ", "))
		fmt.Fprintf(b, "%s}\n", indent)
		for range act.Params {
			indent = indent[:len(indent)-1]
			fmt.Fprintf(b, "%s}\n", indent)
		}
	}
	fmt.Fprintln(b, "\treturn out")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)

	fmt.Fprintln(b, "// ApplyStep fires one step, returning the new state. Panics on an unknown")
	fmt.Fprintln(b, "// action name.")
	fmt.Fprintln(b, "func (s State) ApplyStep(st Step) State {")
	fmt.Fprintln(b, "\tswitch st.Action {")
	for _, act := range cg.Actions {
		fmt.Fprintf(b, "\tcase %q:\n", act.Name)
		if n := len(act.Params); n > 0 {
			args := make([]string, n)
			for i := range act.Params {
				args[i] = fmt.Sprintf("st.Args[%d]", i)
			}
			fmt.Fprintf(b, "\t\treturn s.%s(%s)\n", act.Name, strings.Join(args, ", "))
		} else {
			fmt.Fprintf(b, "\t\treturn s.%s()\n", act.Name)
		}
	}
	fmt.Fprintln(b, "\tdefault:")
	fmt.Fprintln(b, "\t\tpanic(\"unknown action: \" + st.Action)")
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

// writeStateSerializer emits TLAState, which renders a State as a
// conform-compatible TLA+ value map. The generated Go stores sets as
// map[string]bool and functions as lazy (sparse) map[string]V, but conform's
// value model wants tagged sets {"set": [...]} and TOTAL functions
// {"fn": {...}}. TLAState bridges the two: functions are densified over domain
// (absent keys read their Init default via funcGet), sets are collected and
// tagged, scalars pass through. Keys are the Go field names, which is what
// conform's bind() looks up first. Only param-lib (PATH C) modules — the ones
// with set/function state a raw json.Marshal would misrepresent — get it.
func (cg *CodeGen) writeStateSerializer(b *strings.Builder) {
	fmt.Fprintln(b, "// TLAState renders the state as a conform-compatible TLA+ value map:")
	fmt.Fprintln(b, "// sets become {\"set\": [...]}, functions become {\"fn\": {...}} made total")
	fmt.Fprintln(b, "// over domain (absent keys read their Init default), scalars pass through.")
	fmt.Fprintln(b, "func (s State) TLAState(domain []string) map[string]any {")
	fmt.Fprintln(b, "\tout := map[string]any{}")
	for _, v := range cg.Spec.Variables {
		field := goField(v)
		t := cg.inferType(v)
		switch {
		case cg.isSetVar(v):
			fmt.Fprintf(b, "\t{\n")
			fmt.Fprintf(b, "\t\telems := []any{}\n")
			fmt.Fprintf(b, "\t\tfor k, in := range s.%s {\n\t\t\tif in {\n\t\t\t\telems = append(elems, k)\n\t\t\t}\n\t\t}\n", field)
			fmt.Fprintf(b, "\t\tout[%q] = map[string]any{\"set\": elems}\n", field)
			fmt.Fprintf(b, "\t}\n")
		case strings.HasPrefix(t, "map[string]"):
			def := cg.funcDefault(v)
			fmt.Fprintf(b, "\t{\n")
			fmt.Fprintf(b, "\t\tfnv := map[string]any{}\n")
			fmt.Fprintf(b, "\t\tfor _, k := range domain {\n\t\t\tfnv[k] = funcGet(s.%s, k, %s)\n\t\t}\n", field, def)
			fmt.Fprintf(b, "\t\tout[%q] = map[string]any{\"fn\": fnv}\n", field)
			fmt.Fprintf(b, "\t}\n")
		default:
			fmt.Fprintf(b, "\tout[%q] = s.%s\n", field, field)
		}
	}
	fmt.Fprintln(b, "\treturn out")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

// paramName converts a TLA+ CONSTANT name to a Go parameter name: HasSel ->
// hasSel. Mirrors goField's single-rune-case-flip, in the other direction.
func paramName(name string) string {
	if name == "" {
		return name
	}
	r := []rune(name)
	r[0] = []rune(strings.ToLower(string(r[0])))[0]
	return string(r)
}

// stepExtraParamsSig renders the extra CONSTANT parameters (unbound free
// vars) as a Go signature suffix: ", hasSel bool" or "" if none.
func (cg *CodeGen) stepExtraParamsSig() string {
	var sb strings.Builder
	for _, fv := range cg.FreeVars {
		sb.WriteString(", ")
		sb.WriteString(paramName(fv))
		sb.WriteString(" bool")
	}
	return sb.String()
}

// stepExtraArgsCall renders the extra params as a call-site argument suffix:
// ", hasSel" or "".
func (cg *CodeGen) stepExtraArgsCall() string {
	var sb strings.Builder
	for _, fv := range cg.FreeVars {
		sb.WriteString(", ")
		sb.WriteString(paramName(fv))
	}
	return sb.String()
}

// stepLocals maps the TLA+ names visible inside a Step body — the \E-bound
// quantifier variable and every free CONSTANT — to the Go expression that
// refers to them (their own function parameter, by construction).
func (cg *CodeGen) stepLocals() map[string]string {
	locals := map[string]string{cg.Dispatch.QuantVar: cg.Dispatch.QuantVar}
	for _, fv := range cg.FreeVars {
		locals[fv] = paramName(fv)
	}
	return locals
}

// writeStep generates Step, the PATH B decision function: mode-dispatch (on
// Dispatch.DispatchVar) into each per-mode operator's inlined CASE/IF body.
// Step is total — CASE's OTHER arm makes every per-mode operator exhaustive
// over the domain, and the done-check makes the whole function a no-op past
// completion — so there is no separate Can/EnabledActions pair to generate.
func (cg *CodeGen) writeStep(b *strings.Builder) {
	sd := cg.Dispatch
	fmt.Fprintf(b, "// Step advances the state given one input value (the spec's \\E-bound %s).\n", sd.QuantVar)
	fmt.Fprintf(b, "func (s State) Step(%s string%s) State {\n", sd.QuantVar, cg.stepExtraParamsSig())
	if sd.DoneVar != "" {
		fmt.Fprintf(b, "\tif s.%s {\n\t\treturn s\n\t}\n", goField(sd.DoneVar))
	}
	// pre freezes the state as of entry. Every guard/update-value expression
	// reads pre, never s — TLA+'s primed assignments within one conjunction
	// are simultaneous, not sequential, so `actMode' = mode` (Commit's
	// pattern) must see the PRE-transition mode even after this same Step
	// call already wrote a new value to s.Mode. Only assignment targets
	// (s.Field = ...) touch the mutating receiver. pre is declared only if
	// some branch actually reads it — a spec whose branches assign only
	// literals never references pre, and Go rejects an unused local.
	var body strings.Builder
	locals := cg.stepLocals()
	fmt.Fprintf(&body, "\tswitch s.%s {\n", goField(sd.DispatchVar))
	for _, branch := range sd.Branches {
		fmt.Fprintf(&body, "\tcase %s:\n", cg.exprToGo(branch.Value, locals, "pre"))
		inlined, err := tla.Inline(cg.Spec, tla.Call{Name: branch.OpName, Args: []tla.Expr{tla.VarRef{Name: sd.QuantVar}}})
		if err != nil {
			fmt.Fprintf(&body, "\t\t// specgen: failed to inline %s: %s\n", branch.OpName, err)
			continue
		}
		// Map-typed fields this branch's TOP-LEVEL conjuncts don't directly
		// assign (UNCHANGED, or only touched inside a nested CASE/IF arm) are
		// cloned from pre up front — mirrors writeParamActions' cloneMap
		// discipline: pre := s is a shallow struct copy, so an untouched map
		// field would otherwise still alias the receiver's backing map. A
		// nested arm that DOES assign the field overwrites this default with
		// its own (also cloneMap-wrapped) value, since it's emitted after.
		touched := topLevelAssignedVars(inlined)
		for _, v := range cg.Spec.Variables {
			if touched[v] || !strings.HasPrefix(cg.inferType(v), "map[") {
				continue
			}
			fmt.Fprintf(&body, "\t\ts.%s = cloneMap(pre.%s)\n", goField(v), goField(v))
		}
		cg.writeAssignBody(&body, inlined, locals, "\t\t")
	}
	fmt.Fprintln(&body, "\tdefault:")
	fmt.Fprintf(&body, "\t\t// s.%s has a value no branch models — no-op.\n", goField(sd.DispatchVar))
	fmt.Fprintln(&body, "\t}")

	if strings.Contains(body.String(), "pre.") {
		fmt.Fprintln(b, "\tpre := s")
	}
	b.WriteString(body.String())
	fmt.Fprintln(b, "\treturn s")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

// topLevelAssignedVars returns the set of variable names directly assigned by
// a TOP-LEVEL `var' = expr` conjunct in body — NOT recursing into a nested
// CASE/IF's arms (those are branch-dependent; writeAssignBody handles their
// assignments when it descends into them). Used by writeStep to decide which
// map-typed fields need a default cloneMap(pre.Field): a var this scan
// doesn't find is either UNCHANGED or assigned only inside a nested arm, and
// either way isn't yet covered by a real assignment at this level.
func topLevelAssignedVars(body tla.Expr) map[string]bool {
	touched := map[string]bool{}
	for _, c := range tla.FlattenConjunction(body) {
		bin, ok := c.(tla.BinOp)
		if !ok || bin.Op != "=" {
			continue
		}
		if pv, ok := bin.Lhs.(tla.PrimedVar); ok {
			touched[pv.Name] = true
		}
	}
	return touched
}

// writeAssignBody emits Go statements assigning primed variables per an
// already-Inline()'d step-operator body: a conjunction of primed-eq
// assignments, UNCHANGED markers (no-ops — Go value semantics already leave
// untouched fields alone), and at most one CASE or IF providing the
// branch-dependent remainder.
//
// A map-typed assignment target is wrapped in cloneMap — see
// writeParamActions' cloneMap discipline: pre := s is a shallow struct copy,
// so an assigned value that itself resolves to pre's own map (e.g. a bare
// `y' = x` alongside x's own UNCHANGED) would otherwise alias the receiver's
// backing map. The untouched-field half of that discipline (cloning map
// fields this operator never assigns at all) is writeStep's job, once per
// dispatch branch — see the comment there.
func (cg *CodeGen) writeAssignBody(b *strings.Builder, body tla.Expr, locals map[string]string, indent string) {
	for _, c := range tla.FlattenConjunction(body) {
		switch v := c.(type) {
		case tla.BinOp:
			if v.Op == "=" {
				if pv, ok := v.Lhs.(tla.PrimedVar); ok {
					val := cg.exprToGo(v.Rhs, locals, "pre")
					if strings.HasPrefix(cg.inferType(pv.Name), "map[") {
						val = fmt.Sprintf("cloneMap(%s)", val)
					}
					fmt.Fprintf(b, "%ss.%s = %s\n", indent, goField(pv.Name), val)
					continue
				}
			}
			fmt.Fprintf(b, "%s// specgen: unhandled conjunct %+v\n", indent, c)
		case tla.UnchangedVars:
			// no-op
		case tla.CaseExpr:
			cg.writeCase(b, v, locals, indent)
		case tla.IfExpr:
			cg.writeIf(b, v, locals, indent)
		default:
			fmt.Fprintf(b, "%s// specgen: unhandled conjunct %+v\n", indent, c)
		}
	}
}

func (cg *CodeGen) writeCase(b *strings.Builder, ce tla.CaseExpr, locals map[string]string, indent string) {
	for i, arm := range ce.Arms {
		kw := "if"
		if i > 0 {
			kw = "} else if"
		}
		fmt.Fprintf(b, "%s%s %s {\n", indent, kw, cg.exprToGo(arm.Cond, locals, "pre"))
		cg.writeAssignBody(b, arm.Result, locals, indent+"\t")
	}
	if ce.Other != nil {
		fmt.Fprintf(b, "%s} else {\n", indent)
		cg.writeAssignBody(b, ce.Other, locals, indent+"\t")
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

func (cg *CodeGen) writeIf(b *strings.Builder, ie tla.IfExpr, locals map[string]string, indent string) {
	fmt.Fprintf(b, "%sif %s {\n", indent, cg.exprToGo(ie.Cond, locals, "pre"))
	cg.writeAssignBody(b, ie.Then, locals, indent+"\t")
	fmt.Fprintf(b, "%s} else {\n", indent)
	cg.writeAssignBody(b, ie.Else, locals, indent+"\t")
	fmt.Fprintf(b, "%s}\n", indent)
}

// exprToGo translates an already-Inline()'d expression to Go, for PATH B and
// PATH C (guards and update values alike — unlike PATH A's separate
// guardToGo/updateValToGo, one function suffices here since dispatch/library
// bodies need \\in/~/IfExpr in both positions). locals maps the quantifier
// variable and free CONSTANTS to their Go parameter names; anything else
// falls back to a state-field reference on recv.
//
// recv is the receiver an unprimed variable reads from — "pre" inside a
// transition body (TLA+ primed assignments in one action are simultaneous, so
// an unprimed read must see entry-state even after an earlier line wrote the
// field) and "s" inside a guard (no writes have happened yet, and a guard
// must not reference a `pre` that its enclosing method never declared).
func (cg *CodeGen) exprToGo(e tla.Expr, locals map[string]string, recv string) string {
	switch v := e.(type) {
	case tla.BoolLit:
		return fmt.Sprintf("%v", v.Val)
	case tla.IntLit:
		// Integer state is int64 (see inferType), so literals must carry that
		// type — a bare `1` is an untyped constant that Go defaults to int, and
		// `pre.Pending + specgenIf(cond, 1, 0)` would then mix int64 with int.
		return fmt.Sprintf("int64(%d)", v.Val)
	case tla.StrLit:
		return fmt.Sprintf("%q", v.Val)
	case tla.VarRef:
		if goExpr, ok := locals[v.Name]; ok {
			return goExpr
		}
		if g, ok := cg.constGo(v.Name, locals, recv); ok {
			return g
		}
		return recv + "." + goField(v.Name)
	case tla.SetLit:
		// A set literal becomes a map[K]bool{elem: true, ...}. Elements are model
		// values (strings), locals, or tuples; the key type follows the elements
		// (a set of pairs — an attach-graph edge set — keys on [2]string).
		var parts []string
		for _, el := range v.Elems {
			parts = append(parts, cg.exprToGo(el, locals, recv)+": true")
		}
		return "map[" + setLitKeyType(v.Elems) + "]bool{" + strings.Join(parts, ", ") + "}"
	case tla.TupleLit:
		// <<a, b>> is a fixed-arity tuple; rendered as a Go array so it stays
		// comparable and can serve as a set/map key (edges of an attach graph).
		var parts []string
		for _, el := range v.Elems {
			parts = append(parts, cg.exprToGo(el, locals, recv))
		}
		return fmt.Sprintf("[%d]string{%s}", len(v.Elems), strings.Join(parts, ", "))
	case tla.BoolSet:
		return "true /* BOOLEAN */"
	case tla.FuncApply:
		// f[x] on a total function stored as a lazy map: read through the
		// default (Init's FuncLit body). Absent keys read back the default,
		// so the map can start empty over an abstract CONSTANT domain.
		fnGo := cg.exprToGo(v.Fn, locals, recv)
		argGo := cg.exprToGo(v.Arg, locals, recv)
		def := `""`
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			def = cg.funcDefaultAt(name, d+1)
		}
		return fmt.Sprintf("funcGet(%s, %s, %s)", fnGo, argGo, def)
	case tla.ExceptExpr:
		// [f EXCEPT ![i] = v] copy-on-writes one key: funcSet returns a fresh
		// map, preserving value semantics. Multiple updates chain left-to-right.
		// Inside v, `@` (AtExpr) is the pre-update value at that index.
		out := cg.exprToGo(v.Fn, locals, recv)
		def := `""`
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			def = cg.funcDefaultAt(name, d+1)
		}
		for _, u := range v.Updates {
			idxGo := cg.exprToGo(u.Index, locals, recv)
			prevAt := cg.atExpr
			cg.atExpr = fmt.Sprintf("funcGet(%s, %s, %s)", out, idxGo, def)
			valGo := cg.exprToGo(u.Val, locals, recv)
			cg.atExpr = prevAt
			out = fmt.Sprintf("funcSet(%s, %s, %s)", out, idxGo, valGo)
		}
		return out
	case tla.AtExpr:
		return cg.atExpr
	case tla.FuncLit:
		// A function literal used as a VALUE, not an update target — e.g. the
		// RHS of a whole-func equality guard (`tbl[k] = [b \in Bs |-> "none"]`).
		// Same lowering as updateValToGo's FuncLit case (concrete literal
		// domain builds the map outright; abstract CONSTANT domain has no
		// enumerable extent, so it becomes a typed nil map, matching Init's
		// own lazy-map default), over locals/recv instead of updateValToGo's
		// fixed "pre" receiver.
		keyType := "string"
		if sl, ok := v.Domain.(tla.SetLit); ok {
			if k := setLitKeyType(sl.Elems); k != "" {
				keyType = k
			}
		}
		valType := valTypeOrDefault(v.Body)
		if sl, ok := v.Domain.(tla.SetLit); ok {
			var elems []string
			for _, el := range sl.Elems {
				keyGo := cg.exprToGo(el, locals, recv)
				// v.Var is bound to THIS element's own key for each entry —
				// same per-key rebinding updateValToGo's FuncLit case does.
				inner := make(map[string]string, len(locals)+1)
				for k, val := range locals {
					inner[k] = val
				}
				inner[v.Var] = keyGo
				valGo := cg.exprToGo(v.Body, inner, recv)
				elems = append(elems, keyGo+": "+valGo)
			}
			return "map[" + keyType + "]" + valType + "{" + strings.Join(elems, ", ") + "}"
		}
		return "map[" + keyType + "]" + valType + "(nil)"
	case tla.UnaryOp:
		if v.Op == "~" {
			return "!(" + cg.exprToGo(v.Operand, locals, recv) + ")"
		}
	case tla.IfExpr:
		return fmt.Sprintf("specgenIf(%s, %s, %s)",
			cg.exprToGo(v.Cond, locals, recv), cg.exprToGo(v.Then, locals, recv), cg.exprToGo(v.Else, locals, recv))
	case tla.CaseExpr:
		// A CASE in value position (SessAttach's `ok' = Allowed(...)`) lowers to a
		// right-nested specgenIf chain. With an OTHER arm that is the final else;
		// without one, TLA+ leaves an all-false CASE undefined, so a well-formed
		// spec's arms are exhaustive and the LAST arm's result becomes the else
		// (its condition dropped) — matching the statement-form writeCase.
		n := len(v.Arms)
		var final string
		start := n - 1
		if v.Other != nil {
			final = cg.exprToGo(v.Other, locals, recv)
		} else {
			final = cg.exprToGo(v.Arms[n-1].Result, locals, recv)
			start = n - 2
		}
		for i := start; i >= 0; i-- {
			final = fmt.Sprintf("specgenIf(%s, %s, %s)",
				cg.exprToGo(v.Arms[i].Cond, locals, recv),
				cg.exprToGo(v.Arms[i].Result, locals, recv),
				final)
		}
		return final
	case tla.ForallExpr:
		return cg.quantToGo("forAll", v.Var, v.Domain, v.Body, locals, recv)
	case tla.ExistsExpr:
		return cg.quantToGo("exists", v.Var, v.Domain, v.Body, locals, recv)
	case tla.SetCompr:
		// `{x \in D : P(x)}` — a filter — becomes setFilter over D's extent, with
		// x a local in the predicate (same shape as a quantifier fold, but it
		// returns the built set rather than a bool).
		inner := make(map[string]string, len(locals)+1)
		for k, val := range locals {
			inner[k] = val
		}
		inner[v.Var] = v.Var
		return fmt.Sprintf("setFilter(%s, func(%s string) bool { return %s })",
			cg.quantDomainGo(v.Domain, locals, recv), v.Var, cg.exprToGo(v.Pred, inner, recv))
	case tla.Call:
		// Only RECURSIVE operators reach codegen as a Call — every other operator
		// is inlined away. Emit a Go call to the standalone recursive func that
		// writeRecursiveFuncs will emit (gated on this call site existing).
		var args []string
		for _, a := range v.Args {
			args = append(args, cg.exprToGo(a, locals, recv))
		}
		return fmt.Sprintf("%s(%s)", recFuncName(v.Name), strings.Join(args, ", "))
	case tla.BinOp:
		switch v.Op {
		case "\\cup":
			return fmt.Sprintf("setUnion(%s, %s)", cg.exprToGo(v.Lhs, locals, recv), cg.exprToGo(v.Rhs, locals, recv))
		case "\\cap":
			return fmt.Sprintf("setInter(%s, %s)", cg.exprToGo(v.Lhs, locals, recv), cg.exprToGo(v.Rhs, locals, recv))
		case "\\":
			return fmt.Sprintf("setDiff(%s, %s)", cg.exprToGo(v.Lhs, locals, recv), cg.exprToGo(v.Rhs, locals, recv))
		}
		lhs := cg.exprToGo(v.Lhs, locals, recv)
		rhs := cg.exprToGo(v.Rhs, locals, recv)
		switch v.Op {
		case "=":
			if cg.isSetExpr(v.Lhs) || cg.isSetExpr(v.Rhs) {
				return fmt.Sprintf("setEq(%s, %s)", lhs, rhs)
			}
			if cg.isFuncExpr(v.Lhs) || cg.isFuncExpr(v.Rhs) {
				return fmt.Sprintf("funcEq(%s, %s)", lhs, rhs)
			}
			return fmt.Sprintf("%s == %s", lhs, rhs)
		case "/=":
			if cg.isSetExpr(v.Lhs) || cg.isSetExpr(v.Rhs) {
				return fmt.Sprintf("!setEq(%s, %s)", lhs, rhs)
			}
			if cg.isFuncExpr(v.Lhs) || cg.isFuncExpr(v.Rhs) {
				return fmt.Sprintf("!funcEq(%s, %s)", lhs, rhs)
			}
			return fmt.Sprintf("%s != %s", lhs, rhs)
		case "/\\":
			return fmt.Sprintf("%s && %s", lhs, rhs)
		case "\\/":
			return fmt.Sprintf("%s || %s", lhs, rhs)
		case "=>":
			// TLA+ implication P => Q is !P \/ Q. Guarding a quantifier with a
			// CONSTANT flag (Guarded => \A ...) compiles to (!flag || forAll(...)).
			return fmt.Sprintf("(!(%s) || (%s))", lhs, rhs)
		case "+":
			return fmt.Sprintf("%s + %s", lhs, rhs)
		case "-":
			return fmt.Sprintf("%s - %s", lhs, rhs)
		case "*":
			return fmt.Sprintf("%s * %s", lhs, rhs)
		case ">":
			return fmt.Sprintf("%s > %s", lhs, rhs)
		case "<":
			return fmt.Sprintf("%s < %s", lhs, rhs)
		case ">=":
			return fmt.Sprintf("%s >= %s", lhs, rhs)
		case "<=":
			return fmt.Sprintf("%s <= %s", lhs, rhs)
		case "\\in":
			return cg.inToGoCtx(v.Lhs, v.Rhs, locals, recv)
		case "\\notin":
			return "!(" + cg.inToGoCtx(v.Lhs, v.Rhs, locals, recv) + ")"
		}
		return fmt.Sprintf("/* unhandled op %s */", v.Op)
	}
	return fmt.Sprintf("/* unhandled: %T */", e)
}

// quantToGo translates a TLA+ quantifier (`\A`/`\E x \in D : body`) into a Go
// fold over D: the bound variable becomes the closure parameter and fn is the
// runtime helper ("forAll" short-circuits on the first false, "exists" on the
// first true). The body sees x as a local, so a guard like
// `\A o \in Ops : pc[o] # "checked"` reads pc[o] through funcGet as usual.
func (cg *CodeGen) quantToGo(fn, boundVar string, domain, body tla.Expr, locals map[string]string, recv string) string {
	inner := make(map[string]string, len(locals)+1)
	for k, val := range locals {
		inner[k] = val
	}
	inner[boundVar] = boundVar
	return fmt.Sprintf("%s(%s, func(%s string) bool { return %s })",
		fn, cg.quantDomainGo(domain, locals, recv), boundVar, cg.exprToGo(body, inner, recv))
}

// quantDomainGo renders the set a quantifier ranges over as a Go []string. A
// set-typed state variable yields its live keys (keysOf); the abstract \E
// CONSTANT (any other bare name, unknown at codegen time) is the
// caller-supplied `domain` parameter; a set literal becomes a slice literal.
func (cg *CodeGen) quantDomainGo(domain tla.Expr, locals map[string]string, recv string) string {
	switch d := cg.resolveDomain(domain).(type) {
	case tla.VarRef:
		if _, ok := locals[d.Name]; !ok && cg.isSetVar(d.Name) {
			return fmt.Sprintf("keysOf(%s.%s)", recv, goField(d.Name))
		}
		return "domain"
	case tla.SetLit:
		var parts []string
		for _, el := range d.Elems {
			parts = append(parts, cg.exprToGo(el, locals, recv))
		}
		return "[]string{" + strings.Join(parts, ", ") + "}"
	}
	return "domain"
}

// inToGoCtx is inToGo's locals-aware counterpart for PATH B/C. recv threads
// through to any state-field reference (see exprToGo).
func (cg *CodeGen) inToGoCtx(lhs, rhs tla.Expr, locals map[string]string, recv string) string {
	lhsGo := cg.exprToGo(lhs, locals, recv)
	switch r := cg.resolveDomain(rhs).(type) {
	case tla.SetLit:
		var parts []string
		for _, el := range r.Elems {
			parts = append(parts, fmt.Sprintf("%s == %s", lhsGo, cg.exprToGo(el, locals, recv)))
		}
		if len(parts) == 0 {
			return "false"
		}
		return strings.Join(parts, " || ")
	case tla.RangeLit:
		return fmt.Sprintf("%s >= %s && %s <= %s",
			lhsGo, cg.exprToGo(r.Lo, locals, recv), lhsGo, cg.exprToGo(r.Hi, locals, recv))
	case tla.BoolSet, tla.IntSet:
		return "true"
	case tla.VarRef:
		// Membership in a set-typed state variable (or set-typed local): the
		// set is a map[string]bool, so `x \in held` is a map index. A plain
		// CONSTANT domain (e.g. Items) that is NOT a set-typed field also lands
		// here — those are the whole universe, so membership is vacuously true.
		if cg.isSetVar(r.Name) {
			return fmt.Sprintf("%s[%s]", cg.exprToGo(rhs, locals, recv), lhsGo)
		}
		return "true"
	}
	// `x \in {h \in D : P(h)}` — membership in a comprehension (or any built
	// set): index the map the set-expr evaluates to. `setFilter(...)[x]` is a
	// legal map index on a call result, so no temp is needed. This is what keeps
	// `t \notin Reach(edges, h)` from silently degrading to a constant.
	if cg.isSetExpr(rhs) {
		return fmt.Sprintf("(%s)[%s]", cg.exprToGo(rhs, locals, recv), lhsGo)
	}
	return "/* \\in unhandled set type */"
}

func (cg *CodeGen) writeEnabledActions(b *strings.Builder) {
	fmt.Fprintln(b, "// EnabledActions returns the names of actions whose guards are satisfied.")
	fmt.Fprintln(b, "func (s State) EnabledActions() []string {")
	fmt.Fprintln(b, "\tvar out []string")
	for _, act := range cg.Actions {
		fmt.Fprintf(b, "\tif s.Can%s() {\n", act.Name)
		fmt.Fprintf(b, "\t\tout = append(out, %q)\n", act.Name)
		fmt.Fprintln(b, "\t}")
	}
	fmt.Fprintln(b, "\treturn out")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)
}

func (cg *CodeGen) writeTrace(b *strings.Builder) {
	fmt.Fprintln(b, `
// --- Trace emission for conformance checking ---

// TraceEntry is a single recorded transition for conformance validation.
// Arg and Consts are empty/nil for a named action (PATH A); for a
// Step-dispatch spec (PATH B), Arg carries the input value Step was called
// with, and Consts carries the value of every free CONSTANT parameter (e.g.
// HasSel) — without it, a replayed trace couldn't reconstruct what Step
// actually saw whenever a free var varies between calls.
type TraceEntry struct {
	Action string          `+"`"+`json:"action"`+"`"+`
	Arg    string          `+"`"+`json:"arg,omitempty"`+"`"+`
	Args   []string        `+"`"+`json:"args,omitempty"`+"`"+`
	Consts map[string]bool `+"`"+`json:"consts,omitempty"`+"`"+`
	Pre    State           `+"`"+`json:"pre"`+"`"+`
	Post   State           `+"`"+`json:"post"`+"`"+`
}`)
	fmt.Fprintln(b)

	if cg.Dispatch != nil {
		cg.writeStepTrace(b)
		return
	}

	if cg.isParamLib() {
		// ApplyStep already exists (writeStepLib). Trace just wraps it, recording
		// the (action, arg[s]) pair conform replays. Multi-param specs carry a
		// value per \E binder (st.Args); single-param specs keep the compact
		// st.Arg surface.
		field := "Arg:    st.Arg"
		if cg.anyMultiParam() {
			field = "Args:   st.Args"
		}
		fmt.Fprintln(b, `// Trace records a transition: applies the step and returns
// both the trace entry and the new state.
func (s State) Trace(st Step) (TraceEntry, State) {
	pre := s
	post := s.ApplyStep(st)
	return TraceEntry{
		Action: st.Action,
		`+field+`,
		Pre:    pre,
		Post:   post,
	}, post
}`)
		return
	}

	fmt.Fprintln(b, `// ApplyAction dispatches a named action, returning the new state.
// Panics on unknown action names.
func (s State) ApplyAction(name string) State {
	switch name {`)

	for _, act := range cg.Actions {
		fmt.Fprintf(b, "\tcase %q:\n", act.Name)
		fmt.Fprintf(b, "\t\treturn s.%s()\n", act.Name)
	}
	fmt.Fprintln(b, "\tdefault:")
	fmt.Fprintln(b, "\t\tpanic(\"unknown action: \" + name)")
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
	fmt.Fprintln(b)

	fmt.Fprintln(b, `// Trace records a transition: applies the action and returns
// both the trace entry and the new state.
func (s State) Trace(action string) (TraceEntry, State) {
	pre := s
	post := s.ApplyAction(action)
	return TraceEntry{
		Action: action,
		Pre:    pre,
		Post:   post,
	}, post
}`)
}

// writeStepTrace is writeTrace's PATH B half: Trace calls Step directly and
// records the input value as Arg, so conform can replay it.
func (cg *CodeGen) writeStepTrace(b *strings.Builder) {
	sd := cg.Dispatch
	fmt.Fprintf(b, "// Trace records a Step transition: applies it and returns both the trace\n")
	fmt.Fprintf(b, "// entry and the new state. Arg carries the %s Step was called with.\n", sd.QuantVar)
	fmt.Fprintf(b, "func (s State) Trace(%s string%s) (TraceEntry, State) {\n", sd.QuantVar, cg.stepExtraParamsSig())
	fmt.Fprintln(b, "\tpre := s")
	fmt.Fprintf(b, "\tpost := s.Step(%s%s)\n", sd.QuantVar, cg.stepExtraArgsCall())
	fmt.Fprintln(b, "\treturn TraceEntry{")
	fmt.Fprintln(b, "\t\tAction: \"Step\",")
	fmt.Fprintf(b, "\t\tArg:    %s,\n", sd.QuantVar)
	if len(cg.FreeVars) > 0 {
		fmt.Fprintln(b, "\t\tConsts: map[string]bool{")
		for _, fv := range cg.FreeVars {
			fmt.Fprintf(b, "\t\t\t%q: %s,\n", fv, paramName(fv))
		}
		fmt.Fprintln(b, "\t\t},")
	}
	fmt.Fprintln(b, "\t\tPre:    pre,")
	fmt.Fprintln(b, "\t\tPost:   post,")
	fmt.Fprintln(b, "\t}, post")
	fmt.Fprintln(b, "}")
}

// --- Type inference ---

func (cg *CodeGen) inferType(varName string) string {
	// Try Init
	if initBody := cg.Spec.FindDef("Init"); initBody != nil {
		if t := typeFromInit(varName, initBody); t != "" {
			return t
		}
	}
	// Try the type predicate under the names that appear in the wild.
	for _, name := range []string{"TypeOK", "TypeOk", "TypeInvariant"} {
		if typeOk := cg.Spec.FindDef(name); typeOk != nil {
			if t := typeFromInExpr(varName, typeOk); t != "" {
				return t
			}
		}
	}
	// Fallback: an empty-set Init (`x = {}`) deferred above (its element type is
	// unknown). If TypeOK didn't type it either, it is still a set — default to
	// a set of strings, the overwhelmingly common case.
	if initBody := cg.Spec.FindDef("Init"); initBody != nil {
		if _, ok := findInitVal(varName, initBody).(tla.SetLit); ok {
			return "map[string]bool"
		}
	}
	return "interface{}"
}

// isSetVar reports whether a state variable is set-typed (a map[K]bool — of
// strings OR of tuples, map[[2]string]bool), so membership compiles to a map
// index rather than a scalar comparison. A function-typed field (map[K]V with
// V other than bool, e.g. map[string]string OR map[string]map[string]bool —
// a func-of-set — is deliberately excluded: a plain HasSuffix(t, "]bool")
// check would wrongly also match map[string]map[string]bool (it too ends in
// "]bool"), so mapValueType is used to look at V alone, not just t's tail.
func (cg *CodeGen) isSetVar(varName string) bool {
	v, ok := mapValueType(cg.inferType(varName))
	return ok && v == "bool"
}

// mapValueType splits a Go "map[K]V" type string into its value V, returning
// ok=false if t isn't (syntactically) a map type. Brackets in K are balanced
// so a tuple key like "map[[2]string]bool" splits at the right ']' rather
// than the first one.
func mapValueType(t string) (string, bool) {
	if !strings.HasPrefix(t, "map[") {
		return "", false
	}
	depth := 0
	for i := len("map"); i < len(t); i++ {
		switch t[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return t[i+1:], true
			}
		}
	}
	return "", false
}

// isSetConstant reports whether a CONSTANT is used anywhere as a set — as a
// quantifier/comprehension domain, a function domain/range, or an operand of a
// set operator (\cup, \in, \subseteq, \X, ...). Such a constant (Sessions,
// Clients) is a runtime domain, NOT a scalar model value: it must never lower
// to a string literal. Memoized: one Walk over the whole spec.
func (cg *CodeGen) isSetConstant(name string) bool {
	if cg.setConsts == nil {
		cg.setConsts = map[string]bool{}
		mark := func(e tla.Expr) {
			if vr, ok := e.(tla.VarRef); ok {
				cg.setConsts[vr.Name] = true
			}
		}
		for _, d := range cg.Spec.Defs {
			tla.Walk(d.Body, func(n tla.Expr) bool {
				switch v := n.(type) {
				case tla.ExistsExpr:
					mark(v.Domain)
				case tla.ForallExpr:
					mark(v.Domain)
				case tla.SetCompr:
					mark(v.Domain)
				case tla.SetMap:
					mark(v.Domain)
				case tla.FuncLit:
					mark(v.Domain)
				case tla.ChooseExpr:
					mark(v.Domain)
				case tla.FuncSet:
					mark(v.Domain)
					mark(v.Range)
				case tla.BinOp:
					switch v.Op {
					case "\\cup", "\\cap", "\\", "\\in", "\\notin", "\\subseteq", "\\X":
						mark(v.Lhs)
						mark(v.Rhs)
					}
				}
				return true
			})
		}
	}
	return cg.setConsts[name]
}

// constGo resolves a VarRef that names a CONSTANT to its Go scalar form,
// reporting false when the name is not a scalar constant (so the caller falls
// back to a state-field read). A bound constant lowers to its literal; an
// unbound, non-set constant is a model value — an opaque token equal only to
// itself — and lowers to its own name as a string, the exact representation
// conform writes in trace JSON ("lock":"NoOne").
func (cg *CodeGen) constGo(name string, locals map[string]string, recv string) (string, bool) {
	isConst := false
	for _, c := range cg.Spec.Constants {
		if c == name {
			isConst = true
			break
		}
	}
	if !isConst {
		return "", false
	}
	if cg.Spec.ConstBindings != nil {
		if bound, ok := cg.Spec.ConstBindings[name]; ok {
			return cg.exprToGo(bound, locals, recv), true
		}
	}
	if cg.isSetConstant(name) {
		return "", false
	}
	return fmt.Sprintf("%q", name), true
}

// isSetExpr reports whether an expression evaluates to a set (map[string]bool):
// a set literal, a set-typed state variable, a set operator, or a func-of-set
// application (`tbl[k]` where tbl's values are themselves sets). Used to
// route `=`/`#` on sets to setEq instead of Go's map-incomparable == / !=, and
// to route `\in`/`\notin` on such a value to a map index in inToGoCtx instead
// of falling through to the "unhandled set type" placeholder.
func (cg *CodeGen) isSetExpr(e Expr) bool {
	switch v := e.(type) {
	case SetLit, SetCompr:
		return true
	case VarRef:
		return cg.isSetVar(v.Name)
	case BinOp:
		return v.Op == "\\cup" || v.Op == "\\cap" || v.Op == "\\"
	case FuncApply:
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			return cg.funcApplyValueIsSet(name, d+1)
		}
	}
	return false
}

// funcApplyValueIsSet reports whether a func-of-func chain depth levels deep
// into varName's Init value (funcDefaultAt's own depth convention — depth=1
// is a single application, e.g. `tbl[k]`) is itself set-typed: it peels
// FuncLit layers off Init's value the same way funcDefaultAt does, then asks
// valTypeOrDefault whether the leaf body's own type is a set
// (map[...]bool) — reusing the existing literal-shape type inference rather
// than parsing map[K]V type strings by hand.
func (cg *CodeGen) funcApplyValueIsSet(varName string, depth int) bool {
	val := findInitVal(varName, cg.Spec.FindDef("Init"))
	for i := 1; i < depth; i++ {
		fl, ok := val.(tla.FuncLit)
		if !ok {
			return false
		}
		val = fl.Body
	}
	fl, ok := val.(tla.FuncLit)
	if !ok {
		return false
	}
	if _, ok := fl.Body.(tla.FuncLit); ok {
		return false // func-of-func: this depth's value is itself a function, not a set.
	}
	t := valTypeOrDefault(fl.Body)
	return strings.HasPrefix(t, "map[") && strings.HasSuffix(t, "]bool")
}

// isFuncExpr reports whether an expression evaluates to a total function
// (map[K]V with V != bool — a func-of-scalar, func-of-set, or func-of-func),
// as opposed to a set (map[K]bool, see isSetExpr): a func-typed state
// variable, a func-of-func application (`tbl[k]` where tbl's values are
// themselves funcs), or a literal FuncLit whose body isn't itself boolean
// (a boolean-bodied FuncLit is, by isSetExpr's own convention, a set). Used
// to route `=`/`#` on such a value to funcEq (reflect.DeepEqual) instead of
// Go's map-incomparable == / !=, mirroring isSetExpr's own routing to setEq.
func (cg *CodeGen) isFuncExpr(e Expr) bool {
	switch v := e.(type) {
	case FuncLit:
		return valTypeOrDefault(v.Body) != "bool"
	case VarRef:
		return cg.isFuncVar(v.Name)
	case FuncApply:
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			return cg.funcApplyValueIsFunc(name, d+1)
		}
	}
	return false
}

// isFuncVar reports whether varName's Go type is a non-bool-valued map —
// i.e. a TLA+ function, as opposed to isSetVar's bool-valued map (a set).
func (cg *CodeGen) isFuncVar(varName string) bool {
	v, ok := mapValueType(cg.inferType(varName))
	return ok && v != "bool"
}

// funcApplyValueIsFunc is funcApplyValueIsSet's complement: it reports
// whether the same chain-depth leaf value is itself func-typed (map[...]V,
// V != bool) rather than set-typed — the func-of-func corner
// funcApplyValueIsSet deliberately excludes.
func (cg *CodeGen) funcApplyValueIsFunc(varName string, depth int) bool {
	val := findInitVal(varName, cg.Spec.FindDef("Init"))
	for i := 1; i < depth; i++ {
		fl, ok := val.(tla.FuncLit)
		if !ok {
			return false
		}
		val = fl.Body
	}
	fl, ok := val.(tla.FuncLit)
	if !ok {
		return false
	}
	t := valTypeOrDefault(fl.Body)
	return strings.HasPrefix(t, "map[") && !strings.HasSuffix(t, "]bool")
}

func typeFromInit(varName string, e Expr) string {
	switch v := e.(type) {
	case BinOp:
		if v.Op == "=" {
			if vr, ok := v.Lhs.(VarRef); ok && vr.Name == varName {
				return typeFromLit(v.Rhs)
			}
		}
		if v.Op == "/\\" {
			if t := typeFromInit(varName, v.Lhs); t != "" {
				return t
			}
			return typeFromInit(varName, v.Rhs)
		}
	}
	return ""
}

func typeFromInExpr(varName string, e Expr) string {
	switch v := e.(type) {
	case BinOp:
		// held \subseteq Items → held is a set of model values. When the RHS is a
		// cross product (edges \subseteq (S \X S)), the elements are tuples, so
		// the set keys on a fixed-arity array [N]string.
		if v.Op == "\\subseteq" || v.Op == "\\subset" {
			if vr, ok := v.Lhs.(VarRef); ok && vr.Name == varName {
				if n := tupleArity(v.Rhs); n > 0 {
					return fmt.Sprintf("map[[%d]string]bool", n)
				}
				return "map[string]bool"
			}
		}
		if v.Op == "\\in" {
			if vr, ok := v.Lhs.(VarRef); ok && vr.Name == varName {
				switch r := v.Rhs.(type) {
				case BoolSet:
					return "bool"
				case IntSet:
					return "int64"
				case RangeLit:
					return "int64"
				case SetLit:
					if len(r.Elems) > 0 {
						return typeFromLit(r.Elems[0])
					}
				case FuncSet:
					// pc \in [Ops -> {"idle",...}] → a total function; domain
					// elements are model values (strings), so map[string]<range>.
					return "map[string]" + typeFromRange(r.Range)
				}
			}
		}
		if v.Op == "/\\" {
			if t := typeFromInExpr(varName, v.Lhs); t != "" {
				return t
			}
			return typeFromInExpr(varName, v.Rhs)
		}
	}
	return ""
}

// setLitKeyType picks the Go map-key type for a set literal from its elements:
// a set of tuples (<<a,b>>) keys on a fixed-arity array [N]string, everything
// else on string. An empty literal defaults to string — its type, when it
// matters, is fixed by the state field's inferred type, not the literal.
func setLitKeyType(elems []Expr) string {
	if len(elems) > 0 {
		if tup, ok := elems[0].(TupleLit); ok {
			return fmt.Sprintf("[%d]string", len(tup.Elems))
		}
	}
	return "string"
}

// tupleArity returns the arity of a cross product `S \X T \X ...` (2 for a
// pair), or 0 if e is not a cross product. Used to type a set-of-pairs state
// variable declared `edges \subseteq (S \X S)`.
func tupleArity(e Expr) int {
	bin, ok := e.(BinOp)
	if !ok || bin.Op != "\\X" {
		return 0
	}
	// Left-associative chain: (S \X T) \X U. Count leaves.
	n := tupleArity(bin.Lhs)
	if n == 0 {
		n = 1 // Lhs is a plain set, contributing one component.
	}
	return n + 1
}

func typeFromLit(e Expr) string {
	switch v := e.(type) {
	case BoolLit:
		return "bool"
	case IntLit:
		return "int64"
	case StrLit:
		return "string"
	case SetLit:
		// A non-empty set-valued literal types the variable as a set of its
		// element kind. The EMPTY set `{}` carries no element type, so it defers
		// to TypeOK — which alone can tell a set of strings from a set of tuples
		// (edges \subseteq (S \X S)). Both then agree on the field type.
		if len(v.Elems) == 0 {
			return ""
		}
		return "map[" + setLitKeyType(v.Elems) + "]bool"
	case tla.FuncLit:
		// `[k \in Keys |-> "none"]` types the variable as a total function: a Go
		// map keyed on the domain's element type (a bare CONSTANT domain, e.g.
		// Keys, holds opaque model values — string — same as any other unbound
		// constant; a literal domain of tuples keys on [N]string like a tuple
		// set), valued on the default expr's Go type. No TypeOK needed — this is
		// the fallback typeFromInExpr's `\in [Dom -> Range]` case doesn't reach
		// when there is no such TypeOK clause at all.
		keyType := "string"
		if sl, ok := v.Domain.(SetLit); ok {
			if k := setLitKeyType(sl.Elems); k != "" {
				keyType = k
			}
		}
		return "map[" + keyType + "]" + valTypeOrDefault(v.Body)
	case IfExpr:
		// `IF k = "k1" THEN {"x"} ELSE {}` — the only way one FuncLit body
		// varies a set-typed value per key without an EXCEPT chain. Neither
		// branch alone need resolve (an empty-set branch is as uninformative
		// as a bare `{}` literal); take whichever branch DOES, same
		// first-to-resolve recursion typeFromInExpr already uses for `/\`.
		if t := typeFromLit(v.Then); t != "" {
			return t
		}
		return typeFromLit(v.Else)
	case tla.CaseExpr:
		for _, arm := range v.Arms {
			if t := typeFromLit(arm.Result); t != "" {
				return t
			}
		}
		if v.Other != nil {
			return typeFromLit(v.Other)
		}
	}
	return ""
}

// valTypeOrDefault is typeFromLit(body), with the one ambiguous case every
// call site used to resolve on its own resolved consistently: an empty-set
// body (`[k \in Keys |-> {}]`) has no element type and no TypeOK, but it is
// still a SET, so it defaults to map[string]bool — the same call
// inferType's own top-level fallback makes for a bare empty-set var, just
// one layer of func-nesting up. Anything else typeFromLit can't resolve
// falls back to the scalar default, string.
func valTypeOrDefault(body Expr) string {
	if valType := typeFromLit(body); valType != "" {
		return valType
	}
	if _, ok := body.(SetLit); ok {
		return "map[string]bool"
	}
	return "string"
}

// typeFromRange yields the Go element type of a function's range set, e.g.
// {"idle","running"} → "string". Domain elements are model values (strings),
// so a `[Dom -> Range]` function becomes map[string]<this>.
func typeFromRange(e Expr) string {
	switch r := e.(type) {
	case SetLit:
		if len(r.Elems) > 0 {
			if t := typeFromLit(r.Elems[0]); t != "" {
				return t
			}
		}
	case BoolSet:
		return "bool"
	case IntSet, RangeLit:
		return "int64"
	}
	return "string"
}

// funcDefault returns the Go literal an absent key of a function-typed variable
// reads back — the Init FuncLit body (`[o \in Ops |-> "idle"]` → "idle"). A
// total function over an abstract CONSTANT domain has no codegen-time extent, so
// it is stored as a lazy map and missing keys resolve to this default.
func (cg *CodeGen) funcDefault(varName string) string {
	initBody := cg.Spec.FindDef("Init")
	if fl, ok := findInitVal(varName, initBody).(tla.FuncLit); ok {
		// Func-of-func (`[a \in As |-> [b \in Bs |-> "none"]]`): the inner
		// FuncLit's own domain (Bs) is abstract too, so its default can't be
		// rendered as a literal any more than the outer one can. It resolves
		// to a typed nil map, same idiom as updateValToGo's FuncLit case — a
		// subsequent funcGet against that nil inner map falls through to ITS
		// default on any key, so nesting composes without a concrete literal.
		if inner, ok := fl.Body.(tla.FuncLit); ok {
			keyType := "string"
			if sl, ok := inner.Domain.(SetLit); ok {
				if k := setLitKeyType(sl.Elems); k != "" {
					keyType = k
				}
			}
			return "map[" + keyType + "]" + valTypeOrDefault(inner.Body) + "(nil)"
		}
		return cg.exprToGo(fl.Body, nil, "")
	}
	// No FuncLit Init: fall back to the element type's Go zero value.
	switch cg.inferType(varName) {
	case "map[string]bool":
		return "false"
	case "map[string]int64":
		return "0"
	}
	return `""`
}

// funcApplyChainDepth walks a chain of nested FuncApply nodes down to the
// base VarRef, e.g. `tbl[a][b]`'s Fn side (`tbl[a]`) has depth 1 (one
// FuncApply layer wraps VarRef tbl). Returns ok=false for anything that
// doesn't bottom out at a bare VarRef (a local, a Call result, ...) — callers
// fall back to the untyped "" default in that case, same as before this
// existed.
func funcApplyChainDepth(e tla.Expr) (varName string, depth int, ok bool) {
	switch v := e.(type) {
	case tla.VarRef:
		return v.Name, 0, true
	case tla.FuncApply:
		name, d, ok := funcApplyChainDepth(v.Fn)
		if !ok {
			return "", 0, false
		}
		return name, d + 1, true
	}
	return "", 0, false
}

// funcDefaultAt generalizes funcDefault to chained FuncApply lookups:
// funcDefaultAt(varName, 1) is exactly funcDefault(varName) (a single
// application, e.g. `tbl[a]`); funcDefaultAt(varName, 2) is the default for a
// second chained application (`tbl[a][b]`), and so on. depth is always >= 1
// at call sites (funcApplyChainDepth's own +1).
//
// A func-of-func Init value (`[a \in As |-> [b \in Bs |-> "none"]]`) nests
// FuncLit inside FuncLit, one layer per chained FuncApply. Peeling depth-1
// layers off Init's RHS lands on the FuncLit that the (depth)-th index reads
// through; from there this applies funcDefault's own one-layer rule (nil map
// stand-in if that FuncLit's body is itself a FuncLit — i.e. more chaining
// still to come — otherwise the leaf default expression itself).
func (cg *CodeGen) funcDefaultAt(varName string, depth int) string {
	val := findInitVal(varName, cg.Spec.FindDef("Init"))
	for i := 1; i < depth; i++ {
		fl, ok := val.(tla.FuncLit)
		if !ok {
			return `""` // fewer FuncLit layers than the chain is deep: give up safely.
		}
		val = fl.Body
	}
	fl, ok := val.(tla.FuncLit)
	if !ok {
		if val == nil {
			return `""`
		}
		return cg.exprToGo(val, nil, "")
	}
	if inner, ok := fl.Body.(tla.FuncLit); ok {
		keyType := "string"
		if sl, ok := inner.Domain.(tla.SetLit); ok {
			if k := setLitKeyType(sl.Elems); k != "" {
				keyType = k
			}
		}
		return "map[" + keyType + "]" + valTypeOrDefault(inner.Body) + "(nil)"
	}
	return cg.exprToGo(fl.Body, nil, "")
}

// --- Expression translation ---

// inToGo translates `lhs \in rhs` to Go guard code. recv is the receiver
// identifier VarRef reads resolve against ("s" for a pure guard method,
// "pre" inside a mutating transition method — see guardToGo).
func (cg *CodeGen) inToGo(lhs, rhs Expr, recv string) string {
	lhsGo := cg.guardToGo(lhs, recv)
	switch r := cg.resolveDomain(rhs).(type) {
	case SetLit:
		// x \in {v1, v2} → s.X == v1 || s.X == v2
		var parts []string
		for _, el := range r.Elems {
			parts = append(parts, fmt.Sprintf("%s == %s", lhsGo, cg.guardToGo(el, recv)))
		}
		if len(parts) == 0 {
			return "false"
		}
		return strings.Join(parts, " || ")
	case RangeLit:
		// x \in a..b → s.X >= a && s.X <= b
		return fmt.Sprintf("%s >= %s && %s <= %s",
			lhsGo, cg.guardToGo(r.Lo, recv), lhsGo, cg.guardToGo(r.Hi, recv))
	case BoolSet:
		return "true"
	case IntSet:
		return "true"
	case VarRef:
		// Membership in a set-typed state variable (or set-typed local): the
		// set is a map[string]bool, so `x \in held` is a map index. A plain
		// CONSTANT domain (e.g. Items) that is NOT a set-typed field is the
		// whole universe, so membership is vacuously true — same rule
		// inToGoCtx (PATH B/C's counterpart) already applies.
		if cg.isSetVar(r.Name) {
			return fmt.Sprintf("%s[%s]", cg.guardToGo(rhs, recv), lhsGo)
		}
		return "true"
	}
	// `x \in {h \in D : P(h)}` — membership in a comprehension or any other
	// built set expression: guardToGo has no comprehension/quantifier case
	// of its own, so delegate the whole `\in` to exprToGo's locals-aware
	// inToGoCtx rather than duplicating quantDomainGo/setFilter here.
	if cg.isSetExpr(rhs) {
		return cg.inToGoCtx(lhs, rhs, nil, recv)
	}
	return "/* \\in unhandled set type */"
}

// guardToGo translates a guard expression (no primed vars) to Go. recv is
// the receiver identifier VarRef reads resolve against: "s" for a pure guard
// method (Can<Action>, never mutates); "pre" for anything evaluated inside a
// mutating transition method (an update-value IfExpr's condition) — TLA+
// primed assignments within one conjunction are simultaneous, so such a
// condition must see the PRE-transition value even after an earlier
// assignment in the same method already wrote a new value to that field.
func (cg *CodeGen) guardToGo(e Expr, recv string) string {
	switch v := e.(type) {
	case BoolLit:
		return fmt.Sprintf("%v", v.Val)
	case IntLit:
		return fmt.Sprintf("%d", v.Val)
	case StrLit:
		return fmt.Sprintf("%q", v.Val)
	case VarRef:
		if g, ok := cg.constGo(v.Name, nil, recv); ok {
			return g
		}
		return recv + "." + goField(v.Name)
	case BoolSet:
		return "true /* BOOLEAN */"
	case BinOp:
		lhs := cg.guardToGo(v.Lhs, recv)
		rhs := cg.guardToGo(v.Rhs, recv)
		switch v.Op {
		case "=":
			// A set-typed operand (e.g. `items = {}` as an IF-condition inside an
			// update value) can't use Go's == on a map — same isSetExpr routing to
			// setEq that exprToGo's own "=" case already does.
			if cg.isSetExpr(v.Lhs) || cg.isSetExpr(v.Rhs) {
				return fmt.Sprintf("setEq(%s, %s)", lhs, rhs)
			}
			if cg.isFuncExpr(v.Lhs) || cg.isFuncExpr(v.Rhs) {
				return fmt.Sprintf("funcEq(%s, %s)", lhs, rhs)
			}
			return fmt.Sprintf("%s == %s", lhs, rhs)
		case "/=":
			if cg.isSetExpr(v.Lhs) || cg.isSetExpr(v.Rhs) {
				return fmt.Sprintf("!setEq(%s, %s)", lhs, rhs)
			}
			if cg.isFuncExpr(v.Lhs) || cg.isFuncExpr(v.Rhs) {
				return fmt.Sprintf("!funcEq(%s, %s)", lhs, rhs)
			}
			return fmt.Sprintf("%s != %s", lhs, rhs)
		case "/\\":
			return fmt.Sprintf("%s && %s", lhs, rhs)
		case "\\/":
			return fmt.Sprintf("%s || %s", lhs, rhs)
		case "+":
			return fmt.Sprintf("%s + %s", lhs, rhs)
		case "-":
			return fmt.Sprintf("%s - %s", lhs, rhs)
		case ">":
			return fmt.Sprintf("%s > %s", lhs, rhs)
		case "<":
			return fmt.Sprintf("%s < %s", lhs, rhs)
		case ">=":
			return fmt.Sprintf("%s >= %s", lhs, rhs)
		case "<=":
			return fmt.Sprintf("%s <= %s", lhs, rhs)
		case "\\in":
			return cg.inToGo(v.Lhs, v.Rhs, recv)
		}
		// Any op guardToGo's own switch doesn't cover (\notin, =>, ...):
		// delegate the whole BinOp to exprToGo, the fully general expression
		// lowering PATH B/C already funnel through — a proven superset of
		// this hand-rolled guard translator, so this is exactly the same
		// output PATH B/C would already emit for the identical construct.
		return cg.exprToGo(e, nil, recv)
	case UnaryOp:
		if v.Op == "~" {
			return "!(" + cg.guardToGo(v.Operand, recv) + ")"
		}
	case IfExpr:
		return fmt.Sprintf("specgenIf(%s, %s, %s)",
			cg.guardToGo(v.Cond, recv), cg.guardToGo(v.Then, recv), cg.guardToGo(v.Else, recv))
	}
	// Any construct guardToGo has no case for at all (CASE, \A, \E, f[x], ...):
	// same delegation to exprToGo as the BinOp fallback above.
	return cg.exprToGo(e, nil, recv)
}

// updateValToGo translates an update value expression to Go. It always reads
// through "pre" (never "s"): it is only ever used inside a mutating
// transition method, where earlier lines may have already written new values
// into s — and TLA+ primed assignments in one action are simultaneous, so
// this expression must see the state as of entry, not a partially-mutated s.
func (cg *CodeGen) updateValToGo(e Expr) string {
	switch v := e.(type) {
	case BoolLit:
		return fmt.Sprintf("%v", v.Val)
	case IntLit:
		return fmt.Sprintf("%d", v.Val)
	case StrLit:
		return fmt.Sprintf("%q", v.Val)
	case VarRef:
		if g, ok := cg.constGo(v.Name, nil, "pre"); ok {
			return g
		}
		return "pre." + goField(v.Name)
	case BinOp:
		switch v.Op {
		case "\\cup":
			return fmt.Sprintf("setUnion(%s, %s)", cg.updateValToGo(v.Lhs), cg.updateValToGo(v.Rhs))
		case "\\cap":
			return fmt.Sprintf("setInter(%s, %s)", cg.updateValToGo(v.Lhs), cg.updateValToGo(v.Rhs))
		case "\\":
			return fmt.Sprintf("setDiff(%s, %s)", cg.updateValToGo(v.Lhs), cg.updateValToGo(v.Rhs))
		}
		lhs := cg.updateValToGo(v.Lhs)
		rhs := cg.updateValToGo(v.Rhs)
		switch v.Op {
		case "+":
			return fmt.Sprintf("%s + %s", lhs, rhs)
		case "-":
			return fmt.Sprintf("%s - %s", lhs, rhs)
		case "*":
			return fmt.Sprintf("%s * %s", lhs, rhs)
		}
	case IfExpr:
		// e.g. `n' = IF n < MaxLen THEN n + 1 ELSE MaxLen` (Bump-style
		// bounded increment) — an IF used as the update's VALUE, not a guard.
		return fmt.Sprintf("specgenIf(%s, %s, %s)",
			cg.guardToGo(v.Cond, "pre"), cg.updateValToGo(v.Then), cg.updateValToGo(v.Else))
	case SetLit:
		// A set-literal update value or recursive-call argument. The empty set is
		// a nil map — assignable to any key type, and membership reads on it are
		// false — so it needs no element-type guess. A non-empty literal keys on
		// its elements (string, or [N]string for a tuple set).
		if len(v.Elems) == 0 {
			return "nil"
		}
		var elems []string
		for _, el := range v.Elems {
			elems = append(elems, cg.updateValToGo(el)+": true")
		}
		return "map[" + setLitKeyType(v.Elems) + "]bool{" + strings.Join(elems, ", ") + "}"
	case FuncApply:
		// f[x] on a total function stored as a lazy map: read through the
		// default (Init's FuncLit body) exactly like exprToGo's FuncApply case.
		fnGo := cg.updateValToGo(v.Fn)
		argGo := cg.updateValToGo(v.Arg)
		def := `""`
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			def = cg.funcDefaultAt(name, d+1)
		}
		return fmt.Sprintf("funcGet(%s, %s, %s)", fnGo, argGo, def)
	case ExceptExpr:
		// [f EXCEPT ![i] = v] copy-on-writes one key: funcSet returns a fresh
		// map, preserving value semantics. Multiple updates chain left-to-right.
		// Inside v, `@` (AtExpr) is the pre-update value at that index — same
		// lowering as exprToGo's ExceptExpr case, over "pre" instead of locals/recv.
		out := cg.updateValToGo(v.Fn)
		def := `""`
		if name, d, ok := funcApplyChainDepth(v.Fn); ok {
			def = cg.funcDefaultAt(name, d+1)
		}
		for _, u := range v.Updates {
			idxGo := cg.updateValToGo(u.Index)
			prevAt := cg.atExpr
			cg.atExpr = fmt.Sprintf("funcGet(%s, %s, %s)", out, idxGo, def)
			valGo := cg.updateValToGo(u.Val)
			cg.atExpr = prevAt
			out = fmt.Sprintf("funcSet(%s, %s, %s)", out, idxGo, valGo)
		}
		return out
	case AtExpr:
		return cg.atExpr
	case FuncLit:
		// Full function-literal reassignment `x' = [k \in Dom |-> Body]`. A
		// concrete literal domain builds the map outright, one entry per
		// element. An abstract (CONSTANT) domain has no enumerable extent at
		// codegen time — the same shape Init encodes as an empty lazy map (see
		// funcDefault) — so it resets to a typed nil map: funcGet reads every
		// absent key back through the Init-derived default, exactly
		// reproducing this FuncLit's own semantics. Typed (not bare nil) so
		// the caller's cloneMap/funcGet generic K/V still infer.
		keyType := "string"
		if sl, ok := v.Domain.(SetLit); ok {
			if k := setLitKeyType(sl.Elems); k != "" {
				keyType = k
			}
		}
		valType := valTypeOrDefault(v.Body)
		if sl, ok := v.Domain.(SetLit); ok {
			var elems []string
			for _, el := range sl.Elems {
				keyGo := cg.updateValToGo(el)
				// v.Var is bound to THIS element's own key for each entry — a
				// body that reads its own bound variable (`[k \in {...} |->
				// k]`) must get a per-key value, not one rendering reused for
				// every key. updateValToGo has no locals param (unlike
				// exprToGo), so the binding goes through exprToGo instead —
				// the general, locals-aware lowering this function already
				// delegates unhandled constructs to.
				valGo := cg.exprToGo(v.Body, map[string]string{v.Var: keyGo}, "pre")
				elems = append(elems, keyGo+": "+valGo)
			}
			return "map[" + keyType + "]" + valType + "{" + strings.Join(elems, ", ") + "}"
		}
		return "map[" + keyType + "]" + valType + "(nil)"
	case Call:
		// Only RECURSIVE operators survive inlining as a Call. The update value
		// `n' = Countdown(3)` becomes a Go call to the standalone recursive func
		// writeRecursiveFuncs emits.
		var args []string
		for _, a := range v.Args {
			args = append(args, cg.updateValToGo(a))
		}
		return fmt.Sprintf("%s(%s)", recFuncName(v.Name), strings.Join(args, ", "))
	}
	// Any construct updateValToGo has no case for (CASE, \A, \E in value
	// position, ...): delegate to exprToGo, PATH B/C's proven general
	// lowering, over "pre" — the same receiver convention updateValToGo's
	// own IfExpr case already uses for a condition inside a mutating method.
	return cg.exprToGo(e, nil, "pre")
}

func litToGo(e Expr) string {
	switch v := e.(type) {
	case BoolLit:
		return fmt.Sprintf("%v", v.Val)
	case IntLit:
		return fmt.Sprintf("%d", v.Val)
	case StrLit:
		return fmt.Sprintf("%q", v.Val)
	case SetLit:
		// A set-typed Init value. The empty set MUST be a non-nil map — actions
		// build sets with setUnion/setDiff (always non-nil), and a nil Init would
		// make the same empty-set state serialize two ways (null vs {}).
		var elems []string
		for _, el := range v.Elems {
			elems = append(elems, litToGo(el)+": true")
		}
		return "map[string]bool{" + strings.Join(elems, ", ") + "}"
	}
	return "nil"
}

func findInitVal(varName string, e Expr) Expr {
	if e == nil {
		return nil
	}
	if bin, ok := e.(BinOp); ok && bin.Op == "=" {
		if vr, ok := bin.Lhs.(VarRef); ok && vr.Name == varName {
			return bin.Rhs
		}
		return nil
	}
	if bin, ok := e.(BinOp); ok && bin.Op == "/\\" {
		if v := findInitVal(varName, bin.Lhs); v != nil {
			return v
		}
		return findInitVal(varName, bin.Rhs)
	}
	return nil
}

func isSelfAssign(u Assign) bool {
	if vr, ok := u.Val.(VarRef); ok {
		return vr.Name == u.Var
	}
	return false
}

// goField converts a TLA+ variable name to a Go exported field name.
// It lives in package tla because it defines the trace JSON contract:
// specgen emits these keys, conform reads them.
func goField(name string) string { return tla.GoField(name) }

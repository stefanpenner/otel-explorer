package tla

import (
	"fmt"
	"sort"
)

// StepDispatch describes a Next of the shape:
//
//	\/ (doneVar /\ UNCHANGED vars)          -- optional stay-put branch
//	\/ /\ ~doneVar                          -- optional guard, not required
//	   /\ \E quantVar \in Domain :
//	        \/ (dispatchVar = M1 /\ Op1(quantVar))
//	        \/ (dispatchVar = M2 /\ Op2(quantVar))
//	        ...
//
// This is an input-driven decision machine (mode-dispatched key handling)
// rather than a set of independently-named actions. specgen (codegen) and
// conform (interpretation) both call DetectStepDispatch — neither has its own
// recognizer — so they cannot disagree about what a Next like this means.
type StepDispatch struct {
	// DoneVar is the boolean variable guarding the stay-put branch, or "" if
	// Next has no such branch. When set, the generated Step no-ops (returns
	// the state unchanged) whenever that variable is true.
	DoneVar string

	QuantVar    string // the \E-bound variable name (e.g. "k")
	Domain      Expr   // the \E domain, inlined to a literal (e.g. a SetLit)
	DispatchVar string // the variable each branch equality-tests (e.g. "mode")

	Branches []StepBranch
}

// StepBranch is one `dispatchVar = Value /\ OpName(quantVar)` disjunct.
type StepBranch struct {
	Value  Expr
	OpName string
}

// DetectStepDispatch inspects Next and reports whether it matches the
// \E-dispatch shape. Returns (nil, nil) if it does not — callers fall back to
// the ordinary named-disjunction path (ExtractActions). Returns an error only
// when Next looks like a dispatch but is malformed in a way that would
// otherwise fail silently or confusingly downstream.
func DetectStepDispatch(spec *Spec) (*StepDispatch, error) {
	nextBody := spec.FindDef("Next")
	if nextBody == nil {
		return nil, fmt.Errorf("no Next definition")
	}

	var doneVar string
	var candidates []Expr
	for _, d := range FlattenDisjunction(nextBody) {
		if v, ok := matchDoneBranch(d); ok {
			doneVar = v
			continue
		}
		candidates = append(candidates, d)
	}
	if len(candidates) != 1 {
		return nil, nil // not the shape we recognize; PATH A may still apply
	}

	var ex *ExistsExpr
	for _, c := range FlattenConjunction(candidates[0]) {
		if e, ok := c.(ExistsExpr); ok {
			if ex != nil {
				return nil, fmt.Errorf("Next has more than one \\E — dispatch shape supports exactly one")
			}
			cp := e
			ex = &cp
		}
	}
	if ex == nil {
		return nil, nil // no \E found; not this shape
	}

	branches, dispatchVar, err := parseDispatchBranches(ex.Body)
	if err != nil {
		if noBranchLooksLikeDispatch(ex.Body) {
			// `\E c \in Clients : Check(c) \/ Register(c)` — a quantified
			// named-action disjunction, not a dispatch. Decline; the caller
			// falls through to ExtractActionRefs.
			return nil, nil
		}
		return nil, err
	}

	domain, err := Inline(spec, ex.Domain)
	if err != nil {
		return nil, fmt.Errorf("Next \\E domain: %w", err)
	}

	return &StepDispatch{
		DoneVar:     doneVar,
		QuantVar:    ex.Var,
		Domain:      domain,
		DispatchVar: dispatchVar,
		Branches:    branches,
	}, nil
}

// matchDoneBranch recognizes `doneVar /\ UNCHANGED ...` and returns doneVar.
func matchDoneBranch(e Expr) (string, bool) {
	bin, ok := e.(BinOp)
	if !ok || bin.Op != "/\\" {
		return "", false
	}
	vr, ok := bin.Lhs.(VarRef)
	if !ok {
		return "", false
	}
	if _, ok := bin.Rhs.(UnchangedVars); !ok {
		return "", false
	}
	return vr.Name, true
}

// parseDispatchBranches parses `\/ (dispatchVar = M1 /\ Op1(k)) \/ ...` into
// StepBranch entries, verifying every branch equality-tests the SAME
// variable — a dispatch that switched variables mid-way would silently
// generate a Step that ignores some branches, so this is an error, not a
// skip.
func parseDispatchBranches(body Expr) ([]StepBranch, string, error) {
	var branches []StepBranch
	dispatchVar := ""

	for _, d := range FlattenDisjunction(body) {
		bin, ok := d.(BinOp)
		if !ok || bin.Op != "/\\" {
			return nil, "", fmt.Errorf("dispatch branch %+v is not (var = value /\\ Op(arg))", d)
		}
		eq, ok := bin.Lhs.(BinOp)
		if !ok || eq.Op != "=" {
			return nil, "", fmt.Errorf("dispatch branch guard is not an equality test: %+v", bin.Lhs)
		}
		vr, ok := eq.Lhs.(VarRef)
		if !ok {
			return nil, "", fmt.Errorf("dispatch branch guard left side is not a variable: %+v", eq.Lhs)
		}
		if dispatchVar == "" {
			dispatchVar = vr.Name
		} else if dispatchVar != vr.Name {
			return nil, "", fmt.Errorf("dispatch branches guard on different variables (%s vs %s)", dispatchVar, vr.Name)
		}
		call, ok := bin.Rhs.(Call)
		if !ok || len(call.Args) != 1 {
			return nil, "", fmt.Errorf("dispatch branch body is not a single-argument operator call: %+v", bin.Rhs)
		}
		branches = append(branches, StepBranch{Value: eq.Rhs, OpName: call.Name})
	}
	if len(branches) == 0 {
		return nil, "", fmt.Errorf("\\E body has no dispatch branches")
	}
	return branches, dispatchVar, nil
}

// FlattenDisjunction splits `a \/ b \/ c` into [a, b, c]. The disjunction
// counterpart of FlattenConjunction.
func FlattenDisjunction(e Expr) []Expr {
	if bin, ok := e.(BinOp); ok && bin.Op == "\\/" {
		return append(FlattenDisjunction(bin.Lhs), FlattenDisjunction(bin.Rhs)...)
	}
	return []Expr{e}
}

// CollectFreeVars scans every dispatch branch's fully-inlined body for
// VarRefs that are neither state variables nor the \E-bound quantifier
// variable — these are the spec's CONSTANTS, unresolved because they were
// not bound via Spec.ConstBindings. The generated Step needs one extra
// parameter per name.
//
// Only boolean usage is supported (a free var used solely as an IfExpr
// condition or a boolean-connective operand): that is the "is X true right
// now" pattern real specs use CONSTANTS for in a decision function (e.g.
// HasSel). A free var used arithmetically or relationally (e.g. MaxLen
// inside `n < MaxLen`) cannot be safely generated as a bool parameter, so
// CollectFreeVars errors and asks for an explicit -const binding instead of
// silently emitting code that would mistype it.
func CollectFreeVars(spec *Spec, sd *StepDispatch) ([]string, error) {
	seen := map[string]bool{}
	nonBool := map[string]bool{}
	var names []string

	for _, br := range sd.Branches {
		body, err := Inline(spec, Call{Name: br.OpName, Args: []Expr{VarRef{Name: sd.QuantVar}}})
		if err != nil {
			return nil, fmt.Errorf("branch %s: %w", br.OpName, err)
		}
		walkFreeVars(spec, sd.QuantVar, body, true, seen, nonBool, &names)
	}

	sort.Strings(names)
	for _, n := range names {
		if nonBool[n] {
			return nil, fmt.Errorf(
				"constant %s is used in a non-boolean context; bind it with -const %s=<value> before generating",
				n, n)
		}
	}
	return names, nil
}

// walkFreeVars records every non-variable, non-quantifier VarRef it finds.
// boolCtx tracks whether the current position requires a boolean value
// (IfExpr conditions, boolean-connective operands); a VarRef found outside a
// boolean context is flagged in nonBool.
func walkFreeVars(spec *Spec, quantVar string, e Expr, boolCtx bool, seen, nonBool map[string]bool, names *[]string) {
	switch v := e.(type) {
	case VarRef:
		if v.Name == quantVar || isSpecVariable(spec, v.Name) {
			return
		}
		if !seen[v.Name] {
			seen[v.Name] = true
			*names = append(*names, v.Name)
		}
		if !boolCtx {
			nonBool[v.Name] = true
		}
	case PrimedVar, BoolLit, IntLit, StrLit, BoolSet, IntSet:
		return
	case BinOp:
		switch v.Op {
		case "/\\", "\\/":
			walkFreeVars(spec, quantVar, v.Lhs, true, seen, nonBool, names)
			walkFreeVars(spec, quantVar, v.Rhs, true, seen, nonBool, names)
		case "=", "/=":
			// Equality doesn't require either side to be boolean by itself,
			// but a bare CONSTANT compared to TRUE/FALSE is still a boolean
			// use; anything else, treat conservatively as non-boolean.
			eqBool := isBoolLit(v.Lhs) || isBoolLit(v.Rhs)
			walkFreeVars(spec, quantVar, v.Lhs, eqBool, seen, nonBool, names)
			walkFreeVars(spec, quantVar, v.Rhs, eqBool, seen, nonBool, names)
		default:
			walkFreeVars(spec, quantVar, v.Lhs, false, seen, nonBool, names)
			walkFreeVars(spec, quantVar, v.Rhs, false, seen, nonBool, names)
		}
	case UnaryOp:
		walkFreeVars(spec, quantVar, v.Operand, v.Op == "~", seen, nonBool, names)
	case IfExpr:
		walkFreeVars(spec, quantVar, v.Cond, true, seen, nonBool, names)
		walkFreeVars(spec, quantVar, v.Then, boolCtx, seen, nonBool, names)
		walkFreeVars(spec, quantVar, v.Else, boolCtx, seen, nonBool, names)
	case CaseExpr:
		for _, a := range v.Arms {
			walkFreeVars(spec, quantVar, a.Cond, true, seen, nonBool, names)
			walkFreeVars(spec, quantVar, a.Result, boolCtx, seen, nonBool, names)
		}
		if v.Other != nil {
			walkFreeVars(spec, quantVar, v.Other, boolCtx, seen, nonBool, names)
		}
	case UnchangedVars:
		return
	case SetLit:
		for _, el := range v.Elems {
			walkFreeVars(spec, quantVar, el, false, seen, nonBool, names)
		}
	case RangeLit:
		walkFreeVars(spec, quantVar, v.Lo, false, seen, nonBool, names)
		walkFreeVars(spec, quantVar, v.Hi, false, seen, nonBool, names)
	case TupleLit:
		for _, el := range v.Elems {
			walkFreeVars(spec, quantVar, el, false, seen, nonBool, names)
		}
	}
}

func isBoolLit(e Expr) bool {
	_, ok := e.(BoolLit)
	return ok
}

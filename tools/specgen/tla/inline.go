package tla

import "fmt"

// maxInlineDepth bounds operator expansion. TLA+ operator definitions have no
// recursion in this subset; a spec that manages to reference itself (directly
// or through a cycle) hits this instead of hanging Inline.
const maxInlineDepth = 64

// Inline expands every reference to a user-defined operator — a bare VarRef
// naming a nullary def (e.g. `Idle` used as an atomic conjunct), or a Call to
// a parameterised one (e.g. `Bump(x)`) — into that operator's body, with
// arguments substituted for formal parameters. The result contains only
// VARIABLES, primed vars, literals, and built-in operators/sets: no Def
// reference survives.
//
// This is the shared primitive specgen (codegen) and conform (interpretation)
// both call before doing anything else with an action body. Neither compiles
// or evaluates a raw, un-inlined tree — so operator semantics (what Idle
// means, what Bump(n) computes) cannot drift between the two tools. It
// extends the same discipline the shared parser already provides.
func Inline(spec *Spec, e Expr) (Expr, error) {
	return inline(spec, e, nil, 0)
}

// findRawDef returns the full Def (including Params) for name, or nil.
func (s *Spec) findRawDef(name string) *Def {
	for i := range s.Defs {
		if s.Defs[i].Name == name {
			return &s.Defs[i]
		}
	}
	return nil
}

func isSpecVariable(spec *Spec, name string) bool {
	for _, v := range spec.Variables {
		if v == name {
			return true
		}
	}
	return false
}

func inline(spec *Spec, e Expr, locals map[string]Expr, depth int) (Expr, error) {
	if depth > maxInlineDepth {
		return nil, fmt.Errorf("operator expansion exceeded depth %d (recursive definition?)", maxInlineDepth)
	}

	switch v := e.(type) {
	case VarRef:
		if locals != nil {
			if bound, ok := locals[v.Name]; ok {
				return bound, nil // a call argument or bound quantifier variable
			}
		}
		if isSpecVariable(spec, v.Name) {
			return v, nil // a real state variable
		}
		if spec.ConstBindings != nil {
			if bound, ok := spec.ConstBindings[v.Name]; ok {
				return bound, nil // a CONSTANT bound via -const
			}
		}
		if def := spec.findRawDef(v.Name); def != nil {
			if len(def.Params) != 0 {
				return nil, fmt.Errorf("operator %s takes %d argument(s), referenced with none", v.Name, len(def.Params))
			}
			return inline(spec, def.Body, locals, depth+1)
		}
		return v, nil // a CONSTANT or otherwise-unbound name — caller's problem

	case Call:
		def := spec.findRawDef(v.Name)
		if def == nil {
			return nil, fmt.Errorf("undefined operator %s", v.Name)
		}
		if len(def.Params) != len(v.Args) {
			return nil, fmt.Errorf("operator %s takes %d argument(s), got %d", v.Name, len(def.Params), len(v.Args))
		}
		if spec.Recursive[v.Name] {
			// A RECURSIVE operator cannot be inlined — expanding its body would
			// re-encounter the self-call forever (the depth bound). Inline the
			// ARGUMENTS in the caller's scope and keep the Call node so codegen
			// emits a Go function call to a standalone recursive func.
			args := make([]Expr, len(v.Args))
			for i, a := range v.Args {
				ia, err := inline(spec, a, locals, depth+1)
				if err != nil {
					return nil, err
				}
				args[i] = ia
			}
			return Call{Name: v.Name, Args: args}, nil
		}
		next := make(map[string]Expr, len(def.Params))
		for i, param := range def.Params {
			arg, err := inline(spec, v.Args[i], locals, depth+1) // resolved in the CALLER's scope
			if err != nil {
				return nil, err
			}
			next[param] = arg
		}
		return inline(spec, def.Body, next, depth+1)

	case BinOp:
		lhs, err := inline(spec, v.Lhs, locals, depth+1)
		if err != nil {
			return nil, err
		}
		rhs, err := inline(spec, v.Rhs, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return BinOp{Op: v.Op, Lhs: lhs, Rhs: rhs}, nil

	case UnaryOp:
		operand, err := inline(spec, v.Operand, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return UnaryOp{Op: v.Op, Operand: operand}, nil

	case IfExpr:
		cond, err := inline(spec, v.Cond, locals, depth+1)
		if err != nil {
			return nil, err
		}
		then_, err := inline(spec, v.Then, locals, depth+1)
		if err != nil {
			return nil, err
		}
		else_, err := inline(spec, v.Else, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return IfExpr{Cond: cond, Then: then_, Else: else_}, nil

	case CaseExpr:
		arms := make([]CaseArm, len(v.Arms))
		for i, a := range v.Arms {
			cond, err := inline(spec, a.Cond, locals, depth+1)
			if err != nil {
				return nil, err
			}
			res, err := inline(spec, a.Result, locals, depth+1)
			if err != nil {
				return nil, err
			}
			arms[i] = CaseArm{Cond: cond, Result: res}
		}
		var other Expr
		if v.Other != nil {
			o, err := inline(spec, v.Other, locals, depth+1)
			if err != nil {
				return nil, err
			}
			other = o
		}
		return CaseExpr{Arms: arms, Other: other}, nil

	case ExistsExpr:
		domain, err := inline(spec, v.Domain, locals, depth+1)
		if err != nil {
			return nil, err
		}
		// The bound variable shadows any same-named local for the body only.
		bodyLocals := shadow(locals, v.Var)
		body, err := inline(spec, v.Body, bodyLocals, depth+1)
		if err != nil {
			return nil, err
		}
		return ExistsExpr{Var: v.Var, Domain: domain, Body: body}, nil

	case ForallExpr:
		domain, err := inline(spec, v.Domain, locals, depth+1)
		if err != nil {
			return nil, err
		}
		bodyLocals := shadow(locals, v.Var)
		body, err := inline(spec, v.Body, bodyLocals, depth+1)
		if err != nil {
			return nil, err
		}
		return ForallExpr{Var: v.Var, Domain: domain, Body: body}, nil

	case SetCompr:
		// `{x \in D : P(x)}` — a filter. Substitute into the domain (caller's
		// scope) and the predicate (with x shadowing any same-named local), so a
		// Call whose body is a comprehension expands correctly (Step's E, S).
		domain, err := inline(spec, v.Domain, locals, depth+1)
		if err != nil {
			return nil, err
		}
		pred, err := inline(spec, v.Pred, shadow(locals, v.Var), depth+1)
		if err != nil {
			return nil, err
		}
		return SetCompr{Var: v.Var, Domain: domain, Pred: pred}, nil

	case LetExpr:
		// `LET d == e IN body` is pure local naming: bind each def (evaluated in
		// the scope built up so far, so a later def can reference an earlier one)
		// and inline the body with those bindings visible. Because the result is
		// substituted, codegen never sees a LetExpr — the same discipline that
		// keeps operator Calls out of exprToGo. This is the SessAttach `nxt`
		// binding inside Closure.
		scope := locals
		for _, d := range v.Defs {
			if len(d.Params) != 0 {
				return nil, fmt.Errorf("LET %s: parameterised local definitions are not supported", d.Name)
			}
			val, err := inline(spec, d.Body, scope, depth+1)
			if err != nil {
				return nil, err
			}
			next := make(map[string]Expr, len(scope)+1)
			for k, vv := range scope {
				next[k] = vv
			}
			next[d.Name] = val
			scope = next
		}
		return inline(spec, v.Body, scope, depth+1)

	case SetLit:
		elems := make([]Expr, len(v.Elems))
		for i, el := range v.Elems {
			ie, err := inline(spec, el, locals, depth+1)
			if err != nil {
				return nil, err
			}
			elems[i] = ie
		}
		return SetLit{Elems: elems}, nil

	case RangeLit:
		lo, err := inline(spec, v.Lo, locals, depth+1)
		if err != nil {
			return nil, err
		}
		hi, err := inline(spec, v.Hi, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return RangeLit{Lo: lo, Hi: hi}, nil

	case TupleLit:
		elems := make([]Expr, len(v.Elems))
		for i, el := range v.Elems {
			ie, err := inline(spec, el, locals, depth+1)
			if err != nil {
				return nil, err
			}
			elems[i] = ie
		}
		return TupleLit{Elems: elems}, nil

	case UnchangedVars:
		// UNCHANGED vars where `vars == <<a, b>>` names a tuple-alias def, not
		// a real variable — TLA+ specs almost always write it this way rather
		// than listing every variable. Expand the alias to the real names.
		var names []string
		for _, n := range v.Names {
			if isSpecVariable(spec, n) {
				names = append(names, n)
				continue
			}
			def := spec.findRawDef(n)
			if def == nil {
				return nil, fmt.Errorf("UNCHANGED %s: not a variable or a defined alias", n)
			}
			if len(def.Params) != 0 {
				return nil, fmt.Errorf("UNCHANGED %s: cannot use a parameterised operator here", n)
			}
			expanded, err := expandUnchangedAlias(spec, def.Body, depth+1)
			if err != nil {
				return nil, err
			}
			names = append(names, expanded...)
		}
		return UnchangedVars{Names: names}, nil

	case FuncSet:
		domain, err := inline(spec, v.Domain, locals, depth+1)
		if err != nil {
			return nil, err
		}
		rng, err := inline(spec, v.Range, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return FuncSet{Domain: domain, Range: rng}, nil

	case FuncLit:
		domain, err := inline(spec, v.Domain, locals, depth+1)
		if err != nil {
			return nil, err
		}
		// The bound variable shadows any same-named local for the body only.
		body, err := inline(spec, v.Body, shadow(locals, v.Var), depth+1)
		if err != nil {
			return nil, err
		}
		return FuncLit{Var: v.Var, Domain: domain, Body: body}, nil

	case FuncApply:
		fn, err := inline(spec, v.Fn, locals, depth+1)
		if err != nil {
			return nil, err
		}
		arg, err := inline(spec, v.Arg, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return FuncApply{Fn: fn, Arg: arg}, nil

	case ExceptExpr:
		fn, err := inline(spec, v.Fn, locals, depth+1)
		if err != nil {
			return nil, err
		}
		updates := make([]ExceptUpdate, len(v.Updates))
		for i, u := range v.Updates {
			idx, err := inline(spec, u.Index, locals, depth+1)
			if err != nil {
				return nil, err
			}
			val, err := inline(spec, u.Val, locals, depth+1)
			if err != nil {
				return nil, err
			}
			updates[i] = ExceptUpdate{Index: idx, Val: val}
		}
		return ExceptExpr{Fn: fn, Updates: updates}, nil

	case DomainExpr:
		of, err := inline(spec, v.Of, locals, depth+1)
		if err != nil {
			return nil, err
		}
		return DomainExpr{Of: of}, nil

	case PrimedVar, BoolLit, IntLit, StrLit, BoolSet, IntSet, AtExpr:
		return v, nil // atoms — nothing to inline

	default:
		return nil, fmt.Errorf("inline: unhandled expression type %T", e)
	}
}

// expandUnchangedAlias resolves `vars == <<a, b>>`-style aliases used inside
// UNCHANGED down to the real variable names they name.
func expandUnchangedAlias(spec *Spec, e Expr, depth int) ([]string, error) {
	if depth > maxInlineDepth {
		return nil, fmt.Errorf("UNCHANGED alias expansion exceeded depth %d", maxInlineDepth)
	}
	switch v := e.(type) {
	case VarRef:
		if isSpecVariable(spec, v.Name) {
			return []string{v.Name}, nil
		}
		if def := spec.findRawDef(v.Name); def != nil && len(def.Params) == 0 {
			return expandUnchangedAlias(spec, def.Body, depth+1)
		}
		return nil, fmt.Errorf("UNCHANGED alias references %q, which is not a variable", v.Name)
	case TupleLit:
		var out []string
		for _, el := range v.Elems {
			names, err := expandUnchangedAlias(spec, el, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, names...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("UNCHANGED alias body is not a variable tuple (%T)", e)
	}
}

// shadow returns a copy of locals with name removed, so a quantifier's bound
// variable is not accidentally replaced by an outer call's argument of the
// same name.
func shadow(locals map[string]Expr, name string) map[string]Expr {
	if locals == nil {
		return nil
	}
	out := make(map[string]Expr, len(locals))
	for k, v := range locals {
		if k != name {
			out[k] = v
		}
	}
	return out
}

package tla

// Walk calls f on e and, unless f returns false, on every sub-expression.
// Returning false prunes that subtree.
func Walk(e Expr, f func(Expr) bool) {
	if e == nil || !f(e) {
		return
	}
	switch x := e.(type) {
	case BoolLit, IntLit, StrLit, VarRef, PrimedVar, BoolSet, IntSet, AtExpr, UnchangedVars:
		// leaves
	case SetLit:
		walkAll(x.Elems, f)
	case TupleLit:
		walkAll(x.Elems, f)
	case RangeLit:
		Walk(x.Lo, f)
		Walk(x.Hi, f)
	case BinOp:
		Walk(x.Lhs, f)
		Walk(x.Rhs, f)
	case UnaryOp:
		Walk(x.Operand, f)
	case IfExpr:
		Walk(x.Cond, f)
		Walk(x.Then, f)
		Walk(x.Else, f)
	case CaseExpr:
		for _, a := range x.Arms {
			Walk(a.Cond, f)
			Walk(a.Result, f)
		}
		Walk(x.Other, f)
	case Call:
		walkAll(x.Args, f)
	case ExistsExpr:
		Walk(x.Domain, f)
		Walk(x.Body, f)
	case ForallExpr:
		Walk(x.Domain, f)
		Walk(x.Body, f)
	case ChooseExpr:
		Walk(x.Domain, f)
		Walk(x.Pred, f)
	case SetCompr:
		Walk(x.Domain, f)
		Walk(x.Pred, f)
	case SetMap:
		Walk(x.Domain, f)
		Walk(x.Body, f)
	case FuncSet:
		Walk(x.Domain, f)
		Walk(x.Range, f)
	case FuncLit:
		Walk(x.Domain, f)
		Walk(x.Body, f)
	case FuncApply:
		Walk(x.Fn, f)
		Walk(x.Arg, f)
	case ExceptExpr:
		Walk(x.Fn, f)
		for _, u := range x.Updates {
			Walk(u.Index, f)
			Walk(u.Val, f)
		}
	case LetExpr:
		for _, d := range x.Defs {
			Walk(d.Body, f)
		}
		Walk(x.Body, f)
	}
}

func walkAll(es []Expr, f func(Expr) bool) {
	for _, e := range es {
		Walk(e, f)
	}
}

// HasPrimed reports whether e constrains any post-state value — a primed
// variable or an UNCHANGED clause. It does NOT descend into operator calls:
// use it to split an action's top-level conjunction into guard and update.
func HasPrimed(e Expr) bool {
	found := false
	Walk(e, func(n Expr) bool {
		switch n.(type) {
		case PrimedVar, UnchangedVars:
			found = true
		}
		return !found
	})
	return found
}

// ConstrainedVars returns every variable whose post-state value the expression
// pins down, following operator calls into their bodies.
//
// It exists to enforce specgen's value-semantics convention: a variable an
// action never mentions in primed form is unchanged, not unconstrained. conform
// must apply the same rule or it would accept post-states the generated Go can
// never produce.
func ConstrainedVars(spec *Spec, e Expr) map[string]bool {
	out := map[string]bool{}
	seen := map[string]bool{}
	var visit func(Expr)
	visit = func(x Expr) {
		Walk(x, func(n Expr) bool {
			switch v := n.(type) {
			case PrimedVar:
				out[v.Name] = true
			case UnchangedVars:
				for _, name := range v.Names {
					out[name] = true
				}
			case Call:
				if seen[v.Name] {
					return true
				}
				if op := FindOpIn(spec, v.Name); op != nil {
					seen[v.Name] = true
					visit(op.Body)
				}
			case VarRef:
				// A nullary operator used bare (e.g. `DaemonExit`) still has a body.
				if seen[v.Name] {
					return true
				}
				if op := FindOpIn(spec, v.Name); op != nil && len(op.Params) == 0 {
					seen[v.Name] = true
					visit(op.Body)
				}
			}
			return true
		})
	}
	visit(e)
	return out
}

// FindOpIn is FindOp as a free function, for callers holding a possibly-nil spec.
func FindOpIn(spec *Spec, name string) *Def {
	if spec == nil {
		return nil
	}
	return spec.FindOp(name)
}

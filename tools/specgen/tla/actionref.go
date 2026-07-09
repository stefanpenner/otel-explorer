package tla

import "fmt"

// ActionRef names one action Next can take.
//
// Real specs write two forms side by side (Hub.tla does both):
//
//	Next ==
//	  \/ \E c \in Conns : Connect(c) \/ Disconnect(c)   -- parameterised
//	  \/ ApplyMark                                       -- nullary
//	  \/ DaemonExit
//
// A parameterised action carries the \E binder name and its (inlined) domain;
// a nullary one leaves both zero. This is a purely syntactic view: it does not
// split guard from update, so it accommodates actions whose bodies contain
// nested quantifiers, EXCEPT, or conditional assignments — shapes ExtractActions
// cannot represent. conform's predicate mode consumes it.
type ActionRef struct {
	Name   string
	Param  string // \E-bound variable, "" when nullary
	Domain Expr   // \E domain, inlined; nil when nullary
}

// ExtractActionRefs reads Next as a disjunction of actions.
//
// An action named but never defined is an error: a checker that shrugged at it
// would accept every trace entry carrying that name.
func ExtractActionRefs(spec *Spec) ([]ActionRef, error) {
	next := spec.FindDef("Next")
	if next == nil {
		return nil, fmt.Errorf("no Next definition")
	}

	var refs []ActionRef
	for _, d := range FlattenDisjunction(next) {
		switch v := d.(type) {
		case ExistsExpr:
			domain, err := Inline(spec, v.Domain)
			if err != nil {
				return nil, fmt.Errorf("Next \\E domain: %w", err)
			}
			for _, inner := range FlattenDisjunction(v.Body) {
				call, ok := inner.(Call)
				if !ok {
					return nil, fmt.Errorf("Next: \\E %s body disjunct %+v is not an operator call", v.Var, inner)
				}
				if len(call.Args) != 1 {
					return nil, fmt.Errorf("Next: %s takes %d arguments; a quantified action takes exactly one", call.Name, len(call.Args))
				}
				arg, ok := call.Args[0].(VarRef)
				if !ok || arg.Name != v.Var {
					return nil, fmt.Errorf("Next: %s must be applied to the \\E binder %s, got %+v", call.Name, v.Var, call.Args[0])
				}
				refs = append(refs, ActionRef{Name: call.Name, Param: v.Var, Domain: domain})
			}
		case VarRef:
			refs = append(refs, ActionRef{Name: v.Name})
		case Call:
			if len(v.Args) != 0 {
				return nil, fmt.Errorf("Next: unquantified action %s takes arguments", v.Name)
			}
			refs = append(refs, ActionRef{Name: v.Name})
		default:
			return nil, fmt.Errorf("Next: disjunct %+v is neither a named action nor \\E x \\in S : ...", d)
		}
	}

	for _, r := range refs {
		if spec.FindOp(r.Name) == nil {
			return nil, fmt.Errorf("Next references undefined action %q", r.Name)
		}
	}
	return refs, nil
}

// ExtractParamActions reads Next as a guarded-transition library (PATH C): a
// disjunction that may mix parameterised actions (`\E x \in D : A(x) \/ B(x)`)
// and nullary ones (`ApplyMark`). Unlike ExtractActions (PATH A, nullary only)
// it carries each action's \E-bound parameter through into Action.Params, so
// codegen can emit Can<A>(x)/<A>(x) methods; unlike ExtractActionRefs (purely
// syntactic) it splits each action body into guard + update.
//
// It inlines every action against its parameter kept as a free VarRef, so an
// action defined in terms of a helper operator still splits cleanly.
func ExtractParamActions(spec *Spec) ([]Action, error) {
	refs, err := ExtractActionRefs(spec)
	if err != nil {
		return nil, err
	}
	var actions []Action
	for _, r := range refs {
		var call Call
		if r.Param != "" {
			call = Call{Name: r.Name, Args: []Expr{VarRef{Name: r.Param}}}
		} else {
			call = Call{Name: r.Name}
		}
		inlined, err := Inline(spec, call)
		if err != nil {
			return nil, fmt.Errorf("action %s: %w", r.Name, err)
		}
		act, err := splitAction(r.Name, inlined)
		if err != nil {
			return nil, fmt.Errorf("action %s: %w", r.Name, err)
		}
		// splitAction may have lifted binders from quantifiers inside the body
		// (`\E h \in Hosts : host' = ... h`). The Next-level binder c precedes
		// them: Check(c) with an inner \E h reads as Params [c, h].
		if r.Param != "" {
			act.Params = append([]string{r.Param}, act.Params...)
			act.ParamDomains = append([]Expr{r.Domain}, act.ParamDomains...)
		}
		actions = append(actions, act)
	}
	return actions, nil
}

// AnyParameterised reports whether any extracted action carries a \E-bound
// parameter — the signal to generate the PATH C library surface (per-arg
// Can/Do methods + EnabledSteps/ApplyStep) rather than PATH A's nullary
// EnabledActions/ApplyAction.
func AnyParameterised(actions []Action) bool {
	for _, a := range actions {
		if len(a.Params) > 0 {
			return true
		}
	}
	return false
}

// HasQuantifiedAction reports whether any ref is parameterised — the signal
// that guard/update extraction cannot model this Next.
func HasQuantifiedAction(refs []ActionRef) bool {
	for _, r := range refs {
		if r.Param != "" {
			return true
		}
	}
	return false
}

// noBranchLooksLikeDispatch reports whether NONE of the \E body's disjuncts is
// shaped `var = value /\ Op(arg)`. All-or-nothing: a Next where some branches
// match is a malformed dispatch (an error), one where none match is simply a
// different shape (decline).
func noBranchLooksLikeDispatch(body Expr) bool {
	for _, d := range FlattenDisjunction(body) {
		bin, ok := d.(BinOp)
		if !ok || bin.Op != "/\\" {
			continue
		}
		eq, ok := bin.Lhs.(BinOp)
		if !ok || eq.Op != "=" {
			continue
		}
		if _, ok := eq.Lhs.(VarRef); !ok {
			continue
		}
		if _, ok := bin.Rhs.(Call); ok {
			return false
		}
	}
	return true
}

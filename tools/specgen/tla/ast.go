// Package tla parses the TLA+ subset shared by specgen and conform.
//
// It is the single source of truth for the spec AST: specgen compiles it to
// Go, conform interprets it against runtime traces.
package tla

import "strings"

// GoField converts a TLA+ variable name to the Go exported field name that
// specgen emits for it.
//
// This is the trace JSON contract. specgen's generated State struct carries no
// json tags, so encoding/json uses the Go field name verbatim: TLA+ `on`
// becomes JSON key `On`. conform reads traces back through this same function,
// so the two tools cannot drift on key naming.
func GoField(name string) string {
	if name == "" {
		return name
	}
	r := []rune(name)
	r[0] = []rune(strings.ToUpper(string(r[0])))[0]
	return string(r)
}

// Spec is a parsed TLA+ specification.
type Spec struct {
	Name      string
	Extends   []string
	Constants []string
	Variables []string
	Defs      []Def
	// Unsupported holds definitions the parser could not model and skipped.
	// Codegen only needs Init/Next/actions, so a real spec's TypeOK or temporal
	// properties land here instead of failing the whole parse.
	Unsupported []SkippedDef
	// ConstBindings binds CONSTANTS to literal values before codegen/eval —
	// the .tla file itself never assigns them (that's a .cfg's job). A
	// CONSTANT referenced in a dispatch operator body and NOT bound here
	// surfaces via CollectFreeVars as an extra Step parameter instead.
	ConstBindings map[string]Expr
	// Recursive holds the names declared with `RECURSIVE Op(_, _)`. TLA+ requires
	// the declaration for any self-referential operator, so this is the reliable
	// signal that a call must NOT be inlined away (it would hit the depth bound)
	// but instead survive as a Call for codegen to emit as a Go function call.
	Recursive map[string]bool
}

// SkippedDef records a definition the parser could not handle, for reporting.
type SkippedDef struct {
	Name   string
	Reason string
}

// Def is a top-level TLA+ definition: Name == body, or Name(p1, p2, ...) == body
// for a parameterised operator. Params is empty for a nullary def.
type Def struct {
	Name   string
	Params []string
	Body   Expr
}

// Action is a definition used as a state-transition action.
// Extracted from Defs by matching names against the Next disjunction.
type Action struct {
	Name string
	// Params names the \E-bound arguments of a parameterised action
	// (PATH C: `\E x \in D : A(x)`), empty for a nullary action. Each becomes
	// a Go method parameter; guard/update expressions read these by name
	// rather than from state. A param may be bound at the Next dispatch level
	// (`\E c \in Clients : Check(c)`) or lifted from a quantifier inside the
	// body (`Check(c) == \E h \in Hosts : host' = ... h ...`).
	Params []string
	// ParamDomains carries each param's \E domain (inlined), indexed parallel
	// to Params, so codegen can enumerate it. Different params may range over
	// different CONSTANT sets (c over Clients, h over Hosts).
	ParamDomains []Expr
	Guard        Expr     // precondition (unprimed part of the conjunction)
	Update       []Assign // primed assignments (state transitions)
}

// Assign is a single primed-variable assignment: var' = expr.
type Assign struct {
	Var string
	Val Expr
}

// Expr is the interface for all TLA+ expressions.
type Expr interface {
	exprNode()
}

// --- Literals ---

type BoolLit struct{ Val bool }  // TRUE, FALSE
type IntLit struct{ Val int64 }  // 42
type StrLit struct{ Val string } // "hello"

// --- References ---

type VarRef struct{ Name string }    // x
type PrimedVar struct{ Name string } // x'

// --- Built-in sets ---

type BoolSet struct{} // BOOLEAN
type IntSet struct{}  // Int

// --- Collections ---

type SetLit struct{ Elems []Expr }  // {1, 2, 3}
type RangeLit struct{ Lo, Hi Expr } // a..b

// --- Operators ---

type BinOp struct {
	Op       string // "=", "/=", "/\", "\/", "\in", "\notin", "+", "-", ...
	Lhs, Rhs Expr
}

type UnaryOp struct {
	Op      string // "~", "UNARY_CHG"
	Operand Expr
}

type IfExpr struct {
	Cond, Then, Else Expr
}

// --- Records / Tuples (later) ---

type TupleLit struct{ Elems []Expr } // <<a, b, c>>

// UnchangedVars marks variables as unchanged by an action.
// UNCHANGED <<x, y>> or UNCHANGED vars
type UnchangedVars struct{ Names []string }

// --- Operator application ---

// Call is a user-defined operator applied to arguments: Name(arg1, arg2, ...).
// Resolved by Inline, which substitutes Args for the operator's Params and
// expands to the operator's body. No Call survives past Inline.
type Call struct {
	Name string
	Args []Expr
}

// --- CASE ---

// CaseArm is one arm of a CASE expression: Cond -> Result.
type CaseArm struct {
	Cond   Expr
	Result Expr
}

// CaseExpr is `CASE c1 -> e1 [] c2 -> e2 [] ... [] OTHER -> eN`. Other is nil
// if the CASE has no OTHER arm (TLA+ then requires the arms be exhaustive;
// this subset does not check that — an unmatched CASE with no OTHER is a
// runtime error at codegen/eval time, same as real TLA+).
type CaseExpr struct {
	Arms  []CaseArm
	Other Expr
}

// --- Quantifiers ---

// ExistsExpr is `\E Var \in Domain : Body`.
type ExistsExpr struct {
	Var    string
	Domain Expr
	Body   Expr
}

// ForallExpr is `\A Var \in Domain : Body`.
type ForallExpr struct {
	Var    string
	Domain Expr
	Body   Expr
}

// --- Functions and set constructors ---
//
// A multi-binding quantifier (`\E x \in S, y \in T : P`) is DESUGARED by the
// parser into nested ExistsExpr/ForallExpr, so no node above changes shape.

// FuncSet is `[Domain -> Range]`, the set of functions from Domain to Range.
// Used in type invariants: `pc \in [Ops -> {"idle","done"}]`.
type FuncSet struct{ Domain, Range Expr }

// FuncLit is `[Var \in Domain |-> Body]`.
type FuncLit struct {
	Var    string
	Domain Expr
	Body   Expr
}

// FuncApply is `Fn[Arg]`.
type FuncApply struct{ Fn, Arg Expr }

// DomainExpr is `DOMAIN f` — the set of keys a function is defined over.
// Typically used as `k \in DOMAIN f` or a quantifier's domain
// (`\A k \in DOMAIN f : ...`).
type DomainExpr struct{ Of Expr }

// ExceptUpdate is one `![Index] = Val` clause of an EXCEPT.
type ExceptUpdate struct{ Index, Val Expr }

// ExceptExpr is `[Fn EXCEPT ![i] = v, ![j] = w]`. Inside Val, `@` refers to the
// function's old value at that index (AtExpr).
type ExceptExpr struct {
	Fn      Expr
	Updates []ExceptUpdate
}

// AtExpr is `@` — the pre-update value inside an EXCEPT clause.
type AtExpr struct{}

// SetCompr is `{Var \in Domain : Pred}` (filter).
type SetCompr struct {
	Var    string
	Domain Expr
	Pred   Expr
}

// SetMap is `{Body : Var \in Domain}` (image).
type SetMap struct {
	Body   Expr
	Var    string
	Domain Expr
}

// LetExpr is `LET d1 == e1  d2 == e2  IN Body`.
type LetExpr struct {
	Defs []Def
	Body Expr
}

// ChooseExpr is `CHOOSE Var \in Domain : Pred`.
type ChooseExpr struct {
	Var    string
	Domain Expr
	Pred   Expr
}

// exprNode implementations

func (DomainExpr) exprNode() {}
func (FuncSet) exprNode()    {}
func (FuncLit) exprNode()    {}
func (FuncApply) exprNode()  {}
func (ExceptExpr) exprNode() {}
func (AtExpr) exprNode()     {}
func (SetCompr) exprNode()   {}
func (SetMap) exprNode()     {}
func (LetExpr) exprNode()    {}
func (ChooseExpr) exprNode() {}

func (BoolLit) exprNode()       {}
func (IntLit) exprNode()        {}
func (StrLit) exprNode()        {}
func (VarRef) exprNode()        {}
func (PrimedVar) exprNode()     {}
func (BoolSet) exprNode()       {}
func (IntSet) exprNode()        {}
func (SetLit) exprNode()        {}
func (RangeLit) exprNode()      {}
func (BinOp) exprNode()         {}
func (UnaryOp) exprNode()       {}
func (IfExpr) exprNode()        {}
func (TupleLit) exprNode()      {}
func (UnchangedVars) exprNode() {}
func (Call) exprNode()          {}
func (CaseExpr) exprNode()      {}
func (ExistsExpr) exprNode()    {}
func (ForallExpr) exprNode()    {}

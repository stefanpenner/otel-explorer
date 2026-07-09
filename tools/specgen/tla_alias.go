package main

import "github.com/stefanpenner/otel-explorer/tools/specgen/tla"

// The TLA+ AST and parser live in package tla so that conform can share them.
// These aliases keep the codegen (and its tests) reading as if the types were
// still local. Nothing here adds behaviour — it is a re-export, nothing more.

type (
	Spec   = tla.Spec
	Def    = tla.Def
	Action = tla.Action
	Assign = tla.Assign
	Expr   = tla.Expr

	BoolLit   = tla.BoolLit
	IntLit    = tla.IntLit
	StrLit    = tla.StrLit
	VarRef    = tla.VarRef
	PrimedVar = tla.PrimedVar
	BoolSet   = tla.BoolSet
	IntSet    = tla.IntSet
	SetLit    = tla.SetLit
	SetCompr  = tla.SetCompr
	RangeLit  = tla.RangeLit
	BinOp     = tla.BinOp
	UnaryOp   = tla.UnaryOp
	IfExpr    = tla.IfExpr
	TupleLit  = tla.TupleLit
	Call      = tla.Call

	FuncSet    = tla.FuncSet
	FuncLit    = tla.FuncLit
	FuncApply  = tla.FuncApply
	ExceptExpr = tla.ExceptExpr
	AtExpr     = tla.AtExpr
	DomainExpr = tla.DomainExpr

	UnchangedVars = tla.UnchangedVars
)

var (
	Parse               = tla.Parse
	ExtractActions      = tla.ExtractActions
	ExtractParamActions = tla.ExtractParamActions
)

package tla

import (
	"fmt"
	"strings"
)

// Render prints an expression back as TLA+ source, near enough for an error
// message to be greppable against the spec file. It is not a formatter: it
// parenthesises freely and does not preserve layout.
func Render(e Expr) string {
	switch x := e.(type) {
	case nil:
		return ""
	case BoolLit:
		if x.Val {
			return "TRUE"
		}
		return "FALSE"
	case IntLit:
		return fmt.Sprintf("%d", x.Val)
	case StrLit:
		return fmt.Sprintf("%q", x.Val)
	case VarRef:
		return x.Name
	case PrimedVar:
		return x.Name + "'"
	case BoolSet:
		return "BOOLEAN"
	case IntSet:
		return "Int"
	case AtExpr:
		return "@"
	case SetLit:
		return "{" + renderList(x.Elems) + "}"
	case TupleLit:
		return "<<" + renderList(x.Elems) + ">>"
	case RangeLit:
		return Render(x.Lo) + ".." + Render(x.Hi)
	case BinOp:
		return "(" + Render(x.Lhs) + " " + x.Op + " " + Render(x.Rhs) + ")"
	case UnaryOp:
		return x.Op + Render(x.Operand)
	case IfExpr:
		return "IF " + Render(x.Cond) + " THEN " + Render(x.Then) + " ELSE " + Render(x.Else)
	case CaseExpr:
		parts := make([]string, 0, len(x.Arms)+1)
		for _, a := range x.Arms {
			parts = append(parts, Render(a.Cond)+" -> "+Render(a.Result))
		}
		if x.Other != nil {
			parts = append(parts, "OTHER -> "+Render(x.Other))
		}
		return "CASE " + strings.Join(parts, " [] ")
	case UnchangedVars:
		return "UNCHANGED <<" + strings.Join(x.Names, ", ") + ">>"
	case Call:
		return x.Name + "(" + renderList(x.Args) + ")"
	case ExistsExpr:
		return "\\E " + x.Var + " \\in " + Render(x.Domain) + " : " + Render(x.Body)
	case ForallExpr:
		return "\\A " + x.Var + " \\in " + Render(x.Domain) + " : " + Render(x.Body)
	case ChooseExpr:
		return "CHOOSE " + x.Var + " \\in " + Render(x.Domain) + " : " + Render(x.Pred)
	case SetCompr:
		return "{" + x.Var + " \\in " + Render(x.Domain) + " : " + Render(x.Pred) + "}"
	case SetMap:
		return "{" + Render(x.Body) + " : " + x.Var + " \\in " + Render(x.Domain) + "}"
	case FuncSet:
		return "[" + Render(x.Domain) + " -> " + Render(x.Range) + "]"
	case FuncLit:
		return "[" + x.Var + " \\in " + Render(x.Domain) + " |-> " + Render(x.Body) + "]"
	case FuncApply:
		return Render(x.Fn) + "[" + Render(x.Arg) + "]"
	case ExceptExpr:
		parts := make([]string, len(x.Updates))
		for i, u := range x.Updates {
			parts[i] = "![" + Render(u.Index) + "] = " + Render(u.Val)
		}
		return "[" + Render(x.Fn) + " EXCEPT " + strings.Join(parts, ", ") + "]"
	case LetExpr:
		parts := make([]string, len(x.Defs))
		for i, d := range x.Defs {
			parts[i] = d.Name + " == " + Render(d.Body)
		}
		return "LET " + strings.Join(parts, " ") + " IN " + Render(x.Body)
	default:
		return fmt.Sprintf("%T", e)
	}
}

func renderList(es []Expr) string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = Render(e)
	}
	return strings.Join(parts, ", ")
}

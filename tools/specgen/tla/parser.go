package tla

import (
	"fmt"
	"strconv"
)

// Parse tokenizes and parses a TLA+ spec string.
func Parse(src string) (*Spec, error) {
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks, pos: 0}
	return p.parseSpec()
}

type parser struct {
	toks []token
	pos  int
	// jcol is the column of the innermost junction list being parsed (0 = none).
	// A bullet at the start of a line closes every list indented at or to the
	// right of it; see token.col/bol and parseJunction.
	jcol int
}

// bulletEndsList reports whether the current token is a junction bullet that
// belongs to an ENCLOSING list rather than the expression being parsed. Only a
// token that begins its line can close a list; an infix `\/` mid-line, or one
// continuing an item on the next line but indented past the list's column, is
// part of the current expression.
func (p *parser) bulletEndsList() bool {
	t := p.cur()
	if t.kind != tAnd && t.kind != tOr {
		return false
	}
	return t.bol && p.jcol > 0 && t.col <= p.jcol
}

func (p *parser) cur() token  { return p.toks[p.pos] }
func (p *parser) peek() token { return p.toks[p.pos+1] }
func (p *parser) advance() token {
	t := p.toks[p.pos]
	p.pos++
	return t
}

func (p *parser) expect(k tokKind, what string) (token, error) {
	if p.cur().kind != k {
		return token{}, fmt.Errorf("line %d: expected %s, got %q",
			p.cur().line, what, p.cur().val)
	}
	return p.advance(), nil
}

func (p *parser) expectIdent(val string) error {
	t := p.cur()
	if t.kind != tIdent || t.val != val {
		return fmt.Errorf("line %d: expected %q, got %q", t.line, val, t.val)
	}
	p.advance()
	return nil
}

func (p *parser) isKW(val string) bool {
	return p.cur().kind == tIdent && p.cur().val == val
}

func (p *parser) parseSpec() (*Spec, error) {
	spec := &Spec{}

	// Skip leading module bar line: ---- MODULE Name ----
	if _, err := p.expect(tModuleBar, "----"); err != nil {
		return nil, err
	}
	if err := p.expectIdent("MODULE"); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(tIdent, "module name")
	if err != nil {
		return nil, err
	}
	spec.Name = nameTok.val
	// Optional trailing ----
	if p.cur().kind == tModuleBar {
		p.advance()
	}

	// Parse declarations until ==== (module end)
	for p.cur().kind != tModuleEq && p.cur().kind != tEOF {
		if err := p.parseDecl(spec); err != nil {
			return nil, err
		}
	}

	return spec, nil
}

func (p *parser) parseDecl(spec *Spec) error {
	switch {
	case p.isKW("EXTENDS"):
		p.advance()
		for {
			name, err := p.expect(tIdent, "module name")
			if err != nil {
				return err
			}
			spec.Extends = append(spec.Extends, name.val)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}

	case p.isKW("CONSTANTS"), p.isKW("CONSTANT"):
		p.advance()
		for {
			name, err := p.expect(tIdent, "constant name")
			if err != nil {
				return err
			}
			spec.Constants = append(spec.Constants, name.val)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}

	// `RECURSIVE Closure(_, _)` announces that a later definition may call
	// itself. conform resolves calls by name at eval time and needs nothing
	// from this, but codegen does: a recursive call must survive inlining (not
	// hit the depth bound) so it can become a Go function call. Record the names.
	case p.isKW("RECURSIVE"):
		p.advance()
		for p.cur().kind == tIdent {
			if spec.Recursive == nil {
				spec.Recursive = map[string]bool{}
			}
			spec.Recursive[p.cur().val] = true
			p.advance()
			if p.cur().kind == tLparen {
				for p.cur().kind != tRparen && p.cur().kind != tEOF {
					p.advance()
				}
				p.advance() // )
			}
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}

	case p.isKW("ASSUME"):
		// An ASSUME constrains CONSTANTS; it is not a definition and conform
		// never evaluates it. Skip its expression.
		p.advance()
		if _, err := p.parseExpr(); err != nil {
			p.skipToNextDecl()
		}

	case p.isKW("VARIABLES"), p.isKW("VARIABLE"):
		p.advance()
		for {
			name, err := p.expect(tIdent, "variable name")
			if err != nil {
				return err
			}
			spec.Variables = append(spec.Variables, name.val)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}

	default:
		// Definition: ident == expr, or ident(p1, p2, ...) == expr
		nameTok, err := p.expect(tIdent, "definition name")
		if err != nil {
			// Not even a name here — a stray token. Skip it and move on rather
			// than failing the whole spec.
			p.skipToNextDecl()
			return nil
		}

		var params []string
		if p.cur().kind == tLparen {
			p.advance()
			for p.cur().kind != tRparen {
				pt, err := p.expect(tIdent, "parameter name")
				if err != nil {
					p.recordUnsupported(spec, nameTok.val, "malformed parameter list: "+err.Error())
					p.skipToNextDecl()
					return nil
				}
				params = append(params, pt.val)
				if p.cur().kind == tComma {
					p.advance()
					continue
				}
				break
			}
			if _, err := p.expect(tRparen, ")"); err != nil {
				p.recordUnsupported(spec, nameTok.val, "malformed parameter list: "+err.Error())
				p.skipToNextDecl()
				return nil
			}
		}

		if p.cur().kind != tDefEq {
			p.recordUnsupported(spec, nameTok.val, "unexpected token after name: "+p.cur().val)
			p.skipToNextDecl()
			return nil
		}
		p.advance() // ==
		expr, perr := p.parseExpr()
		// A clean definition ends exactly at the next decl boundary. If parsing
		// errored, or left tokens dangling (an unmodeled operator mid-body), the
		// def is outside our subset — record it and resync.
		if perr != nil || !p.atDefBoundary() {
			reason := "unsupported operator"
			if perr != nil {
				reason = perr.Error()
			} else if p.cur().kind == tUnknown {
				reason = "unsupported operator " + p.cur().val
			}
			p.recordUnsupported(spec, nameTok.val, reason)
			p.skipToNextDecl()
			return nil
		}
		spec.Defs = append(spec.Defs, Def{Name: nameTok.val, Params: params, Body: expr})
	}
	return nil
}

func (p *parser) recordUnsupported(spec *Spec, name, reason string) {
	spec.Unsupported = append(spec.Unsupported, SkippedDef{Name: name, Reason: reason})
}

// atDeclStart reports whether the cursor is at the head of a new declaration:
// a section keyword, or an identifier introducing a definition (Name == or
// Name(params) ==).
func (p *parser) atDeclStart() bool {
	switch {
	case p.isKW("EXTENDS"), p.isKW("CONSTANTS"), p.isKW("CONSTANT"),
		p.isKW("VARIABLES"), p.isKW("VARIABLE"):
		return true
	case p.isKW("RECURSIVE"), p.isKW("ASSUME"):
		return true
	case p.cur().kind == tIdent:
		if p.peek().kind == tDefEq {
			return true
		}
		// `Name(` only starts a declaration when a `==` follows the closing
		// paren. Without this lookahead, resync mistakes a CALL for a definition
		// head — `WF_vars(DaemonExit)` and `~> (daemon = "gone")` both got
		// recorded as phantom defs named "WF_vars" and ">".
		if p.peek().kind == tLparen {
			return p.defEqFollowsParen(p.pos + 1)
		}
	}
	return false
}

// defEqFollowsParen reports whether the parenthesised group starting at toks[i]
// (which must be tLparen) is immediately followed by `==`.
func (p *parser) defEqFollowsParen(i int) bool {
	depth := 0
	for ; i < len(p.toks); i++ {
		switch p.toks[i].kind {
		case tLparen:
			depth++
		case tRparen:
			depth--
			if depth == 0 {
				return i+1 < len(p.toks) && p.toks[i+1].kind == tDefEq
			}
		case tEOF, tModuleEq:
			return false
		}
	}
	return false
}

// atDefBoundary reports whether a definition body has ended cleanly.
func (p *parser) atDefBoundary() bool {
	return p.cur().kind == tModuleEq || p.cur().kind == tEOF || p.atDeclStart()
}

// skipToNextDecl advances past the current (unparseable) region until the next
// declaration head or module end. It always advances at least one token, so a
// run of bad defs cannot loop.
func (p *parser) skipToNextDecl() {
	if p.cur().kind != tModuleEq && p.cur().kind != tEOF {
		p.advance()
	}
	for p.cur().kind != tModuleEq && p.cur().kind != tEOF && !p.atDeclStart() {
		p.advance()
	}
}

// --- Expression parsing (recursive descent, lowest precedence first) ---

func (p *parser) parseExpr() (Expr, error) {
	return p.parseImplies()
}

// `=>` is the weakest operator and right-associative: A => B => C is A => (B => C).
func (p *parser) parseImplies() (Expr, error) {
	left, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tImplies {
		p.advance()
		right, err := p.parseImplies()
		if err != nil {
			return nil, err
		}
		return BinOp{Op: "=>", Lhs: left, Rhs: right}, nil
	}
	if p.cur().kind == tLeadsTo {
		return nil, fmt.Errorf("line %d: temporal operator ~> (conform checks single transitions, not behaviors)", p.cur().line)
	}
	return left, nil
}

func (p *parser) parseOr() (Expr, error) {
	if p.cur().kind == tOr {
		return p.parseJunction(tOr, "\\/")
	}
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tOr && !p.bulletEndsList() {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = BinOp{Op: "\\/", Lhs: left, Rhs: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	if p.cur().kind == tAnd {
		return p.parseJunction(tAnd, "/\\")
	}
	left, err := p.parseEq()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tAnd && !p.bulletEndsList() {
		p.advance()
		right, err := p.parseEq()
		if err != nil {
			return nil, err
		}
		left = BinOp{Op: "/\\", Lhs: left, Rhs: right}
	}
	return left, nil
}

// parseJunction parses a bulleted junction list opened by the bullet under the
// cursor. Items are collected while a bullet of the same kind appears at
// EXACTLY the opening column; the first bullet further left ends the list and
// is left for the caller. Each item is parsed with jcol set to the list's
// column, so a nested list (indented further right) parses as its own list and
// an outer bullet terminates the item.
func (p *parser) parseJunction(kind tokKind, op string) (Expr, error) {
	col := p.cur().col
	outer := p.jcol
	p.jcol = col
	defer func() { p.jcol = outer }()

	var items []Expr
	for {
		p.advance() // the bullet
		item, err := p.parseImplies()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		// Continue only on a bullet of the same kind opening a line at this
		// exact column. An infix operator (same line) was already consumed by
		// the item's own parse.
		t := p.cur()
		if t.kind != kind || !t.bol || t.col != col {
			break
		}
	}

	res := items[0]
	for _, it := range items[1:] {
		res = BinOp{Op: op, Lhs: res, Rhs: it}
	}
	return res, nil
}

func (p *parser) parseEq() (Expr, error) {
	left, err := p.parseRel()
	if err != nil {
		return nil, err
	}
	switch p.cur().kind {
	case tEq:
		p.advance()
		right, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		return BinOp{Op: "=", Lhs: left, Rhs: right}, nil
	case tNeq:
		p.advance()
		right, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		return BinOp{Op: "/=", Lhs: left, Rhs: right}, nil
	case tIn:
		p.advance()
		right, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		return BinOp{Op: "\\in", Lhs: left, Rhs: right}, nil
	case tNotIn:
		p.advance()
		right, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		return BinOp{Op: "\\notin", Lhs: left, Rhs: right}, nil
	}
	return left, nil
}

func (p *parser) parseRel() (Expr, error) {
	left, err := p.parseSetBin()
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tSetOp {
		switch p.cur().val {
		case "\\subseteq", "\\subset", "\\supseteq":
			op := p.advance().val
			right, err := p.parseSetBin()
			if err != nil {
				return nil, err
			}
			return BinOp{Op: op, Lhs: left, Rhs: right}, nil
		}
	}
	if p.cur().kind == tIdent {
		switch p.cur().val {
		case ">", "<", ">=", "<=":
			op := p.advance().val
			right, err := p.parseSetBin()
			if err != nil {
				return nil, err
			}
			return BinOp{Op: op, Lhs: left, Rhs: right}, nil
		}
	}
	return left, nil
}

// parseSetBin handles the left-associative set builders: union, intersection
// and difference. `open \ {c}` is set difference, not a stray backslash.
func (p *parser) parseSetBin() (Expr, error) {
	left, err := p.parseRange()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tSetOp {
		switch p.cur().val {
		case "\\cup", "\\cap", "\\":
			op := p.advance().val
			right, err := p.parseRange()
			if err != nil {
				return nil, err
			}
			left = BinOp{Op: op, Lhs: left, Rhs: right}
		default:
			return left, nil
		}
	}
	return left, nil
}

func (p *parser) parseRange() (Expr, error) {
	left, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	if p.cur().kind == tRange {
		p.advance()
		right, err := p.parseAdd()
		if err != nil {
			return nil, err
		}
		return RangeLit{Lo: left, Hi: right}, nil
	}
	return left, nil
}

func (p *parser) parseAdd() (Expr, error) {
	left, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for {
		switch p.cur().kind {
		case tPlus:
			p.advance()
			right, err := p.parseMul()
			if err != nil {
				return nil, err
			}
			left = BinOp{Op: "+", Lhs: left, Rhs: right}
		case tMinus:
			p.advance()
			right, err := p.parseMul()
			if err != nil {
				return nil, err
			}
			left = BinOp{Op: "-", Lhs: left, Rhs: right}
		default:
			return left, nil
		}
	}
}

func (p *parser) parseMul() (Expr, error) {
	left, err := p.parseCart()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tStar {
		p.advance()
		right, err := p.parseCart()
		if err != nil {
			return nil, err
		}
		left = BinOp{Op: "*", Lhs: left, Rhs: right}
	}
	return left, nil
}

// parseCart handles `S \X T`, the set of pairs. `edges \subseteq (Sessions \X Sessions)`.
func (p *parser) parseCart() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur().kind == tSetOp && (p.cur().val == "\\X" || p.cur().val == "\\div") {
		op := p.advance().val
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = BinOp{Op: op, Lhs: left, Rhs: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.cur().kind == tNot {
		p.advance()
		operand, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return UnaryOp{Op: "~", Operand: operand}, nil
	}
	return p.parsePostfix()
}

func (p *parser) parsePostfix() (Expr, error) {
	e, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		switch p.cur().kind {
		case tPrime: // x'
			p.advance()
			vr, ok := e.(VarRef)
			if !ok {
				return nil, fmt.Errorf("line %d: prime applied to non-variable", p.cur().line)
			}
			e = PrimedVar{Name: vr.Name}
		case tLbracket: // function application: pc[o]
			// `[]` is never an application: it separates CASE arms and spells
			// the temporal box. Leave it for the enclosing parser.
			if p.peek().kind == tRbracket {
				return e, nil
			}
			p.advance()
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tRbracket, "]"); err != nil {
				return nil, err
			}
			e = FuncApply{Fn: e, Arg: arg}
		default:
			return e, nil
		}
	}
}

func (p *parser) parsePrimary() (Expr, error) {
	t := p.cur()

	// Keywords that are identifiers
	if t.kind == tIdent {
		switch t.val {
		case "TRUE":
			p.advance()
			return BoolLit{Val: true}, nil
		case "FALSE":
			p.advance()
			return BoolLit{Val: false}, nil
		case "BOOLEAN":
			p.advance()
			return BoolSet{}, nil
		case "Int":
			p.advance()
			return IntSet{}, nil
		case "Nat":
			p.advance()
			return IntSet{}, nil
		case "IF":
			return p.parseIf()
		case "CASE":
			return p.parseCase()
		case "UNCHANGED":
			p.advance()
			return p.parseUnchanged()
		case "LET":
			return p.parseLet()
		case "CHOOSE":
			return p.parseChoose()
		case "DOMAIN":
			return p.parseDomain()
		}
		// Regular identifier — but check if it's a known keyword that shouldn't be consumed
		switch t.val {
		case "EXTENDS", "VARIABLES", "VARIABLE", "CONSTANTS", "CONSTANT", "MODULE":
			return nil, fmt.Errorf("line %d: unexpected %q", t.line, t.val)
		}
		p.advance()
		if p.cur().kind == tLparen {
			return p.parseCallArgs(t.val)
		}
		return VarRef{Name: t.val}, nil
	}

	switch t.kind {
	case tAt:
		p.advance()
		return AtExpr{}, nil

	case tExists:
		return p.parseQuant(false)

	case tForall:
		return p.parseQuant(true)

	case tInt:
		p.advance()
		v, err := strconv.ParseInt(t.val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: bad integer %q", t.line, t.val)
		}
		return IntLit{Val: v}, nil

	case tString:
		p.advance()
		return StrLit{Val: t.val}, nil

	case tLparen:
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRparen, ")"); err != nil {
			return nil, err
		}
		return e, nil

	case tLbracket:
		return p.parseBracket()

	case tLbrace:
		return p.parseBrace()

	case tLt2:
		p.advance()
		var elems []Expr
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			elems = append(elems, e)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tGt2, ">>"); err != nil {
			return nil, err
		}
		return TupleLit{Elems: elems}, nil
	}

	return nil, fmt.Errorf("line %d: unexpected token %q", t.line, t.val)
}

// parseCallArgs parses the (arg1, arg2, ...) following an operator name into
// a Call. Cursor is at the opening '(' on entry.
func (p *parser) parseCallArgs(name string) (Expr, error) {
	p.advance() // (
	var args []Expr
	if p.cur().kind != tRparen {
		for {
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, e)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(tRparen, ")"); err != nil {
		return nil, err
	}
	return Call{Name: name, Args: args}, nil
}

// parseCase parses `CASE c1 -> e1 [] c2 -> e2 [] ... [] OTHER -> eN`.
// Cursor is at CASE on entry. "[]" is two adjacent tLbracket/tRbracket
// tokens (TLA+ renders it as a box character); OTHER is a plain identifier
// recognized positionally, not a keyword token.
func (p *parser) parseCase() (Expr, error) {
	p.advance() // CASE
	var arms []CaseArm
	var other Expr

	for {
		if p.isKW("OTHER") {
			p.advance()
			if _, err := p.expect(tCaseArrow, "->"); err != nil {
				return nil, err
			}
			o, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			other = o
			break
		}

		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tCaseArrow, "->"); err != nil {
			return nil, err
		}
		result, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		arms = append(arms, CaseArm{Cond: cond, Result: result})

		if p.cur().kind == tLbracket && p.peek().kind == tRbracket {
			p.advance()
			p.advance()
			continue
		}
		break
	}

	return CaseExpr{Arms: arms, Other: other}, nil
}

// parseQuant parses `\E x \in S : Body` / `\A x \in S : Body`, including the
// multi-binding form `\E h \in Hosts, t \in Sessions : Body`, which desugars
// into nested quantifiers (right to left) so the AST stays single-binding.
// Cursor is at the \E/\A token on entry.
func (p *parser) parseQuant(isForall bool) (Expr, error) {
	p.advance() // \E or \A

	type binding struct {
		name   string
		domain Expr
	}
	var binds []binding
	for {
		varTok, err := p.expect(tIdent, "quantifier variable")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tIn, "\\in"); err != nil {
			return nil, err
		}
		// The domain must not swallow the `,` separating bindings, and `\in`
		// binds tighter than the comma — parseRel is the right level.
		domain, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		binds = append(binds, binding{varTok.val, domain})
		if p.cur().kind != tComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tColon, ":"); err != nil {
		return nil, err
	}
	body, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	for i := len(binds) - 1; i >= 0; i-- {
		if isForall {
			body = ForallExpr{Var: binds[i].name, Domain: binds[i].domain, Body: body}
		} else {
			body = ExistsExpr{Var: binds[i].name, Domain: binds[i].domain, Body: body}
		}
	}
	return body, nil
}

// parseLet parses `LET a == e1  b(x) == e2  IN body`.
func (p *parser) parseLet() (Expr, error) {
	p.advance() // LET
	var defs []Def
	for !p.isKW("IN") {
		nameTok, err := p.expect(tIdent, "LET definition name")
		if err != nil {
			return nil, err
		}
		var params []string
		if p.cur().kind == tLparen {
			p.advance()
			for p.cur().kind != tRparen {
				pt, err := p.expect(tIdent, "parameter name")
				if err != nil {
					return nil, err
				}
				params = append(params, pt.val)
				if p.cur().kind == tComma {
					p.advance()
				}
			}
			p.advance() // )
		}
		if _, err := p.expect(tDefEq, "=="); err != nil {
			return nil, err
		}
		// A LET body is delimited by the next name or IN, not by indentation.
		outer := p.jcol
		p.jcol = 0
		body, err := p.parseExpr()
		p.jcol = outer
		if err != nil {
			return nil, err
		}
		defs = append(defs, Def{Name: nameTok.val, Params: params, Body: body})
	}
	p.advance() // IN
	body, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return LetExpr{Defs: defs, Body: body}, nil
}

// parseDomain parses `DOMAIN f` — a prefix operator binding as tightly as
// function application, so `DOMAIN f[a]` is `DOMAIN (f[a])`. Cursor is at
// DOMAIN on entry.
func (p *parser) parseDomain() (Expr, error) {
	p.advance() // DOMAIN
	operand, err := p.parsePostfix()
	if err != nil {
		return nil, err
	}
	return DomainExpr{Of: operand}, nil
}

// parseChoose parses `CHOOSE x \in S : P`.
func (p *parser) parseChoose() (Expr, error) {
	p.advance() // CHOOSE
	varTok, err := p.expect(tIdent, "CHOOSE variable")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tIn, "\\in"); err != nil {
		return nil, err
	}
	domain, err := p.parseRel()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tColon, ":"); err != nil {
		return nil, err
	}
	pred, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return ChooseExpr{Var: varTok.val, Domain: domain, Pred: pred}, nil
}

// desugarExceptIndices turns `![i1][i2]...[iN] = v` (fn is the function the
// EXCEPT clause as a whole applies to) into the single-index ExceptUpdate
// shape codegen already handles: `![i1] = [fn[i1] EXCEPT ![i2] = ...]`,
// bottoming out at `![iN] = v`. len(indices) == 1 is the plain, already-legal
// form and returns unchanged.
func desugarExceptIndices(fn Expr, indices []Expr, val Expr) ExceptUpdate {
	if len(indices) == 1 {
		return ExceptUpdate{Index: indices[0], Val: val}
	}
	inner := FuncApply{Fn: fn, Arg: indices[0]}
	nested := desugarExceptIndices(inner, indices[1:], val)
	return ExceptUpdate{Index: indices[0], Val: ExceptExpr{Fn: inner, Updates: []ExceptUpdate{nested}}}
}

// parseBracket parses the four `[`-headed forms:
//
//	[]              temporal box  -> rejected (conform checks transitions)
//	[S -> T]        FuncSet
//	[x \in S |-> e] FuncLit
//	[f EXCEPT ![i] = v, ...]  ExceptExpr
func (p *parser) parseBracket() (Expr, error) {
	line := p.cur().line
	p.advance() // [

	if p.cur().kind == tRbracket {
		return nil, fmt.Errorf("line %d: temporal operator [] (conform checks single transitions, not behaviors)", line)
	}

	// [x \in S |-> e] — a binder, so the variable name comes first.
	if p.cur().kind == tIdent && p.peek().kind == tIn {
		name := p.advance().val
		p.advance() // \in
		domain, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tArrow, "|->"); err != nil {
			return nil, err
		}
		body, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRbracket, "]"); err != nil {
			return nil, err
		}
		return FuncLit{Var: name, Domain: domain, Body: body}, nil
	}

	first, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	switch {
	case p.isKW("EXCEPT"):
		p.advance()
		var ups []ExceptUpdate
		for {
			if _, err := p.expect(tBang, "!"); err != nil {
				return nil, err
			}
			// One or more `[idx]` brackets before `=`. A single bracket is the
			// plain `![i] = v` form; two or more is the `![a][b] = v` shorthand
			// for `![a] = [f[a] EXCEPT ![b] = v]` — desugared here rather than
			// in codegen, so every downstream consumer only ever sees the
			// single-index ExceptUpdate shape.
			var indices []Expr
			for {
				if _, err := p.expect(tLbracket, "["); err != nil {
					return nil, err
				}
				idx, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(tRbracket, "]"); err != nil {
					return nil, err
				}
				indices = append(indices, idx)
				if p.cur().kind != tLbracket {
					break
				}
			}
			if _, err := p.expect(tEq, "="); err != nil {
				return nil, err
			}
			val, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			ups = append(ups, desugarExceptIndices(first, indices, val))
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tRbracket, "]"); err != nil {
			return nil, err
		}
		return ExceptExpr{Fn: first, Updates: ups}, nil

	case p.cur().kind == tCaseArrow: // [S -> T]
		p.advance()
		rng, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRbracket, "]"); err != nil {
			return nil, err
		}
		return FuncSet{Domain: first, Range: rng}, nil
	}

	return nil, fmt.Errorf("line %d: unsupported [...] form", line)
}

// parseBrace parses `{}`, `{a, b}`, `{x \in S : P}` and `{e : x \in S}`.
func (p *parser) parseBrace() (Expr, error) {
	p.advance() // {

	if p.cur().kind == tRbrace {
		p.advance()
		return SetLit{}, nil
	}

	// {x \in S : P} — a binder, recognised before parsing an expression.
	if p.cur().kind == tIdent && p.peek().kind == tIn {
		name := p.advance().val
		p.advance() // \in
		domain, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		if p.cur().kind == tColon {
			p.advance()
			pred, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tRbrace, "}"); err != nil {
				return nil, err
			}
			return SetCompr{Var: name, Domain: domain, Pred: pred}, nil
		}
		// Not a comprehension after all: `{x \in S}` is a singleton whose only
		// element is the boolean `x \in S`.
		elems := []Expr{BinOp{Op: "\\in", Lhs: VarRef{Name: name}, Rhs: domain}}
		for p.cur().kind == tComma {
			p.advance()
			e, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			elems = append(elems, e)
		}
		if _, err := p.expect(tRbrace, "}"); err != nil {
			return nil, err
		}
		return SetLit{Elems: elems}, nil
	}

	first, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	// {e : x \in S} — the image of S under e.
	if p.cur().kind == tColon {
		p.advance()
		nameTok, err := p.expect(tIdent, "set-map variable")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tIn, "\\in"); err != nil {
			return nil, err
		}
		domain, err := p.parseRel()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRbrace, "}"); err != nil {
			return nil, err
		}
		return SetMap{Body: first, Var: nameTok.val, Domain: domain}, nil
	}

	elems := []Expr{first}
	for p.cur().kind == tComma {
		p.advance()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		elems = append(elems, e)
	}
	if _, err := p.expect(tRbrace, "}"); err != nil {
		return nil, err
	}
	return SetLit{Elems: elems}, nil
}

func (p *parser) parseIf() (Expr, error) {
	p.advance() // IF
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectIdent("THEN"); err != nil {
		return nil, err
	}
	thenE, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectIdent("ELSE"); err != nil {
		return nil, err
	}
	elseE, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return IfExpr{Cond: cond, Then: thenE, Else: elseE}, nil
}

func (p *parser) parseUnchanged() (Expr, error) {
	// UNCHANGED <<x, y>> or UNCHANGED x or UNCHANGED vars
	if p.cur().kind == tLt2 {
		// tuple of vars
		p.advance()
		var names []string
		for {
			if p.cur().kind != tIdent {
				return nil, fmt.Errorf("line %d: expected variable in UNCHANGED", p.cur().line)
			}
			names = append(names, p.advance().val)
			if p.cur().kind != tComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tGt2, ">>"); err != nil {
			return nil, err
		}
		// Represent as a special marker: UnchangedVars
		return UnchangedVars{Names: names}, nil
	}
	if p.cur().kind == tIdent {
		name := p.advance().val
		return UnchangedVars{Names: []string{name}}, nil
	}
	return nil, fmt.Errorf("line %d: expected variable after UNCHANGED", p.cur().line)
}

// --- Action extraction ---

// ExtractActions finds the Next definition, identifies action names from its
// disjunction, then for each action splits the conjunction into guard (unprimed)
// and updates (primed assignments).
func ExtractActions(spec *Spec) ([]Action, error) {
	nextDef := spec.FindDef("Next")
	if nextDef == nil {
		return nil, fmt.Errorf("no Next definition")
	}
	actionNames := collectDisjunctionNames(nextDef)

	var actions []Action
	for _, name := range actionNames {
		actDef := spec.FindDef(name)
		if actDef == nil {
			return nil, fmt.Errorf("Next references undefined action %q", name)
		}
		// Inline first: an action may reference a nullary helper formula
		// (e.g. `Idle` as a bare conjunct) or a parameterised one. splitAction
		// only understands primed-eq/UNCHANGED conjuncts, so any operator
		// reference must be expanded before it runs.
		inlined, err := Inline(spec, actDef)
		if err != nil {
			return nil, fmt.Errorf("action %s: %w", name, err)
		}
		act, err := splitAction(name, inlined)
		if err != nil {
			return nil, fmt.Errorf("action %s: %w", name, err)
		}
		actions = append(actions, act)
	}
	return actions, nil
}

// FindDef returns the body of the named top-level definition, or nil.
func (s *Spec) FindDef(name string) Expr {
	for _, d := range s.Defs {
		if d.Name == name {
			return d.Body
		}
	}
	return nil
}

// FindOp returns the named top-level definition itself (params included), or
// nil. FindDef hands back only the body, which cannot distinguish a missing
// definition from one whose body is nil.
func (s *Spec) FindOp(name string) *Def {
	for i := range s.Defs {
		if s.Defs[i].Name == name {
			return &s.Defs[i]
		}
	}
	return nil
}

// collectDisjunctionNames flattens A \/ B \/ C into ["A", "B", "C"].
// Non-name branches are returned as-is (will error in ExtractActions).
func collectDisjunctionNames(e Expr) []string {
	if bin, ok := e.(BinOp); ok && bin.Op == "\\/" {
		return append(collectDisjunctionNames(bin.Lhs), collectDisjunctionNames(bin.Rhs)...)
	}
	if vr, ok := e.(VarRef); ok {
		return []string{vr.Name}
	}
	return []string{"<expr>"} // not a simple name reference
}

// splitAction separates an action body into guard + updates.
// A guard is any conjunct that is purely unprimed (no primed vars).
// An update is a conjunct of the form var' = expr.
func splitAction(name string, body Expr) (Action, error) {
	act := Action{Name: name}

	// A worklist so an inner `\E h \in D : <body with primed vars>` can be
	// flattened in place: we lift h to an action parameter and feed the inner
	// body's conjuncts back through the same classification. Nested binders
	// (`\E h,t` desugars to nested ExistsExpr) unwind one level per pass.
	work := FlattenConjunction(body)
	for len(work) > 0 {
		c := work[0]
		work = work[1:]
		switch {
		case IsPrimedEq(c):
			bin := c.(BinOp)
			pv := bin.Lhs.(PrimedVar)
			act.Update = append(act.Update, Assign{Var: pv.Name, Val: bin.Rhs})
		case IsUnchanged(c):
			uv := c.(UnchangedVars)
			for _, v := range uv.Names {
				act.Update = append(act.Update, Assign{Var: v, Val: VarRef{Name: v}}) // x' = x
			}
		case isPrimedExists(c):
			ex := c.(ExistsExpr)
			act.Params = append(act.Params, ex.Var)
			act.ParamDomains = append(act.ParamDomains, ex.Domain)
			// Splice the quantified body in front of the remaining conjuncts.
			work = append(FlattenConjunction(ex.Body), work...)
		default:
			if act.Guard == nil {
				act.Guard = c
			} else {
				act.Guard = BinOp{Op: "/\\", Lhs: act.Guard, Rhs: c}
			}
		}
	}

	// Fill in UNCHANGED for variables not explicitly assigned
	// (caller will add them based on spec.Variables)

	if act.Guard == nil {
		act.Guard = BoolLit{Val: true} // always enabled
	}
	return act, nil
}

// FlattenConjunction splits `a /\ b /\ c` into [a, b, c]. Shared by specgen's
// action-splitting and CASE-arm codegen, and by conform's interpreter, so the
// two agree on how a conjunction decomposes.
func FlattenConjunction(e Expr) []Expr {
	if bin, ok := e.(BinOp); ok && bin.Op == "/\\" {
		return append(FlattenConjunction(bin.Lhs), FlattenConjunction(bin.Rhs)...)
	}
	return []Expr{e}
}

// IsPrimedEq reports whether e is `var' = expr`.
func IsPrimedEq(e Expr) bool {
	bin, ok := e.(BinOp)
	if !ok || bin.Op != "=" {
		return false
	}
	_, ok = bin.Lhs.(PrimedVar)
	return ok
}

// IsUnchanged reports whether e is an UNCHANGED marker.
func IsUnchanged(e Expr) bool {
	_, ok := e.(UnchangedVars)
	return ok
}

// isPrimedExists reports whether e is `\E v \in D : Body` whose Body assigns a
// primed variable — a quantifier that chooses the value of a transition rather
// than an ordinary guard. splitAction lifts its binder to an action parameter.
// A quantifier with no primed var in its body (a real precondition, e.g.
// `\E s \in owners : s.active`) is left as a guard.
func isPrimedExists(e Expr) bool {
	ex, ok := e.(ExistsExpr)
	return ok && ContainsPrimedVar(ex.Body)
}

// ContainsPrimedVar reports whether e contains any PrimedVar anywhere — through
// quantifier bodies (\E/\A), EXCEPT updates, CASE arms, and every other node.
// It walks exhaustively so a primed assignment nested inside `\E h,t : ...`
// (which desugars to nested ExistsExpr) is still found; a hand-rolled switch
// that forgot the ExistsExpr case would miss it and leave the binder unlifted.
func ContainsPrimedVar(e Expr) bool {
	found := false
	Walk(e, func(n Expr) bool {
		if _, ok := n.(PrimedVar); ok {
			found = true
		}
		return true
	})
	return found
}

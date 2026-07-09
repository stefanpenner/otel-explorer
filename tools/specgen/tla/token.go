package tla

import (
	"fmt"
	"strings"
	"unicode"
)

// --- Token ---

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tInt
	tString
	tModuleBar // ----
	tModuleEq  // ====
	tDefEq     // ==
	tAnd       // /\
	tOr        // \/
	tNot       // ~
	tEq        // =
	tNeq       // /=
	tIn        // \in
	tNotIn     // \notin
	tPrime     // '
	tRange     // ..
	tLt2       // <<
	tGt2       // >>
	tArrow     // |->
	tLparen    // (
	tRparen    // )
	tLbrace    // {
	tRbrace    // }
	tLbracket  // [
	tRbracket  // ]
	tComma     // ,
	tDot       // .
	tColon     // :
	tPlus      // +
	tMinus     // -
	tStar      // *
	tSlash     // /
	tCaseArrow // -> (CASE's arrow; distinct from |-> / tArrow)
	tExists    // \E
	tForall    // \A
	tImplies   // =>
	tLeadsTo   // ~> (temporal; parsed only to reject the def cleanly)
	tBang      // ! (EXCEPT's field selector)
	tAt        // @ (EXCEPT's "old value")
	tSetOp     // \cup \cap \ \subseteq \subset \X ... (val holds the operator)
	// tUnknown carries an operator/char the lexer does not model (e.g. \cup,
	// \subseteq, |). It never fails tokenization; the parser skips any def that
	// contains one. val holds the raw text, for reporting.
	tUnknown
)

type token struct {
	kind tokKind
	val  string
	line int
	// col is the 1-based column the token starts at, and bol reports whether it
	// is the FIRST token on its line. TLA+ junction lists are delimited by
	// indentation, not punctuation: a `/\` bullet at column c continues the list
	// opened at column c, while one at a smaller column closes it. Without this,
	//   Next == \/ \E c \in Conns : Connect(c) \/ Register(c)
	//           \/ ApplyMark
	// parses as \E c : (Connect(c) \/ Register(c) \/ ApplyMark) — the sibling
	// disjunct silently swallowed into the quantifier.
	col int
	bol bool
}

// --- Tokenizer ---

type lexer struct {
	src       string
	pos       int
	line      int
	lineStart int // byte offset of the current line's first character
}

func tokenize(src string) ([]token, error) {
	l := &lexer{src: src, line: 1}
	var toks []token
	lastLine := 0
	for {
		l.skipWS()
		if l.pos >= len(l.src) {
			break
		}
		startLine, startCol := l.line, l.pos-l.lineStart+1
		tok, err := l.next()
		if err != nil {
			return nil, err
		}
		tok.col = startCol
		tok.bol = startLine != lastLine
		lastLine = startLine
		toks = append(toks, tok)
	}
	toks = append(toks, token{kind: tEOF, line: l.line, col: 1, bol: true})
	return toks, nil
}

func (l *lexer) skipWS() {
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		// whitespace
		if c == ' ' || c == '\t' || c == '\r' {
			l.pos++
			continue
		}
		if c == '\n' {
			l.line++
			l.pos++
			l.lineStart = l.pos
			continue
		}
		// line comment: \*
		if c == '\\' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '*' {
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		// block comment: (* ... *)
		if c == '(' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '*' {
			l.pos += 2
			depth := 1
			for l.pos < len(l.src) && depth > 0 {
				if l.src[l.pos] == '(' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '*' {
					depth++
					l.pos += 2
				} else if l.src[l.pos] == '*' && l.pos+1 < len(l.src) && l.src[l.pos+1] == ')' {
					depth--
					l.pos += 2
				} else {
					if l.src[l.pos] == '\n' {
						l.line++
						l.lineStart = l.pos + 1
					}
					l.pos++
				}
			}
			continue
		}
		break
	}
}

// backslashOps maps TLA+'s \-spelled operators onto a canonical form the
// parser and evaluator agree on. Order matters: longest first, so \subseteq is
// never split into \subset + "eq". matchWord enforces a word boundary, so
// \cup never matches the head of a longer name.
var backslashOps = []struct{ word, canon string }{
	{"subseteq", "\\subseteq"},
	{"supseteq", "\\supseteq"},
	{"subset", "\\subset"},
	{"union", "\\cup"},
	{"times", "\\X"},
	{"cup", "\\cup"},
	{"cap", "\\cap"},
	{"div", "\\div"},
	{"X", "\\X"},
	{"o", "\\o"},
}

// matchWord reports whether rest begins with word, not followed by an
// identifier character (so `\cup` matches but `\cupola` does not).
func matchWord(rest, word string) bool {
	return strings.HasPrefix(rest, word) && !isIdentContAt(rest, len(word))
}

func isIdentStart(c byte) bool {
	return unicode.IsLetter(rune(c)) || c == '_'
}
func isIdentCont(c byte) bool {
	return unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c)) || c == '_'
}

// isIdentContAt reports whether s[i] is an identifier-continuation character,
// false if i is out of range. Used to make sure `\E`/`\A` only match the
// quantifier when not actually the start of a longer \-word (e.g. a
// hypothetical `\Excl`), which instead falls through to tUnknown.
func isIdentContAt(s string, i int) bool {
	if i >= len(s) {
		return false
	}
	return isIdentCont(s[i])
}

func (l *lexer) next() (token, error) {
	startLine := l.line
	c := l.src[l.pos]

	// 4+ dashes: module bar
	if c == '-' && l.peekRun('-', 4) {
		l.pos += l.runLen('-')
		return token{kind: tModuleBar, line: startLine}, nil
	}
	// 4+ equals: module end (but == is defEq, check 4+)
	if c == '=' && l.peekRun('=', 4) {
		l.pos += l.runLen('=')
		return token{kind: tModuleEq, line: startLine}, nil
	}

	switch c {
	case '=':
		l.pos++
		if l.pos < len(l.src) {
			switch l.src[l.pos] {
			case '=':
				l.pos++
				return token{kind: tDefEq, line: startLine}, nil
			case '>':
				l.pos++
				return token{kind: tImplies, line: startLine}, nil
			case '<': // TLA+ spells <= either way
				l.pos++
				return token{kind: tIdent, val: "<=", line: startLine}, nil
			}
		}
		return token{kind: tEq, line: startLine}, nil

	case '/':
		if l.pos+1 < len(l.src) {
			switch l.src[l.pos+1] {
			case '\\':
				l.pos += 2
				return token{kind: tAnd, line: startLine}, nil
			case '=':
				l.pos += 2
				return token{kind: tNeq, line: startLine}, nil
			}
		}
		l.pos++
		return token{kind: tSlash, line: startLine}, nil

	case '\\':
		if l.pos+1 < len(l.src) {
			rest := l.src[l.pos+1:]
			switch {
			case strings.HasPrefix(rest, "/"):
				l.pos += 2
				return token{kind: tOr, line: startLine}, nil
			case strings.HasPrefix(rest, "notin"):
				l.pos += 6
				return token{kind: tNotIn, line: startLine}, nil
			// \intersect must be tried before \in — \in is a prefix of it.
			case matchWord(rest, "intersect"):
				l.pos += 10
				return token{kind: tSetOp, val: "\\cap", line: startLine}, nil
			case matchWord(rest, "in"):
				l.pos += 3
				return token{kind: tIn, line: startLine}, nil
			case strings.HasPrefix(rest, "E") && !isIdentContAt(rest, 1):
				l.pos += 2
				return token{kind: tExists, line: startLine}, nil
			case strings.HasPrefix(rest, "A") && !isIdentContAt(rest, 1):
				l.pos += 2
				return token{kind: tForall, line: startLine}, nil
			}
			// Set / arithmetic operators spelled as \-words. Longest match first,
			// so \subseteq is never lexed as \subset + "eq".
			for _, op := range backslashOps {
				if matchWord(rest, op.word) {
					l.pos += 1 + len(op.word)
					return token{kind: tSetOp, val: op.canon, line: startLine}, nil
				}
			}
		}
		// A lone backslash is set difference: `open \ {c}`.
		if l.pos+1 >= len(l.src) || !isIdentStart(l.src[l.pos+1]) {
			l.pos++
			return token{kind: tSetOp, val: "\\", line: startLine}, nil
		}
		// Unmodeled backslash operator. Consume the whole \word so resync isn't
		// confused by leftover letters.
		start := l.pos
		l.pos++ // the backslash
		for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
			l.pos++
		}
		return token{kind: tUnknown, val: l.src[start:l.pos], line: startLine}, nil

	case '!':
		l.pos++
		return token{kind: tBang, line: startLine}, nil

	case '@':
		l.pos++
		return token{kind: tAt, line: startLine}, nil

	case '#': // TLA+ alternate spelling of /=
		l.pos++
		return token{kind: tNeq, line: startLine}, nil

	case '~':
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '>' {
			l.pos += 2
			return token{kind: tLeadsTo, line: startLine}, nil
		}
		l.pos++
		return token{kind: tNot, line: startLine}, nil

	case '\'':
		l.pos++
		return token{kind: tPrime, line: startLine}, nil

	case '(':
		l.pos++
		return token{kind: tLparen, line: startLine}, nil
	case ')':
		l.pos++
		return token{kind: tRparen, line: startLine}, nil
	case '{':
		l.pos++
		return token{kind: tLbrace, line: startLine}, nil
	case '}':
		l.pos++
		return token{kind: tRbrace, line: startLine}, nil
	case '[':
		l.pos++
		return token{kind: tLbracket, line: startLine}, nil
	case ']':
		l.pos++
		return token{kind: tRbracket, line: startLine}, nil
	case ',':
		l.pos++
		return token{kind: tComma, line: startLine}, nil
	case '.':
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '.' {
			l.pos += 2
			return token{kind: tRange, line: startLine}, nil
		}
		l.pos++
		return token{kind: tDot, line: startLine}, nil
	case ':':
		l.pos++
		return token{kind: tColon, line: startLine}, nil
	case '+':
		l.pos++
		return token{kind: tPlus, line: startLine}, nil
	case '-':
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '>' {
			l.pos += 2
			return token{kind: tCaseArrow, line: startLine}, nil
		}
		l.pos++
		return token{kind: tMinus, line: startLine}, nil
	case '*':
		l.pos++
		return token{kind: tStar, line: startLine}, nil

	// `<` and `>` stay tIdent (parseRel matches on their value), so `<=` and
	// `>=` must be lexed as single tIdent tokens too — otherwise `pending <= 4`
	// lexes as `<` then `=` and the whole definition is dropped.
	case '<':
		if l.pos+1 < len(l.src) {
			switch l.src[l.pos+1] {
			case '<':
				l.pos += 2
				return token{kind: tLt2, line: startLine}, nil
			case '=':
				l.pos += 2
				return token{kind: tIdent, val: "<=", line: startLine}, nil
			}
		}
		l.pos++
		return token{kind: tIdent, val: "<", line: startLine}, nil
	case '>':
		if l.pos+1 < len(l.src) {
			switch l.src[l.pos+1] {
			case '>':
				l.pos += 2
				return token{kind: tGt2, line: startLine}, nil
			case '=':
				l.pos += 2
				return token{kind: tIdent, val: ">=", line: startLine}, nil
			}
		}
		l.pos++
		return token{kind: tIdent, val: ">", line: startLine}, nil

	case '|':
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '-' && l.pos+2 < len(l.src) && l.src[l.pos+2] == '>' {
			l.pos += 3
			return token{kind: tArrow, line: startLine}, nil
		}
		return token{}, fmt.Errorf("line %d: unexpected |", startLine)

	case '"':
		l.pos++
		var sb strings.Builder
		for l.pos < len(l.src) && l.src[l.pos] != '"' {
			sb.WriteByte(l.src[l.pos])
			l.pos++
		}
		l.pos++ // closing "
		return token{kind: tString, val: sb.String(), line: startLine}, nil
	}

	// integer
	if c >= '0' && c <= '9' {
		start := l.pos
		for l.pos < len(l.src) && l.src[l.pos] >= '0' && l.src[l.pos] <= '9' {
			l.pos++
		}
		return token{kind: tInt, val: l.src[start:l.pos], line: startLine}, nil
	}

	// identifier / keyword
	if isIdentStart(c) {
		start := l.pos
		l.pos++
		for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
			l.pos++
		}
		return token{kind: tIdent, val: l.src[start:l.pos], line: startLine}, nil
	}

	// Anything else (e.g. EXCEPT's '!', or a stray symbol) becomes tUnknown so
	// the parser can skip the enclosing def rather than failing the whole spec.
	l.pos++
	return token{kind: tUnknown, val: string(c), line: startLine}, nil
}

func (l *lexer) peekRun(c byte, min int) bool {
	count := 0
	for i := l.pos; i < len(l.src) && l.src[i] == c; i++ {
		count++
	}
	return count >= min
}

func (l *lexer) runLen(c byte) int {
	n := 0
	for l.pos+n < len(l.src) && l.src[l.pos+n] == c {
		n++
	}
	return n
}

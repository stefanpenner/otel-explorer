package otlpfile

import (
	"bytes"
	"testing"
)

// These harnesses hit each sub-parser directly, bypassing Parse's content
// sniffing so the fuzzer spends its whole budget inside one parser's decode +
// span-conversion logic (time math, ID handling, attribute coercion, stack
// nesting) instead of mostly exercising the detector. Contract is the same:
// never panic on arbitrary input.

func fuzzReader(f *testing.F, seeds []string, parse func([]byte) error) {
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_ = parse(data)
	})
}

func FuzzParseChrome(f *testing.F) {
	fuzzReader(f, []string{
		`{"traceEvents":[]}`,
		`{"traceEvents":[{"ph":"X","name":"n","ts":0,"dur":1,"pid":1,"tid":1}]}`,
		`[{"ph":"B","name":"a","ts":0,"pid":1,"tid":1},{"ph":"E","ts":5,"pid":1,"tid":1}]`,
		`[{"ph":"E","ts":5,"pid":1,"tid":1}]`,                                 // unmatched End (empty-stack pop)
		`{"traceEvents":[{"ph":"X","ts":1e308,"dur":1e308,"pid":1,"tid":1}]}`, // float overflow into time.Unix
		`{"traceEvents":[{"ph":"X","ts":-1,"dur":-1,"pid":1,"tid":1}]}`,
	}, func(b []byte) error { _, err := ParseChrome(bytes.NewReader(b)); return err })
}

func FuzzParseJaeger(f *testing.F) {
	fuzzReader(f, []string{
		`{"data":[]}`,
		`{"data":[{"traceID":"abc","spans":[{"traceID":"abc","spanID":"def","operationName":"op","startTime":1,"duration":2}]}]}`,
		`{"data":[{"spans":[{"startTime":9223372036854775807,"duration":9223372036854775807}]}]}`, // int64 overflow in time math
		`{"data":[{"spans":[{"references":[{"refType":"CHILD_OF","spanID":"x"}]}]}]}`,
	}, func(b []byte) error { _, err := ParseJaeger(bytes.NewReader(b)); return err })
}

func FuzzParseZipkin(f *testing.F) {
	fuzzReader(f, []string{
		`[]`,
		`[{"traceId":"1","id":"2","name":"n","timestamp":1,"duration":2,"localEndpoint":{"serviceName":"s"}}]`,
		`[{"timestamp":9223372036854775807,"duration":9223372036854775807}]`, // overflow
		`[{"parentId":"x","id":"y","traceId":"z"}]`,
	}, func(b []byte) error { _, err := ParseZipkin(bytes.NewReader(b)); return err })
}

func FuzzParseProtobufRequest(f *testing.F) {
	fuzzReader(f, []string{
		"",
		"\x0a\x00",
		"\x0a\x05\x12\x03\x01\x02\x03",
	}, func(b []byte) error { _, err := ParseProtobufRequest(b); return err })
}

func FuzzParseProtoJSON(f *testing.F) {
	fuzzReader(f, []string{
		`{"resourceSpans":[]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}]}]}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"spans":[{"startTimeUnixNano":"99999999999999999999"}]}]}]}`, // unparseable uint64 string
	}, func(b []byte) error { _, err := parseProtoJSONL(b); return err })
}

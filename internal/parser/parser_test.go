package parser

import "testing"

func TestParseGraylogMessage(t *testing.T) {
	raw := `[2026-05-01T02:40:14.325][ERROR] [request_id=e0821c9f-205c-4c9f-b629-8a072fa12652] [tenant_id=e4b6db52-baaf-494c-b7cf-424d161251a0] [thread=.0-8080-exec-30] [class=o.s.transaction.interceptor.TransactionInterceptor ] [method=mpleteTransactionAfterThrowing] [version=v1] [traceId=9a5c8d111c125dc5d794ad8d281fe11c] [spanId=737c41b7c787f469] [originating_bi_id=- ] [business_identifiers=- ] Application exception overridden by rollback exception`

	parsed := ParseMessage(raw)

	if parsed.ParsedTimestamp != "2026-05-01T02:40:14.325" {
		t.Fatalf("unexpected timestamp: %q", parsed.ParsedTimestamp)
	}
	if parsed.Level != "ERROR" {
		t.Fatalf("unexpected level: %q", parsed.Level)
	}
	if got := parsed.PrefixFields["class"]; got != "o.s.transaction.interceptor.TransactionInterceptor" {
		t.Fatalf("unexpected class: %#v", got)
	}
	if got := parsed.PrefixFields["originating_bi_id"]; got != nil {
		t.Fatalf("expected nil placeholder, got %#v", got)
	}
	if parsed.MessageText != "Application exception overridden by rollback exception" {
		t.Fatalf("unexpected message text: %q", parsed.MessageText)
	}
}

func TestParseJSONMessage(t *testing.T) {
	raw := `{"message":"boom","http":{"status":500},"user":{"id":"u-1"},"success":false}`

	parsed := ParseMessage(raw)

	if parsed.MessageText != "boom" {
		t.Fatalf("unexpected message text: %q", parsed.MessageText)
	}
	if got := parsed.JSONFields["http__status"]; got != int64(500) {
		t.Fatalf("unexpected flattened JSON field: %#v", got)
	}
	if got := parsed.JSONFields["user__id"]; got != "u-1" {
		t.Fatalf("unexpected user id: %#v", got)
	}
	if got := parsed.JSONFields["success"]; got != false {
		t.Fatalf("unexpected bool field: %#v", got)
	}
}

func TestFingerprintIgnoresTimestampAndRequestID(t *testing.T) {
	first := map[string]any{
		"class":      "demo.Service",
		"method":     "DoThing",
		"request_id": "abc",
		"timestamp":  "2026-05-01T01:00:00Z",
	}
	second := map[string]any{
		"class":      "demo.Service",
		"method":     "DoThing",
		"request_id": "xyz",
		"timestamp":  "2026-05-02T01:00:00Z",
	}

	left := ComputeFingerprint("database failed", "ERROR", first)
	right := ComputeFingerprint("database failed", "ERROR", second)
	if left != right {
		t.Fatalf("expected matching fingerprints, got %q != %q", left, right)
	}
}

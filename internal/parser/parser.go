package parser

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var timestampTokenRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T`)

type ParsedRecord struct {
	ParsedTimestamp string
	Level           string
	MessageText     string
	PrefixFields    map[string]any
	JSONFields      map[string]any
	Fields          map[string]any
}

func ParseMessage(raw string) ParsedRecord {
	record := ParsedRecord{
		MessageText:  strings.TrimSpace(raw),
		PrefixFields: make(map[string]any),
		JSONFields:   make(map[string]any),
		Fields:       make(map[string]any),
	}

	if timestamp, level, prefixFields, remainder, ok := parseGraylogPrefix(raw); ok {
		record.ParsedTimestamp = timestamp
		record.Level = level
		record.PrefixFields = prefixFields
		record.MessageText = strings.TrimSpace(remainder)
	}

	jsonSource := strings.TrimSpace(record.MessageText)
	if jsonSource == "" {
		jsonSource = strings.TrimSpace(raw)
	}

	if flattened, messageText, ok := parseJSONObject(jsonSource); ok {
		record.JSONFields = flattened
		record.MessageText = messageText
	}

	for key, value := range record.PrefixFields {
		record.Fields[key] = value
	}
	for key, value := range record.JSONFields {
		if _, exists := record.Fields[key]; !exists {
			record.Fields[key] = value
		}
	}

	if record.MessageText == "" {
		record.MessageText = strings.TrimSpace(raw)
	}

	return record
}

func LooksLikeLogStart(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	_, _, _, _, ok := parseGraylogPrefix(trimmed)
	return ok
}

func NormalizeStringValue(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "-" {
		return nil
	}
	return trimmed
}

func ComputeFingerprint(messageText, level string, fields map[string]any) string {
	volatileKeys := map[string]struct{}{
		"parsed_timestamp":     {},
		"timestamp":            {},
		"request_id":           {},
		"traceid":              {},
		"trace_id":             {},
		"spanid":               {},
		"span_id":              {},
		"thread":               {},
		"pod":                  {},
		"container":            {},
		"filename":             {},
		"source_format":        {},
		"row_number":           {},
		"business_identifiers": {},
		"originating_bi_id":    {},
	}

	lines := []string{
		fmt.Sprintf("level=%s", strings.TrimSpace(level)),
		fmt.Sprintf("message=%s", strings.TrimSpace(messageText)),
	}

	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		normalizedKey := strings.ToLower(strings.TrimSpace(key))
		if _, skip := volatileKeys[normalizedKey]; skip {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s=%s", normalizedKey, stableValue(fields[key])))
	}

	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

func parseGraylogPrefix(raw string) (string, string, map[string]any, string, bool) {
	cursor := strings.TrimLeft(raw, " \t")
	timestampToken, ok, next := consumeBracketToken(cursor)
	if !ok || !timestampTokenRE.MatchString(strings.TrimSpace(timestampToken)) {
		return "", "", nil, raw, false
	}

	cursor = strings.TrimLeft(cursor[next:], " ")
	levelToken, ok, next := consumeBracketToken(cursor)
	if !ok {
		return "", "", nil, raw, false
	}

	level := strings.TrimSpace(levelToken)
	if level == "" {
		return "", "", nil, raw, false
	}

	cursor = strings.TrimLeft(cursor[next:], " ")
	fields := make(map[string]any)
	for {
		token, ok, next := consumeBracketToken(cursor)
		if !ok {
			break
		}

		key, value, hasKV := strings.Cut(token, "=")
		if !hasKV {
			break
		}
		fields[strings.TrimSpace(key)] = NormalizeStringValue(value)
		cursor = strings.TrimLeft(cursor[next:], " ")
	}

	return strings.TrimSpace(timestampToken), level, fields, cursor, true
}

func consumeBracketToken(input string) (string, bool, int) {
	if !strings.HasPrefix(input, "[") {
		return "", false, 0
	}
	end := strings.IndexByte(input, ']')
	if end <= 0 {
		return "", false, 0
	}
	return input[1:end], true, end + 1
}

func parseJSONObject(input string) (map[string]any, string, bool) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "{") {
		return nil, "", false
	}

	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()

	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, "", false
	}

	root, ok := payload.(map[string]any)
	if !ok {
		return nil, "", false
	}

	fields := make(map[string]any)
	flattenJSONMap(fields, "", root)

	messageText := ""
	for _, candidate := range []string{"message", "msg", "error"} {
		if value, exists := root[candidate]; exists {
			messageText = strings.TrimSpace(fmt.Sprint(value))
			break
		}
	}
	if messageText == "" {
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(input)); err == nil {
			messageText = compact.String()
		} else {
			messageText = input
		}
	}

	return fields, messageText, true
}

func flattenJSONMap(target map[string]any, prefix string, input map[string]any) {
	for key, value := range input {
		canonical := key
		if prefix != "" {
			canonical = prefix + "__" + key
		}

		switch typed := value.(type) {
		case map[string]any:
			flattenJSONMap(target, canonical, typed)
		case []any:
			bytes, _ := json.Marshal(typed)
			target[canonical] = string(bytes)
		case json.Number:
			if i64, err := typed.Int64(); err == nil {
				target[canonical] = i64
				continue
			}
			if f64, err := typed.Float64(); err == nil {
				target[canonical] = f64
				continue
			}
			target[canonical] = typed.String()
		case string:
			target[canonical] = NormalizeStringValue(typed)
		case nil:
			target[canonical] = nil
		default:
			target[canonical] = typed
		}
	}
}

func stableValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		bytes, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(bytes)
	}
}

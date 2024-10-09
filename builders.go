package k6avrogen

func toMap(schema any) map[string]any {
	return schema.(map[string]any)
}
func copyMap(source map[string]any) map[string]any {
	buf := make(map[string]any)
	for k, v := range source {
		buf[k] = v
	}
	return buf
}

func makeNullable(schema map[string]any, def any) map[string]any {
	return map[string]any{
		"name":    schema["name"],
		"type":    []any{"null", copyMap(schema)},
		"default": def,
	}
}

func PrimitiveBuilder(schema map[string]any, t string, isNullable bool) any {
	_, ok := schema["default"]
	var typ []string
	if ok {
		typ = []string{t, "null"}
	} else {
		typ = []string{"null", t}
	}
	if isNullable {
		schema["type"] = typ
	}
	return schema
}

func RecordBuilder(schema map[string]any, t string, isNullable bool) any {
	schema["type"] = t
	sFields := schema["fields"].([]any)
	fields := make([]any, len(sFields))
	for i, field := range sFields {
		fields[i] = toAvroSchema(toMap(field))
	}
	schema["fields"] = fields
	return schema
}

func ArrayBuilder(schema map[string]any, t string, isNullable bool) any {
	schema["type"] = t
	schema["items"] = toAvroSchema(toMap(schema["items"]))
	if isNullable {
		return makeNullable(schema, []any{})
	}
	return schema
}
func MapBuilder(schema map[string]any, t string, isNullable bool) any {
	schema["type"] = t
	schema["values"] = toAvroSchema(toMap(schema["values"]))
	if isNullable {
		return makeNullable(schema, map[string]any{})
	}
	return schema
}
func UnionBuilder(schema map[string]any, t string, isNullable bool) any {
	sVariants := schema["variants"].([]any)
	variants := make([]any, len(sVariants))
	for _, variant := range sVariants {
		switch variant.(type) {
		case string:
			variant = map[string]any{
				"type": variant,
			}
		}
		variants = append(variants, toAvroSchema(toMap(variant)))
	}
	return schema
}

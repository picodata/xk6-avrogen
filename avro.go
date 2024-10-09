package k6avrogen

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro"
	"go.k6.io/k6/js/modules"
)

type builderFunc = func(map[string]any, string, bool) any

var (
	primitiveBuilder builderFunc
	recordBuilder    builderFunc
	arrayBuilder     builderFunc
	mapBuilder       builderFunc
	unionBuilder     builderFunc
	builders         map[string]builderFunc
)

func init() {
	modules.Register("k6/x/avrogen", new(Avro))

	primitiveBuilder = PrimitiveBuilder
	recordBuilder = RecordBuilder
	arrayBuilder = ArrayBuilder
	mapBuilder = MapBuilder
	unionBuilder = UnionBuilder

	builders = map[string]builderFunc{
		"null":    primitiveBuilder,
		"boolean": primitiveBuilder,
		"int":     primitiveBuilder,
		"long":    primitiveBuilder,
		"float":   primitiveBuilder,
		"double":  primitiveBuilder,
		"bytes":   primitiveBuilder,
		"string":  primitiveBuilder,
		"fixed":   primitiveBuilder,
		"enum":    primitiveBuilder,

		"record": recordBuilder,
		"array":  arrayBuilder,
		"map":    mapBuilder,
		"union":  unionBuilder,
	}
}

type Avro struct{}

type AvroSchema struct {
	schema avro.Schema
}

func (*Avro) XNew(schema any) any {
	sh, err := json.Marshal(schema)
	if err != nil {
		panic(err)
		// return nil
	}
	s, err := avro.Parse(string(sh))
	if err != nil {
		panic(err)
		// return nil
	}
	return &AvroSchema{schema: s}
}

func (as *AvroSchema) GenerateValue() any {
	return generateValue(as.schema, false)
}

func generateValue(schema avro.Schema, nested bool) any {
	switch schema.Type() {
	case avro.Null:
        panic("avro.Null")
		// return nil
	case avro.Boolean:
		return true
	case avro.Int:
		return rand.Int31()
	case avro.Long:
		return rand.Int63n(math.MaxInt64)
	case avro.Float:
		return rand.Float32()
	case avro.Double:
		return rand.Float64()
	case avro.Bytes:
		return []byte{97, 98, 99, 100, 101}
	case avro.String:
		return uuid.NewString()
	case avro.Array:
		schema := schema.(*avro.ArraySchema)
		fields := []any{}
		isNested := false
		if schema.Items().Type() == avro.Record {
			isNested = true
		}
		for i := 0; i < rand.Intn(5)+1; i++ {
			fields = append(fields, generateValue(schema.Items(), isNested))
		}
		return fields
	case avro.Map:
		schema := schema.(*avro.MapSchema)
		fields := map[string]any{}
		isNested := false
		if schema.Values().Type() == avro.Record {
			isNested = true
		}
		for i := 0; i < rand.Intn(5)+1; i++ {
			fields[fmt.Sprintf("key_%d", i)] = generateValue(schema.Values(), isNested)
		}
		return fields
	case avro.Enum:
		schema := schema.(*avro.EnumSchema)
		return schema.Symbols()[0]
	case avro.Union:
		schema := schema.(*avro.UnionSchema)
		nested_type := schema.Types()[0]
		if nested_type.Type() == "null" {
			nested_type = schema.Types()[1]
		}
		return generateValue(nested_type, false)
	case avro.Record:
		schema := schema.(*avro.RecordSchema)
		fields := map[string]any{}
		for _, field := range schema.Fields() {
			isNested := false
			if field.Type().Type() == avro.Record {
				isNested = true
			}
			if field.HasDefault() {
				fields[field.Name()] = field.Default()
			} else {
				fields[field.Name()] = generateValue(field.Type(), isNested)
			}
		}
		if nested {
			return fields
		} else {
			return map[string]any{
				schema.Name(): fields,
			}
		}
	case avro.Fixed:
		schema, _ := schema.(*avro.FixedSchema)
		return make([]byte, schema.Size())
	case avro.Type(avro.Decimal):
		schema := schema.(*avro.PrimitiveSchema)
		decimal := schema.Logical().(*avro.DecimalLogicalSchema)
		bytes := make([]byte, decimal.Precision())
		rand.Read(bytes)
		return math.Float64frombits(binary.LittleEndian.Uint64(bytes))
	case avro.Type(avro.UUID):
		return uuid.New().String()
	case avro.Type(avro.Date):
		return time.Now().Unix() / (60 * 60 * 24)
	case avro.Type(avro.TimeMillis):
		return time.Now().Second() * 1e3
	case avro.Type(avro.TimeMicros):
		return time.Now().Second() * 1e6
	case avro.Type(avro.TimestampMillis):
		return time.Now().UnixMilli()
	case avro.Type(avro.TimestampMicros):
		return time.Now().UnixMicro()
	case avro.Type(avro.Duration):
		return make([]byte, 12)
	}
	return ""
}

func (*Avro) XPrepareSchema(schema any) any {
	return toAvroSchema(schema.(map[string]any))
}

func toAvroSchema(schema map[string]any) any {
	switch schema["type"].(type) {
	case string:
		t := schema["type"].(string)
		isNullable := strings.HasSuffix(t, "*")
		if isNullable {
			t = t[:len(t)-1]
		} else {
			delete(schema, "default")
		}
		if builder, ok := builders[t]; ok {
			return builder(schema, t, isNullable)
		}

		panic(fmt.Sprintf("Unknown type %s", t))
	case []any:
		delete(schema, "default")
		schema["type"] = toAvroSchema(map[string]any{
			"type":     "union",
			"variants": schema["type"],
		})
	default:
		delete(schema, "default")
		schema["type"] = toAvroSchema(schema["type"].(map[string]any))
	}
	return schema
}

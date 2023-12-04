import { check } from "k6";
import avro from "k6/x/avrogen";

let file = open('./schema.json')
const tnt_schema = JSON.parse(file)

function assert(obj, paths, func) {
  paths.forEach((path) => obj = obj[path])
  check(obj, {
    'assert': (v) => func(v)
  })
}

export function setup() {
  let schema = avro.PrepareSchema(tnt_schema)
  let avro_schema = avro.New(schema)
  let avro_obj = avro_schema.generateValue()

  assert(
    avro_obj,
    ["agreement", "agreementSpecification", "validFor", "startDateTime",],
    (v) => { return v.length > 0 }
  )
  assert(
    avro_obj,
    ["agreement", "agreementSpecification", "status",],
    (v) => { return v.length > 0 }
  )
}

export default function () { }

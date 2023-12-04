# xk6-avrogen

Генератор avro объектов для тестирования с помощью k6


## Пример
Соберите k6 с модулем xk6-avrogen:
```bash
xk6 build --with github.com/picodata/xk6-avrogen
```
Добавьте в тест:
```javascript
import avro from "k6/x/avrogen"

let file = open('./schema.json')
const tnt_schema = JSON.parse(file)

let schema = avro.PrepareSchema(tnt_schema) // если вы используете тарантульные avro схемы
let avro_schema = avro.New(schema)
let avro_obj = avro_schema.generateValue()
```

Запустите тест:
```bash
./k6 run test/test.js
```

package dev.embeddedcdc.infrastructure.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.embeddedcdc.domain.model.ChangeEvent;
import dev.embeddedcdc.domain.model.Operation;
import dev.embeddedcdc.domain.model.RowData;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Debezium JSON 문자열을 도메인 ChangeEvent 로 바꾼다.
 * Jackson 이 등장하는 유일한 클래스이며, 여기서 막아야 도메인이 이벤트 포맷에 묶이지 않는다.
 *
 * converter.schemas.enable=false 이므로 이벤트는 envelope 없이
 * {before, after, source{table, lsn, ts_ms}, op} 형태로 온다.
 * 설정이 바뀌어 schema envelope 이 켜져도 동작하도록 payload 를 방어적으로 벗긴다.
 */
@Component
public class DebeziumEventDeserializer {

    private final ObjectMapper mapper = new ObjectMapper();

    /** 처리 대상이 아닌 이벤트(tombstone, truncate 등)는 empty 를 돌려준다. */
    public Optional<ChangeEvent> deserialize(String json) {
        if (json == null || json.isBlank()) {
            return Optional.empty(); // tombstone
        }

        JsonNode root;
        try {
            root = mapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("변경 이벤트 JSON 이 아니다: " + json, e);
        }

        JsonNode payload = root.has("payload") ? root.get("payload") : root;

        Optional<Operation> op = Operation.fromCode(payload.path("op").asText(null));
        if (op.isEmpty()) {
            return Optional.empty(); // truncate(t)·message(m) 등은 이 파이프라인의 관심 밖
        }

        JsonNode source = payload.path("source");
        return Optional.of(new ChangeEvent(
                source.path("table").asText(),
                op.get(),
                toRowData(payload.get("before")),
                toRowData(payload.get("after")),
                source.path("lsn").asLong(0L),
                source.path("ts_ms").asLong(0L)));
    }

    /** 행 하나를 컬럼명에서 문자열로 가는 map 으로 편다. 행 자체가 없으면 null 이다. */
    private RowData toRowData(JsonNode row) {
        if (row == null || row.isNull()) {
            return null;
        }

        Map<String, String> values = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = row.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode value = field.getValue();
            // NullNode 에 asText() 를 부르면 문자열 "null" 이 나온다. 진짜 null 로 담아야 한다.
            values.put(field.getKey(), value.isNull() ? null : value.asText());
        }
        return new RowData(values);
    }
}

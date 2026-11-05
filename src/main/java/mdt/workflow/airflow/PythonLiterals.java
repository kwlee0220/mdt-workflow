package mdt.workflow.airflow;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * JSON 텍스트를 같은 값의 Python 리터럴 식(dict·list·str·숫자·{@code True}/{@code False}/{@code None})으로 바꾼다.
 * <p>
 * 짧은 컨테이너(80자·원소 6개 이하)는 한 줄로, 긴 것은 들여쓴 여러 줄로 출력한다. 문자열 이스케이프는
 * {@link PythonStringEscaper}를 쓰고, 유한하지 않은 실수는 {@code float('inf')}/{@code float('nan')}으로 출력한다.
 * literal 인자의 polymorphic JSON을 DAG 코드에 넣을 때 쓴다({@link DagArgument.Literal}).
 * <p>
 * 2026-11-05 이전 이름은 {@code JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid}였다(별칭 없음).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class PythonLiterals {
    // 들여쓰기 크기
    private static final int INDENT_SIZE = 2;

    // 한 줄로 출력할 최대 길이 (원하시면 조정)
    private static final int MAX_INLINE_LENGTH = 80;

    // 한 줄 판단 시, 컨테이너(객체/배열)의 최대 원소 수(필드/요소 수) (원하시면 조정)
    private static final int MAX_INLINE_ITEMS = 6;

    private static final ObjectMapper MAPPER = buildMapperPreserveOrder();

    private static ObjectMapper buildMapperPreserveOrder() {
        // Jackson JsonNode는 기본적으로 Object field order를 입력 순서대로 보존합니다.
        // 다만 안전하게 파서 설정(중복 필드 허용 등)은 필요에 따라 조정하세요.
        JsonFactory jf = JsonFactory.builder()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION) // 중복 키가 들어오면 실패(정책 선택)
                .build();
        return new ObjectMapper(jf);
    }

    private PythonLiterals() {
        throw new AssertionError("Should not be called: class=" + getClass().getName());
    }

    /**
     * JSON 텍스트를 같은 값의 Python 리터럴 식으로 바꾼다.
     *
     * @param json	JSON 텍스트.
     * @return	Python 리터럴 식.
     * @throws JsonProcessingException	JSON 텍스트가 올바르지 않거나 중복 키가 있는 경우.
     */
    public static String fromJson(String json) throws JsonMappingException, JsonProcessingException {
        JsonNode root = MAPPER.readTree(json);
        StringBuilder sb = new StringBuilder();
        renderHybrid(root, sb, 0);
        return sb.toString();
    }

    // -------------------------
    // Hybrid renderer
    // -------------------------

    private static void renderHybrid(JsonNode node, StringBuilder sb, int indentLevel) {
        if (isContainer(node)) {
            String inline = renderInline(node);
            if (shouldInline(node, inline)) {
                sb.append(inline);
            } else {
                if (node.isObject()) {
                    renderObjectMultiline(node, sb, indentLevel);
                } else {
                    renderArrayMultiline(node, sb, indentLevel);
                }
            }
            return;
        }

        // scalar
        renderScalar(node, sb);
    }

    private static boolean isContainer(JsonNode node) {
        return node.isObject() || node.isArray();
    }

    private static boolean shouldInline(JsonNode node, String inline) {
        if (inline.length() > MAX_INLINE_LENGTH) return false;

        if (node.isObject()) {
            // 입력 순 유지: node.fields() 순서를 그대로 사용
            int fields = node.size();
            return fields <= MAX_INLINE_ITEMS;
        } else if (node.isArray()) {
            int n = node.size();
            return n <= MAX_INLINE_ITEMS;
        }
        return true;
    }

    // -------------------------
    // Inline renderer (single line)
    // -------------------------

    private static String renderInline(JsonNode node) {
        StringBuilder sb = new StringBuilder();
        renderInlineInternal(node, sb);
        return sb.toString();
    }

    private static void renderInlineInternal(JsonNode node, StringBuilder sb) {
        if (node.isObject()) {
            sb.append("{");
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            boolean first = true;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> e = fields.next();
                if (!first) sb.append(", ");
                first = false;

                sb.append('\'').append(escapePythonSingleQuoted(e.getKey())).append('\'')
                  .append(": ");
                renderInlineInternal(e.getValue(), sb);
            }
            sb.append("}");
            return;
        }

        if (node.isArray()) {
            sb.append("[");
            for (int i = 0; i < node.size(); i++) {
                if (i > 0) sb.append(", ");
                renderInlineInternal(node.get(i), sb);
            }
            sb.append("]");
            return;
        }

        renderScalar(node, sb);
    }

    // -------------------------
    // Multiline renderers
    // -------------------------

    private static void renderObjectMultiline(JsonNode obj, StringBuilder sb, int indentLevel) {
        sb.append("{");
        Iterator<Map.Entry<String, JsonNode>> fields = obj.fields();
        if (!fields.hasNext()) {
            sb.append("}");
            return;
        }

        sb.append("\n");
        boolean first = true;
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            if (!first) sb.append(",\n");
            first = false;

            indent(sb, indentLevel + 1);
            sb.append('\'').append(escapePythonSingleQuoted(e.getKey())).append('\'')
              .append(": ");

            renderHybrid(e.getValue(), sb, indentLevel + 1);
        }

        sb.append("\n");
        indent(sb, indentLevel);
        sb.append("}");
    }

    private static void renderArrayMultiline(JsonNode arr, StringBuilder sb, int indentLevel) {
        sb.append("[");
        if (arr.size() == 0) {
            sb.append("]");
            return;
        }

        sb.append("\n");
        for (int i = 0; i < arr.size(); i++) {
            if (i > 0) sb.append(",\n");
            indent(sb, indentLevel + 1);
            renderHybrid(arr.get(i), sb, indentLevel + 1);
        }

        sb.append("\n");
        indent(sb, indentLevel);
        sb.append("]");
    }

    // -------------------------
    // Scalar rendering (Python literal)
    // -------------------------

    private static void renderScalar(JsonNode node, StringBuilder sb) {
        if (node.isTextual()) {
            sb.append('\'').append(escapePythonSingleQuoted(node.textValue())).append('\'');
            return;
        }

        if (node.isNumber()) {
            if (node.isBigDecimal()) {
                BigDecimal bd = node.decimalValue();
                sb.append(bd.toPlainString());
            } else if ( node.isFloatingPointNumber() && !Double.isFinite(node.doubleValue()) ) {
                // Infinity/NaN은 Python에서 정의되지 않은 이름이라 DAG import가 NameError로 실패한다.
                // 2026-11-04 이전에는 그대로 출력했다.
                double d = node.doubleValue();
                sb.append(Double.isNaN(d) ? "float('nan')" : (d > 0 ? "float('inf')" : "float('-inf')"));
            } else {
                sb.append(node.numberValue().toString());
            }
            return;
        }

        if (node.isBoolean()) {
            sb.append(node.booleanValue() ? "True" : "False");
            return;
        }

        if (node.isNull()) {
            sb.append("None");
            return;
        }

        // fallback
        sb.append('\'').append(escapePythonSingleQuoted(node.asText())).append('\'');
    }

    private static void indent(StringBuilder sb, int indentLevel) {
        int spaces = indentLevel * INDENT_SIZE;
        sb.append(" ".repeat(Math.max(0, spaces)));
    }

    /**
     * Python string literal에 맞춘 이스케이프.
     * <p>
     * 구현은 {@link PythonStringEscaper}가 담당한다. 이 프로젝트에서 Python 문자열을 이스케이프하는
     * 경로는 반드시 이 하나로 모아, 규칙이 갈라지지 않도록 한다.
     */
    private static String escapePythonSingleQuoted(String s) {
        return PythonStringEscaper.escape(s);
    }
}

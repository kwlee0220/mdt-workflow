package mdt.workflow.airflow;

import java.time.Instant;
import java.util.LinkedHashMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mdt.model.sm.value.ElementCollectionValue;
import mdt.model.sm.value.ElementValue;
import mdt.model.sm.value.PropertyValue;
import mdt.workflow.airflow.OperatorArgumentSpec.LiteralArgument;
import mdt.workflow.airflow.OperatorArgumentSpec.ReferenceArgument;
import mdt.workflow.airflow.OperatorArgumentSpec.SinkArgument;
import mdt.workflow.airflow.OperatorArgumentSpec.TaskOutputArgument;
import mdt.workflow.model.ArgumentSpec;

/**
 * {@link OperatorArgumentSpec}가 생성하는 Python 구문이 올바르게 이스케이프되는지 검증한다.
 * <p>
 * 이들 구문은 워크플로우 기술서에서 온 문자열(요소 참조 문자열, 태스크 식별자 등)을 Python 소스에
 * 직접 삽입하기 때문에, 이스케이프가 누락되면 임의의 Python 코드가 삽입될 수 있다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class OperatorArgumentSpecTest {
	/** dict 문맥을 유지한 채 항목을 하나 더 끼워 넣으려는 전형적인 삽입 페이로드. */
	private static final String INJECTION =
			"x\"), \"pwn\": __import__(\"os\").system(\"id\"), \"z\": mdtpy.airflow.reference(\"y";

	@Test
	public void referenceArgumentRendersPlainReference() {
		String stmt = new ReferenceArgument("arg1", "param:heater:CycleTime").getPythonStatement();
		Assertions.assertEquals("\"arg1\": mdtpy.airflow.reference(\"param:heater:CycleTime\")", stmt);
	}

	@Test
	public void sinkArgumentRendersPlainReference() {
		String stmt = new SinkArgument("out1", "param:heater:Result").getPythonStatement();
		Assertions.assertEquals("\"out1\": mdtpy.airflow.sink(\"param:heater:Result\")", stmt);
	}

	@Test
	public void taskOutputArgumentRendersPlainNames() {
		String stmt = new TaskOutputArgument("arg1", "task1", "out1").getPythonStatement();
		Assertions.assertEquals("\"arg1\": mdtpy.airflow.task_output(\"task1\", \"out1\")", stmt);
	}

	@Test
	public void literalArgumentDoesNotEscapeItsPythonExpression() {
		// literal은 이미 Python 리터럴 표현식이므로 그대로 삽입되고, parse_json_node()로 복원된다.
		String stmt = new LiteralArgument("arg1", "{'@type': 'mdt:value:integer', 'value': 1}")
							.getPythonStatement();
		Assertions.assertEquals("\"arg1\": mdtpy.airflow.literal(mdtpy.value.parse_json_node("
								+ "{'@type': 'mdt:value:integer', 'value': 1}))", stmt);
	}

	@Test
	public void literalFromTaskArgumentPreservesScalarType() {
		// 값 JSON만 넘기면 Long이 Python에서 Integer로 바뀌므로, @type이 보존되어야 한다.
		ArgumentSpec spec = ArgumentSpec.LiteralArgument.builder()
										.id("n").value(PropertyValue.LONG(12345678901L)).build();
		String stmt = OperatorSpec.fromTaskArgument("n", spec).getPythonStatement();
		Assertions.assertEquals("\"n\": mdtpy.airflow.literal(mdtpy.value.parse_json_node("
								+ "{'@type': 'mdt:value:long', 'value': 12345678901}))", stmt);
	}

	@Test
	public void literalFromTaskArgumentPreservesNestedTypes() {
		// 컬렉션은 값 JSON만으로는 Python에서 복원할 수 없으므로, 멤버의 @type까지 보존되어야 한다.
		LinkedHashMap<String,ElementValue> members = new LinkedHashMap<>();
		members.put("count", PropertyValue.INTEGER(2));
		members.put("when", PropertyValue.DATE_TIME(Instant.parse("2026-10-07T05:00:00Z")));
		ArgumentSpec spec = ArgumentSpec.LiteralArgument.builder()
										.id("c").value(new ElementCollectionValue(members)).build();
		String stmt = OperatorSpec.fromTaskArgument("c", spec).getPythonStatement();

		Assertions.assertTrue(stmt.startsWith("\"c\": mdtpy.airflow.literal(mdtpy.value.parse_json_node("), stmt);
		Assertions.assertTrue(stmt.contains("'@type': 'mdt:value:collection'"), stmt);
		Assertions.assertTrue(stmt.contains("'count': {'@type': 'mdt:value:integer', 'value': 2}"), stmt);
		Assertions.assertTrue(stmt.contains("'when': {'@type': 'mdt:value:dateTime', 'value': '2026-10-07T05:00:00Z'}"),
								stmt);
	}

	@Test
	public void injectionViaReferenceStringIsNeutralized() {
		String stmt = new ReferenceArgument("arg1", INJECTION).getPythonStatement();
		Assertions.assertFalse(hasUnbalancedQuotes(stmt), "injection escaped the literal: " + stmt);
		Assertions.assertFalse(stmt.contains("__import__(\""), "raw injection remains: " + stmt);
	}

	@Test
	public void injectionViaSinkReferenceStringIsNeutralized() {
		String stmt = new SinkArgument("out1", INJECTION).getPythonStatement();
		Assertions.assertFalse(hasUnbalancedQuotes(stmt), "injection escaped the literal: " + stmt);
	}

	@Test
	public void injectionViaTaskIdIsNeutralized() {
		String stmt = new TaskOutputArgument("arg1", INJECTION, "out1").getPythonStatement();
		Assertions.assertFalse(hasUnbalancedQuotes(stmt), "injection escaped the literal: " + stmt);
	}

	@Test
	public void injectionViaOutputArgumentNameIsNeutralized() {
		String stmt = new TaskOutputArgument("arg1", "task1", INJECTION).getPythonStatement();
		Assertions.assertFalse(hasUnbalancedQuotes(stmt), "injection escaped the literal: " + stmt);
	}

	/**
	 * 이스케이프되지 않은 큰따옴표의 개수가 홀수이면 리터럴이 의도치 않은 위치에서 열리거나 닫힌 것이다.
	 * 정상적으로 이스케이프된 구문은 리터럴을 여닫는 따옴표만 남으므로 항상 짝수 개이다.
	 */
	private static boolean hasUnbalancedQuotes(String stmt) {
		int count = 0;
		for ( int i = 0; i < stmt.length(); ++i ) {
			char c = stmt.charAt(i);
			if ( c == '\\' ) {
				++i;	// escape sequence의 다음 문자는 건너뛴다.
			}
			else if ( c == '"' ) {
				++count;
			}
		}
		return (count % 2) != 0;
	}
}

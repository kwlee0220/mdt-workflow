package mdt.workflow.airflow;

import java.time.Duration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 각 {@link OperatorSpec} 하위 클래스의 {@link OperatorSpec#getOperatorCall()}이
 * 올바른 operator 이름과 인자를 렌더링하는지 검증한다.
 * <p>
 * 반환 문자열은 operator 이름으로 시작하고 개행으로 끝나야 하며, 인자 값은 Python 문자열 리터럴로
 * 이스케이프되어야 한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class OperatorCallTest {
	@Test
	public void aasOperationRendersAllKwargs() {
		OperatorSpec spec = AASOperationSpec.builder()
											.taskId("t").instanceId("inst").submodelId("sm").path("Op")
											.build();
		Assertions.assertEquals(
				"AASOperationOperator(\n"
				+ "      instance = \"inst\",\n"
				+ "      submodel = \"sm\",\n"
				+ "      path = \"Op\",\n",
				spec.getOperatorCall());
	}

	@Test
	public void operationSubmodelRendersInstanceAndSubmodel() {
		OperatorSpec spec = OperationSubmodelSpec.builder()
											.taskId("t").instanceId("inst").submodelId("sm")
											.build();
		Assertions.assertEquals(
				"OperationSubmodelOperator(\n"
				+ "      instance = \"inst\",\n"
				+ "      submodel = \"sm\",\n",
				spec.getOperatorCall());
	}

	@Test
	public void setElementRendersOperatorNameOnly() {
		OperatorSpec spec = SetElementSpec.builder().taskId("t").build();
		Assertions.assertEquals("SetElementOperator(\n", spec.getOperatorCall());
	}

	@Test
	public void restfulAsyncRpcRendersUrlAndEndpoint() {
		OperatorSpec spec = RESTfulAsyncRpcSpec.builder()
											.taskId("t").baseUrl("http://h:8080").endpoint("/op")
											.build();
		Assertions.assertEquals(
				"RestfulAsyncRpcOperator(\n"
				+ "      base_url = \"http://h:8080\",\n"
				+ "      op_endpoint = \"/op\",\n",
				spec.getOperatorCall());
	}

	@Test
	public void restfulRpcRendersOpUrlOnlyWhenTimeoutIsAbsent() {
		// timeout이 없으면 kwarg를 생략해서 Python 쪽 기본값(제한 없음)을 따르게 한다.
		OperatorSpec spec = RESTfulRpcSpec.builder()
											.taskId("t").opUrl("http://h:8080/operations/Detect")
											.build();
		Assertions.assertEquals(
				"RestfulRpcOperator(\n"
				+ "      op_url = \"http://h:8080/operations/Detect\",\n",
				spec.getOperatorCall());
	}

	@Test
	public void restfulRpcRendersTimeoutAsUnquotedSeconds() {
		// timeout은 Python float이므로 따옴표 없이 초 단위 숫자로 렌더링되어야 한다.
		OperatorSpec spec = RESTfulRpcSpec.builder()
											.taskId("t").opUrl("http://h:8080/op")
											.timeout(Duration.ofMillis(1500))
											.build();
		Assertions.assertEquals(
				"RestfulRpcOperator(\n"
				+ "      op_url = \"http://h:8080/op\",\n"
				+ "      timeout = 1.5,\n",
				spec.getOperatorCall());
	}

	@Test
	public void pythonScriptRendersMultilineScriptAsOneEscapedLiteral() {
		// 여러 줄 script도 개행/따옴표가 이스케이프되어 하나의 Python 문자열 리터럴로 렌더링되어야 한다.
		OperatorSpec spec = PythonScriptSpec.builder()
											.taskId("t").script("a = 1\nb = \"q\"")
											.build();
		Assertions.assertEquals(
				"PythonScriptOperator(\n"
				+ "      script = \"a = 1\\nb = \\\"q\\\"\",\n",
				spec.getOperatorCall());
	}

	@Test
	public void everyCallStartsWithOperatorNameAndEndsWithNewline() {
		OperatorSpec[] specs = {
			AASOperationSpec.builder().taskId("t").instanceId("i").submodelId("s").path("p").build(),
			OperationSubmodelSpec.builder().taskId("t").instanceId("i").submodelId("s").build(),
			SetElementSpec.builder().taskId("t").build(),
			RESTfulAsyncRpcSpec.builder().taskId("t").baseUrl("u").endpoint("e").build(),
			RESTfulRpcSpec.builder().taskId("t").opUrl("u").build(),
			PythonScriptSpec.builder().taskId("t").script("outputs['x'] = 1").build(),
		};
		for ( OperatorSpec spec: specs ) {
			String call = spec.getOperatorCall();
			Assertions.assertTrue(call.endsWith("\n"), "does not end with newline: " + call);
			Assertions.assertTrue(call.contains("Operator("), "missing operator name: " + call);
		}
	}

	@Test
	public void kwargValuesAreEscaped() {
		// path에 큰따옴표가 들어가면 리터럴을 깨뜨리지 않도록 이스케이프되어야 한다.
		OperatorSpec spec = AASOperationSpec.builder()
											.taskId("t").instanceId("i").submodelId("s").path("a\"b")
											.build();
		Assertions.assertTrue(spec.getOperatorCall().contains("path = \"a\\\"b\","),
							"path not escaped: " + spec.getOperatorCall());
	}
}

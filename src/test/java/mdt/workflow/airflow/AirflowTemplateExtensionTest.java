package mdt.workflow.airflow;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.pebbletemplates.pebble.error.PebbleException;

import mdt.workflow.airflow.AirflowTemplateExtension.FailFunction;
import mdt.workflow.airflow.AirflowTemplateExtension.PythonEscapeFilter;

/**
 * {@link AirflowTemplateExtension}이 제공하는 {@code py} 필터와 {@code fail} 함수를 검증한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowTemplateExtensionTest {
	@Test
	public void extensionRegistersPyFilterAndFailFunction() {
		AirflowTemplateExtension ext = new AirflowTemplateExtension();

		Assertions.assertTrue(ext.getFilters().containsKey("py"));
		Assertions.assertTrue(ext.getFunctions().containsKey("fail"));
	}

	@Test
	public void pyFilterEscapesQuotes() {
		Object escaped = new PythonEscapeFilter().apply("say \"hi\"", Map.of(), null, null, 1);
		Assertions.assertEquals("say \\\"hi\\\"", escaped);
	}

	@Test
	public void pyFilterMapsNullToEmptyString() {
		Assertions.assertEquals("", new PythonEscapeFilter().apply(null, Map.of(), null, null, 1));
	}

	@Test
	public void failFunctionThrowsWithGivenMessage() {
		FailFunction fn = new FailFunction();

		PebbleException thrown = Assertions.assertThrows(PebbleException.class,
									() -> fn.execute(Map.of("message", "unsupported task type: Foo"),
													null, null, 1));
		Assertions.assertTrue(thrown.getMessage().contains("unsupported task type: Foo"),
							"unexpected message: " + thrown.getMessage());
	}

	@Test
	public void failFunctionDeclaresMessageArgument() {
		Assertions.assertEquals(java.util.List.of("message"), new FailFunction().getArgumentNames());
	}
}

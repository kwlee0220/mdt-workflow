package mdt.workflow.airflow;

import java.util.List;
import java.util.Map;

import io.pebbletemplates.pebble.error.PebbleException;
import io.pebbletemplates.pebble.extension.AbstractExtension;
import io.pebbletemplates.pebble.extension.Filter;
import io.pebbletemplates.pebble.extension.Function;
import io.pebbletemplates.pebble.template.EvaluationContext;
import io.pebbletemplates.pebble.template.PebbleTemplate;

/**
 * Airflow DAG용 Python 소스를 생성하는 Pebble 템플릿이 사용하는 확장.
 * <p>
 * 두 가지를 제공한다:
 * <ul>
 * 	<li>{@code py} 필터: Python 문자열 리터럴 안에 삽입되는 값을 이스케이프한다.
 * 		{@code {{ value | py }}} 형태로 사용하며, 생략하면 값에 포함된 따옴표나 개행이 리터럴을 깨뜨리고
 * 		임의의 Python 코드가 삽입될 수 있다.</li>
 * 	<li>{@code fail} 함수: 템플릿 렌더링을 즉시 중단시킨다. 처리할 수 없는 입력을 만났을 때
 * 		실행 불가능한 Python 파일을 생성하는 대신 생성 시점에 실패하기 위해 사용한다.</li>
 * </ul>
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowTemplateExtension extends AbstractExtension {
	/** Python 문자열 이스케이프 필터의 이름. */
	public static final String FILTER_PY = "py";
	/** 렌더링 중단 함수의 이름. */
	public static final String FUNCTION_FAIL = "fail";

	private static final String ARG_MESSAGE = "message";

	@Override
	public Map<String,Filter> getFilters() {
		return Map.of(FILTER_PY, new PythonEscapeFilter());
	}

	@Override
	public Map<String,Function> getFunctions() {
		return Map.of(FUNCTION_FAIL, new FailFunction());
	}

	/**
	 * 입력 값의 문자열 표현을 Python 문자열 리터럴용으로 이스케이프하는 필터.
	 */
	public static class PythonEscapeFilter implements Filter {
		@Override
		public List<String> getArgumentNames() {
			return null;
		}

		@Override
		public Object apply(Object input, Map<String,Object> args, PebbleTemplate self,
							EvaluationContext context, int lineNumber) {
			return (input != null) ? PythonStringEscaper.escape(input.toString()) : "";
		}
	}

	/**
	 * 주어진 메시지와 함께 {@link PebbleException}을 던져 렌더링을 중단시키는 함수.
	 * <p>
	 * 템플릿이 처리할 수 없는 입력을 만난 경우, 깨진 Python 파일을 생성하는 대신 생성 자체를 실패시킨다.
	 */
	public static class FailFunction implements Function {
		@Override
		public List<String> getArgumentNames() {
			return List.of(ARG_MESSAGE);
		}

		@Override
		public Object execute(Map<String,Object> args, PebbleTemplate self,
							EvaluationContext context, int lineNumber) {
			Object message = args.get(ARG_MESSAGE);
			String filename = (self != null) ? self.getName() : null;
			throw new PebbleException(null, (message != null) ? message.toString() : "template failed",
										lineNumber, filename);
		}
	}
}

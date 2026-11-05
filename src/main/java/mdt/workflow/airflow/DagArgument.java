package mdt.workflow.airflow;

import utils.Preconditions;

/**
 * DAG 코드의 operator {@code inputs} dict에 들어가는 입력 인자 하나.
 * <p>
 * 하위 유형별로 {@link #toPythonDictEntry()}가 {@code "<id>": mdtpy.airflow.<함수>(...)} 형태의 dict 항목을 만든다.
 * <ul>
 *   <li>{@link Reference}: MDT 요소 참조({@code mdtpy.airflow.reference})</li>
 *   <li>{@link Literal}: 고정 값({@code mdtpy.airflow.literal(mdtpy.value.parse_json_node(...))})</li>
 *   <li>{@link TaskOutput}: 앞 task의 출력({@code mdtpy.airflow.task_output})</li>
 * </ul>
 * 출력은 별도 타입 {@link DagSink}이다. 인자 id는 Python 식별자여야 한다.
 * <p>
 * 2026-11-05 이전에는 이름이 {@code OperatorArgumentSpec}이었고(중첩 클래스 {@code ReferenceArgument}·
 * {@code LiteralArgument}·{@code TaskOutputArgument}가 mdt-client {@code ArgumentSpec}의 중첩 클래스와 이름이 같았다),
 * 출력({@code SinkArgument})도 같은 계층에 있었다(별칭 없음).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public abstract sealed class DagArgument
	permits DagArgument.Reference, DagArgument.Literal, DagArgument.TaskOutput {
	private final String m_argId;
	
	/**
	 * operator {@code inputs} dict의 항목({@code "<id>": <식>})을 반환한다.
	 *
	 * @return	dict 항목 Python 코드.
	 */
	public abstract String toPythonDictEntry();
	
	protected DagArgument(String argId) {
		m_argId = checkArgId(argId);
	}
	
	/**
	 * 인자 id를 반환한다.
	 *
	 * @return	인자 id(Python 식별자).
	 */
	public String getId() {
		return m_argId;
	}
	
	static String checkArgId(String argId) {
		Preconditions.checkNotNullArgument(argId, "argId should not be null");
		Preconditions.checkArgument(PythonIdentifiers.isValid(argId),
									"argId should be a valid Python identifier: %s", argId);
		return argId;
	}
	
	/**
	 * MDT 요소 참조를 전달하는 입력 인자({@code mdtpy.airflow.reference(<참조 표현식>)}).
	 */
	public static final class Reference extends DagArgument {
		private final String m_refString;

		public Reference(String argId, String refString) {
			super(argId);
			Preconditions.checkNotNullArgument(refString, "refString should not be null");
			
			m_refString = refString;
		}

		public String getReference() {
			return m_refString;
		}

		@Override
		public String toPythonDictEntry() {
			return String.format("%s: mdtpy.airflow.reference(%s)",
								PythonStringEscaper.quote(getId()),
								PythonStringEscaper.quote(m_refString));
		}
	}

	/**
	 * 고정된 리터럴 값을 전달하는 입력 인자.
	 * <p>
	 * 값은 타입 정보({@code @type})를 포함한 polymorphic JSON({@code ElementValue#toJsonString()})을
	 * Python 리터럴로 옮긴 형태로 보관하고, 생성된 코드에서 {@code mdtpy.value.parse_json_node()}로
	 * {@code ElementValue}를 복원한다. 타입 정보 없는 값 JSON을 넘기면 Python의 {@code literal()}이
	 * 스칼라만 {@code mdt_value()}로 변환할 수 있어, 컬렉션/리스트/File/Range 값은 실패하고
	 * DateTime/Duration/Long 등은 String/Integer로 타입이 바뀌기 때문이다.
	 */
	public static final class Literal extends DagArgument {
		private final String m_typedJsonLiteral;

		/**
		 * @param argId				인자 이름.
		 * @param typedJsonLiteral	polymorphic JSON을 {@link PythonLiterals}로 변환한
		 * 							Python 리터럴 표현식. 이스케이프 없이 그대로 삽입된다.
		 */
		public Literal(String argId, String typedJsonLiteral) {
			super(argId);
			Preconditions.checkNotNullArgument(typedJsonLiteral, "typedJsonLiteral should not be null");

			m_typedJsonLiteral = typedJsonLiteral;
		}

		@Override
		public String toPythonDictEntry() {
			// m_typedJsonLiteral은 PythonLiterals가 생성한 Python 리터럴 표현식이므로 이스케이프하지 않는다.
			return String.format("%s: mdtpy.airflow.literal(mdtpy.value.parse_json_node(%s))",
								PythonStringEscaper.quote(getId()), m_typedJsonLiteral);
		}
	}
	
	/**
	 * 앞 task의 출력을 전달하는 입력 인자({@code mdtpy.airflow.task_output(<task id>, <출력 인자 id>)}).
	 */
	public static final class TaskOutput extends DagArgument {
		private final String m_taskId;
		private final String m_outputArgumentId;

		public TaskOutput(String argId, String taskId, String outputArgumentId) {
			super(argId);
			Preconditions.checkNotNullArgument(taskId, "taskId should not be null");
			Preconditions.checkNotNullArgument(outputArgumentId, "outputArgumentId should not be null");

			m_taskId = taskId;
			m_outputArgumentId = outputArgumentId;
		}

		@Override
		public String toPythonDictEntry() {
			return String.format("%s: mdtpy.airflow.task_output(%s, %s)",
								PythonStringEscaper.quote(getId()),
								PythonStringEscaper.quote(m_taskId),
								PythonStringEscaper.quote(m_outputArgumentId));
		}
	}
}

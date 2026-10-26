package mdt.workflow.airflow;

import utils.Preconditions;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
 public abstract sealed class OperatorArgumentSpec
	permits OperatorArgumentSpec.ReferenceArgument, OperatorArgumentSpec.LiteralArgument, OperatorArgumentSpec.TaskOutputArgument,
			OperatorArgumentSpec.SinkArgument {
	private final String m_argId;
	
	public abstract String getPythonStatement();
	
	protected OperatorArgumentSpec(String argId) {
		Preconditions.checkNotNullArgument(argId, "argId should not be null");
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(argId),
									"argId should be a valid Python identifier: %s", argId);
		m_argId = argId;
	}
	
	public String getId() {
		return m_argId;
	}
	
	public static final class ReferenceArgument extends OperatorArgumentSpec {
		private final String m_refString;

		public ReferenceArgument(String argId, String refString) {
			super(argId);
			Preconditions.checkNotNullArgument(refString, "refString should not be null");
			
			m_refString = refString;
		}

		public String getReference() {
			return m_refString;
		}

		@Override
		public String getPythonStatement() {
			return String.format("%s: mdtpy.airflow.reference(%s)",
								PythonStringEscaper.quote(getId()),
								PythonStringEscaper.quote(m_refString));
		}
	}

	/**
	 * 고정된 리터럴 값을 전달하는 인자.
	 * <p>
	 * 값은 타입 정보({@code @type})를 포함한 polymorphic JSON({@code ElementValue#toJsonString()})을
	 * Python 리터럴로 옮긴 형태로 보관하고, 생성된 코드에서 {@code mdtpy.value.parse_json_node()}로
	 * {@code ElementValue}를 복원한다. 타입 정보 없는 값 JSON을 넘기면 Python의 {@code literal()}이
	 * 스칼라만 {@code mdt_value()}로 변환할 수 있어, 컬렉션/리스트/File/Range 값은 실패하고
	 * DateTime/Duration/Long 등은 String/Integer로 타입이 바뀌기 때문이다.
	 */
	public static final class LiteralArgument extends OperatorArgumentSpec {
		private final String m_typedJsonLiteral;

		/**
		 * @param argId				인자 이름.
		 * @param typedJsonLiteral	polymorphic JSON을 {@link JsonToPythonLiteralHybrid}로 변환한
		 * 							Python 리터럴 표현식. 이스케이프 없이 그대로 삽입된다.
		 */
		public LiteralArgument(String argId, String typedJsonLiteral) {
			super(argId);
			Preconditions.checkNotNullArgument(typedJsonLiteral, "typedJsonLiteral should not be null");

			m_typedJsonLiteral = typedJsonLiteral;
		}

		@Override
		public String getPythonStatement() {
			// m_typedJsonLiteral은 JsonToPythonLiteralHybrid가 생성한 Python 리터럴 표현식이므로 이스케이프하지 않는다.
			return String.format("%s: mdtpy.airflow.literal(mdtpy.value.parse_json_node(%s))",
								PythonStringEscaper.quote(getId()), m_typedJsonLiteral);
		}
	}
	
	public static final class TaskOutputArgument extends OperatorArgumentSpec {
		private final String m_taskId;
		private final String m_outputArgumentName;

		public TaskOutputArgument(String argId, String taskId, String outputArgumentName) {
			super(argId);
			Preconditions.checkNotNullArgument(taskId, "taskId should not be null");
			Preconditions.checkNotNullArgument(outputArgumentName, "outputName should not be null");

			m_taskId = taskId;
			m_outputArgumentName = outputArgumentName;
		}

		@Override
		public String getPythonStatement() {
			return String.format("%s: mdtpy.airflow.task_output(%s, %s)",
								PythonStringEscaper.quote(getId()),
								PythonStringEscaper.quote(m_taskId),
								PythonStringEscaper.quote(m_outputArgumentName));
		}
	}
	
	public static final class SinkArgument extends OperatorArgumentSpec {
		private final String m_refString;

		public SinkArgument(String argId, String refString) {
			super(argId);
			Preconditions.checkNotNullArgument(refString, "refString should not be null");
			
			m_refString = refString;
		}

		public String getReference() {
			return m_refString;
		}

		@Override
		public String getPythonStatement() {
			return String.format("%s: mdtpy.airflow.sink(%s)",
								PythonStringEscaper.quote(getId()),
								PythonStringEscaper.quote(m_refString));
		}
	}
}
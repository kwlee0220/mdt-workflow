package mdt.workflow.airflow;

import utils.Preconditions;

/**
 * DAG 코드의 operator {@code outputs} dict에 들어가는 출력 하나. task 출력을 기록할 MDT 요소를 가리킨다
 * ({@code "<id>": mdtpy.airflow.sink(<참조 표현식>)}).
 * <p>
 * 2026-11-05 이전에는 입력 인자와 같은 계층의 {@code OperatorArgumentSpec.SinkArgument}였다(별칭 없음).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class DagSink {
	private final String m_argId;
	private final String m_refString;

	/**
	 * @param argId		출력 인자 id(Python 식별자).
	 * @param refString	출력을 기록할 요소의 참조 표현식.
	 */
	public DagSink(String argId, String refString) {
		m_argId = DagArgument.checkArgId(argId);
		Preconditions.checkNotNullArgument(refString, "refString should not be null");
		
		m_refString = refString;
	}
	
	/**
	 * 출력 인자 id를 반환한다.
	 *
	 * @return	출력 인자 id.
	 */
	public String getId() {
		return m_argId;
	}

	public String getReference() {
		return m_refString;
	}

	/**
	 * operator {@code outputs} dict의 항목({@code "<id>": mdtpy.airflow.sink(...)})을 반환한다.
	 *
	 * @return	dict 항목 Python 코드.
	 */
	public String toPythonDictEntry() {
		return String.format("%s: mdtpy.airflow.sink(%s)",
							PythonStringEscaper.quote(m_argId),
							PythonStringEscaper.quote(m_refString));
	}
}

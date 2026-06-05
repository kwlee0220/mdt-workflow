package mdt.workflow.airflow;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import utils.Preconditions;
import utils.Split;


/**
 * Airflow DAG 실행(DAG run) 하나를 식별하는 불변 값 객체이다.
 * <p>
 * Airflow에서 하나의 워크플로우 실행은 <b>DAG id</b>와 그 DAG의 <b>DAG run id</b> 쌍으로 식별된다.
 * 본 클래스는 이 두 값을 묶어, Airflow REST API URL 구성({@link #toUrl(String)})과
 * 단일 문자열 표현({@link #toStringExpr()}) / 그 역변환({@link #parse(String)})을 제공한다.
 * <p>
 * 문자열 표현은 {@code dagId__dagRunId} 형식으로, {@code "__"}를 구분자로 사용한다.
 * DAG run id는 {@code "manual__2024-01-01T00:00:00+00:00"}처럼 {@code "__"}를 포함할 수 있으나,
 * {@link #parse(String)}가 <b>처음 등장하는</b> {@code "__"}를 기준으로 분할하므로
 * {@code dagId}가 {@code "__"}를 포함하지 않는 한 라운드트립이 보장된다. 이를 위해 생성자에서
 * {@code dagId}에 {@code "__"}가 포함되지 않음을 강제한다.
 * <p>
 * Java {@code record}로 정의되어 있어 {@code equals}, {@code hashCode}와 접근자가 자동 제공된다.
 *
 * @param dagId		Airflow DAG id. {@code null}이거나 {@code "__"}를 포함하면 안 된다.
 * @param dagRunId	Airflow DAG run id. {@code null}이면 안 된다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public record AirflowWorkflowId(String dagId, String dagRunId) {
	/**
	 * {@code dagId}, {@code dagRunId}를 검증하여 {@code AirflowWorkflowId}를 생성한다.
	 *
	 * @throws IllegalArgumentException	{@code dagId} 또는 {@code dagRunId}가 {@code null}이거나,
	 *									{@code dagId}가 {@code "__"}를 포함하는 경우.
	 */
	public AirflowWorkflowId {
		Preconditions.checkNotNullArgument(dagId, "dagId must not be null");
		Preconditions.checkNotNullArgument(dagRunId, "dagRunId must not be null");
		Preconditions.checkArgument(!dagId.contains("__"), "dagId must not contain '__': dagId=%s", dagId);
	}

	/**
	 * 이 DAG run을 가리키는 Airflow REST API URL을 구성한다.
	 * <p>
	 * 구성된 URL은 {@code <prefix>/dags/<dagId>/dagRuns/<dagRunId>} 형식이며,
	 * {@code dagId}와 {@code dagRunId}는 각각 UTF-8로 URL 인코딩된다.
	 *
	 * @param prefix	URL 접두사 (예: Airflow API 베이스 URL).
	 * @return	DAG run을 가리키는 URL 문자열.
	 */
	public String toUrl(String prefix) {
		String dagEncoded = URLEncoder.encode(dagId, StandardCharsets.UTF_8);
		String dagRunEncoded = URLEncoder.encode(dagRunId, StandardCharsets.UTF_8);

		return String.format("%s/dags/%s/dagRuns/%s", prefix, dagEncoded, dagRunEncoded);
	}

	/**
	 * {@code dagId}와 {@code dagRunId}를 {@code "__"}로 결합한 단일 문자열 표현을 반환한다.
	 * <p>
	 * 반환된 문자열은 {@link #parse(String)}로 다시 {@code AirflowWorkflowId}로 복원할 수 있다.
	 *
	 * @return	{@code dagId__dagRunId} 형식의 문자열.
	 */
	public String toStringExpr() {
		return String.format("%s__%s", dagId, dagRunId);
	}

	/**
	 * {@link #toStringExpr()}와 동일하게 {@code dagId__dagRunId} 형식의 문자열을 반환한다.
	 */
	@Override
	public String toString() {
		return toStringExpr();
	}

	/**
	 * {@code dagId__dagRunId} 형식의 문자열을 {@code AirflowWorkflowId}로 파싱한다.
	 * <p>
	 * {@link #toStringExpr()}의 역연산이다. 처음 등장하는 {@code "__"}를 기준으로 분할하여
	 * 앞부분을 {@code dagId}, 뒷부분을 {@code dagRunId}로 사용한다.
	 *
	 * @param taskIdStr	파싱할 문자열. {@code dagId__dagRunId} 형식이어야 한다.
	 * @return	파싱된 {@code AirflowWorkflowId} 객체.
	 * @throws IllegalArgumentException	{@code "__"}가 없거나, 분할 결과 {@code dagId} 또는
	 *									{@code dagRunId}가 빈 문자열인 경우.
	 */
	public static AirflowWorkflowId parse(String taskIdStr) {
		Split split = Split.split(taskIdStr, "__");
		String dagId = split.head();
		String dagRunId = split.tail().orElse("");
		if ( dagId.isEmpty() || dagRunId.isEmpty() ) {
			throw new IllegalArgumentException("invalid Airflow task id: " + taskIdStr);
		}

		return new AirflowWorkflowId(dagId, dagRunId);
	}
}

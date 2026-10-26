package mdt.workflow.airflow;

import java.time.Duration;
import java.util.List;

import utils.Preconditions;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

import mdt.task.MDTTask;
import mdt.task.RESTfulRpcTask;
import mdt.workflow.airflow.OperatorArgumentSpec.SinkArgument;
import mdt.workflow.model.TaskDescriptor;

/**
 * 원격 연산을 동기 방식의 RESTful RPC로 호출하는 operator의 spec.
 * <p>
 * {@link RESTfulAsyncRpcSpec}와 달리 연산 상태를 폴링하지 않기 때문에 {@code baseUrl}과
 * {@code endpoint}로 나누지 않고 연산 호출 URL 전체를 받는다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class RESTfulRpcSpec extends OperatorSpec {
	private final String m_opUrl;
	private final Duration m_timeout;	// null이면 제한 없이 대기한다.

	private RESTfulRpcSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_opUrl, "opUrl must not be null");

		m_opUrl = builder.m_opUrl;
		m_timeout = builder.m_timeout;
	}

	public String getOpUrl() {
		return m_opUrl;
	}

	/**
	 * 연산 호출의 제한 시간을 반환한다.
	 *
	 * @return	제한 시간. 설정되지 않은 경우는 {@link FOption#empty()}.
	 */
	public FOption<Duration> getTimeout() {
		return FOption.ofNullable(m_timeout);
	}

	@Override
	public String getOperatorCall() {
		String call = "RestfulRpcOperator(\n"
					+ kwargLine("op_url", m_opUrl);
		if ( m_timeout != null ) {
			// RestfulRpcOperator의 timeout은 초 단위 float이므로 따옴표 없이 숫자 리터럴로 넘긴다.
			// 제한 시간이 설정되지 않으면 kwarg 자체를 생략해서, 제한 없이 대기하는 Python 쪽
			// 기본값(None)을 따르게 한다. 동기 호출은 단일 응답이 연산 종료 시점까지 지연되기 때문에
			// 임의의 기본 제한 시간을 여기서 만들어 넣지 않는다.
			call += rawKwargLine("timeout", String.valueOf(m_timeout.toMillis() / 1000.0));
		}

		return call;
	}

	public static RESTfulRpcSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		var opUrlOpt = task.getOptions().get(RESTfulRpcTask.OPTION_URL);
		if ( opUrlOpt == null ) {
			throw new IllegalArgumentException("'" + RESTfulRpcTask.OPTION_URL + "' option is required"
												+ " for RESTfulRpcSpec: " + task.getId());
		}
		String opUrl = opUrlOpt.getValue();

		// RESTfulRpcTask와 동일한 파서를 사용해서 "30s"/"60"/"1.5" 형식을 모두 받아들인다.
		Duration timeout = FOption.ofNullable(task.getOptions().get(MDTTask.OPTION_TIMEOUT))
									.map(opt -> UnitUtils.parseSecondDuration(opt.getValue()))
									.getOrNull();

		List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
													.map(arg -> fromTaskArgument(arg.getId(), arg))
													.toList();
		List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
													.map((arg) -> fromOutputVariable(arg.getId(), arg))
													.toList();

		return builder()
					.taskId(task.getId())
					.opUrl(opUrl)
					.timeout(timeout)
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,RESTfulRpcSpec> {
		private String m_opUrl;
		private Duration m_timeout;

		@Override
		public RESTfulRpcSpec build() {
			return new RESTfulRpcSpec(this);
		}

		public Builder opUrl(String opUrl) {
			m_opUrl = opUrl;
			return this;
		}

		/**
		 * 연산 호출의 제한 시간을 설정한다.
		 *
		 * @param timeout	제한 시간. {@code null}이면 제한 없이 대기한다.
		 * @return	이 빌더.
		 */
		public Builder timeout(Duration timeout) {
			m_timeout = timeout;
			return this;
		}
	}
}

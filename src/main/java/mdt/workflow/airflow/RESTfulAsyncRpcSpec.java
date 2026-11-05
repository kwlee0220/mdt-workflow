package mdt.workflow.airflow;


import utils.Preconditions;

import mdt.task.RESTfulAsyncRpcTask;
import mdt.workflow.model.TaskDescriptor;

/**
 * 원격 연산을 비동기 RESTful RPC로 호출하는 task({@code mdt.task.RESTfulAsyncRpcTask})의 Airflow spec. mdtpy
 * {@code RestfulAsyncRpcOperator(base_url, op_endpoint, ...)} 호출을 만든다.
 * <p>
 * task option {@code baseUrl}과 {@code endpoint}가 필요하다. {@code timeout}·{@code poll} option은 아직 operator에
 * 전달하지 못하며, 있으면 DAG를 만들 때 경고를 남긴다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class RESTfulAsyncRpcSpec extends OperatorSpec {
	private final String m_baseUrl;
	private final String m_endpoint;
	
	private RESTfulAsyncRpcSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_baseUrl, "baseUrl must not be null");
		Preconditions.checkNotNullArgument(builder.m_endpoint, "endpoint must not be null");

		m_baseUrl = builder.m_baseUrl;
		m_endpoint = builder.m_endpoint;
	}

	public String getBaseUrl() {
		return m_baseUrl;
	}

	public String getEndpoint() {
		return m_endpoint;
	}

	@Override
	public String getOperatorCall() {
		return "RestfulAsyncRpcOperator(\n"
				+ kwargLine("base_url", m_baseUrl)
				+ kwargLine("op_endpoint", m_endpoint);
	}

	public static RESTfulAsyncRpcSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		return builder()
					.fromTask(task)
					.baseUrl(requireOption(task, RESTfulAsyncRpcTask.OPTION_BASE_URL, RESTfulAsyncRpcSpec.class))
					.endpoint(requireOption(task, RESTfulAsyncRpcTask.OPTION_OPERATION_ENDPOINT,
											RESTfulAsyncRpcSpec.class))
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,RESTfulAsyncRpcSpec> {
		private String m_baseUrl;
		private String m_endpoint;

		@Override
		public RESTfulAsyncRpcSpec build() {
			return new RESTfulAsyncRpcSpec(this);
		}

		public Builder baseUrl(String baseUrl) {
			m_baseUrl = baseUrl;
			return this;
		}

		public Builder endpoint(String endpoint) {
			m_endpoint = endpoint;
			return this;
		}
	}
}

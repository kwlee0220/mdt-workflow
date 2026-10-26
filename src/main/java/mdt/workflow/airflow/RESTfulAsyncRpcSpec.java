package mdt.workflow.airflow;

import java.util.List;

import utils.Preconditions;
import utils.stream.FStream;

import mdt.workflow.airflow.OperatorArgumentSpec.SinkArgument;
import mdt.workflow.model.TaskDescriptor;

/**
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

		var baseUrlOpt = task.getOptions().get("baseUrl");
		if ( baseUrlOpt == null ) {
			throw new IllegalArgumentException("baseUrl option is required for RESTfulAsyncRpcSpec: " + task.getId());
		}
		String baseUrl = baseUrlOpt.getValue();

		var endpointOpt = task.getOptions().get("endpoint");
		if ( endpointOpt == null ) {
			throw new IllegalArgumentException("endpoint option is required for RESTfulAsyncRpcSpec: " + task.getId());
		}
		String endpoint = endpointOpt.getValue();
		
		List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
													.map(arg -> fromTaskArgument(arg.getId(), arg))
													.toList();
		List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
													.map((arg) -> fromOutputVariable(arg.getId(), arg))
													.toList();
		
		return builder()
					.taskId(task.getId())
					.baseUrl(baseUrl)
					.endpoint(endpoint)
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
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

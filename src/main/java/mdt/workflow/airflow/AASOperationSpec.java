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
public final class AASOperationSpec extends OperatorSpec {
	private final String m_instanceId;
	private final String m_submodelId;
	private final String m_path;
	
	private AASOperationSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_instanceId, "instanceId must not be null");
		Preconditions.checkNotNullArgument(builder.m_submodelId, "submodelId must not be null");
		Preconditions.checkNotNullArgument(builder.m_path, "path must not be null");

		m_instanceId = builder.m_instanceId;
		m_submodelId = builder.m_submodelId;
		m_path = builder.m_path;
	}

	public String getInstanceId() {
		return m_instanceId;
	}

	public String getSubmodelId() {
		return m_submodelId;
	}
	
	public String getPath() {
		return m_path;
	}

	@Override
	public String getOperatorCall() {
		return "AASOperationOperator(\n"
				+ kwargLine("instance", m_instanceId)
				+ kwargLine("submodel", m_submodelId)
				+ kwargLine("path", m_path);
	}

	public static AASOperationSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		var opOpt = task.getOptions().get("operation");
		if ( opOpt == null ) {
			throw new IllegalArgumentException("operation option is required for AASOperationTask: " + task.getId());
		}

		// path에 ':'가 포함될 수 있으므로 limit을 주어 세 번째 세그먼트 이후를 path에 보존한다.
		String[] splits = opOpt.getValue().split(":", 3);
		if ( splits.length < 3 ) {
			String msg = String.format("invalid operation option value: %s "
										+ "(expected format: <instanceId>:<submodelIdShort>:<path>)",
										opOpt.getValue());
			throw new IllegalArgumentException(msg);
		}
		String instanceId = splits[0];
		String submodelIdShort = splits[1];
		String path = splits[2];
		
		List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
													.map(arg -> fromTaskArgument(arg.getId(), arg))
													.toList();
		List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
													.map(arg -> fromOutputVariable(arg.getId(), arg))
													.toList();
		
		return builder()
					.taskId(task.getId())
					.instanceId(instanceId)
					.submodelId(submodelIdShort)
					.path(path)
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,AASOperationSpec> {
		private String m_instanceId;
		private String m_submodelId;
		private String m_path;

		@Override
		public AASOperationSpec build() {
			return new AASOperationSpec(this);
		}

		public Builder instanceId(String instanceId) {
			m_instanceId = instanceId;
			return this;
		}

		public Builder submodelId(String submodelId) {
			m_submodelId = submodelId;
			return this;
		}

		public Builder path(String path) {
			m_path = path;
			return this;
		}
	}
}

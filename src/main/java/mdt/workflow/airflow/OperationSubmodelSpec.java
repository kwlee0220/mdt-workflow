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
public final class OperationSubmodelSpec extends OperatorSpec {
	private final String m_instanceId;
	private final String m_submodelId;
	
	private OperationSubmodelSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_instanceId, "instanceId must not be null");
		Preconditions.checkNotNullArgument(builder.m_submodelId, "submodelId must not be null");

		m_instanceId = builder.m_instanceId;
		m_submodelId = builder.m_submodelId;
	}

	public String getInstanceId() {
		return m_instanceId;
	}

	public String getSubmodelId() {
		return m_submodelId;
	}

	@Override
	public String getOperatorCall() {
		return "OperationSubmodelOperator(\n"
				+ kwargLine("instance", m_instanceId)
				+ kwargLine("submodel", m_submodelId);
	}

	public static OperationSubmodelSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		var opOpt = task.getOptions().get("operationSubmodel");
		if ( opOpt == null ) {
			throw new IllegalArgumentException("operationSubmodel option is required for OperationSubmodelTask: "
												+ task.getId());
		}

		String[] splits = opOpt.getValue().split(":");
		if ( splits.length != 2 ) {
			String msg = String.format("invalid operationSubmodel option value: %s "
										+ "(expected format: <instanceId>:<submodelIdShort>)",
										opOpt.getValue());
			throw new IllegalArgumentException(msg);
		}
		String instanceId = splits[0];
		String submodelIdShort = splits[1];
		
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
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,OperationSubmodelSpec> {
		private String m_instanceId;
		private String m_submodelId;

		@Override
		public OperationSubmodelSpec build() {
			return new OperationSubmodelSpec(this);
		}

		public Builder instanceId(String instanceId) {
			m_instanceId = instanceId;
			return this;
		}

		public Builder submodelId(String submodelId) {
			m_submodelId = submodelId;
			return this;
		}
	}
}

package mdt.workflow.airflow;


import utils.Preconditions;

import mdt.workflow.model.TaskDescriptor;

/**
 * Operation Submodel을 호출하는 task({@code mdt.task.OperationSubmodelTask})의 Airflow spec. mdtpy
 * {@code OperationSubmodelOperator(instance, submodel, ...)} 호출을 만든다.
 * <p>
 * task option {@code operationSubmodel}({@code <instanceId>:<submodelIdShort>})에서 두 값을 꺼낸다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class OperationSubmodelSpec extends OperatorSpec {
	private final String m_instanceId;
	private final String m_submodelIdShort;
	
	private OperationSubmodelSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_instanceId, "instanceId must not be null");
		Preconditions.checkNotNullArgument(builder.m_submodelIdShort, "submodelIdShort must not be null");

		m_instanceId = builder.m_instanceId;
		m_submodelIdShort = builder.m_submodelIdShort;
	}

	public String getInstanceId() {
		return m_instanceId;
	}

	public String getSubmodelIdShort() {
		return m_submodelIdShort;
	}

	@Override
	public String getOperatorCall() {
		return "OperationSubmodelOperator(\n"
				+ kwargLine("instance", m_instanceId)
				+ kwargLine("submodel", m_submodelIdShort);
	}

	public static OperationSubmodelSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		String smExpr = requireOption(task, TaskDescriptor.OPTION_OPERATION_SUBMODEL, OperationSubmodelSpec.class);
		String[] splits = smExpr.split(":");
		if ( splits.length != 2 ) {
			String msg = String.format("invalid operationSubmodel option value: %s "
										+ "(expected format: <instanceId>:<submodelIdShort>)", smExpr);
			throw new IllegalArgumentException(msg);
		}

		return builder()
					.fromTask(task)
					.instanceId(splits[0])
					.submodelIdShort(splits[1])
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,OperationSubmodelSpec> {
		private String m_instanceId;
		private String m_submodelIdShort;

		@Override
		public OperationSubmodelSpec build() {
			return new OperationSubmodelSpec(this);
		}

		public Builder instanceId(String instanceId) {
			m_instanceId = instanceId;
			return this;
		}

		public Builder submodelIdShort(String submodelIdShort) {
			m_submodelIdShort = submodelIdShort;
			return this;
		}
	}
}

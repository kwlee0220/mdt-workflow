package mdt.workflow.airflow;


import utils.Preconditions;

import mdt.workflow.model.TaskDescriptor;

/**
 * AAS Operation 요소를 호출하는 task({@code mdt.task.AASOperationTask})의 Airflow spec. mdtpy
 * {@code AASOperationOperator(instance, submodel, path, ...)} 호출을 만든다.
 * <p>
 * task option {@code operation}({@code <instanceId>:<submodelIdShort>:<path>})에서 세 값을 꺼낸다. path에는 {@code ':'}가
 * 들어갈 수 있다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class AASOperationSpec extends OperatorSpec {
	private final String m_instanceId;
	private final String m_submodelIdShort;
	private final String m_path;
	
	private AASOperationSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_instanceId, "instanceId must not be null");
		Preconditions.checkNotNullArgument(builder.m_submodelIdShort, "submodelIdShort must not be null");
		Preconditions.checkNotNullArgument(builder.m_path, "path must not be null");

		m_instanceId = builder.m_instanceId;
		m_submodelIdShort = builder.m_submodelIdShort;
		m_path = builder.m_path;
	}

	public String getInstanceId() {
		return m_instanceId;
	}

	public String getSubmodelIdShort() {
		return m_submodelIdShort;
	}
	
	public String getPath() {
		return m_path;
	}

	@Override
	public String getOperatorCall() {
		return "AASOperationOperator(\n"
				+ kwargLine("instance", m_instanceId)
				+ kwargLine("submodel", m_submodelIdShort)
				+ kwargLine("path", m_path);
	}

	public static AASOperationSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		String opExpr = requireOption(task, TaskDescriptor.OPTION_OPERATION, AASOperationSpec.class);
		// path에 ':'가 포함될 수 있으므로 limit을 주어 세 번째 세그먼트 이후를 path에 보존한다.
		String[] splits = opExpr.split(":", 3);
		if ( splits.length < 3 ) {
			String msg = String.format("invalid operation option value: %s "
										+ "(expected format: <instanceId>:<submodelIdShort>:<path>)", opExpr);
			throw new IllegalArgumentException(msg);
		}

		return builder()
					.fromTask(task)
					.instanceId(splits[0])
					.submodelIdShort(splits[1])
					.path(splits[2])
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,AASOperationSpec> {
		private String m_instanceId;
		private String m_submodelIdShort;
		private String m_path;

		@Override
		public AASOperationSpec build() {
			return new AASOperationSpec(this);
		}

		public Builder instanceId(String instanceId) {
			m_instanceId = instanceId;
			return this;
		}

		public Builder submodelIdShort(String submodelIdShort) {
			m_submodelIdShort = submodelIdShort;
			return this;
		}

		public Builder path(String path) {
			m_path = path;
			return this;
		}
	}
}

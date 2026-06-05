package mdt.workflow.airflow;

import java.util.List;
import java.util.Set;

import utils.Preconditions;
import utils.stream.KeyValueFStream;

import mdt.workflow.model.TaskDescriptor;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class AASOperationTaskSpec extends TaskSpec {
	private final String m_instanceId;
	private final String m_submodelId;
	
	public AASOperationTaskSpec(String taskId, String instanceId, String submodelId,
								List<TaskArgument> inputs, List<TaskArgument> outputs,
								Set<String> dependencies) {
		super(taskId, inputs, outputs, dependencies);
		Preconditions.checkNotNullArgument(instanceId, "instanceId must not be null");
		Preconditions.checkNotNullArgument(submodelId, "submodelId must not be null");
		
		m_instanceId = instanceId;
		m_submodelId = submodelId;
	}

	public String getInstanceId() {
		return m_instanceId;
	}

	public String getSubmodelId() {
		return m_submodelId;
	}
	
	public static AASOperationTaskSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		var opOpt = task.getOptions().get("operation");
		Preconditions.checkArgument(opOpt != null,
									"operation option is required for AASOperationTask: %s", task.getId());

		String[] splits = opOpt.getValue().split(":");
		if ( splits.length < 2 ) {
			String msg = String.format("invalid operation option value: %s "
										+ "(expected format: <instanceId>:<submodelIdShort>)",
										opOpt.getValue());
			throw new IllegalArgumentException(msg);
		}
		String instanceId = splits[0];
		String submodelIdShort = splits[1];
		
		List<TaskArgument> inputs = KeyValueFStream.from(task.getInputArgumentSpecs())
													.map((argId, arg) -> fromVariable(argId, arg))
													.toList();
		List<TaskArgument> outputs = KeyValueFStream.from(task.getOutputArgumentSpecs())
													.map((argId, arg) -> fromVariable(argId, arg))
													.toList();
		
		return new AASOperationTaskSpec(task.getId(),
										instanceId,
										submodelIdShort,
										inputs, outputs,
										task.getDependencies());

	}
}

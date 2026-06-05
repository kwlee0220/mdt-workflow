package mdt.workflow.airflow;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import utils.Preconditions;

import mdt.task.builtin.AASOperationTask;
import mdt.task.builtin.SetTask;
import mdt.workflow.airflow.TaskArgument.LiteralArgument;
import mdt.workflow.airflow.TaskArgument.ReferenceArgument;
import mdt.workflow.airflow.TaskArgument.TaskOutputArgument;
import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.ArgumentSpec.LiteralArgumentSpec;
import mdt.workflow.model.ArgumentSpec.ReferenceArgumentSpec;
import mdt.workflow.model.TaskDescriptor;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TaskSpec {
	private final String m_taskId;
	private final List<TaskArgument> m_inputs;
	private final List<TaskArgument> m_outputs;
	private final Set<String> m_dependencies;
	
	public TaskSpec(String taskId, List<TaskArgument> inputs, List<TaskArgument> outputs,
					Set<String> dependencies) {
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(taskId),
									"invalid Task id: %s (invalid Python identifier)", taskId);
		Preconditions.checkNotNullIterableArgument(inputs, "inputs must not be null");
		Preconditions.checkNotNullIterableArgument(outputs, "outputs must not be null");
		Preconditions.checkNotNullIterableArgument(dependencies, "dependencies must not be null");
		
		m_taskId = taskId;
		m_inputs = inputs;
		m_outputs = outputs;
		m_dependencies = dependencies;
	}
	
	public String getTaskType() {
		return this.getClass().getSimpleName();
	}

	public String getTaskId() {
		return m_taskId;
	}

	public List<TaskArgument> getInputs() {
		return m_inputs;
	}

	public List<TaskArgument> getOutputs() {
		return m_outputs;
	}

	public Set<String> getDependencies() {
		return m_dependencies;
	}
	
	public static TaskSpec from(TaskDescriptor task) {
		if ( AASOperationTask.class.getName().equals(task.getType()) ) {
			return AASOperationTaskSpec.from(task);
		}
		else if ( SetTask.class.getName().equals(task.getType()) ) {
			return SetTaskSpec.from(task);
		}
		else {
			throw new IllegalArgumentException("unsupported task type: " + task.getType());
		}
	}
	
	protected static TaskArgument fromVariable(String argId, ArgumentSpec argSpec) {
		if ( argSpec instanceof ReferenceArgumentSpec refArgSpec ) {
			return new ReferenceArgument(argId, refArgSpec.getElementReference().toStringExpr());
		}
		else if ( argSpec instanceof LiteralArgumentSpec literalSpec ) {
			try {
				String jsonStr = literalSpec.readValue().toValueJsonString();
				String pythonExpr = JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid(jsonStr);
				return new LiteralArgument(argId, pythonExpr);
			}
			catch ( IOException e ) {
				throw new IllegalArgumentException("failed to convert variable to JSON: " + argId, e);
			}
		}
		else if ( argSpec instanceof ArgumentSpec.TaskOutputArgumentSpec taskOutArgSpec ) {
			return new TaskOutputArgument(argId, taskOutArgSpec.getTaskId(), taskOutArgSpec.getArgumentName());
		}
		else {
			throw new IllegalArgumentException("unsupported variable: " + argSpec);
		}
	}
}

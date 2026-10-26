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
public final class SetElementSpec extends OperatorSpec {
	private SetElementSpec(Builder builder) {
		super(builder);
	}

	@Override
	public String getOperatorCall() {
		// SetElementOperator는 operator 고유 인자가 없고 inputs/outputs만 받는다.
		return "SetElementOperator(\n";
	}

	public static SetElementSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");
		
		List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
													.map(arg -> fromTaskArgument(arg.getId(), arg))
													.toList();
		List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
													.map((arg) -> fromOutputVariable(arg.getId(), arg))
													.toList();
		
		return builder()
					.taskId(task.getId())
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,SetElementSpec> {
		@Override
		public SetElementSpec build() {
			return new SetElementSpec(this);
		}
	}
}

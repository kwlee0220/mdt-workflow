package mdt.workflow.airflow;



import mdt.workflow.model.TaskDescriptor;

/**
 * 입력 값을 출력 요소에 그대로 기록하는 task({@code mdt.task.SetTask})의 Airflow spec. mdtpy
 * {@code SetElementOperator(inputs, outputs)} 호출을 만든다. 고유 option은 없다.
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
		return builder().fromTask(task).build();
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

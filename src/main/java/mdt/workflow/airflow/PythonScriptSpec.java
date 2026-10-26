package mdt.workflow.airflow;

import java.util.List;
import java.util.Set;

import utils.Preconditions;
import utils.stream.FStream;

import mdt.workflow.airflow.OperatorArgumentSpec.SinkArgument;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;

/**
 * Python script를 실행하는 operator(mdtpy {@code PythonScriptOperator})의 spec.
 * <p>
 * 중요 task 두 개 사이의 입출력 형식이 맞지 않을 때 그 사이를 메우는 간단한 연결용 로직을 위한 것이다.
 * script는 Airflow worker에서 {@code exec()}로 실행되며, 전역 변수 {@code inputs}(입력 이름 → 해석된 값)를
 * 읽고 {@code outputs} dict에 결과를 넣는다. {@code outputs} dict에 넣은 값은 <b>모두</b> 후속 task에
 * 전달되므로, 출력 인자({@code outputArguments})는 SubmodelElement에 기록할 위치가 있는 값에만 지정한다
 * (연결용 로직에서는 대개 비어 있다).
 * <p>
 * task type은 {@link TaskDescriptors#PYTHON_SCRIPT_TASK_TYPE}이고 대응하는 Java task 클래스는 없다.
 * script 본문은 {@value #OPTION_SCRIPT} option으로 받는다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class PythonScriptSpec extends OperatorSpec {
	/** script 본문을 담는 {@link TaskDescriptor} option 이름. */
	public static final String OPTION_SCRIPT = "script";

	private final String m_script;

	private PythonScriptSpec(Builder builder) {
		super(builder);
		Preconditions.checkNotNullArgument(builder.m_script, "script must not be null");
		// 빈 script는 실행되어도 아무 출력도 넘기지 않아, 후속 task에서야 KeyError로 드러난다.
		// 생성 시점에 실패시킨다.
		Preconditions.checkArgument(!builder.m_script.isBlank(), "script must not be blank: task=%s",
									builder.m_taskId);

		m_script = builder.m_script;
	}

	public String getScript() {
		return m_script;
	}

	@Override
	public String getOperatorCall() {
		// kwargLine이 script를 Python 문자열 리터럴로 이스케이프하므로(개행 포함) 여러 줄 script도 안전하다.
		return "PythonScriptOperator(\n"
				+ kwargLine("script", m_script);
	}

	/**
	 * 인자를 직접 받아 spec을 생성한다.
	 *
	 * @param taskId		task 식별자. Python 식별자여야 한다.
	 * @param script		실행할 Python script 본문. 비어 있으면 안 된다.
	 * @param inputs		script 입력 인자 목록.
	 * @param outputs		기록할 위치가 있는 출력 목록. 기록하지 않고 전달만 할 출력은 지정하지 않는다.
	 * @param dependencies	선행 task 식별자 집합.
	 * @return	생성된 spec.
	 */
	public static PythonScriptSpec of(String taskId, String script, List<OperatorArgumentSpec> inputs,
										List<SinkArgument> outputs, Set<String> dependencies) {
		return builder()
					.taskId(taskId)
					.script(script)
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(dependencies)
					.build();
	}

	/**
	 * {@link TaskDescriptor}에서 값을 꺼내 spec을 생성한다.
	 * <p>
	 * {@link #of(String, String, List, List, Set)}와 같은 값을 채우고, task의 trigger rule도 옮긴다.
	 *
	 * @param task	{@link TaskDescriptors#PYTHON_SCRIPT_TASK_TYPE} 유형의 task.
	 * @return	생성된 spec.
	 * @throws IllegalArgumentException	{@value #OPTION_SCRIPT} option이 없거나 비어 있는 경우.
	 */
	public static PythonScriptSpec from(TaskDescriptor task) {
		Preconditions.checkNotNullArgument(task, "task must not be null");

		var scriptOpt = task.getOptions().get(OPTION_SCRIPT);
		if ( scriptOpt == null || scriptOpt.getValue() == null ) {
			throw new IllegalArgumentException("'" + OPTION_SCRIPT + "' option is required for PythonScriptSpec: "
												+ task.getId());
		}

		List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
													.map(arg -> fromTaskArgument(arg.getId(), arg))
													.toList();
		List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
													.map(arg -> fromOutputVariable(arg.getId(), arg))
													.toList();
		return builder()
					.taskId(task.getId())
					.script(scriptOpt.getValue())
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule())
					.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder extends OperatorSpec.AbstractBuilder<Builder,PythonScriptSpec> {
		private String m_script;

		@Override
		public PythonScriptSpec build() {
			return new PythonScriptSpec(this);
		}

		/**
		 * 실행할 Python script 본문을 설정한다.
		 *
		 * @param script	script 본문. 비어 있으면 안 된다.
		 * @return	이 빌더.
		 */
		public Builder script(String script) {
			m_script = script;
			return this;
		}
	}
}

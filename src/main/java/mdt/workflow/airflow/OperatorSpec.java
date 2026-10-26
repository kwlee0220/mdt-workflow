package mdt.workflow.airflow;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import utils.Preconditions;

import mdt.task.AASOperationTask;
import mdt.task.OperationSubmodelTask;
import mdt.task.RESTfulAsyncRpcTask;
import mdt.task.RESTfulRpcTask;
import mdt.task.SetTask;
import mdt.workflow.airflow.OperatorArgumentSpec.SinkArgument;
import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;
import mdt.workflow.model.TriggerRule;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class OperatorSpec {
	private final String m_taskId;
	private final List<OperatorArgumentSpec> m_inputs;
	private final List<SinkArgument> m_outputs;
	private final Set<String> m_dependencies;
	private final TriggerRule m_triggerRule;

	protected OperatorSpec(AbstractBuilder<?,?> builder) {
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(builder.m_taskId),
									"invalid Task id: %s (invalid Python identifier)", builder.m_taskId);
		Preconditions.checkNotNullIterableArgument(builder.m_inputs, "inputs must not be null");
		Preconditions.checkNotNullIterableArgument(builder.m_outputs, "outputs must not be null");
		Preconditions.checkNotNullIterableArgument(builder.m_dependencies, "dependencies must not be null");
		Preconditions.checkNotNullArgument(builder.m_triggerRule, "trigger rule must not be null");

		m_taskId = builder.m_taskId;
		m_inputs = builder.m_inputs;
		m_outputs = builder.m_outputs;
		m_dependencies = builder.m_dependencies;
		m_triggerRule = builder.m_triggerRule;
	}
	
	/**
	 * 이 spec에 대응하는 Python operator 생성자 호출문의 시작 부분을 반환한다.
	 * <p>
	 * operator 이름과 그 operator 고유의 키워드 인자(예: {@code instance}, {@code path})까지만 포함하며,
	 * 모든 operator에 공통인 {@code inputs}/{@code outputs}와 닫는 괄호는 템플릿이 이어붙인다.
	 * 반환 문자열은 operator 이름으로 시작하고(들여쓰기는 템플릿이 준다) 개행으로 끝나며, 각 인자 줄은
	 * 6칸 들여쓰기에 뒤따르는 쉼표를 포함한다.
	 * <p>
	 * 개행으로 끝나야 하는 이유: 템플릿에서 이 값을 담는 {@code {{ ... }}} 표현식 태그가 (앞 들여쓰기를
	 * 제외하고) 줄 전체를 차지하면 Pebble이 그 줄의 후행 개행을 제거한다. 반환 문자열이 자체 개행으로
	 * 끝남으로써 그 소실을 보완하여, 뒤이어 오는 {@code inputs = {}가 다음 줄에 오도록 한다.
	 * <p>
	 * 인자 값은 {@link PythonStringEscaper}로 이스케이프되므로, 이 결과를 템플릿에서 다시 {@code | py}로
	 * 이스케이프해서는 안 된다.
	 *
	 * @return	operator 생성자 호출문의 시작 부분. 항상 개행으로 끝난다.
	 */
	public abstract String getOperatorCall();

	/**
	 * operator 생성자에 전달할 키워드 인자 한 줄을 만든다.
	 * <p>
	 * 값은 Python 문자열 리터럴로 이스케이프되고, 줄은 6칸 들여쓰기, 뒤따르는 쉼표, 개행을 포함한다.
	 *
	 * @param name	키워드 인자의 이름.
	 * @param value	인자 값. Python 문자열 리터럴로 이스케이프된다.
	 * @return	{@code "      name = \"value\",\n"} 형식의 문자열.
	 */
	protected static String kwargLine(String name, String value) {
		return String.format("      %s = %s,\n", name, PythonStringEscaper.quote(value));
	}

	/**
	 * operator 생성자에 전달할 키워드 인자 한 줄을 만든다. 값은 이스케이프하지 않고 그대로 출력한다.
	 * <p>
	 * 숫자나 {@code None}처럼 Python 문자열 리터럴이 아닌 값을 넘길 때 사용한다. 값이 그대로
	 * 삽입되므로 신뢰할 수 없는 문자열을 넘겨서는 안 된다. 문자열 값에는
	 * {@link #kwargLine(String, String)}을 사용해야 한다.
	 *
	 * @param name			키워드 인자의 이름.
	 * @param pythonLiteral	Python 리터럴 표현식. 이스케이프 없이 그대로 삽입된다.
	 * @return	{@code "      name = literal,\n"} 형식의 문자열.
	 */
	protected static String rawKwargLine(String name, String pythonLiteral) {
		return String.format("      %s = %s,\n", name, pythonLiteral);
	}

	public String getTaskId() {
		return m_taskId;
	}

	public List<OperatorArgumentSpec> getInputs() {
		return m_inputs;
	}

	public List<SinkArgument> getOutputs() {
		return m_outputs;
	}

	public Set<String> getDependencies() {
		return m_dependencies;
	}

	public TriggerRule getTriggerRule() {
		return m_triggerRule;
	}

	/**
	 * 템플릿이 {@code @task}의 {@code trigger_rule} 인자로 출력할 규칙 이름을 반환한다.
	 * <p>
	 * 기본값({@link TriggerRule#ALL_SUCCESS})이면 인자를 생략하여 기존 DAG와 같은 코드를 생성하도록
	 * {@code null}을 반환한다.
	 *
	 * @return	Airflow trigger rule 이름. 기본값이면 {@code null}.
	 */
	public String getTriggerRuleName() {
		return (m_triggerRule != TriggerRule.ALL_SUCCESS) ? m_triggerRule.getName() : null;
	}

	public static OperatorSpec from(TaskDescriptor task) {
		OperatorSpec spec = fromTaskType(task);
		// 하위 spec의 from()이 trigger rule을 옮기지 않으면 실행 조건이 조용히 기본값으로 바뀌기 때문에,
		// 새 task 유형을 추가할 때 빠뜨리지 않도록 여기서 확인한다.
		if ( spec.getTriggerRule() != task.getTriggerRule() ) {
			throw new IllegalStateException(String.format("%s did not carry the trigger rule of task %s: %s",
															spec.getClass().getSimpleName(), task.getId(),
															task.getTriggerRule().getName()));
		}
		return spec;
	}

	private static OperatorSpec fromTaskType(TaskDescriptor task) {
		if ( AASOperationTask.class.getName().equals(task.getType()) ) {
			return AASOperationSpec.from(task);
		}
		else if ( SetTask.class.getName().equals(task.getType()) ) {
			return SetElementSpec.from(task);
		}
		else if ( RESTfulAsyncRpcTask.class.getName().equals(task.getType()) ) {
			return RESTfulAsyncRpcSpec.from(task);
		}
		else if ( RESTfulRpcTask.class.getName().equals(task.getType()) ) {
			return RESTfulRpcSpec.from(task);
		}
		else if ( OperationSubmodelTask.class.getName().equals(task.getType()) ) {
			return OperationSubmodelSpec.from(task);
		}
		// PythonScriptTask는 Java task 클래스가 없는 Airflow 전용 type이므로 type 문자열 상수와 비교한다.
		else if ( TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE.equals(task.getType()) ) {
			return PythonScriptSpec.from(task);
		}
		else {
			throw new IllegalArgumentException("unsupported task type: " + task.getType());
		}
	}
	
	protected static OperatorArgumentSpec fromTaskArgument(String argId, ArgumentSpec argSpec) {
		return switch( argSpec ) {
			case ArgumentSpec.ReferenceArgument refArgSpec
					-> new OperatorArgumentSpec.ReferenceArgument(argId, refArgSpec.getReferenceString());
			case ArgumentSpec.LiteralArgument literalSpec -> {
				try {
					// 타입 정보를 보존하기 위해 값 JSON이 아닌 polymorphic JSON을 사용한다.
					String jsonStr = literalSpec.getValue().toJsonString();
					String pythonExpr = JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid(jsonStr);
					yield new OperatorArgumentSpec.LiteralArgument(argId, pythonExpr);
				}
				catch ( IOException e ) {
					throw new IllegalArgumentException("failed to convert variable to JSON: " + argId, e);
				}
			}
			case ArgumentSpec.TaskOutputArgument taskOutArgSpec
					-> new OperatorArgumentSpec.TaskOutputArgument(argId, taskOutArgSpec.getTaskId(),
																	taskOutArgSpec.getArgumentName());
			default -> throw new IllegalArgumentException("unsupported variable: " + argSpec);
		};
	}
	
	protected static SinkArgument fromOutputVariable(String argId, ArgumentSpec.ReferenceArgument argSpec) {
		return new SinkArgument(argId, argSpec.getReferenceString());
	}

	/**
	 * {@link OperatorSpec}와 그 하위 클래스들의 builder를 위한 공통 추상 builder 클래스.
	 *
	 * @param <B>	builder 타입 (method chaining을 위한 self type)
	 * @param <T>	build 대상 {@link OperatorSpec} 타입
	 */
	public abstract static class AbstractBuilder<B extends AbstractBuilder<B,T>, T extends OperatorSpec> {
		protected String m_taskId;
		protected List<OperatorArgumentSpec> m_inputs = Lists.newArrayList();
		protected List<SinkArgument> m_outputs = Lists.newArrayList();
		protected Set<String> m_dependencies = Sets.newLinkedHashSet();
		protected TriggerRule m_triggerRule = TriggerRule.ALL_SUCCESS;

		public abstract T build();

		@SuppressWarnings("unchecked")
		protected B self() {
			return (B)this;
		}

		public B taskId(String taskId) {
			m_taskId = taskId;
			return self();
		}

		public B inputs(List<OperatorArgumentSpec> inputs) {
			Preconditions.checkNotNullArgument(inputs, "inputs must not be null");

			m_inputs = Lists.newArrayList(inputs);
			return self();
		}

		public B addInput(OperatorArgumentSpec input) {
			Preconditions.checkNotNullArgument(input, "input must not be null");

			m_inputs.add(input);
			return self();
		}

		public B outputs(List<SinkArgument> outputs) {
			Preconditions.checkNotNullArgument(outputs, "outputs must not be null");

			m_outputs = Lists.newArrayList(outputs);
			return self();
		}

		public B addOutput(SinkArgument output) {
			Preconditions.checkNotNullArgument(output, "output must not be null");

			m_outputs.add(output);
			return self();
		}

		public B dependencies(Set<String> dependencies) {
			Preconditions.checkNotNullArgument(dependencies, "dependencies must not be null");

			m_dependencies = Sets.newLinkedHashSet(dependencies);
			return self();
		}

		public B addDependency(String taskId) {
			Preconditions.checkNotNullArgument(taskId, "dependency taskId must not be null");

			m_dependencies.add(taskId);
			return self();
		}

		public B triggerRule(TriggerRule rule) {
			Preconditions.checkNotNullArgument(rule, "trigger rule must not be null");

			m_triggerRule = rule;
			return self();
		}
	}
}

package mdt.workflow.airflow;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Preconditions;
import utils.stream.FStream;

import mdt.task.AASOperationTask;
import mdt.task.OperationSubmodelTask;
import mdt.task.RESTfulAsyncRpcTask;
import mdt.task.RESTfulRpcTask;
import mdt.task.SetTask;
import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;
import mdt.workflow.model.TriggerRule;

/**
 * DAG의 task 하나(mdtpy Airflow operator 호출 하나)를 나타내는 spec의 공통 부모.
 * <p>
 * {@link #from(TaskDescriptor)}가 task type에 맞는 하위 spec을 등록표에서 골라 만든다. 하위 spec은
 * {@link #getOperatorCall()}로 operator 이름과 고유 인자를 만들고, 템플릿이 {@code inputs}/{@code outputs}를 이어붙인다.
 * 새 task 유형을 추가하는 방법은 {@code docs/adding-operator.md}에 있다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class OperatorSpec {
	private static final Logger s_logger = LoggerFactory.getLogger(OperatorSpec.class);
	/** Java 태스크가 쓰지만 Airflow operator에는 아직 전달하지 못할 수 있는 실행 옵션. */
	private static final List<String> EXECUTION_OPTIONS = List.of(TaskDescriptor.OPTION_TIMEOUT, TaskDescriptor.OPTION_POLL_INTERVAL);
	
	private final String m_taskId;
	private final List<DagArgument> m_inputs;
	private final List<DagSink> m_outputs;
	private final Set<String> m_dependencies;
	private final TriggerRule m_triggerRule;

	protected OperatorSpec(AbstractBuilder<?,?> builder) {
		Preconditions.checkArgument(PythonIdentifiers.isValid(builder.m_taskId),
									"invalid Task id: %s (invalid Python identifier)", builder.m_taskId);
		Preconditions.checkNotNullIterableArgument(builder.m_inputs, "inputs must not be null");
		Preconditions.checkNotNullIterableArgument(builder.m_outputs, "outputs must not be null");
		Preconditions.checkNotNullIterableArgument(builder.m_dependencies, "dependencies must not be null");
		Preconditions.checkNotNullArgument(builder.m_triggerRule, "trigger rule must not be null");

		m_taskId = builder.m_taskId;
		// builder의 컬렉션을 복사해 불변으로 보관한다. 2026-11-04 이전에는 그대로 공유해서 builder를 다시 쓰거나
		// getter 결과를 고치면 이미 만든 spec이 바뀌었다. 의존 task는 선언 순서를 유지한다.
		m_inputs = List.copyOf(builder.m_inputs);
		m_outputs = List.copyOf(builder.m_outputs);
		m_dependencies = Collections.unmodifiableSet(new LinkedHashSet<>(builder.m_dependencies));
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
	 * 끝남으로써 그 소실을 보완하여, 뒤이어 오는 <code>inputs = {</code>가 다음 줄에 오도록 한다.
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

	public List<DagArgument> getInputs() {
		return m_inputs;
	}

	public List<DagSink> getOutputs() {
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

	/**
	 * 이 spec이 Airflow operator에 전달하는 실행 옵션({@link TaskDescriptor#OPTION_TIMEOUT}, {@link TaskDescriptor#OPTION_POLL_INTERVAL})
	 * 이름을 반환한다. 기본은 없음이다.
	 *
	 * @return	전달하는 실행 옵션 이름.
	 */
	protected Set<String> getSupportedExecutionOptions() {
		return Set.of();
	}
	
	/**
	 * task에 지정되어 있지만 이 spec이 Airflow operator에 전달하지 못해 무시되는 실행 옵션을 반환한다.
	 *
	 * @param task	task 기술자.
	 * @param spec	task에서 만든 spec.
	 * @return	무시되는 옵션 이름(지정 순서). 없으면 빈 목록.
	 */
	static List<String> getIgnoredExecutionOptions(TaskDescriptor task, OperatorSpec spec) {
		Set<String> supported = spec.getSupportedExecutionOptions();
		return EXECUTION_OPTIONS.stream()
								.filter(opt -> task.getOptions().containsKey(opt) && !supported.contains(opt))
								.toList();
	}
	
	public static OperatorSpec from(TaskDescriptor task) {
		OperatorSpec spec = fromTaskType(task);
		// Java 태스크로 실행할 때는 쓰이는 옵션이 Airflow에서는 조용히 무시되었다(2026-11-04 이전). mdtpy operator가
		// 받을 때까지는 DAG를 만들 때 경고를 남긴다.
		List<String> ignored = getIgnoredExecutionOptions(task, spec);
		if ( !ignored.isEmpty() ) {
			s_logger.warn("task options are not supported by the Airflow operator and will be ignored: "
							+ "task={}, type={}, options={}", task.getId(), spec.getClass().getSimpleName(), ignored);
		}
		// 하위 spec의 from()이 trigger rule을 옮기지 않으면 실행 조건이 조용히 기본값으로 바뀌기 때문에,
		// 새 task 유형을 추가할 때 빠뜨리지 않도록 여기서 확인한다.
		if ( spec.getTriggerRule() != task.getTriggerRule() ) {
			throw new IllegalStateException(String.format("%s did not carry the trigger rule of task %s: %s",
															spec.getClass().getSimpleName(), task.getId(),
															task.getTriggerRule().getName()));
		}
		return spec;
	}

	/**
	 * task type → spec 생성 함수 등록표. 새 task 유형은 여기에 한 줄을 더한다.
	 * <p>
	 * Java task 클래스가 없는 Airflow 전용 유형({@link TaskDescriptors#PYTHON_SCRIPT_TASK_TYPE})은 type 문자열 상수로
	 * 등록한다. 2026-11-04 이전에는 if-else 연쇄였다.
	 */
	private static final Map<String,Function<TaskDescriptor,? extends OperatorSpec>> FACTORIES = Map.of(
		AASOperationTask.class.getName(), AASOperationSpec::from,
		SetTask.class.getName(), SetElementSpec::from,
		RESTfulAsyncRpcTask.class.getName(), RESTfulAsyncRpcSpec::from,
		RESTfulRpcTask.class.getName(), RESTfulRpcSpec::from,
		OperationSubmodelTask.class.getName(), OperationSubmodelSpec::from,
		TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE, PythonScriptSpec::from
	);
	
	/**
	 * DAG로 만들 수 있는 task type 목록을 반환한다.
	 *
	 * @return	지원하는 task type(정렬됨).
	 */
	public static List<String> getSupportedTaskTypes() {
		return FACTORIES.keySet().stream().sorted().toList();
	}
	
	private static OperatorSpec fromTaskType(TaskDescriptor task) {
		var factory = FACTORIES.get(task.getType());
		if ( factory == null ) {
			throw new IllegalArgumentException("unsupported task type: " + task.getType()
												+ " (supported: " + getSupportedTaskTypes() + ")");
		}
		return factory.apply(task);
	}
	
	/**
	 * task의 필수 option 값을 반환한다.
	 *
	 * @param task		task 기술자.
	 * @param key		option 이름.
	 * @param specClass	오류 메시지에 쓸 spec 클래스.
	 * @return	option 값.
	 * @throws IllegalArgumentException	option이 없거나 값이 {@code null}인 경우.
	 */
	protected static String requireOption(TaskDescriptor task, String key, Class<? extends OperatorSpec> specClass) {
		var opt = task.getOptions().get(key);
		if ( opt == null || opt.getValue() == null ) {
			throw new IllegalArgumentException(String.format("'%s' option is required for %s: %s",
															key, specClass.getSimpleName(), task.getId()));
		}
		return opt.getValue();
	}
	
	protected static DagArgument fromArgumentSpec(String argId, ArgumentSpec argSpec) {
		return switch( argSpec ) {
			case ArgumentSpec.ReferenceArgument refArgSpec
					-> new DagArgument.Reference(argId, refArgSpec.getReferenceExpr());
			case ArgumentSpec.LiteralArgument literalSpec -> {
				try {
					// 타입 정보를 보존하기 위해 값 JSON이 아닌 polymorphic JSON을 사용한다.
					String jsonStr = literalSpec.getValue().toJsonString();
					String pythonExpr = PythonLiterals.fromJson(jsonStr);
					yield new DagArgument.Literal(argId, pythonExpr);
				}
				catch ( IOException e ) {
					throw new IllegalArgumentException("failed to convert variable to JSON: " + argId, e);
				}
			}
			case ArgumentSpec.TaskOutputArgument taskOutArgSpec
					-> new DagArgument.TaskOutput(argId, taskOutArgSpec.getTaskId(),
																	taskOutArgSpec.getOutputArgumentId());
			default -> throw new IllegalArgumentException("unsupported variable: " + argSpec);
		};
	}
	
	protected static DagSink fromOutputVariable(String argId, ArgumentSpec.ReferenceArgument argSpec) {
		return new DagSink(argId, argSpec.getReferenceExpr());
	}

	/**
	 * {@link OperatorSpec}와 그 하위 클래스들의 builder를 위한 공통 추상 builder 클래스.
	 *
	 * @param <B>	builder 타입 (method chaining을 위한 self type)
	 * @param <T>	build 대상 {@link OperatorSpec} 타입
	 */
	public abstract static class AbstractBuilder<B extends AbstractBuilder<B,T>, T extends OperatorSpec> {
		protected String m_taskId;
		protected List<DagArgument> m_inputs = Lists.newArrayList();
		protected List<DagSink> m_outputs = Lists.newArrayList();
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

		public B inputs(List<DagArgument> inputs) {
			Preconditions.checkNotNullArgument(inputs, "inputs must not be null");

			m_inputs = Lists.newArrayList(inputs);
			return self();
		}

		public B addInput(DagArgument input) {
			Preconditions.checkNotNullArgument(input, "input must not be null");

			m_inputs.add(input);
			return self();
		}

		public B outputs(List<DagSink> outputs) {
			Preconditions.checkNotNullArgument(outputs, "outputs must not be null");

			m_outputs = Lists.newArrayList(outputs);
			return self();
		}

		public B addOutput(DagSink output) {
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

		/**
		 * task 기술자에서 모든 spec에 공통인 값(task id, 입력·출력 인자, 의존 task, trigger rule)을 채운다.
		 * <p>
		 * 하위 spec의 {@code from(TaskDescriptor)}는 이 메소드를 부른 뒤 자기 고유의 option만 처리한다.
		 * 2026-11-04 이전에는 여섯 spec이 이 변환을 각자 반복했다.
		 *
		 * @param task	task 기술자.
		 * @return	이 빌더.
		 */
		public B fromTask(TaskDescriptor task) {
			Preconditions.checkNotNullArgument(task, "task must not be null");

			List<DagArgument> inputs = FStream.from(task.getInputArguments())
														.map(arg -> fromArgumentSpec(arg.getId(), arg))
														.toList();
			List<DagSink> outputs = FStream.from(task.getOutputArguments())
														.map(arg -> fromOutputVariable(arg.getId(), arg))
														.toList();
			return taskId(task.getId())
					.inputs(inputs)
					.outputs(outputs)
					.dependencies(task.getDependencies())
					.triggerRule(task.getTriggerRule());
		}
	}
}

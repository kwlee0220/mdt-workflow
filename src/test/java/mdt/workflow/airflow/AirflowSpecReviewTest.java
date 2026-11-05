package mdt.workflow.airflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import mdt.task.MDTTask;
import mdt.task.RESTfulAsyncRpcTask;
import mdt.task.RESTfulRpcTask;
import mdt.workflow.model.TaskDescriptor;


/**
 * mdt-workflow 검토 D17·D20 회귀 테스트.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowSpecReviewTest {
	/**
	 * 무한대·NaN은 Python에서 정의된 표현으로 출력한다(D17).
	 */
	@Test
	public void nonFiniteNumbersBecomeValidPython() throws Exception {
		assertEquals("[float('inf'), float('-inf'), 1.5]",
					PythonLiterals.fromJson("[1e400, -1e400, 1.5]"));
	}

	/**
	 * Airflow operator가 받지 못하는 timeout·poll 옵션을 찾는다(D20). RESTfulRpcSpec만 timeout을 전달한다.
	 */
	@Test
	public void ignoredExecutionOptionsAreDetected() {
		TaskDescriptor async = new TaskDescriptor("a", "a", RESTfulAsyncRpcTask.class.getName());
		async.addOption(RESTfulAsyncRpcTask.OPTION_BASE_URL, "http://h:8080");
		async.addOption(RESTfulAsyncRpcTask.OPTION_OPERATION_ENDPOINT, "/op");
		async.addOption(MDTTask.OPTION_TIMEOUT, "30s");
		async.addOption(MDTTask.OPTION_POLL_INTERVAL, "1s");
		assertEquals(List.of(MDTTask.OPTION_TIMEOUT, MDTTask.OPTION_POLL_INTERVAL),
					OperatorSpec.getIgnoredExecutionOptions(async, OperatorSpec.from(async)));

		TaskDescriptor rpc = new TaskDescriptor("r", "r", RESTfulRpcTask.class.getName());
		rpc.addOption(RESTfulRpcTask.OPTION_URL, "http://h:8080/op");
		rpc.addOption(MDTTask.OPTION_TIMEOUT, "30s");
		assertEquals(List.of(), OperatorSpec.getIgnoredExecutionOptions(rpc, OperatorSpec.from(rpc)));
	}

	/**
	 * task type 등록표(I3)와 필수 option 검사(I2).
	 */
	@Test
	public void registryAndRequiredOptions() {
		assertEquals(6, OperatorSpec.getSupportedTaskTypes().size());

		IllegalArgumentException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
								() -> OperatorSpec.from(new TaskDescriptor("x", "x", "Foo")));
		org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().startsWith("unsupported task type: Foo (supported: ["),
													e.getMessage());

		TaskDescriptor async = new TaskDescriptor("a", "a", RESTfulAsyncRpcTask.class.getName());
		async.addOption(RESTfulAsyncRpcTask.OPTION_BASE_URL, "http://h:8080");
		e = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> OperatorSpec.from(async));
		assertEquals("'endpoint' option is required for RESTfulAsyncRpcSpec: a", e.getMessage());
	}

	/**
	 * 공통 값(입력·출력·의존·trigger rule)은 {@code AbstractBuilder.fromTask}가 옮긴다(I1).
	 */
	@Test
	public void fromTaskCopiesCommonFields() {
		TaskDescriptor set = new TaskDescriptor("t2", "t2", mdt.task.SetTask.class.getName());
		set.addInputArgument(mdt.workflow.model.ArgumentSpec.reference("In", "param:inst:In"));
		set.addOutputArgument(mdt.workflow.model.ArgumentSpec.reference("Out", "param:inst:Out"));
		set.addDependency("t1");
		set.setTriggerRule(mdt.workflow.model.TriggerRule.ALL_DONE);

		OperatorSpec spec = OperatorSpec.from(set);
		assertEquals("t2", spec.getTaskId());
		assertEquals(List.of("In"), spec.getInputs().stream().map(DagArgument::getId).toList());
		assertEquals(List.of("Out"), spec.getOutputs().stream().map(DagSink::getId).toList());
		assertEquals(java.util.Set.of("t1"), spec.getDependencies());
		assertEquals(mdt.workflow.model.TriggerRule.ALL_DONE, spec.getTriggerRule());
	}

	/**
	 * spec은 builder의 컬렉션을 공유하지 않고, getter 결과는 고칠 수 없다(I7).
	 */
	@Test
	public void specCollectionsAreImmutableCopies() {
		SetElementSpec.Builder builder = SetElementSpec.builder().taskId("t").addDependency("a");
		SetElementSpec spec = builder.build();
		builder.addDependency("b");
		assertEquals(java.util.Set.of("a"), spec.getDependencies());
		org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
													() -> spec.getDependencies().add("c"));
		org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
													() -> spec.getInputs().clear());
	}
}

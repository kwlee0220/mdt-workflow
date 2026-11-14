package mdt.workflow.airflow;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mdt.task.SetTask;
import mdt.workflow.WorkflowModel;
import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.TaskDescriptor;

/**
 * {@link DagSpec} 생성 시점의 검증을 확인한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class DagSpecTest {
	@Test
	public void emptyTaskListIsRejected() {
		// task가 없으면 템플릿이 본문 없는 'def _dag():'를 생성하므로 생성 시점에 거부되어야 한다.
		WorkflowModel model = new WorkflowModel();
		model.setId("empty_wf");
		model.setTaskDescriptors(List.of());

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
																() -> new DagSpec(model));
		Assertions.assertTrue(e.getMessage().contains("DAG has no task"), e.getMessage());
	}

	@Test
	public void taskOutputFromIndirectUpstreamIsAccepted() {
		// c는 b를 거쳐 a 뒤에 수행되므로 a의 출력을 읽을 수 있다.
		TaskDescriptor a = setTask("a");
		TaskDescriptor b = setTask("b", "a");
		TaskDescriptor c = setTask("c", "b");
		c.addInputArgument(ArgumentSpec.taskOutput("x", "a", "target"));
		Assertions.assertEquals(3, new DagSpec(model(a, b, c)).getTasks().size());
	}

	@Test
	public void taskOutputFromUnknownTaskIsRejected() {
		TaskDescriptor a = setTask("a");
		a.addInputArgument(ArgumentSpec.taskOutput("x", "nope", "target"));

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
																() -> new DagSpec(model(a)));
		Assertions.assertTrue(e.getMessage().contains("unknown task-output source task: nope (input 'x' of task: a)"),
								e.getMessage());
	}

	@Test
	public void taskOutputFromNonUpstreamTaskIsRejected() {
		// b가 a의 출력을 읽지만 a를 선행 task로 두지 않았다.
		TaskDescriptor a = setTask("a");
		TaskDescriptor b = setTask("b");
		b.addInputArgument(ArgumentSpec.taskOutput("x", "a", "target"));

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
																() -> new DagSpec(model(a, b)));
		Assertions.assertTrue(e.getMessage().contains("task-output source task is not upstream: a (input 'x' of task: b)"),
								e.getMessage());
	}

	@Test
	public void taskOutputCheckTerminatesOnDependencyCycle() {
		// 순환 의존은 이 검사의 대상이 아니지만, 선행 task를 모으는 중 무한 반복하지 않아야 한다.
		TaskDescriptor a = setTask("a", "b");
		TaskDescriptor b = setTask("b", "a");
		TaskDescriptor c = setTask("c");
		b.addInputArgument(ArgumentSpec.taskOutput("x", "c", "target"));

		Assertions.assertThrows(IllegalArgumentException.class, () -> new DagSpec(model(a, b, c)));
	}

	private static TaskDescriptor setTask(String id, String... deps) {
		TaskDescriptor task = new TaskDescriptor(id, id, SetTask.class.getName());
		task.addDependency(deps);
		task.addInputArgument(ArgumentSpec.reference("source", "param:i:p"));
		return task;
	}

	private static WorkflowModel model(TaskDescriptor... tasks) {
		WorkflowModel model = new WorkflowModel();
		model.setId("wf");
		model.setTaskDescriptors(List.of(tasks));
		return model;
	}
}

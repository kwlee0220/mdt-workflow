package mdt.workflow.airflow;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mdt.workflow.WorkflowModel;

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
}

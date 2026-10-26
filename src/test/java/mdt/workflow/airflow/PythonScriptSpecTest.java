package mdt.workflow.airflow;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;

/**
 * {@link PythonScriptSpec}의 생성 경로({@code of()} / {@code from(TaskDescriptor)})를 검증한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class PythonScriptSpecTest {
	private static TaskDescriptor newTask(String script) {
		TaskDescriptor task = new TaskDescriptor("dec_by_5", "dec_by_5", TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE);
		task.addDependency("get_heater_cycle_time");
		task.addInputArgument(ArgumentSpec.taskOutput("value", "get_heater_cycle_time", "target"));
		if ( script != null ) {
			task.addOption(PythonScriptSpec.OPTION_SCRIPT, script);
		}
		return task;
	}

	@Test
	public void operatorSpecFromDispatchesByTypeConstant() {
		// Java task 클래스가 없으므로 type 문자열 상수로 PythonScriptSpec이 선택되어야 한다.
		OperatorSpec spec = OperatorSpec.from(newTask("outputs['result'] = 1"));
		Assertions.assertInstanceOf(PythonScriptSpec.class, spec);
	}

	@Test
	public void fromReadsScriptInputsAndDependencies() {
		PythonScriptSpec spec = PythonScriptSpec.from(newTask("outputs['result'] = inputs['value'].to_raw_object() - 5"));

		Assertions.assertEquals("dec_by_5", spec.getTaskId());
		Assertions.assertEquals("outputs['result'] = inputs['value'].to_raw_object() - 5", spec.getScript());
		Assertions.assertEquals(Set.of("get_heater_cycle_time"), spec.getDependencies());
		Assertions.assertEquals(1, spec.getInputs().size());
		Assertions.assertEquals("\"value\": mdtpy.airflow.task_output(\"get_heater_cycle_time\", \"target\")",
								spec.getInputs().get(0).getPythonStatement());
		// 연결용 로직은 출력을 선언하지 않아도 전달되므로 출력 인자가 비어 있다.
		Assertions.assertTrue(spec.getOutputs().isEmpty());
	}

	@Test
	public void ofBuildsSameSpecAsFrom() {
		PythonScriptSpec spec = PythonScriptSpec.of("glue", "outputs['x'] = 1", List.of(), List.of(), Set.of());
		Assertions.assertEquals("glue", spec.getTaskId());
		Assertions.assertEquals("outputs['x'] = 1", spec.getScript());
	}

	@Test
	public void missingScriptOptionIsRejected() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
															() -> PythonScriptSpec.from(newTask(null)));
		Assertions.assertTrue(e.getMessage().contains("'script' option is required"), e.getMessage());
	}

	@Test
	public void blankScriptIsRejected() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> PythonScriptSpec.from(newTask("   \n ")));
	}
}

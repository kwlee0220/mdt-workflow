package mdt.workflow.airflow;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import mdt.task.AASOperationTask;
import mdt.task.OperationSubmodelTask;
import mdt.task.RESTfulAsyncRpcTask;
import mdt.task.RESTfulRpcTask;
import mdt.task.SetTask;
import mdt.workflow.WorkflowModel;
import mdt.workflow.model.TaskDescriptor;
import mdt.workflow.model.TaskDescriptors;
import mdt.workflow.model.TriggerRule;

/**
 * task의 trigger rule이 {@link OperatorSpec}을 거쳐 생성된 DAG의 {@code @task} 인자로 전달되는지 검증한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class TriggerRuleTest {
	/** 지원하는 모든 task 유형을 spec 생성에 필요한 최소 option으로 만든다. */
	private static List<TaskDescriptor> everyTaskType() {
		TaskDescriptor aas = new TaskDescriptor("aas", "aas", AASOperationTask.class.getName());
		aas.addOption("operation", "inst:Data:Operations.Op");
		TaskDescriptor set = new TaskDescriptor("set", "set", SetTask.class.getName());
		TaskDescriptor opSubmodel = new TaskDescriptor("op_submodel", "op_submodel", OperationSubmodelTask.class.getName());
		opSubmodel.addOption("operationSubmodel", "inst:Simulation");
		TaskDescriptor rpc = new TaskDescriptor("rpc", "rpc", RESTfulRpcTask.class.getName());
		rpc.addOption(RESTfulRpcTask.OPTION_URL, "http://h:8080/operations/Detect");
		TaskDescriptor asyncRpc = new TaskDescriptor("async_rpc", "async_rpc", RESTfulAsyncRpcTask.class.getName());
		asyncRpc.addOption(RESTfulAsyncRpcTask.OPTION_BASE_URL, "http://h:8080");
		asyncRpc.addOption(RESTfulAsyncRpcTask.OPTION_OPERATION_ENDPOINT, "/op");
		TaskDescriptor script = new TaskDescriptor("script", "script", TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE);
		script.addOption(PythonScriptSpec.OPTION_SCRIPT, "outputs['x'] = 1");
		return List.of(aas, set, opSubmodel, rpc, asyncRpc, script);
	}

	@Test
	public void everyTaskTypeCarriesTriggerRule() {
		for ( TaskDescriptor task: everyTaskType() ) {
			OperatorSpec spec = OperatorSpec.from(task);
			Assertions.assertEquals(TriggerRule.ALL_SUCCESS, spec.getTriggerRule(), task.getType());
			Assertions.assertNull(spec.getTriggerRuleName(), task.getType());

			for ( TriggerRule rule: TriggerRule.values() ) {
				task.setTriggerRule(rule);
				spec = OperatorSpec.from(task);
				Assertions.assertEquals(rule, spec.getTriggerRule(), task.getType());
			}
			Assertions.assertEquals("one_failed", spec.getTriggerRuleName(), task.getType());
		}
	}

	@Test
	public void dagEmitsTriggerRuleOnlyForNonDefaultTasks() throws IOException {
		TaskDescriptor pick = new TaskDescriptor("pick", "pick", AASOperationTask.class.getName());
		pick.addOption("operation", "panda01:Data:Operations.Grasp");
		TaskDescriptor recover = new TaskDescriptor("recover", "recover", TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE);
		recover.addOption(PythonScriptSpec.OPTION_SCRIPT, "pass");
		recover.addDependency("pick");
		TaskDescriptor home = new TaskDescriptor("return_home", "return_home", AASOperationTask.class.getName());
		home.addOption("operation", "panda01:Data:Operations.MoveToNamedTarget");
		home.addDependency("pick", "recover");
		home.setTriggerRule(TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS);

		String dag = render(List.of(pick, recover, home));
		Assertions.assertTrue(dag.contains("  @task(task_id=\"return_home\", trigger_rule=\"none_failed_min_one_success\")\n"), dag);
		Assertions.assertTrue(dag.contains("  @task(task_id=\"pick\")\n"), dag);
		Assertions.assertTrue(dag.contains("  @task(task_id=\"recover\")\n"), dag);
		Assertions.assertEquals(1, dag.split("trigger_rule=", -1).length - 1, dag);
	}

	@Test
	public void workflowJsonRuleReachesDag() throws IOException {
		String json = """
			{"id":"wf","taskDescriptors":[
			  {"id":"place","type":"mdt.task.AASOperationTask",
			   "options":[{"name":"operation","value":"panda01:Data:Operations.Place"}]},
			  {"id":"return_home","type":"mdt.task.AASOperationTask","dependencies":["place"],"triggerRule":"all_done",
			   "options":[{"name":"operation","value":"panda01:Data:Operations.MoveToNamedTarget"}]}]}
			""";
		WorkflowModel model = WorkflowModel.parseJsonString(json);
		String dag = render(model);
		Assertions.assertTrue(dag.contains("@task(task_id=\"return_home\", trigger_rule=\"all_done\")"), dag);
	}

	private static String render(List<TaskDescriptor> tasks) throws IOException {
		WorkflowModel model = new WorkflowModel();
		model.setId("trigger_rule_wf");
		model.setTaskDescriptors(tasks);
		return render(model);
	}

	private static String render(WorkflowModel model) throws IOException {
		StringWriter writer = new StringWriter();
		AirflowDagGenerator.generate(model, writer);
		return writer.toString();
	}
}

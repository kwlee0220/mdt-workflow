package mdt.workflow.airflow;

import java.io.File;
import java.io.StringWriter;

import utils.StrSubstitutor;
import utils.io.FileUtils;
import utils.io.IOUtils;

import mdt.model.MDTModelSerDe;
import mdt.task.AASOperationTask;
import mdt.task.OperationSubmodelTask;
import mdt.task.RESTfulAsyncRpcTask;
import mdt.task.SetTask;
import mdt.workflow.WorkflowModel;
import mdt.workflow.model.ArgumentSpec;
import mdt.workflow.model.TaskDescriptor;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAirflowDagGenerator {
	private static final String HTTP_OP_SERVER_BASE_URL_FORMAT = "http://${MDT_HOST}:12987";

	public static final void main(String... args) throws Exception {
		WorkflowModel wfDesc;
		TaskDescriptor taskDesc;
		
		String opServerBaseUrl = StrSubstitutor.with(System.getenv()).replace(HTTP_OP_SERVER_BASE_URL_FORMAT);
		
		wfDesc = new WorkflowModel();
		wfDesc.setId("test_full_workflow");
		wfDesc.setName("테스트 시뮬레이션");
		wfDesc.setDescription("본 워크플로우는 시뮬레이션 연동을 확인하기 위한 테스트 목적으로 작성됨.");

		taskDesc = createOperationSubmodelTask("inspection", "inspector:ThicknessInspection");
		taskDesc.addInputArgument(ArgumentSpec.reference("UpperImage", "param:inspector:UpperImage"));
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		taskDesc = createRESTfulAsyncRpcTask("update_defect_list", opServerBaseUrl,
												"/api/v1/operations/UpdateDefectList");
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("Defect", "inspection", "Defect"));
		taskDesc.addInputArgument(ArgumentSpec.reference("DefectList", "param:inspector:DefectList"));
		taskDesc.addOutputArgument(ArgumentSpec.reference("UpdatedDefectList", "param:inspector:DefectList"));
		taskDesc.addDependency("inspection");
		wfDesc.getTaskDescriptors().add(taskDesc);

		taskDesc = createAASOperationTask("simulation", "inspector:ProcessSimulation:Operation");
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("DefectList", "update_defect_list", "UpdatedDefectList"));
		taskDesc.addOutputArgument(ArgumentSpec.reference("AverageCycleTime", "param:inspector:CycleTime"));
		taskDesc.addDependency("update_defect_list");
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		taskDesc = createCopyOperator("get_heater_cycle_time");
		taskDesc.addInputArgument(ArgumentSpec.reference("source", "param:heater:CycleTime"));
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		taskDesc = createCopyOperator("get_trimmer_cycle_time");
		taskDesc.addInputArgument(ArgumentSpec.reference("source", "param:trimmer:CycleTime"));
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		taskDesc = createCopyOperator("get_former_cycle_time");
		taskDesc.addInputArgument(ArgumentSpec.reference("source", "param:former:CycleTime"));
		wfDesc.getTaskDescriptors().add(taskDesc);

		taskDesc = createOperationSubmodelTask("optimization", "innercase:ProcessOptimization");
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("HTCycleTime", "get_heater_cycle_time", "target"));
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("PTCycleTime", "get_trimmer_cycle_time", "target"));
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("VFCycleTime", "get_former_cycle_time", "target"));
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("QICycleTime", "simulation", "AverageCycleTime"));
		taskDesc.addOutputArgument(ArgumentSpec.reference("TotalThroughput", "param:innercase:CycleTime"));
		taskDesc.addDependency("get_heater_cycle_time", "get_trimmer_cycle_time", "get_former_cycle_time", "simulation");
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		taskDesc = createRESTfulAsyncRpcTask("count_records", opServerBaseUrl,
												"/api/v1/operations/CountRecords");
		taskDesc.addInputArgument(ArgumentSpec.reference("TimeSeriesData",
										"timeseries:Welder:NozzleProductionLog#last=50s|Time,QuantityProduced"));
		wfDesc.getTaskDescriptors().add(taskDesc);

		taskDesc = createAASOperationTask("add_and_sleep", "test:AddAndSleep:Operation");
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("Data", "get_heater_cycle_time", "target"));
		taskDesc.addInputArgument(ArgumentSpec.taskOutput("IncAmount", "count_records", "Count"));
		taskDesc.addInputArgument(ArgumentSpec.literal("SleepTime", 3.5));
		taskDesc.addOutputArgument(ArgumentSpec.reference("Output", "param:test:Data"));
		taskDesc.addDependency("get_heater_cycle_time", "count_records");
		wfDesc.getTaskDescriptors().add(taskDesc);
		
		String wfJsonStr = MDTModelSerDe.toJsonString(wfDesc);
		File wfFile = FileUtils.path(FileUtils.getUserHomeDir(), "tmp", "wf_test.json");
		IOUtils.toFile(wfJsonStr, wfFile);

        StringWriter writer = new StringWriter();
        try ( writer ) {
        	DagSpec dag = AirflowDagGenerator.generate(wfDesc, writer);
        }
		System.out.println(writer.toString());

//		WorkflowManager wfManager = mdt.getWorkflowManager();
//		wfDesc = wfManager.addOrReplaceWorkflowModel(wfDesc);
		
//		System.out.println("Workflow id: " + wfDesc.getId());
	}
	
	private static TaskDescriptor createAASOperationTask(String taskId, String opRefStr) {
		TaskDescriptor task = new TaskDescriptor();
		
		task.setId(taskId);
		task.setType(AASOperationTask.class.getName());

		task.addOption("operation", opRefStr);
		task.addOption("timeout", "1m");
		task.addOption("loglevel", "info");
		
		return task;
	}
	
	private static TaskDescriptor createOperationSubmodelTask(String taskId, String smRefStr) {
		TaskDescriptor task = new TaskDescriptor();
		
		task.setId(taskId);
		task.setType(OperationSubmodelTask.class.getName());
		task.addOption("operationSubmodel", smRefStr);
		task.addOption("timeout", "1m");
		task.addOption("loglevel", "info");
		
		return task;
	}
	
	private static TaskDescriptor createRESTfulAsyncRpcTask(String taskId, String baseUrl, String endpoint) {
		TaskDescriptor task = new TaskDescriptor();
		
		task.setId(taskId);
		task.setType(RESTfulAsyncRpcTask.class.getName());

		task.addOption("baseUrl", baseUrl);
		task.addOption("endpoint", endpoint);
		task.addOption("timeout", "1m");
		task.addOption("loglevel", "info");
		
		return task;
		
	}
	
	private static TaskDescriptor createCopyOperator(String taskId) {
		TaskDescriptor task = new TaskDescriptor();
		
		task.setId(taskId);
		task.setType(SetTask.class.getName());

		task.addOption("timeout", "1m");
		task.addOption("loglevel", "info");
		
		return task;
		
	}
}

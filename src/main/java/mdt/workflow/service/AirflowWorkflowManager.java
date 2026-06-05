package mdt.workflow.service;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Preconditions;

import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;

import utils.LocalDateTimes;
import utils.StrSubstitutor;
import utils.http.HttpRESTfulClient;
import utils.http.HttpRESTfulClient.ErrorEntityDeserializer;
import utils.http.HttpRESTfulClient.ResponseBodyDeserializer;
import utils.http.OkHttpClientUtils;
import utils.http.RESTfulErrorEntity;
import utils.http.RESTfulIOException;
import utils.http.RESTfulRemoteException;
import utils.io.IOUtils;
import utils.stream.FStream;

import mdt.model.MDTModelSerDe;
import mdt.model.ResourceNotFoundException;
import mdt.workflow.MDTWorkflowInstanceManagerException;
import mdt.workflow.NodeTask;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowInstanceManagerProvider;
import mdt.workflow.WorkflowModel;
import mdt.workflow.WorkflowStatus;
import mdt.workflow.airflow.AirflowDagGenerator;
import mdt.workflow.airflow.AirflowWorkflowId;
import mdt.workflow.airflow.DagSpec;
import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Service
public class AirflowWorkflowManager implements WorkflowInstanceManagerProvider, InitializingBean {
	private static final Logger s_logger = LoggerFactory.getLogger(AirflowWorkflowManager.class);
	private static final String VARIABLE_MDT_URL = "mdt_manager_url";
	private static final String ADD_VARIABLE_BODY = """			
{
  "key": "${VARIABLE_MDT_URL}",
  "value": "${MDT_URL}/instance-manager",
  "description": "MDT InstanceManager URL"
}""";
	
	private final JpaWorkflowModelManager m_wfModelManager;
	private final AirflowWorkflowManagerConfiguration m_conf;
	private HttpRESTfulClient m_restfulClient = null;
	private String m_jwtToken = null;
	private String m_airflowUrl = null;
	
	public AirflowWorkflowManager(JpaWorkflowModelManager wfModelManager, AirflowWorkflowManagerConfiguration conf) {
		m_wfModelManager = wfModelManager;
		m_conf = conf;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Preconditions.checkArgument(m_conf.getDagsFolder() != null, "dags-folder is not configured");
		Preconditions.checkArgument(m_conf.getDagsFolder().isDirectory(),
									"dags-folder is not a directory: "
											+ m_conf.getDagsFolder().getAbsolutePath());

		m_airflowUrl = m_conf.getAirflowBaseUrl() + "/api/v2";
		try {
			getRestfulClient();
		}
		catch ( Exception e ) {
			s_logger.error("failed to initialize AirflowWorkflowManager: airflow-url={}, cause={}",
							m_airflowUrl, "" + e);
		}
	}

	@Override
	public List<String> listWorkflowIds() {
		String url = String.format("%s/dags", m_airflowUrl);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		
		return FStream.from(result.get("dags").elements())
		        		.filter(dagNode -> existsTag(dagNode, "mdt"))
						.map(dagNode -> dagNode.get("dag_id").asText())
						.flatMap(dagId -> FStream.from(listDagRunIds(dagId))
												.map(runId -> new AirflowWorkflowId(dagId, runId).toStringExpr()))
						.toList();
	}

	@Override
	public WorkflowStatus getWorkflowStatus(String wfIdStr) throws ResourceNotFoundException {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);

		String url = wfId.toUrl(m_airflowUrl);
		JsonNode dagRun = getRestfulClient().get(url, m_jsonNodeDeser);
		
		return switch ( dagRun.get("state").asText() ) {
	        case "success" -> WorkflowStatus.COMPLETED;
	        case "failed", "timeout" -> WorkflowStatus.FAILED;
	        case "running" -> WorkflowStatus.RUNNING;
	        case "queued" -> WorkflowStatus.STARTING;
	        case "scheduled" -> WorkflowStatus.NOT_STARTED;
	        default -> WorkflowStatus.UNKNOWN;
	    };
	}
	
	@Override
	public List<Workflow> getWorkflowAll() {
		// 먼저 Airflow에 등록된 DAG 목록을 조회한 후,
		// 각 DAG에 대응되는 MDT Workflow 인스턴스 정보를 구성한다.

		String url = String.format("%s/dags", m_airflowUrl);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		return FStream.from(result.get("dags").elements())
				        .filter(dagNode -> existsTag(dagNode, "mdt"))
						.map(dagNode -> dagNode.get("dag_id").asText())
						.flatMap(dagId -> listDagRuns(dagId))
						.toList();
	}
	private boolean existsTag(JsonNode dagNode, String tag) {
		for ( JsonNode tagNode : dagNode.get("tags") ) {
			if ( tagNode.get("name").asText().equals(tag) ) {
				return true;
			}
		}
		return false;
	}

	
	@Override
	public Workflow getWorkflow(String wfIdStr) {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		
		JsonNode dags = getRestfulClient().get(wfId.toUrl(m_airflowUrl), m_jsonNodeDeser);
		return getWorkflowFromDagRun(wfId.dagId(), dags);
	}

	@Override
	public void removeWorkflow(String wfIdStr) {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		getRestfulClient().delete(wfId.toUrl(m_airflowUrl));
	}

	@Override
	public void removeWorkflowAll() {
		String url = String.format("%s/dags", m_airflowUrl);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		FStream.from(result.get("dags").elements())
						.map(dagNode -> dagNode.get("dag_id").asText())
						.flatMap(dagId -> FStream.from(listDagRunIds(dagId))
													.map(runId -> new AirflowWorkflowId(dagId, runId)))
						.forEach(wfId -> {
							try {
								removeWorkflow(wfId.toStringExpr());
							}
							catch ( Exception e ) {
								s_logger.error("failed to remove the workflow: id={}, cause={}",
												wfId, "" + e);
							}
						});
	}
	
	private void enableDag(String dagId) {
		String dagIdEncoded = URLEncoder.encode(dagId, StandardCharsets.UTF_8);
		String url = String.format("%s/dags/%s", m_airflowUrl, dagIdEncoded);
		JsonNode dag = getRestfulClient().get(url, m_jsonNodeDeser);
		if ( dag.get("is_paused").asBoolean() ) {
			RequestBody reqBody = RequestBody.create("{\"is_paused\": false}",
													HttpRESTfulClient.MEDIA_TYPE_JSON);
			getRestfulClient().patch(url, reqBody);
		}
	}

	@Override
	public Workflow startWorkflow(@NotNull String modelId) throws ResourceNotFoundException {
		String url = String.format("%s/dags/%s/dagRuns", m_airflowUrl, modelId);

		enableDag(modelId);
		
		try {
			StartWorkflowRequest startReq = new StartWorkflowRequest(modelId);
			String reqBodyStr = MDTModelSerDe.MAPPER.writeValueAsString(startReq);
			RequestBody reqBody = RequestBody.create(reqBodyStr, HttpRESTfulClient.MEDIA_TYPE_JSON);
			
			JsonNode dags = getRestfulClient().post(url, reqBody, m_jsonNodeDeser);
			return getWorkflowFromDagRun(modelId, dags);
		}
		catch ( JsonProcessingException e ) {
			throw new RuntimeException("failed to serialize start workflow request", e);
		}
	}

	@Override
	public void stopWorkflow(String wfIdStr) throws ResourceNotFoundException {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		// DagRun의 상태를 'failed'로 변경하여 중지시킨다.
		if ( updateDagRunState(wfId, "failed", "running") ) {
			listTaskInstances(wfId.toUrl(m_airflowUrl), "running")
				.forEach(taskInstNode -> {
					String taskId = taskInstNode.get("task_id").asText();
					String url = String.format("%s/taskInstances/%s", wfId.toUrl(m_airflowUrl), taskId);
					String reqBodyStr = "{\"state\": \"failed\"}";
					RequestBody reqBody = RequestBody.create(reqBodyStr, HttpRESTfulClient.MEDIA_TYPE_JSON);
					getRestfulClient().patch(url, reqBody);
				});
		}
	}

	@Override
	public Workflow suspendWorkflow(String wfId) throws ResourceNotFoundException {
		throw new RuntimeException("suspendWorkflow is not supported in Airflow");
	}

	@Override
	public Workflow resumeWorkflow(String wfId) throws ResourceNotFoundException {
		throw new RuntimeException("suspendWorkflow is not supported in Airflow");
	}

	@Override
	public String getWorkflowLog(String wfId, String podName) throws ResourceNotFoundException {
		throw new RuntimeException("suspendWorkflow is not supported in Airflow");
	}

	@Override
	public void onWorkflowModelAdded(WorkflowModel wfModel) throws MDTWorkflowInstanceManagerException {
		try {
			StringWriter writer = new StringWriter();
			DagSpec dag = AirflowDagGenerator.generate(wfModel, writer);
			writer.close();
			
			IOUtils.toFile(writer.toString(), new File(m_conf.getDagsFolder(), dag.getId() + ".py"));			
		}
		catch ( IOException e ) {
			throw new MDTWorkflowInstanceManagerException(
					"failed to generate Airflow DAG for workflow model: " + wfModel.getId(), e);
		}
	}

	@Override
	public void onWorkflowModelRemoved(String wfModelId) throws MDTWorkflowInstanceManagerException {
		String dagIdEncoded = URLEncoder.encode(wfModelId, StandardCharsets.UTF_8);
		String url = String.format("%s/dags/%s", m_airflowUrl, dagIdEncoded);
		getRestfulClient().delete(url);
		
		File dagFile = new File(m_conf.getDagsFolder(), wfModelId + ".py");
		if ( dagFile.isFile() ) {
			if ( !dagFile.delete() ) {
				throw new MDTWorkflowInstanceManagerException(
						"failed to delete Airflow DAG file: " + dagFile.getAbsolutePath());
			}
		}
	}

	@Override
	public String getWorkflowScript(String wfModelId) throws ResourceNotFoundException {
		WorkflowModel wfModel = m_wfModelManager.getWorkflowModel(wfModelId);
		try {
			StringWriter writer = new StringWriter();
			AirflowDagGenerator.generate(wfModel, writer);
			writer.close();
			
			return writer.toString();		
		}
		catch ( IOException e ) {
			throw new MDTWorkflowInstanceManagerException(
					"failed to generate Airflow DAG for workflow model: " + wfModel.getId(), e);
		}
	}
	
	@Override
	public String toString() {
        return String.format("%s[%s, dag-folder=%s]",
    						getClass().getSimpleName(), m_conf.getDagsFolder());
	}
	
	private FStream<Workflow> listDagRuns(String dagId) {
		String url = String.format("%s/dags/%s/dagRuns", m_airflowUrl, dagId);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		return FStream.from(result.get("dag_runs").elements())
				.mapOrIgnore(jnode -> {
					try {
						return getWorkflowFromDagRun(dagId, jnode);
					}
					catch ( Exception e ) {
						String dagRunId = jnode.get("dag_run_id").asText();
						s_logger.error("ignore the dagRun causing error: dagRun={}, cause={}", dagRunId,  ""+e);
						throw e;
					}
				});
	}
	private FStream<String> listDagRunIds(String dagId) {
		String url = String.format("%s/dags/%s/dagRuns", m_airflowUrl, dagId);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		return FStream.from(result.get("dag_runs").elements())
						.map(jnode -> jnode.get("dag_run_id").asText());
	}
	
	private JsonNode getDagRunNode(AirflowWorkflowId wfId) {
		return getRestfulClient().get(wfId.toUrl(m_airflowUrl), m_jsonNodeDeser);
	}
	
	private boolean updateDagRunState(AirflowWorkflowId wfId, String newState, String expectedState) {
		JsonNode dagRunNode = getDagRunNode(wfId);
		String currentState = dagRunNode.get("state").asText();
		if ( currentState.equals(expectedState) ) {
			String requestBodyStr = String.format("{\"state\": \"%s\"}", newState);
			RequestBody reqBody = RequestBody.create(requestBodyStr, HttpRESTfulClient.MEDIA_TYPE_JSON);
			getRestfulClient().patch(wfId.toUrl(m_airflowUrl), reqBody);
			
			return true;
		}
		else {
			return false;
		}
	}
	
	private static final class StartWorkflowRequest {
		private final String m_runIdEncoded;
		private final String m_logicalDate;

		public StartWorkflowRequest(String dagId) {
			Instant now = Instant.now();
//			m_runIdEncoded = String.format("%s__%d", dagId, now.toEpochMilli());
			m_runIdEncoded = "" + now.toEpochMilli();
			m_logicalDate = Instant.now().toString();
		}
		
		@JsonProperty("dag_run_id")
		public String getDagRunIdJackson() {
			return m_runIdEncoded;
		}
		
		@JsonProperty("logical_date")
		public String getLogicalDate() {
			return m_logicalDate;
		}
	}
	
	private String getJwtToken(HttpRESTfulClient client, String userName, String password) {
		String url = String.format("%s/auth/token", m_conf.getAirflowBaseUrl());
		String bodyJson = String.format("{\"username\": \"%s\", \"password\": \"%s\"}", userName, password);
        RequestBody reqBody = RequestBody.create(bodyJson, HttpRESTfulClient.MEDIA_TYPE_JSON);
        JsonNode respJson = client.post(url, reqBody, m_jsonNodeDeser);
        return respJson.get("access_token").asText();
	}
	
	private Workflow getWorkflowFromDagRun(String modelId, JsonNode dagRunNode) {
		AirflowWorkflowId wfId = new AirflowWorkflowId(modelId, dagRunNode.get("dag_run_id").asText());
		String url = wfId.toUrl(m_airflowUrl);
		
		JsonNode dagRun = getRestfulClient().get(url, m_jsonNodeDeser);
		
		WorkflowStatus status = switch ( dagRun.get("state").asText() ) {
            case "success" -> WorkflowStatus.COMPLETED;
            case "failed", "timeout" -> WorkflowStatus.FAILED;
            case "running" -> WorkflowStatus.RUNNING;
            case "queued" -> WorkflowStatus.STARTING;
            case "scheduled" -> WorkflowStatus.NOT_STARTED;
            default -> WorkflowStatus.UNKNOWN;
        };
        LocalDateTime creationTime = LocalDateTimes.fromInstant(Instant.parse(dagRun.get("queued_at").asText()));
		LocalDateTime startTime = dagRun.hasNonNull("start_date")
								? LocalDateTimes.fromInstant(Instant.parse(dagRun.get("start_date").asText()))
								: null;
		LocalDateTime finishTime = dagRun.hasNonNull("end_date")
								? LocalDateTimes.fromInstant(Instant.parse(dagRun.get("end_date").asText()))
								: null;
		
		List<NodeTask> tasks = listTaskInstances(url, null)
									.map(this::getNodeTaskFromTaskInstance)
									.toList();
		return new Workflow(wfId.toStringExpr(), modelId, status, creationTime, startTime, finishTime, tasks);
	}
	
	private FStream<JsonNode> listTaskInstances(String url, @Nullable String stateFilter) {
		url = ( stateFilter != null )
			? String.format("%s/taskInstances?state=%s", url, stateFilter)
			:  String.format("%s/taskInstances", url);
		JsonNode result = getRestfulClient().get(url, m_jsonNodeDeser);
		return FStream.from(result.get("task_instances").elements());
	}
	
	private NodeTask getNodeTaskFromTaskInstance(JsonNode taskInstNode) {
		String taskId = taskInstNode.get("task_id").asText();

		String statusStr = taskInstNode.get("state").asText();
		if ( statusStr.equals("null") ) {
			return new NodeTask(taskId, WorkflowStatus.NOT_STARTED, Collections.emptySet(), null, null);
		}
		WorkflowStatus status = switch ( statusStr ) {
			case "success" -> WorkflowStatus.COMPLETED;
			case "failed", "shutdown" -> WorkflowStatus.FAILED;
			case "running" -> WorkflowStatus.RUNNING;
			case "queued" -> WorkflowStatus.STARTING;
			case "scheduled", "upstream_failed", "skipped", "none" -> WorkflowStatus.NOT_STARTED;
			default -> {
				System.out.println("unknown task state: " + statusStr);
				yield WorkflowStatus.UNKNOWN;
			}
		};
		LocalDateTime startTime = taskInstNode.hasNonNull("start_date")
				? LocalDateTimes.fromInstant(Instant.parse(taskInstNode.get("start_date").asText()))
				: null;
		LocalDateTime finishTime = taskInstNode.hasNonNull("end_date")
				? LocalDateTimes.fromInstant(Instant.parse(taskInstNode.get("end_date").asText()))
				: null;

		return new NodeTask(taskId, status, Collections.emptySet(), startTime, finishTime);
	}
	
	private ResponseBodyDeserializer<JsonNode> m_jsonNodeDeser = new ResponseBodyDeserializer<>() {
		@Override
		public JsonNode deserialize(Headers headers, String respBody) throws IOException {
			return MDTModelSerDe.getJsonMapper().readTree(respBody);
		}
	};
	
	public static class AirflowErrorEntityDeserializer implements ErrorEntityDeserializer {
		@Override
		public RESTfulErrorEntity deserialize(String respBody) throws IOException {
			JsonNode root = MDTModelSerDe.getJsonMapper().readTree(respBody);
			if ( root.has("detail") ) {
				return RESTfulErrorEntity.ofMessage(root.get("detail").asText());
			}
			else {
				throw new IOException("invalid Airflow error response: " + respBody);
			}
		}
	}
	
	private HttpRESTfulClient getRestfulClient() {
		if ( m_restfulClient == null ) {
			try {
				OkHttpClient httpClient = OkHttpClientUtils.newTrustAllOkHttpClientBuilder().build();
				JsonMapper mapper = MDTModelSerDe.getJsonMapper();
				m_restfulClient = HttpRESTfulClient.builder()
													.httpClient(httpClient)
													.jsonMapper(mapper)
													.errorEntityDeserializer(new AirflowErrorEntityDeserializer())
													.build();

				m_jwtToken = getJwtToken(m_restfulClient, "airflow", "airflow");
				m_restfulClient = HttpRESTfulClient.builder()
													.httpClient(httpClient)
													.header("Authorization", "Bearer " + m_jwtToken)
													.jsonMapper(mapper)
													.errorEntityDeserializer(new AirflowErrorEntityDeserializer())
													.build();
				
				// 혹시 있을지 모르는 'mdt_manager_url' variable 제거하고 다시 새 endpoint 추가한다.
				try {
					m_restfulClient.delete(String.format("%s/variables/%s", m_airflowUrl, VARIABLE_MDT_URL));
				}
				catch ( RESTfulRemoteException ignored ) { }
				
				
				String reqBody = StrSubstitutor.with(Map.of("VARIABLE_MDT_URL", VARIABLE_MDT_URL,
														"MDT_URL", m_conf.getMdtUrl()))
											.replace(ADD_VARIABLE_BODY);
				m_restfulClient.post(String.format("%s/variables", m_airflowUrl),
										RequestBody.create(reqBody, HttpRESTfulClient.MEDIA_TYPE_JSON));
				s_logger.info("successfully connected to Airflow REST API: airflow-url={}", m_airflowUrl);
			}
			catch ( RESTfulIOException e ) {
				throw new RESTfulIOException("failed to connect to Airflow REST API, airflow-url="
											+ m_airflowUrl, e);
			}
			catch ( Exception e ) {
				throw new RuntimeException("failed to initialize AirflowWorkflowManager", e);
			}
		}
		
		return m_restfulClient;
	}
}

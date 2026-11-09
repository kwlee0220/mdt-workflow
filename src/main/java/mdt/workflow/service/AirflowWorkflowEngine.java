package mdt.workflow.service;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import utils.http.HttpRESTfulClient;
import utils.http.HttpRESTfulClient.ErrorEntityDeserializer;
import utils.http.HttpRESTfulClient.ResponseBodyDeserializer;
import utils.http.OkHttpClientUtils;
import utils.http.RESTfulErrorEntity;
import utils.http.RESTfulException;
import utils.http.RESTfulIOException;
import utils.http.RESTfulRemoteException;
import utils.stream.FStream;

import mdt.model.MDTModelSerDe;
import mdt.model.ResourceNotFoundException;
import mdt.workflow.MDTWorkflowInstanceManagerException;
import mdt.workflow.NodeTask;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowEngine;
import mdt.workflow.WorkflowModel;
import mdt.workflow.WorkflowStatus;
import mdt.workflow.airflow.AirflowDagGenerator;
import mdt.workflow.airflow.AirflowWorkflowId;
import mdt.workflow.airflow.DagSpec;
import mdt.workflow.airflow.PythonIdentifiers;
import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 * Airflow를 워크플로우 엔진으로 쓰는 {@link WorkflowEngine} 구현.
 * <ul>
 *   <li>모델 배포: 모델을 DAG 파일({@code <dagsFolder>/<modelId>.py})로 만들고, 삭제할 때 파일과 Airflow DAG를 지운다.</li>
 *   <li>인스턴스: Airflow REST API v2의 DagRun을 워크플로우 인스턴스로 다룬다. 인스턴스 id는
 *       {@code <dagId>__<dagRunId>}({@link AirflowWorkflowId})이고, 태그 {@code mdt}가 붙은 DAG만 다룬다.</li>
 *   <li>연결: 설정({@code mdt.workflow-manager.airflow.*})의 계정으로 JWT를 받아 요청마다 붙이고, 401을 받으면 다시
 *       받아 한 번 재시도한다. 처음 연결할 때 Airflow 변수 {@code mdt_manager_url}을 MDT 주소로 등록한다.</li>
 * </ul>
 * suspend/resume은 Airflow에 대응 기능이 없어 구현하지 않는다(인터페이스 기본 구현이 {@link UnsupportedOperationException}을
 * 던진다). 로그 조회는 task 단위다({@link #getWorkflowLog(String, String)}).
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Service
public class AirflowWorkflowEngine implements WorkflowEngine, InitializingBean {
	private static final Logger s_logger = LoggerFactory.getLogger(AirflowWorkflowEngine.class);
	private static final String VARIABLE_MDT_URL = "mdt_manager_url";
	/** MDT가 만든 DAG에 붙이는 태그. DAG 템플릿({@code airflow_dag.peb})의 {@code tags=["mdt"]}와 같아야 한다. */
	static final String MDT_DAG_TAG = "mdt";
	/** Airflow 목록 API를 한 번에 읽는 개수. 서버의 최대 limit보다 크면 서버가 줄여서 준다. */
	private static final int PAGE_SIZE = 100;
	
	private final AirflowWorkflowManagerConfiguration m_conf;
	/** 초기화를 마친 Airflow 클라이언트. 모든 초기화 단계가 성공했을 때만 대입한다({@link #getRestfulClient()}). */
	private volatile HttpRESTfulClient m_restfulClient = null;
	/** 지금 쓰는 Airflow JWT. 401을 받으면 {@link #refreshJwtToken(String)}이 갱신한다. */
	private volatile String m_jwtToken = null;
	/** 토큰 발급 전용 클라이언트. 인증 헤더·재발급 처리를 붙이지 않는다. */
	private HttpRESTfulClient m_loginClient = null;
	private String m_airflowUrl = null;
	
	public AirflowWorkflowEngine(AirflowWorkflowManagerConfiguration conf) {
		m_conf = conf;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Preconditions.checkArgument(m_conf.getDagsFolder() != null, "dags-folder is not configured");
		Preconditions.checkArgument(m_conf.getDagsFolder().isDirectory(),
									"dags-folder is not a directory: "
											+ m_conf.getDagsFolder().getAbsolutePath());
		// Airflow 계정은 필수다(2026-11-04 이전에는 airflow/airflow로 고정되어 있었다).
		Preconditions.checkArgument(m_conf.getUsername() != null && !m_conf.getUsername().isBlank(),
									"Airflow username is not configured: mdt.workflow-manager.airflow.username");
		Preconditions.checkArgument(m_conf.getPassword() != null,
									"Airflow password is not configured: mdt.workflow-manager.airflow.password");

		m_airflowUrl = m_conf.getAirflowBaseUrl() + "/api/v2";
		try {
			getRestfulClient();
		}
		catch ( Exception e ) {
			s_logger.error("failed to initialize AirflowWorkflowEngine: airflow-url={}, cause={}",
							m_airflowUrl, "" + e);
		}
	}

	@Override
	public List<String> listWorkflowIds() {
		return listMdtDagIds()
						.flatMap(dagId -> listDagRunIds(dagId)
												.map(runId -> new AirflowWorkflowId(dagId, runId).toStringExpr()))
						.toList();
	}

	@Override
	public WorkflowStatus getWorkflowStatus(String wfIdStr) throws ResourceNotFoundException {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);

		JsonNode dagRun = getDagRunNode(wfId);
		return toDagRunStatus(dagRun.path("state").asText(null));
	}
	
	@Override
	public List<Workflow> getWorkflowAll() {
		// 먼저 Airflow에 등록된 DAG 목록을 조회한 후,
		// 각 DAG에 대응되는 MDT Workflow 인스턴스 정보를 구성한다.

		return listMdtDagIds()
						.flatMap(dagId -> listDagRuns(dagId))
						.toList();
	}
	
	// MDT가 만든 DAG(태그 'mdt')의 id 목록. 서버에서 태그로 거르고, 응답도 한 번 더 확인한다.
	private FStream<String> listMdtDagIds() {
		String url = String.format("%s/dags?tags=%s", m_airflowUrl, MDT_DAG_TAG);
		return listAll(url, "dags")
				.filter(dagNode -> existsTag(dagNode, MDT_DAG_TAG))
				.map(dagNode -> dagNode.get("dag_id").asText());
	}
	
	private boolean existsTag(JsonNode dagNode, String tag) {
		for ( JsonNode tagNode : dagNode.path("tags") ) {
			if ( tagNode.path("name").asText().equals(tag) ) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 페이지 단위로 결과를 주는 Airflow 목록 API를 끝까지 읽는다.
	 * <p>
	 * {@code limit}/{@code offset}을 붙여 {@code total_entries}에 이를 때까지(또는 빈 페이지가 올 때까지) 읽는다.
	 * 2026-11-04 이전에는 첫 페이지만 읽어서, 항목이 서버의 기본 limit을 넘으면 일부만 처리했다.
	 *
	 * @param url	목록 API URL. 질의 파라미터가 있어도 된다.
	 * @param field	응답에서 항목 배열을 담은 필드 이름(예: {@code "dags"}, {@code "dag_runs"}).
	 * @return	모든 페이지의 항목.
	 */
	private FStream<JsonNode> listAll(String url, String field) {
		String sep = url.contains("?") ? "&" : "?";
		List<JsonNode> items = new java.util.ArrayList<>();
		int offset = 0;
		while ( true ) {
			String pageUrl = String.format("%s%slimit=%d&offset=%d", url, sep, PAGE_SIZE, offset);
			JsonNode page = getRestfulClient().get(pageUrl, m_jsonNodeDeser);
			JsonNode pageItems = page.path(field);
			pageItems.forEach(items::add);
			offset += pageItems.size();
			
			int total = page.path("total_entries").asInt(-1);
			boolean done = pageItems.size() == 0
						|| (total >= 0 && offset >= total)
						|| (total < 0 && pageItems.size() < PAGE_SIZE);
			if ( done ) {
				return FStream.from(items);
			}
		}
	}

	
	@Override
	public Workflow getWorkflow(String wfIdStr) throws ResourceNotFoundException {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		
		JsonNode dags = getDagRunNode(wfId);
		return getWorkflowFromDagRun(wfId.dagId(), dags);
	}

	@Override
	public void removeWorkflow(String wfIdStr) throws ResourceNotFoundException {
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		try {
			getRestfulClient().delete(wfId.toUrl(m_airflowUrl));
		}
		catch ( RESTfulException e ) {
			throw toNotFoundIf404(e, "Workflow", wfIdStr);
		}
	}

	@Override
	public void removeWorkflowAll() {
		// MDT가 만든 DAG의 실행만 지운다. 2026-11-04 이전에는 태그로 거르지 않아, 같은 Airflow의
		// 다른 DAG 실행 기록까지 지웠다.
		listMdtDagIds().forEach(this::removeDagRuns);
	}
	
	@Override
	public void removeWorkflowsOfModel(String wfModelId) {
		// 그 모델의 DAG 실행만 조회한다. 2026-11-07 이전에는 컨트롤러가 모든 워크플로우의 상세(run마다
		// task 목록 포함)를 받아 모델 id로 걸렀고, 삭제 실패를 흔적 없이 버렸다(검토 A5).
		try {
			removeDagRuns(wfModelId);
		}
		catch ( RESTfulException e ) {
			if ( !(toNotFoundIf404(e, "Workflow model", wfModelId) instanceof ResourceNotFoundException) ) {
				throw e;
			}
			// 배포되지 않은 모델(DAG 없음)은 지울 실행도 없다.
			s_logger.debug("no deployed DAG to remove workflows: model={}", wfModelId);
		}
	}
	
	// 주어진 DAG의 실행을 모두 지운다. 하나를 지우지 못해도 나머지는 계속 지운다.
	private void removeDagRuns(String dagId) {
		for ( String runId: listDagRunIds(dagId).toList() ) {
			AirflowWorkflowId wfId = new AirflowWorkflowId(dagId, runId);
			try {
				removeWorkflow(wfId.toStringExpr());
			}
			catch ( Exception e ) {
				s_logger.error("failed to remove the workflow: id={}, cause={}", wfId, "" + e);
			}
		}
	}
	
	private void enableDag(String dagId) {
		String url = dagUrl(dagId);
		JsonNode dag = getRestfulClient().get(url, m_jsonNodeDeser);
		if ( dag.get("is_paused").asBoolean() ) {
			RequestBody reqBody = RequestBody.create("{\"is_paused\": false}",
													HttpRESTfulClient.MEDIA_TYPE_JSON);
			getRestfulClient().patch(url, reqBody);
		}
	}

	@Override
	public Workflow startWorkflow(@NotNull String modelId) throws ResourceNotFoundException {
		String url = dagUrl(modelId) + "/dagRuns";

		try {
			enableDag(modelId);
		}
		catch ( RESTfulException e ) {
			// 모델을 막 등록했다면 Airflow가 아직 DAG 파일을 읽지 않았을 수 있다.
			throw toNotFoundIf404(e, "Airflow DAG (not registered, or not parsed by Airflow yet)", modelId);
		}
		
		try {
			StartDagRunRequest startReq = StartDagRunRequest.now();
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
		// 실행 중이거나 대기 중인 DagRun의 상태를 'failed'로 바꿔 중지시킨다. 2026-11-04 이전에는 'running'만
		// 중지했다(대기 중인 run은 그대로 두었다).
		if ( updateDagRunState(wfId, "failed", Set.of("running", "queued")) ) {
			// 아직 실행 중인 task도 실패로 바꾼다. Airflow API v2의 본문 필드는 'new_state'다(정의되지 않은 필드는
			// 거부한다). 2026-11-04 이전에는 'state'를 보내 이 요청이 거부되었다.
			String body = "{\"new_state\": \"failed\"}";
			listTaskInstances(wfId.toUrl(m_airflowUrl), "running")
				.forEach(taskInstNode -> {
					String taskId = taskInstNode.get("task_id").asText();
					String url = String.format("%s/taskInstances/%s", wfId.toUrl(m_airflowUrl),
												encodePathSegment(taskId));
					try {
						getRestfulClient().patch(url, RequestBody.create(body, HttpRESTfulClient.MEDIA_TYPE_JSON));
					}
					catch ( RESTfulException e ) {
						// DagRun을 실패로 바꾸는 사이에 task가 끝났을 수 있다.
						s_logger.warn("failed to mark the task failed: workflow={}, task={}, cause={}",
										wfIdStr, taskId, "" + e);
					}
				});
		}
	}

	/**
	 * 워크플로우 인스턴스(DagRun)에서 한 task의 최신 시도 로그를 텍스트로 반환한다.
	 * <p>
	 * Airflow 로그는 task 시도 단위라, 인터페이스의 {@code podName}을 <b>task id</b>로 해석한다(이전 엔진의
	 * pod 이름 자리를 그대로 쓴다). 시도 번호는 TaskInstance의 {@code try_number}(최신 시도)를 쓰고,
	 * {@code GET .../taskInstances/{task}/logs/{try}?full_content=true}의 구조화 로그를 한 항목당 한 줄
	 * ({@code <timestamp> <LEVEL> <logger>: <event> <나머지 필드 JSON>})로 바꾼다. 실행 중인 task는 그 시점까지의
	 * 로그만 돌려준다. 2026-11-05 이전에는 항상 {@code UnsupportedOperationException}(501)이었다.
	 *
	 * @param wfIdStr	워크플로우 인스턴스 id({@code <dagId>__<dagRunId>}).
	 * @param taskId	로그를 조회할 task id.
	 * @return	로그 텍스트(줄마다 개행으로 끝난다).
	 * @throws ResourceNotFoundException	DagRun이나 task가 없거나, task가 아직 한 번도 실행되지 않은 경우.
	 */
	@Override
	public String getWorkflowLog(String wfIdStr, String taskId) throws ResourceNotFoundException {
		Preconditions.checkArgument(taskId != null, "taskId is null");
		AirflowWorkflowId wfId = AirflowWorkflowId.parse(wfIdStr);
		String taskInstUrl = String.format("%s/taskInstances/%s", wfId.toUrl(m_airflowUrl),
											encodePathSegment(taskId));
		String target = wfIdStr + "/" + taskId;
		try {
			JsonNode taskInst = getRestfulClient().get(taskInstUrl, m_jsonNodeDeser);
			int tryNumber = taskInst.path("try_number").asInt(0);
			if ( tryNumber <= 0 ) {
				// 아직 실행되지 않은 task는 로그가 없다.
				throw new ResourceNotFoundException("TaskLog", target);
			}
			
			String logUrl = String.format("%s/logs/%d?full_content=true", taskInstUrl, tryNumber);
			JsonNode resp = getRestfulClient().get(logUrl, m_jsonNodeDeser);
			return toLogText(resp.path("content"));
		}
		catch ( RESTfulException e ) {
			throw toNotFoundIf404(e, "TaskInstance", target);
		}
	}
	
	private static final Set<String> LOG_LINE_FIELDS = Set.of("timestamp", "level", "logger", "event");
	
	// Airflow 로그 content(구조화 항목 배열 또는 문자열 배열)를 한 항목당 한 줄의 텍스트로 바꾼다.
	static String toLogText(JsonNode content) {
		StringBuilder sb = new StringBuilder();
		for ( JsonNode entry: content ) {
			if ( !entry.isObject() ) {
				sb.append(entry.asText()).append('\n');
				continue;
			}
			
			String timestamp = entry.path("timestamp").asText(null);
			if ( timestamp != null ) {
				sb.append(timestamp).append(' ');
			}
			String level = entry.path("level").asText(null);
			if ( level != null ) {
				sb.append(level.toUpperCase()).append(' ');
			}
			String logger = entry.path("logger").asText(null);
			if ( logger != null ) {
				sb.append(logger).append(": ");
			}
			sb.append(entry.path("event").asText(""));
			
			// 예외 정보(error_detail) 등 나머지 필드는 JSON으로 덧붙인다.
			Map<String,JsonNode> extras = new java.util.LinkedHashMap<>();
			entry.properties().forEach(ent -> {
				if ( !LOG_LINE_FIELDS.contains(ent.getKey()) ) {
					extras.put(ent.getKey(), ent.getValue());
				}
			});
			if ( !extras.isEmpty() ) {
				try {
					sb.append(' ').append(MDTModelSerDe.MAPPER.writeValueAsString(extras));
				}
				catch ( JsonProcessingException e ) {
					sb.append(' ').append(extras);
				}
			}
			sb.append('\n');
		}
		return sb.toString();
	}

	/**
	 * 모델을 DAG 파일 {@code <dagsFolder>/<모델 id>.py}로 쓴다. Airflow DAG processor가 그 파일을 읽어야 실행할 수 있다.
	 */
	@Override
	public void deploy(WorkflowModel wfModel) throws MDTWorkflowInstanceManagerException {
		try {
			StringWriter writer = new StringWriter();
			DagSpec dag = AirflowDagGenerator.generate(wfModel, writer);
			writer.close();
			
			File dagFile = dagFile(dag.getId());
			s_logger.debug("trying to create airflow dag file: {}", dagFile);
			// Airflow DAG processor가 쓰는 중인 파일을 읽지 않도록 임시 파일에 쓴 뒤 한 번에 바꾼다.
			// 임시 파일 이름은 '.py'로 끝나지 않아 Airflow가 읽지 않는다. 2026-11-04 이전에는 제자리에 바로 썼다.
			Path tmp = new File(m_conf.getDagsFolder(), "." + dag.getId() + ".py.tmp").toPath();
			Files.writeString(tmp, writer.toString(), StandardCharsets.UTF_8);
			try {
				Files.move(tmp, dagFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			}
			catch ( IOException e ) {
				Files.deleteIfExists(tmp);
				throw e;
			}
			s_logger.info("created Airflow DAG file: {}", dagFile);
		}
		catch ( IOException e ) {
			throw new MDTWorkflowInstanceManagerException(
					"failed to generate Airflow DAG for workflow model: " + wfModel.getId(), e);
		}
	}

	/**
	 * DAG 파일을 지운 뒤 Airflow의 DAG를 지운다. Airflow가 아직 읽지 않은 DAG(404)는 무시한다.
	 */
	@Override
	public void undeploy(String wfModelId) throws MDTWorkflowInstanceManagerException {
		// 파일을 먼저 지운다. 2026-11-04 이전에는 Airflow API로 DAG를 먼저 지워서, 파일을 지우기 전에
		// DAG processor가 파일을 다시 읽으면 DAG가 다시 등록되었다.
		File dagFile = dagFile(wfModelId);
		s_logger.debug("trying to remove airflow dag file: {}", dagFile);
		if ( dagFile.isFile() ) {
			if ( !dagFile.delete() ) {
				throw new MDTWorkflowInstanceManagerException(
						"failed to delete Airflow DAG file: " + dagFile.getAbsolutePath());
			}
			s_logger.info("removed Airflow DAG file: {}", dagFile);
		}
		
		try {
			getRestfulClient().delete(dagUrl(wfModelId));
		}
		catch ( RESTfulException e ) {
			if ( e.getHttpStatusCode() != 404 ) {		// Airflow가 아직 읽지 않은 DAG면 404다.
				s_logger.warn("failed to delete Airflow DAG: id={}, cause={}", wfModelId, "" + e);
			}
		}
	}
	
	/**
	 * 주어진 DAG id의 DAG 파일 경로를 반환한다.
	 * <p>
	 * DAG id는 {@link DagSpec}과 같은 규칙(Python 식별자)이어야 하고, 결과 경로는 DAG 폴더 바로 아래여야 한다.
	 * 2026-11-04 이전에는 삭제할 때 id를 검사하지 않고 경로를 만들었다.
	 *
	 * @throws IllegalArgumentException	DAG id가 Python 식별자가 아닌 경우.
	 */
	private File dagFile(String dagId) {
		Preconditions.checkArgument(PythonIdentifiers.isValid(dagId),
									"invalid DAG id: %s (invalid Python identifier)", dagId);
		File folder = m_conf.getDagsFolder().getAbsoluteFile();
		File file = new File(folder, dagId + ".py");
		Preconditions.checkArgument(folder.equals(file.getParentFile()), "DAG file outside the dags-folder: %s", file);
		return file;
	}
	
	// DAG id를 path segment로 인코딩한 '<airflow>/dags/<id>' URL. 2026-11-04 이전에는 일부 URL만 인코딩했다.
	private String dagUrl(String dagId) {
		return String.format("%s/dags/%s", m_airflowUrl, encodePathSegment(dagId));
	}
	
	private static String encodePathSegment(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * 모델을 Airflow DAG 파이썬 코드로 바꾼다.
	 */
	@Override
	public String render(WorkflowModel wfModel) throws MDTWorkflowInstanceManagerException {
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
        return String.format("%s[airflow=%s, dag-folder=%s]",
    						getClass().getSimpleName(), m_airflowUrl, m_conf.getDagsFolder());
	}
	
	private FStream<Workflow> listDagRuns(String dagId) {
		return listAll(dagUrl(dagId) + "/dagRuns", "dag_runs")
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
		return listAll(dagUrl(dagId) + "/dagRuns", "dag_runs")
						.map(jnode -> jnode.get("dag_run_id").asText());
	}
	
	private JsonNode getDagRunNode(AirflowWorkflowId wfId) throws ResourceNotFoundException {
		try {
			return getRestfulClient().get(wfId.toUrl(m_airflowUrl), m_jsonNodeDeser);
		}
		catch ( RESTfulException e ) {
			throw toNotFoundIf404(e, "Workflow", wfId.toStringExpr());
		}
	}
	
	// Airflow의 404는 대상이 없다는 뜻이므로 ResourceNotFoundException으로 바꾼다(컨트롤러가 404로 응답).
	// 2026-11-04 이전에는 RESTfulRemoteException으로 그대로 올라가 500이 되었다.
	private static RuntimeException toNotFoundIf404(RESTfulException e, String type, String id) {
		return ( e.getHttpStatusCode() == 404 ) ? new ResourceNotFoundException(type, id) : e;
	}
	
	// 현재 상태가 expectedStates 중 하나일 때만 바꾼다. 읽은 뒤 바꾸기 전에 상태가 바뀔 수 있지만, Airflow는 끝난
	// run의 상태도 바꿀 수 있으므로 그 경우 결과는 '실패'로 남는다.
	private boolean updateDagRunState(AirflowWorkflowId wfId, String newState, Set<String> expectedStates) {
		JsonNode dagRunNode = getDagRunNode(wfId);
		String currentState = dagRunNode.path("state").asText();
		if ( expectedStates.contains(currentState) ) {
			String requestBodyStr = String.format("{\"state\": \"%s\"}", newState);
			RequestBody reqBody = RequestBody.create(requestBodyStr, HttpRESTfulClient.MEDIA_TYPE_JSON);
			getRestfulClient().patch(wfId.toUrl(m_airflowUrl), reqBody);
			
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * DagRun 생성 요청 본문({@code POST /dags/{dag_id}/dagRuns}).
	 * <p>
	 * run id는 시작 시각과 무작위 접미어로 만들어 같은 시각에 두 번 시작해도 충돌하지 않게 하고, logical_date와
	 * 같은 시각을 쓴다. 2026-11-04 이전에는 epoch 밀리초만 써서 같은 밀리초에 시작하면 409로 실패했고,
	 * logical_date는 Instant.now()를 다시 불러 run id와 시각이 달랐다. 2026-11-05 이전 이름은
	 * {@code StartWorkflowRequest}(getter {@code getDagRunIdJackson})였다.
	 */
	private record StartDagRunRequest(@JsonProperty("dag_run_id") String dagRunId,
									@JsonProperty("logical_date") String logicalDate) {
		static StartDagRunRequest now() {
			Instant now = Instant.now();
			String suffix = java.util.UUID.randomUUID().toString().substring(0, 8);
			return new StartDagRunRequest(String.format("mdt__%s_%s", now, suffix), now.toString());
		}
	}
	
	private String getJwtToken(HttpRESTfulClient client, String userName, String password) throws JsonProcessingException {
		String url = String.format("%s/auth/token", m_conf.getAirflowBaseUrl());
		// 비밀번호에 따옴표·백슬래시가 있어도 깨지지 않도록 문자열 조립 대신 JSON으로 직렬화한다.
		String bodyJson = MDTModelSerDe.MAPPER.writeValueAsString(Map.of("username", userName, "password", password));
		RequestBody reqBody = RequestBody.create(bodyJson, HttpRESTfulClient.MEDIA_TYPE_JSON);
		JsonNode respJson = client.post(url, reqBody, m_jsonNodeDeser);
		return respJson.get("access_token").asText();
	}
	
	private Workflow getWorkflowFromDagRun(String modelId, JsonNode dagRunNode) {
		AirflowWorkflowId wfId = new AirflowWorkflowId(modelId, dagRunNode.get("dag_run_id").asText());
		String url = wfId.toUrl(m_airflowUrl);
		
		// 호출자는 목록·조회·실행 응답의 DagRun 노드(DAGRunResponse)를 넘기므로 그대로 쓰고, 상태가 없는 노드만
		// 다시 읽는다. 2026-11-04 이전에는 항상 다시 GET해서 run 하나당 요청이 2번(목록에서는 run마다) 나갔다.
		JsonNode dagRun = dagRunNode.has("state") ? dagRunNode : getDagRunNode(wfId);
		
		WorkflowStatus status = toDagRunStatus(dagRun.path("state").asText(null));
		// queued_at은 null일 수 있다(2026-11-04 이전에는 Instant.parse("null")로 실패해 그 run이 목록에서 빠졌다).
		LocalDateTime creationTime = parseTime(dagRun, "queued_at");
		LocalDateTime startTime = parseTime(dagRun, "start_date");
		LocalDateTime finishTime = parseTime(dagRun, "end_date");
		
		List<NodeTask> tasks = listTaskInstances(url, null)
									.map(this::getNodeTaskFromTaskInstance)
									.toList();
		return new Workflow(wfId.toStringExpr(), modelId, status, creationTime, startTime, finishTime, tasks);
	}
	
	private FStream<JsonNode> listTaskInstances(String url, @Nullable String stateFilter) {
		url = ( stateFilter != null )
			? String.format("%s/taskInstances?state=%s", url, stateFilter)
			:  String.format("%s/taskInstances", url);
		return listAll(url, "task_instances");
	}
	
	private NodeTask getNodeTaskFromTaskInstance(JsonNode taskInstNode) {
		String taskId = taskInstNode.get("task_id").asText();
		WorkflowStatus status = toTaskStatus(taskInstNode.path("state").asText(null));
		return new NodeTask(taskId, status, Collections.emptySet(),
							parseTime(taskInstNode, "start_date"), parseTime(taskInstNode, "end_date"));
	}
	
	/**
	 * Airflow DagRun 상태({@code queued}/{@code running}/{@code success}/{@code failed})를 바꾼다.
	 * <p>
	 * 2026-11-04 이전에는 DagRun 상태가 아닌 {@code timeout}·{@code scheduled}도 다뤘고, 같은 switch가 두 곳에 있었다.
	 */
	static WorkflowStatus toDagRunStatus(@Nullable String state) {
		if ( state == null ) {
			return WorkflowStatus.UNKNOWN;
		}
		return switch ( state ) {
			case "queued" -> WorkflowStatus.STARTING;
			case "running" -> WorkflowStatus.RUNNING;
			case "success" -> WorkflowStatus.COMPLETED;
			case "failed" -> WorkflowStatus.FAILED;
			default -> {
				s_logger.warn("unknown Airflow DagRun state: {}", state);
				yield WorkflowStatus.UNKNOWN;
			}
		};
	}
	
	/**
	 * Airflow TaskInstance 상태를 바꾼다.
	 * <ul>
	 *   <li>{@code null}(아직 스케줄되지 않음), {@code scheduled}, {@code skipped} → {@code NOT_STARTED}.
	 *       {@code skipped}는 실행되지 않고 끝난 상태지만 대응하는 MDT 상태가 없어 '시작 안 함'으로 둔다.</li>
	 *   <li>{@code queued} → {@code STARTING}</li>
	 *   <li>{@code running}, {@code restarting}, {@code up_for_retry}, {@code up_for_reschedule}, {@code deferred}
	 *       → {@code RUNNING}(아직 끝나지 않음)</li>
	 *   <li>{@code success} → {@code COMPLETED}</li>
	 *   <li>{@code failed}, {@code upstream_failed} → {@code FAILED}</li>
	 *   <li>그 밖({@code removed} 등) → {@code UNKNOWN}</li>
	 * </ul>
	 * 2026-11-04 이전에는 {@code upstream_failed}를 {@code NOT_STARTED}로, 재시도·연기 상태를 {@code UNKNOWN}으로
	 * 보았고, 모르는 상태를 {@code System.out}으로 출력했다.
	 */
	static WorkflowStatus toTaskStatus(@Nullable String state) {
		if ( state == null || state.equals("none") ) {
			return WorkflowStatus.NOT_STARTED;
		}
		return switch ( state ) {
			case "scheduled", "skipped" -> WorkflowStatus.NOT_STARTED;
			case "queued" -> WorkflowStatus.STARTING;
			case "running", "restarting", "up_for_retry", "up_for_reschedule", "deferred" -> WorkflowStatus.RUNNING;
			case "success" -> WorkflowStatus.COMPLETED;
			case "failed", "upstream_failed" -> WorkflowStatus.FAILED;
			default -> {
				s_logger.warn("unknown Airflow task state: {}", state);
				yield WorkflowStatus.UNKNOWN;
			}
		};
	}
	
	// ISO 시각 필드를 읽는다. 없거나 null이면 null.
	private static @Nullable LocalDateTime parseTime(JsonNode node, String field) {
		return node.hasNonNull(field) ? LocalDateTimes.fromInstant(Instant.parse(node.get(field).asText())) : null;
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
	
	private okhttp3.Response addBearerToken(okhttp3.Interceptor.Chain chain) throws IOException {
		okhttp3.Request req = chain.request().newBuilder()
									.header("Authorization", "Bearer " + m_jwtToken)
									.build();
		return chain.proceed(req);
	}
	
	// 401 응답을 받으면 OkHttp가 부른다. 새 토큰을 붙인 요청을 돌려주면 그 요청으로 한 번 재시도하고,
	// null을 돌려주면 401을 그대로 전달한다.
	private okhttp3.Request reauthenticate(okhttp3.Route route, okhttp3.Response response) {
		if ( response.priorResponse() != null ) {
			// 새 토큰으로 재시도한 요청도 401이면 더 시도하지 않는다(계정 오류 등).
			return null;
		}
		String used = bearerTokenOf(response.request());
		try {
			String token = refreshJwtToken(used);
			return response.request().newBuilder()
							.header("Authorization", "Bearer " + token)
							.build();
		}
		catch ( Exception e ) {
			s_logger.error("failed to refresh Airflow JWT: airflow-url={}, cause={}", m_conf.getAirflowBaseUrl(), "" + e);
			return null;
		}
	}
	
	/**
	 * Airflow JWT를 다시 받는다.
	 * <p>
	 * 여러 요청이 동시에 401을 받아도 한 번만 발급받도록, 그 요청에 쓴 토큰이 아직 현재 토큰일 때만 새로 받는다.
	 *
	 * @param staleToken	401을 받은 요청에 쓴 토큰.
	 * @return	현재(갱신된) 토큰.
	 */
	private synchronized String refreshJwtToken(@Nullable String staleToken) throws JsonProcessingException {
		if ( staleToken == null || staleToken.equals(m_jwtToken) ) {
			m_jwtToken = getJwtToken(m_loginClient, m_conf.getUsername(), m_conf.getPassword());
			s_logger.info("refreshed Airflow JWT: airflow-url={}", m_conf.getAirflowBaseUrl());
		}
		return m_jwtToken;
	}
	
	private static @Nullable String bearerTokenOf(okhttp3.Request req) {
		String header = req.header("Authorization");
		return (header != null && header.startsWith("Bearer ")) ? header.substring("Bearer ".length()) : null;
	}
	
	/**
	 * 초기화된 Airflow 클라이언트를 반환한다. 처음 부를 때 {@link #connect()}로 초기화한다.
	 * <p>
	 * 초기화는 한 번만 실행되고(동시에 불려도), 모든 단계가 성공했을 때만 결과를 기억한다. 실패하면 예외를 던지고
	 * 다음 호출에서 다시 시도한다. 2026-11-04 이전에는 인증 없는 클라이언트를 먼저 필드에 넣은 뒤 토큰을 받아서,
	 * 그 뒤 단계가 실패하면 미완성 클라이언트가 남아 다시 초기화하지 않았고, 동기화도 없었다.
	 */
	private HttpRESTfulClient getRestfulClient() {
		HttpRESTfulClient client = m_restfulClient;
		if ( client != null ) {
			return client;
		}
		synchronized ( this ) {
			if ( m_restfulClient == null ) {
				m_restfulClient = connect();
			}
			return m_restfulClient;
		}
	}
	
	// 토큰 발급 → 인증 클라이언트 생성 → 'mdt_manager_url' 변수 재등록. 하나라도 실패하면 예외를 던진다.
	private HttpRESTfulClient connect() {
		try {
			// 기본은 서버 인증서를 검증한다. 2026-11-04 이전에는 항상 trust-all 클라이언트를 썼다.
			OkHttpClient baseClient = m_conf.isInsecureTls()
									? OkHttpClientUtils.newTrustAllOkHttpClientBuilder().build()
									: OkHttpClientUtils.newClient();
			if ( m_conf.isInsecureTls() ) {
				s_logger.warn("TLS certificate verification is disabled for Airflow: airflow-url={}", m_airflowUrl);
			}
			JsonMapper mapper = MDTModelSerDe.getJsonMapper();
			HttpRESTfulClient loginClient = HttpRESTfulClient.builder()
															.httpClient(baseClient)
															.jsonMapper(mapper)
															.errorEntityDeserializer(new AirflowErrorEntityDeserializer())
															.build();
			String token = getJwtToken(loginClient, m_conf.getUsername(), m_conf.getPassword());
			// 아래 요청이 401을 받으면 reauthenticate()가 m_loginClient로 토큰을 다시 받으므로 먼저 설정한다.
			m_loginClient = loginClient;
			m_jwtToken = token;

			// 토큰을 헤더에 고정하지 않고 요청마다 붙이며, 401을 받으면 토큰을 다시 받아 한 번 재시도한다(D2).
			OkHttpClient authClient = baseClient.newBuilder()
												.addInterceptor(this::addBearerToken)
												.authenticator(this::reauthenticate)
												.build();
			HttpRESTfulClient client = HttpRESTfulClient.builder()
														.httpClient(authClient)
														.jsonMapper(mapper)
														.errorEntityDeserializer(new AirflowErrorEntityDeserializer())
														.build();
			
			// 혹시 있을지 모르는 'mdt_manager_url' variable 제거하고 다시 새 endpoint 추가한다.
			try {
				client.delete(String.format("%s/variables/%s", m_airflowUrl, VARIABLE_MDT_URL));
			}
			catch ( RESTfulRemoteException | RESTfulIOException ignored ) { }
			
			String reqBody = MDTModelSerDe.MAPPER.writeValueAsString(Map.of(
															"key", VARIABLE_MDT_URL,
															"value", m_conf.getMdtUrl() + "/instance-manager",
															"description", "MDT InstanceManager URL"));
			client.post(String.format("%s/variables", m_airflowUrl),
						RequestBody.create(reqBody, HttpRESTfulClient.MEDIA_TYPE_JSON));
			s_logger.info("successfully connected to Airflow REST API: airflow-url={}", m_airflowUrl);
			
			return client;
		}
		catch ( RESTfulIOException e ) {
			throw new RESTfulIOException("failed to connect to Airflow REST API, airflow-url="
										+ m_airflowUrl, e);
		}
		catch ( Exception e ) {
			throw new RuntimeException("failed to initialize AirflowWorkflowEngine", e);
		}
	}
}

package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import mdt.model.ResourceNotFoundException;
import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 * {@link AirflowWorkflowEngine}의 Airflow REST 호출을 MockWebServer로 확인한다(mdt-workflow 검토 D4–D7).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowWorkflowEngineApiTest {
	@TempDir File m_dagsFolder;
	private MockWebServer m_server;
	private AirflowWorkflowEngine m_manager;

	@BeforeEach
	public void setUp() throws Exception {
		m_server = new MockWebServer();
		m_server.start();

		AirflowWorkflowManagerConfiguration conf = new AirflowWorkflowManagerConfiguration();
		conf.setMdtUrl("http://mdt:12985");
		String base = m_server.url("/").toString();
		conf.setAirflowBaseUrl(base.substring(0, base.length() - 1));
		conf.setDagsFolder(m_dagsFolder);
		conf.setUsername("user");
		conf.setPassword("pw");

		m_server.enqueue(json(200, "{\"access_token\": \"t1\"}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));
		m_server.enqueue(json(200, "{}"));
		m_manager = new AirflowWorkflowEngine(conf);
		m_manager.afterPropertiesSet();
		for ( int i = 0; i < 3; ++i ) {
			take();
		}
	}

	@AfterEach
	public void tearDown() throws Exception {
		m_server.shutdown();
	}

	private static MockResponse json(int code, String body) {
		return new MockResponse().setResponseCode(code)
								.setHeader("Content-Type", "application/json")
								.setBody(body);
	}

	private RecordedRequest take() throws InterruptedException {
		RecordedRequest req = m_server.takeRequest(5, TimeUnit.SECONDS);
		assertTrue(req != null, "expected another request");
		return req;
	}

	/**
	 * 전체 삭제는 MDT DAG의 실행만 지우고(D4), 목록은 모든 페이지를 읽는다(D5).
	 */
	@Test
	public void removeAllDeletesOnlyMdtRunsAcrossPages() throws Exception {
		// 서버 limit이 1인 것처럼 한 페이지에 하나씩 돌려준다. 두 번째 DAG는 태그가 다르다.
		m_server.enqueue(json(200, "{\"dags\": [{\"dag_id\": \"a\", \"tags\": [{\"name\": \"mdt\"}]}], "
									+ "\"total_entries\": 2}"));
		m_server.enqueue(json(200, "{\"dags\": [{\"dag_id\": \"other\", \"tags\": [{\"name\": \"x\"}]}], "
									+ "\"total_entries\": 2}"));
		m_server.enqueue(json(200, "{\"dag_runs\": [{\"dag_run_id\": \"r1\"}], \"total_entries\": 1}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));

		m_manager.removeWorkflowAll();

		RecordedRequest page1 = take();
		assertEquals("/api/v2/dags", page1.getRequestUrl().encodedPath());
		assertEquals("mdt", page1.getRequestUrl().queryParameter("tags"));
		assertEquals("0", page1.getRequestUrl().queryParameter("offset"));
		assertEquals("1", take().getRequestUrl().queryParameter("offset"));
		assertEquals("/api/v2/dags/a/dagRuns", take().getRequestUrl().encodedPath());
		RecordedRequest delete = take();
		assertEquals("DELETE", delete.getMethod());
		assertEquals("/api/v2/dags/a/dagRuns/r1", delete.getPath());
		assertEquals(7, m_server.getRequestCount());		// 초기화 3 + 목록 2 + run 목록 1 + 삭제 1
	}

	@Test
	public void removeWorkflowsOfModelListsOnlyThatDagAndKeepsGoingOnFailure() throws Exception {
		m_server.enqueue(json(200, "{\"dag_runs\": [{\"dag_run_id\": \"r1\"}, {\"dag_run_id\": \"r2\"}], "
									+ "\"total_entries\": 2}"));
		m_server.enqueue(json(500, "{\"detail\": \"boom\"}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));

		m_manager.removeWorkflowsOfModel("m1");

		assertEquals("/api/v2/dags/m1/dagRuns", take().getRequestUrl().encodedPath());
		assertEquals("/api/v2/dags/m1/dagRuns/r1", take().getPath());
		assertEquals("/api/v2/dags/m1/dagRuns/r2", take().getPath());	// r1 실패 뒤에도 계속 지운다
		assertEquals(6, m_server.getRequestCount());		// 초기화 3 + run 목록 1 + 삭제 2
	}

	@Test
	public void removeWorkflowsOfUndeployedModelDoesNothing() throws Exception {
		m_server.enqueue(json(404, "{\"detail\": \"DAG not found\"}"));
		m_manager.removeWorkflowsOfModel("nope");
		assertEquals(4, m_server.getRequestCount());
	}

	/**
	 * Airflow의 404는 ResourceNotFoundException이다(D7).
	 */
	@Test
	public void missingDagRunIsResourceNotFound() {
		m_server.enqueue(json(404, "{\"detail\": \"DagRun not found\"}"));
		assertThrows(ResourceNotFoundException.class, () -> m_manager.getWorkflow("a__r9"));

		m_server.enqueue(json(404, "{\"detail\": \"DagRun not found\"}"));
		assertThrows(ResourceNotFoundException.class, () -> m_manager.removeWorkflow("a__r9"));
	}

	@Test
	public void unsupportedOperationsAndToString() {
		// suspend/resume은 인터페이스 기본 구현(검토 A2 (a))
		assertThrows(UnsupportedOperationException.class, () -> m_manager.suspendWorkflow("a__r1"));
		assertThrows(UnsupportedOperationException.class, () -> m_manager.resumeWorkflow("a__r1"));
		// D6: 이전에는 MissingFormatArgumentException
		assertTrue(m_manager.toString().startsWith("AirflowWorkflowEngine[airflow="), m_manager.toString());
	}

	/**
	 * 로그 조회는 task의 최신 시도 로그를 읽어 한 항목당 한 줄로 바꾼다(검토 A2, 방식 (1): podName = task id).
	 */
	@Test
	public void getWorkflowLogReadsLatestTryOfTask() throws Exception {
		m_server.enqueue(json(200, "{\"task_id\": \"read scene\", \"try_number\": 2}"));
		m_server.enqueue(json(200, "{\"content\": ["
				+ "{\"timestamp\": \"2026-10-25T13:14:15Z\", \"level\": \"info\", \"logger\": \"mdtpy\", "
				+ "\"event\": \"Invoking operation\"},"
				+ "{\"event\": \"::endgroup::\"},"
				+ "{\"level\": \"error\", \"event\": \"Task failed\", \"error_detail\": [{\"exc_type\": \"KeyError\"}]}"
				+ "], \"continuation_token\": null}"));

		String log = m_manager.getWorkflowLog("a__mdt__r1", "read scene");
		assertEquals("2026-10-25T13:14:15Z INFO mdtpy: Invoking operation\n"
					+ "::endgroup::\n"
					+ "ERROR Task failed {\"error_detail\":[{\"exc_type\":\"KeyError\"}]}\n", log);

		assertEquals("/api/v2/dags/a/dagRuns/mdt__r1/taskInstances/read%20scene", take().getRequestUrl().encodedPath());
		RecordedRequest logReq = take();
		assertEquals("/api/v2/dags/a/dagRuns/mdt__r1/taskInstances/read%20scene/logs/2",
					logReq.getRequestUrl().encodedPath());
		assertEquals("true", logReq.getRequestUrl().queryParameter("full_content"));
	}

	@Test
	public void getWorkflowLogOfNotStartedOrMissingTaskIsNotFound() throws Exception {
		m_server.enqueue(json(200, "{\"task_id\": \"t\", \"try_number\": 0}"));
		assertThrows(ResourceNotFoundException.class, () -> m_manager.getWorkflowLog("a__r1", "t"));

		m_server.enqueue(json(404, "{\"detail\": \"TaskInstance not found\"}"));
		assertThrows(ResourceNotFoundException.class, () -> m_manager.getWorkflowLog("a__r1", "nope"));
	}

	/**
	 * 문자열 배열 형식의 로그 content도 줄로 바꾼다.
	 */
	@Test
	public void toLogTextAcceptsPlainStrings() throws Exception {
		com.fasterxml.jackson.databind.JsonNode content
				= new com.fasterxml.jackson.databind.ObjectMapper().readTree("[\"line1\", \"line2\"]");
		assertEquals("line1\nline2\n", AirflowWorkflowEngine.toLogText(content));
	}

	private static mdt.workflow.WorkflowModel oneTaskModel(String id) {
		mdt.workflow.WorkflowModel model = new mdt.workflow.WorkflowModel();
		model.setId(id);
		model.getTaskDescriptors().add(new mdt.workflow.model.TaskDescriptor("set", "set",
																			mdt.task.SetTask.class.getName()));
		return model;
	}

	/**
	 * DAG 파일은 임시 파일 없이 완성된 채로 생기고(D11), 삭제는 파일을 먼저 지운 뒤 Airflow DAG를 지운다(D11).
	 * 잘못된 id로는 DAG 파일 경로를 만들지 않는다(D19).
	 */
	@Test
	public void dagFileIsWrittenAtomicallyAndRemovedBeforeApiDelete() throws Exception {
		m_manager.deploy(oneTaskModel("wf1"));
		File dagFile = new File(m_dagsFolder, "wf1.py");
		assertTrue(dagFile.isFile());
		assertEquals(1, m_dagsFolder.listFiles().length, "no temporary file must remain");

		// API 삭제 요청이 도착했을 때 파일은 이미 없어야 한다.
		m_server.setDispatcher(new okhttp3.mockwebserver.Dispatcher() {
			@Override
			public MockResponse dispatch(RecordedRequest req) {
				return new MockResponse().setResponseCode(dagFile.exists() ? 500 : 204);
			}
		});
		m_manager.undeploy("wf1");
		assertTrue(!dagFile.exists());
		RecordedRequest delete = take();
		assertEquals("DELETE", delete.getMethod());
		assertEquals("/api/v2/dags/wf1", delete.getPath());

		assertThrows(IllegalArgumentException.class, () -> m_manager.undeploy("../x"));
	}

	/**
	 * 시작 요청의 run id는 시작 시각 + 무작위 접미어이고 logical_date와 같은 시각이다(D13).
	 * 중지는 queued run도 실패로 바꾸고, task PATCH 본문은 Airflow API의 'new_state'다(D14).
	 */
	@Test
	public void startAndStopRequests() throws Exception {
		m_server.enqueue(json(200, "{\"is_paused\": false}"));
		// 실행 응답은 DAGRunResponse 전체이므로 DagRun을 다시 읽지 않는다(I6).
		m_server.enqueue(json(200, "{\"dag_run_id\": \"r1\", \"state\": \"queued\", \"queued_at\": null}"));
		m_server.enqueue(json(200, "{\"task_instances\": []}"));
		m_manager.startWorkflow("wf1");
		assertEquals("/api/v2/dags/wf1", take().getPath());
		RecordedRequest start = take();
		com.fasterxml.jackson.databind.JsonNode body
				= new com.fasterxml.jackson.databind.ObjectMapper().readTree(start.getBody().readUtf8());
		String runId = body.get("dag_run_id").asText();
		String logicalDate = body.get("logical_date").asText();
		assertTrue(runId.startsWith("mdt__" + logicalDate + "_"), runId + " / " + logicalDate);
		assertEquals("/api/v2/dags/wf1/dagRuns/r1/taskInstances", take().getRequestUrl().encodedPath());

		m_server.enqueue(json(200, "{\"dag_run_id\": \"r1\", \"state\": \"queued\"}"));
		m_server.enqueue(json(200, "{}"));
		m_server.enqueue(json(200, "{\"task_instances\": [{\"task_id\": \"set\"}], \"total_entries\": 1}"));
		m_server.enqueue(json(200, "{}"));
		m_manager.stopWorkflow("wf1__r1");
		take();
		RecordedRequest patchRun = take();
		assertEquals("PATCH", patchRun.getMethod());
		assertEquals("{\"state\": \"failed\"}", patchRun.getBody().readUtf8());
		take();
		RecordedRequest patchTask = take();
		assertEquals("/api/v2/dags/wf1/dagRuns/r1/taskInstances/set", patchTask.getPath());
		assertEquals("{\"new_state\": \"failed\"}", patchTask.getBody().readUtf8());
	}

	/**
	 * 실행 식별자를 주면 그대로 dag_run_id 가 되고, 워크플로우 식별자는 {@code <modelId>__<runId>}다.
	 */
	@Test
	public void startWithRunIdUsesItAsDagRunId() throws Exception {
		m_server.enqueue(json(200, "{\"is_paused\": false}"));
		m_server.enqueue(json(200, "{\"dag_run_id\": \"insp-1\", \"state\": \"queued\"}"));
		m_server.enqueue(json(200, "{\"task_instances\": []}"));
		mdt.workflow.Workflow wf = m_manager.startWorkflow("wf1", "insp-1");
		take();
		RecordedRequest start = take();
		com.fasterxml.jackson.databind.JsonNode body
				= new com.fasterxml.jackson.databind.ObjectMapper().readTree(start.getBody().readUtf8());
		assertEquals("insp-1", body.get("dag_run_id").asText());
		assertEquals("wf1__insp-1", wf.getName());
	}

	/**
	 * 같은 실행 식별자로 다시 시작하면(Airflow 409) 새로 시작하지 않고 이미 있는 실행을 돌려준다.
	 */
	@Test
	public void startWithExistingRunIdReturnsTheExistingRun() throws Exception {
		m_server.enqueue(json(200, "{\"is_paused\": false}"));
		m_server.enqueue(json(409, "{\"detail\": \"DAGRun with dag_run_id: 'insp-1' already exists\"}"));
		m_server.enqueue(json(200, "{\"dag_run_id\": \"insp-1\", \"state\": \"running\"}"));
		m_server.enqueue(json(200, "{\"task_instances\": []}"));
		mdt.workflow.Workflow wf = m_manager.startWorkflow("wf1", "insp-1");
		take();
		assertEquals("POST", take().getMethod());
		assertEquals("/api/v2/dags/wf1/dagRuns/insp-1", take().getPath());
		assertEquals("wf1__insp-1", wf.getName());
		assertEquals(mdt.workflow.WorkflowStatus.RUNNING, wf.getStatus());
	}

	/**
	 * 형식이 맞지 않는 실행 식별자는 Airflow 에 요청하지 않고 거부한다.
	 */
	@Test
	public void startWithInvalidRunIdIsRejected() throws Exception {
		org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
								() -> m_manager.startWorkflow("wf1", "bad id/../x"));
		assertEquals(null, m_server.takeRequest(200, TimeUnit.MILLISECONDS));
	}

	/**
	 * Airflow 상태 매핑(D12).
	 */
	@Test
	public void airflowStatesMapToWorkflowStatus() {
		assertEquals(mdt.workflow.WorkflowStatus.STARTING, AirflowWorkflowEngine.toDagRunStatus("queued"));
		assertEquals(mdt.workflow.WorkflowStatus.UNKNOWN, AirflowWorkflowEngine.toDagRunStatus("timeout"));
		assertEquals(mdt.workflow.WorkflowStatus.FAILED, AirflowWorkflowEngine.toTaskStatus("upstream_failed"));
		assertEquals(mdt.workflow.WorkflowStatus.RUNNING, AirflowWorkflowEngine.toTaskStatus("up_for_retry"));
		assertEquals(mdt.workflow.WorkflowStatus.RUNNING, AirflowWorkflowEngine.toTaskStatus("deferred"));
		assertEquals(mdt.workflow.WorkflowStatus.NOT_STARTED, AirflowWorkflowEngine.toTaskStatus(null));
		assertEquals(mdt.workflow.WorkflowStatus.NOT_STARTED, AirflowWorkflowEngine.toTaskStatus("skipped"));
		assertEquals(mdt.workflow.WorkflowStatus.UNKNOWN, AirflowWorkflowEngine.toTaskStatus("removed"));
	}
}

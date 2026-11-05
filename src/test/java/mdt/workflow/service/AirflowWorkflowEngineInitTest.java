package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 * {@link AirflowWorkflowEngine}의 Airflow 클라이언트 초기화가 실패하면 미완성 클라이언트를 남기지 않고
 * 다음 호출에서 처음부터 다시 초기화하는지 확인한다(mdt-workflow 검토 D3).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowWorkflowEngineInitTest {
	@TempDir File m_dagsFolder;
	private MockWebServer m_server;
	private AirflowWorkflowManagerConfiguration m_conf;

	@BeforeEach
	public void setUp() throws Exception {
		m_server = new MockWebServer();
		m_server.start();

		m_conf = new AirflowWorkflowManagerConfiguration();
		m_conf.setMdtUrl("http://mdt:12985");
		String base = m_server.url("/").toString();
		m_conf.setAirflowBaseUrl(base.substring(0, base.length() - 1));
		m_conf.setDagsFolder(m_dagsFolder);
		m_conf.setUsername("user");
		m_conf.setPassword("pw");
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

	@Test
	public void failedVariableRegistrationIsRetriedOnNextCall() throws Exception {
		// 시작: 토큰은 받았지만 변수 등록이 실패한다. afterPropertiesSet()은 로그만 남긴다.
		m_server.enqueue(json(200, "{\"access_token\": \"t1\"}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));
		m_server.enqueue(json(500, "{\"detail\": \"boom\"}"));
		AirflowWorkflowEngine manager = new AirflowWorkflowEngine(m_conf);
		manager.afterPropertiesSet();
		assertEquals("/auth/token", take().getPath());
		take();
		take();

		// 다음 호출은 처음부터 다시 초기화한 뒤 요청을 보낸다.
		m_server.enqueue(json(200, "{\"access_token\": \"t2\"}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));
		m_server.enqueue(json(200, "{}"));
		m_server.enqueue(json(200, "{\"dags\": []}"));
		assertEquals(0, manager.listWorkflowIds().size());

		assertEquals("/auth/token", take().getPath());
		assertEquals("/api/v2/variables/mdt_manager_url", take().getPath());
		RecordedRequest register = take();
		assertEquals("/api/v2/variables", register.getPath());
		assertEquals("Bearer t2", register.getHeader("Authorization"));
		String body = register.getBody().readUtf8();
		assertTrue(body.contains("\"key\":\"mdt_manager_url\""), body);
		assertTrue(body.contains("\"value\":\"http://mdt:12985/instance-manager\""), body);
		RecordedRequest dags = take();
		assertEquals("/api/v2/dags", dags.getRequestUrl().encodedPath());
		assertEquals("Bearer t2", dags.getHeader("Authorization"));

		// 초기화가 끝나면 다시 하지 않는다.
		m_server.enqueue(json(200, "{\"dags\": []}"));
		manager.listWorkflowIds();
		assertEquals("/api/v2/dags", take().getRequestUrl().encodedPath());
		assertEquals(8, m_server.getRequestCount());
	}
}

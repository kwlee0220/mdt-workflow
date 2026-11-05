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

import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 * {@link AirflowWorkflowEngine}가 만료된 JWT를 다시 받아 요청을 재시도하는지 확인한다(mdt-workflow 검토 D2).
 * <p>
 * 2026-11-04 이전에는 처음 받은 토큰을 헤더로 고정해서, 토큰이 만료되면 모든 호출이 401로 실패했다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowWorkflowEngineJwtTest {
	@TempDir File m_dagsFolder;
	private MockWebServer m_server;
	private AirflowWorkflowEngine m_manager;

	@BeforeEach
	public void setUp() throws Exception {
		m_server = new MockWebServer();
		m_server.start();

		AirflowWorkflowManagerConfiguration conf = new AirflowWorkflowManagerConfiguration();
		conf.setMdtUrl("http://localhost:12985");
		String base = m_server.url("/").toString();
		conf.setAirflowBaseUrl(base.substring(0, base.length() - 1));
		conf.setDagsFolder(m_dagsFolder);
		conf.setUsername("user");
		conf.setPassword("p\"w");

		// 초기화: 토큰 발급 → mdt_manager_url 변수 삭제 → 등록
		m_server.enqueue(json(200, "{\"access_token\": \"t1\"}"));
		m_server.enqueue(new MockResponse().setResponseCode(204));
		m_server.enqueue(json(200, "{}"));

		m_manager = new AirflowWorkflowEngine(conf);
		m_manager.afterPropertiesSet();

		RecordedRequest login = take();
		assertEquals("/auth/token", login.getPath());
		// Map.of의 순서는 정해져 있지 않으므로 필드 단위로 비교한다.
		com.fasterxml.jackson.databind.JsonNode body
				= new com.fasterxml.jackson.databind.ObjectMapper().readTree(login.getBody().readUtf8());
		assertEquals("user", body.get("username").asText());
		assertEquals("p\"w", body.get("password").asText());
		assertEquals("Bearer t1", take().getHeader("Authorization"));		// 변수 삭제
		assertEquals("Bearer t1", take().getHeader("Authorization"));		// 변수 등록
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
	public void expiredTokenIsRefreshedAndRequestRetried() throws Exception {
		m_server.enqueue(json(401, "{\"detail\": \"Token has expired\"}"));
		m_server.enqueue(json(200, "{\"access_token\": \"t2\"}"));
		m_server.enqueue(json(200, "{\"dags\": []}"));

		assertEquals(0, m_manager.listWorkflowIds().size());

		RecordedRequest first = take();
		assertEquals("/api/v2/dags", first.getRequestUrl().encodedPath());
		assertEquals("Bearer t1", first.getHeader("Authorization"));
		assertEquals("/auth/token", take().getPath());
		RecordedRequest retried = take();
		assertEquals("/api/v2/dags", retried.getRequestUrl().encodedPath());
		assertEquals("Bearer t2", retried.getHeader("Authorization"));

		// 이후 요청은 새 토큰을 쓴다.
		m_server.enqueue(json(200, "{\"dags\": []}"));
		m_manager.listWorkflowIds();
		assertEquals("Bearer t2", take().getHeader("Authorization"));
	}

	@Test
	public void unauthorizedAfterRefreshIsNotRetriedAgain() throws Exception {
		m_server.enqueue(json(401, "{\"detail\": \"Token has expired\"}"));
		m_server.enqueue(json(200, "{\"access_token\": \"t2\"}"));
		m_server.enqueue(json(401, "{\"detail\": \"Forbidden\"}"));

		assertThrows(RuntimeException.class, () -> m_manager.listWorkflowIds());
		assertEquals(3, m_server.getRequestCount() - 3);		// 초기화 3개를 뺀 요청 수
	}
}

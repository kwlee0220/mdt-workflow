package mdt.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;


/**
 * env 파일 변수의 로그용 값에서 비밀번호가 가려지는지 확인한다(mdt-workflow 검토 D10).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvironmentVariablePostProcessorTest {
	@Test
	public void secretsAreMasked() {
		assertEquals("****", EnvironmentVariablePostProcessor.maskValue("AIRFLOW_PASSWORD", "airflow"));
		assertEquals("****", EnvironmentVariablePostProcessor.maskValue("api_token", "abc"));
		assertEquals("****", EnvironmentVariablePostProcessor.maskValue("DB_PASSWD", "x"));
		assertEquals("jdbc:postgresql://db/mdt?user=mdt&password=****",
					EnvironmentVariablePostProcessor.maskValue("POSTGRES_URL",
															"jdbc:postgresql://db/mdt?user=mdt&password=secret"));
		assertEquals("http://localhost:8080", EnvironmentVariablePostProcessor.maskValue("AIRFLOW_URL",
																						"http://localhost:8080"));
	}
}

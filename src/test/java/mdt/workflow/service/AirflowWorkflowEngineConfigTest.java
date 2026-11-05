package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import mdt.workflow.config.AirflowWorkflowManagerConfiguration;


/**
 * {@link AirflowWorkflowEngine}가 Airflow 계정 설정을 필수로 요구하는지 확인한다(mdt-workflow 검토 D1).
 * <p>
 * 계정 검사는 Airflow에 접속하기 전에 하므로 실행 중인 Airflow 없이 확인할 수 있다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowWorkflowEngineConfigTest {
	@TempDir File m_dagsFolder;

	private AirflowWorkflowManagerConfiguration conf(String username, String password) {
		AirflowWorkflowManagerConfiguration conf = new AirflowWorkflowManagerConfiguration();
		conf.setMdtUrl("http://localhost:1");
		conf.setAirflowBaseUrl("http://localhost:1");
		conf.setDagsFolder(m_dagsFolder);
		conf.setUsername(username);
		conf.setPassword(password);
		return conf;
	}

	@Test
	public void missingUsernameFailsAtStartup() {
		for ( String username: new String[] { null, " " } ) {
			AirflowWorkflowEngine mgr = new AirflowWorkflowEngine(conf(username, "secret"));
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, mgr::afterPropertiesSet);
			assertTrue(e.getMessage().contains("mdt.workflow-manager.airflow.username"), e.getMessage());
		}
	}

	@Test
	public void missingPasswordFailsAtStartup() {
		AirflowWorkflowEngine mgr = new AirflowWorkflowEngine(conf("airflow", null));
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, mgr::afterPropertiesSet);
		assertTrue(e.getMessage().contains("mdt.workflow-manager.airflow.password"), e.getMessage());
	}
}

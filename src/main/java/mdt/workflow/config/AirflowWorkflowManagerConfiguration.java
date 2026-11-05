package mdt.workflow.config;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


/**
 * Airflow 연동 설정({@code mdt.workflow-manager.airflow.*}).
 * <p>
 * {@code username}/{@code password}는 Airflow REST API의 JWT 발급({@code /auth/token})에 쓰는 계정이며 필수이다.
 * 비밀번호를 설정 파일에 직접 쓰지 않도록 env 파일의 변수로 넘기는 것을 권장한다
 * (예: {@code mdt.workflow-manager.airflow.password=${AIRFLOW_PASSWORD}}).
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Configuration
@ConfigurationProperties(prefix = "mdt.workflow-manager.airflow")
public class AirflowWorkflowManagerConfiguration {
	private String m_mdtUrl;
	private String m_airflowBaseUrl;
	private File m_dagsFolder;
	private String m_username;
	private String m_password;
	private boolean m_insecureTls = false;
	
	public String getMdtUrl() {
		return m_mdtUrl;
	}
	
	public void setMdtUrl(String url) {
		m_mdtUrl = url;
	}
	
	public String getAirflowBaseUrl() {
		return m_airflowBaseUrl;
	}
	
	public void setAirflowBaseUrl(String url) {
		m_airflowBaseUrl = url;
	}

	public File getDagsFolder() {
		return m_dagsFolder;
	}
	
	public void setDagsFolder(File dagsFolder) {
		m_dagsFolder = dagsFolder;
	}

	/**
	 * Airflow REST API 계정의 사용자 이름을 반환한다.
	 *
	 * @return	사용자 이름.
	 */
	public String getUsername() {
		return m_username;
	}

	public void setUsername(String username) {
		m_username = username;
	}

	/**
	 * Airflow REST API 계정의 비밀번호를 반환한다.
	 *
	 * @return	비밀번호.
	 */
	public String getPassword() {
		return m_password;
	}

	public void setPassword(String password) {
		m_password = password;
	}

	/**
	 * Airflow 서버의 TLS 인증서 검증을 끌지 여부를 반환한다({@code mdt.workflow-manager.airflow.insecureTls}, 기본값
	 * {@code false}).
	 * <p>
	 * 사설·자체 서명 인증서를 쓰는 시험 환경에서만 {@code true}로 둔다. 켜면 중간자가 Airflow 계정과 JWT를 가로챌 수
	 * 있다. 2026-11-04 이전에는 항상 검증하지 않았다.
	 *
	 * @return	인증서 검증을 끄면 {@code true}.
	 */
	public boolean isInsecureTls() {
		return m_insecureTls;
	}

	public void setInsecureTls(boolean insecure) {
		m_insecureTls = insecure;
	}
}

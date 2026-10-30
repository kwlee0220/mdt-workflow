package mdt.workflow.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


/**
 * 워크플로우 엔진과 무관한 워크플로우 관리자 설정({@code mdt.workflow-manager.*}).
 * <p>
 * Airflow 연동 설정은 {@link AirflowWorkflowManagerConfiguration}({@code mdt.workflow-manager.airflow.*})에 있다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Configuration
@ConfigurationProperties(prefix = "mdt.workflow-manager")
public class WorkflowManagerConfiguration {
	private String m_executionTimeEstimatorEndpoint;
	
	/**
	 * AI·시뮬레이션 작업의 실행시간 예측 서비스 주소를 반환한다.
	 *
	 * @return	예측 서비스 URL.
	 */
	public String getExecutionTimeEstimatorEndpoint() {
		return m_executionTimeEstimatorEndpoint;
	}
	
	public void setExecutionTimeEstimatorEndpoint(String ep) {
		m_executionTimeEstimatorEndpoint = ep;
	}
}

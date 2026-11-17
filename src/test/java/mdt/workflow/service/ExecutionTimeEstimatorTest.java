package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import mdt.workflow.config.AirflowWorkflowManagerConfiguration;
import mdt.workflow.config.WorkflowManagerConfiguration;


/**
 * 예측 서비스가 설정되지 않은 경우의 동작과 Airflow TLS 인증서 검증의 기본값을 확인한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class ExecutionTimeEstimatorTest {
	/**
	 * 예측 서비스 주소가 없으면 UnsupportedOperationException(컨트롤러 501)이다.
	 */
	@Test
	public void unconfiguredEstimatorIsUnsupported() {
		ExecutionTimeEstimator estimator = new ExecutionTimeEstimator(new WorkflowManagerConfiguration(),
																	new AirflowWorkflowManagerConfiguration());
		assertThrows(UnsupportedOperationException.class, () -> estimator.estimate("test:AddAndSleep"));
	}

	/**
	 * Airflow TLS 인증서 검증은 기본으로 켜져 있다.
	 */
	@Test
	public void tlsVerificationIsOnByDefault() {
		assertFalse(new AirflowWorkflowManagerConfiguration().isInsecureTls());
	}
}

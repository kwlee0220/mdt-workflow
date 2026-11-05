package mdt.workflow.service;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import okhttp3.MediaType;
import okhttp3.RequestBody;

import utils.http.HttpRESTfulClient;
import utils.http.RESTfulIOException;

import mdt.client.HttpMDTManager;
import mdt.client.instance.HttpMDTInstanceManager;
import mdt.model.MDTModelSerDe;
import mdt.model.expr.MDTExpressionParser;
import mdt.model.sm.ref.MDTSubmodelReference;
import mdt.workflow.config.AirflowWorkflowManagerConfiguration;
import mdt.workflow.config.WorkflowManagerConfiguration;


/**
 * AI·시뮬레이션 작업의 실행시간을 외부 예측 서비스({@code mdt.workflow-manager.executionTimeEstimatorEndpoint})로
 * 예측한다.
 * <p>
 * 주어진 Submodel 참조로 Submodel을 읽어 JSON으로 예측 서비스에 POST하고, 응답 본문을 실행시간(초, 숫자)으로 읽는다.
 * HTTP 클라이언트와 MDT 연결은 처음 쓸 때 한 번 만들어 재사용한다. 2026-11-04 이전에는 컨트롤러가 요청마다 둘 다
 * 새로 만들었다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Service
public class ExecutionTimeEstimator {
	private static final Logger s_logger = LoggerFactory.getLogger(ExecutionTimeEstimator.class);
	private static final MediaType JSON_TYPE = MediaType.parse("application/json; charset=utf-8");
	
	private final WorkflowManagerConfiguration m_conf;
	private final AirflowWorkflowManagerConfiguration m_airflowConf;
	private final HttpRESTfulClient m_client = HttpRESTfulClient.newDefaultClient();
	private volatile HttpMDTInstanceManager m_instanceManager;
	
	public ExecutionTimeEstimator(WorkflowManagerConfiguration conf, AirflowWorkflowManagerConfiguration airflowConf) {
		m_conf = conf;
		m_airflowConf = airflowConf;
	}
	
	/**
	 * 주어진 Submodel 참조가 가리키는 작업의 실행시간을 예측한다.
	 *
	 * @param smRefExpr	Submodel 참조 표현식(예: {@code test:AddAndSleep}).
	 * @return	예측한 실행시간(초).
	 * @throws UnsupportedOperationException	예측 서비스 주소가 설정되지 않은 경우(컨트롤러가 501로 응답).
	 * @throws RESTfulIOException	예측 서비스의 응답이 숫자가 아닌 경우(컨트롤러가 502로 응답).
	 */
	public double estimate(String smRefExpr) {
		String endpoint = m_conf.getExecutionTimeEstimatorEndpoint();
		if ( endpoint == null || endpoint.isBlank() ) {
			throw new UnsupportedOperationException("execution time estimator is not configured: "
													+ "mdt.workflow-manager.executionTimeEstimatorEndpoint");
		}
		s_logger.debug("estimating task execution time: smRef={}", smRefExpr);
		
		MDTSubmodelReference smRef = MDTExpressionParser.parseSubmodelReference(smRefExpr).evaluate();
		smRef.activate(instanceManager());
		Submodel sm = smRef.get().getSubmodel();
		
		RequestBody reqBody = RequestBody.create(MDTModelSerDe.toJsonString(sm), JSON_TYPE);
		String respBody = m_client.post(endpoint, reqBody, HttpRESTfulClient.STRING_DESER);
		try {
			return Double.parseDouble(respBody.strip());
		}
		catch ( NumberFormatException e ) {
			throw new RESTfulIOException("invalid response from the execution time estimator (not a number): "
										+ respBody, e);
		}
	}
	
	private HttpMDTInstanceManager instanceManager() {
		HttpMDTInstanceManager manager = m_instanceManager;
		if ( manager == null ) {
			synchronized ( this ) {
				if ( m_instanceManager == null ) {
					m_instanceManager = HttpMDTManager.connect(m_airflowConf.getMdtUrl()).getInstanceManager();
				}
				manager = m_instanceManager;
			}
		}
		return manager;
	}
}

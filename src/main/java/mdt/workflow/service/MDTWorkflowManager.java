package mdt.workflow.service;

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

import utils.Throwables;
import utils.func.Try;

import mdt.model.ResourceAlreadyExistsException;
import mdt.model.ResourceNotFoundException;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowEngine;
import mdt.workflow.WorkflowManager;
import mdt.workflow.WorkflowModel;
import mdt.workflow.WorkflowModelManager;
import mdt.workflow.WorkflowStatus;

/**
 * 워크플로우 관리자({@link WorkflowManager}) 구현. 모델 저장({@link WorkflowModelManager})과 워크플로우 엔진
 * ({@link WorkflowEngine})을 묶는다.
 * <p>
 * 모델을 등록하면 저장한 뒤 엔진에 배포하고, 배포가 실패하면 저장을 되돌린다(교체였으면 이전 모델을 다시
 * 저장·배포한다). 모델을 삭제하면 엔진 쪽 배포도 정리한다. 인스턴스 관련 메소드는 엔진에 그대로 맡긴다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Service
@RequiredArgsConstructor
public class MDTWorkflowManager implements WorkflowManager {
	private static final Logger s_logger = LoggerFactory.getLogger(MDTWorkflowManager.class);
	
	private final WorkflowModelManager m_modelManager;
	private final WorkflowEngine m_engine;
	
	@Override
	public List<WorkflowModel> getWorkflowModelAll() {
		return m_modelManager.getWorkflowModelAll();
	}
	
	@Override
	public WorkflowModel getWorkflowModel(String wfModelId) throws ResourceNotFoundException {
		return m_modelManager.getWorkflowModel(wfModelId);
	}
	
	@Override
	public WorkflowModel addWorkflowModel(WorkflowModel desc) throws ResourceAlreadyExistsException {
		WorkflowModel wfModel = m_modelManager.addWorkflowModel(desc);
		try {
			m_engine.deploy(wfModel);
			return wfModel;
		}
		catch ( Throwable e ) {
			Try.run(() -> m_modelManager.removeWorkflowModel(wfModel.getId()));
			throw toPostProcessFailure(wfModel, e);
		}
	}
	
	// 후처리 실패의 원래 예외 유형을 유지한다(잘못된 모델의 IllegalArgumentException은 400, 대상 없음은 404).
	// 2026-11-04 이전에는 모두 RuntimeException으로 감싸 500이 되었다.
	private static RuntimeException toPostProcessFailure(WorkflowModel wfModel, Throwable e) {
		Throwable cause = Throwables.unwrapThrowable(e);
		if ( cause instanceof RuntimeException re ) {
			return re;
		}
		if ( cause instanceof Error err ) {
			throw err;
		}
		return new RuntimeException("failed to process added workflow model: " + wfModel.getId(), cause);
	}
	
	/**
	 * 워크플로우 모델을 등록하거나 같은 id의 기존 모델을 교체한다.
	 * <p>
	 * 모델 저장 뒤의 후처리(엔진 배포 {@code deploy})가 실패하면 교체 전 상태로 되돌린다. 기존 모델이
	 * 있었으면 그 모델을 다시 저장하고 후처리를 다시 수행하며, 없었으면 새로 저장한 모델을 삭제한다.
	 */
	@Override
	public WorkflowModel addOrReplaceWorkflowModel(WorkflowModel desc) {
		WorkflowModel previous = findWorkflowModel(desc.getId());
		WorkflowModel wfModel = m_modelManager.addOrReplaceWorkflowModel(desc);
		try {
			m_engine.deploy(wfModel);
			return wfModel;
		}
		catch ( Throwable e ) {
			if ( previous != null ) {
				Try.run(() -> {
					m_modelManager.addOrReplaceWorkflowModel(previous);
					m_engine.deploy(previous);
				})
				.ifFailed(error -> s_logger.error("failed to restore the previous workflow model: id={}, cause={}",
													previous.getId(), error));
			}
			else {
				Try.run(() -> m_modelManager.removeWorkflowModel(wfModel.getId()));
			}
			throw toPostProcessFailure(wfModel, e);
		}
	}
	
	private WorkflowModel findWorkflowModel(String wfModelId) {
		try {
			return m_modelManager.getWorkflowModel(wfModelId);
		}
		catch ( ResourceNotFoundException e ) {
			return null;
		}
	}
	
	/**
	 * 워크플로우 모델을 삭제하고, 워크플로우 엔진에 배포된 것(Airflow DAG 파일 등)도 정리한다.
	 * <p>
	 * 엔진 쪽 정리가 실패해도 모델 삭제는 되돌리지 않고 WARN 로그를 남긴다.
	 *
	 * @throws ResourceNotFoundException	식별자에 해당하는 모델이 없는 경우.
	 */
	@Override
	public void removeWorkflowModel(String wfModelId) throws ResourceNotFoundException {
		s_logger.info("removing workflow model: {}", wfModelId);
		m_modelManager.removeWorkflowModel(wfModelId);
		
		try {
			m_engine.undeploy(wfModelId);
		}
		catch ( Exception e ) {
			// 2026-11-04 이전에는 로그도 없이 무시해서, Airflow에 DAG가 남아도 알 수 없었다.
			s_logger.warn("failed to clean up the removed workflow model in the workflow engine: id={}, cause={}",
							wfModelId, "" + e);
		}
	}
	
	/**
	 * 모든 워크플로우 모델을 {@link #removeWorkflowModel(String)}로 하나씩 삭제한다.
	 * <p>
	 * 한 모델의 삭제가 실패해도 나머지는 계속 삭제하고, 실패는 WARN 로그로 남긴다. 2026-11-04 이전에는 DB 행만
	 * 지우고 엔진 쪽(Airflow DAG 파일 등)을 정리하지 않았으며 오류도 삼켰다.
	 */
	@Override
	public void removeWorkflowModelAll() {
		for ( WorkflowModel wfModel: m_modelManager.getWorkflowModelAll() ) {
			try {
				removeWorkflowModel(wfModel.getId());
			}
			catch ( Exception e ) {
				s_logger.warn("failed to remove workflow model: id={}, cause={}", wfModel.getId(), "" + e);
			}
		}
	}
	
	/**
	 * 저장된 모델을 엔진용 산출물(Airflow DAG 코드)로 렌더링한다. 호출할 때마다 새로 만들므로, 템플릿이 바뀌었다면
	 * 배포된 DAG 파일과 다를 수 있다. 2026-11-07 이전 이름은 {@code getWorkflowScript}였다(별칭 없음).
	 * <p>
	 * 배포 상태와 상관없이 지금 저장된 모델로 만든다.
	 *
	 * @throws ResourceNotFoundException	식별자에 해당하는 모델이 없는 경우.
	 */
	public String renderWorkflowScript(String wfModelId) throws ResourceNotFoundException {
		return m_engine.render(m_modelManager.getWorkflowModel(wfModelId));
	}
	
	@Override
	public List<Workflow> getWorkflowAll() {
		return m_engine.getWorkflowAll();
	}

	@Override
	public List<String> listWorkflowIds() {
		return m_engine.listWorkflowIds();
	}

	@Override
	public WorkflowStatus getWorkflowStatus(String wfIdStr) throws ResourceNotFoundException {
		return m_engine.getWorkflowStatus(wfIdStr);
	}
	
	@Override
	public Workflow getWorkflow(String wfId) throws ResourceNotFoundException {
		return m_engine.getWorkflow(wfId);
	}
	
	@Override
	public Workflow startWorkflow(@NotNull String wfModelId) throws ResourceNotFoundException {
		return m_engine.startWorkflow(wfModelId);
	}
	
	@Override
	public void stopWorkflow(String wfId) throws ResourceNotFoundException {
		m_engine.stopWorkflow(wfId);
	}
	
	@Override
	public void removeWorkflow(String wfId) throws ResourceNotFoundException {
		m_engine.removeWorkflow(wfId);
	}
	
	@Override
	public void removeWorkflowAll() {
		m_engine.removeWorkflowAll();
	}
	
	/**
	 * 주어진 모델로 만든 워크플로우 인스턴스들을 모두 지운다. 모델이 배포되어 있지 않으면 아무 일도 하지 않는다.
	 *
	 * @param wfModelId	워크플로우 모델 id.
	 */
	public void removeWorkflowsOfModel(String wfModelId) {
		m_engine.removeWorkflowsOfModel(wfModelId);
	}
	
	@Override
	public String getWorkflowLog(String wfId, String podName) throws ResourceNotFoundException {
		return m_engine.getWorkflowLog(wfId, podName);
	}
}

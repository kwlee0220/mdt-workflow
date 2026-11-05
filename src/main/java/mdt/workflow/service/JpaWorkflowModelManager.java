package mdt.workflow.service;

import java.util.List;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;

import utils.func.Try;

import mdt.model.ResourceAlreadyExistsException;
import mdt.model.ResourceNotFoundException;
import mdt.workflow.WorkflowModel;
import mdt.workflow.WorkflowModelManager;
import mdt.workflow.domain.JpaWorkflowModel;
import mdt.workflow.repository.JpaWorkflowModelRepository;

/**
 * 워크플로우 모델을 JPA({@link mdt.workflow.domain.JpaWorkflowModel})로 저장하는 {@link WorkflowModelManager} 구현.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Service
@RequiredArgsConstructor
public class JpaWorkflowModelManager implements WorkflowModelManager {
	private final JpaWorkflowModelRepository m_repo;

	@Override
	public WorkflowModel getWorkflowModel(String id) {
		return m_repo.findByModelId(id)
						.map(JpaWorkflowModel::asWorkflowModel)
						.orElseThrow(() -> new ResourceNotFoundException("Workflow model not found: id=" + id));
	}
	
	@Override
	public List<WorkflowModel> getWorkflowModelAll() {
		return m_repo.findAll().stream()
					.map(JpaWorkflowModel::asWorkflowModel)
					.toList();
	}
	
	@Override
	public WorkflowModel addWorkflowModel(WorkflowModel wfModel) {
		try {
			JpaWorkflowModel jpaEntity = new JpaWorkflowModel(wfModel);
			JpaWorkflowModel saved = m_repo.save(jpaEntity);
			return saved.asWorkflowModel();
		}
		catch ( DataIntegrityViolationException e ) {
			throw new ResourceAlreadyExistsException("WorkflowModel", wfModel.getId());	
		}
	}

	/**
	 * 워크플로우 모델을 등록한다. 같은 id의 모델이 있으면 그 내용을 교체한다.
	 * <p>
	 * 기존 모델을 지우고 다시 저장하지 않고 같은 행을 한 트랜잭션 안에서 갱신한다. 따라서 저장이 실패해도
	 * 기존 모델은 남는다.
	 *
	 * @param wfModel	등록할 워크플로우 모델.
	 * @return	저장된 워크플로우 모델.
	 */
	@Override
	@Transactional
	public WorkflowModel addOrReplaceWorkflowModel(WorkflowModel wfModel) {
		JpaWorkflowModel entity = m_repo.findByModelId(wfModel.getId())
										.map(existing -> {
											existing.setJsonModel(new JpaWorkflowModel(wfModel).getJsonModel());
											return existing;
										})
										.orElseGet(() -> new JpaWorkflowModel(wfModel));
		return m_repo.save(entity).asWorkflowModel();
	}
	
	/**
	 * 워크플로우 모델을 삭제한다.
	 *
	 * @param id	삭제할 모델의 식별자.
	 * @throws ResourceNotFoundException	해당 모델이 없는 경우(2026-11-04 이전에는 아무 일 없이 성공했다).
	 */
	@Override
	public void removeWorkflowModel(String id) {
		if ( m_repo.deleteByModelId(id) == 0 ) {
			throw new ResourceNotFoundException("WorkflowModel", id);
		}
	}

	@Override
	public void removeWorkflowModelAll() {
		Try.run(m_repo::deleteAll);
	}
}

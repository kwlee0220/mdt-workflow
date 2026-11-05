package mdt.workflow.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import mdt.workflow.domain.JpaWorkflowModel;

import jakarta.transaction.Transactional;

/**
 * {@link mdt.workflow.domain.JpaWorkflowModel}의 Spring Data 리포지토리. 모델 id({@code modelId})로 조회·확인·삭제한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface JpaWorkflowModelRepository extends JpaRepository<JpaWorkflowModel, Long> {
	/**
	 * 워크플로우 모델을 ID로 조회한다.
	 * 
	 * @param modelId 워크플로우 모델의 ID
	 * @return 워크플로우 모델
	 */
	public Optional<JpaWorkflowModel> findByModelId(String modelId);
	
	/**
	 * 주어진 ID의 워크플로우 모델이 있는지 확인한다.
	 *
	 * @param modelId 워크플로우 모델의 ID
	 * @return 있으면 {@code true}
	 */
	public boolean existsByModelId(String modelId);

	/**
	 * 워크플로우 모델을 ID로 삭제한다.
	 *
	 * @param modelId 워크플로우 모델의 ID
	 * @return 삭제한 행 수(없으면 0)
	 */
	@Transactional
	public long deleteByModelId(String modelId);
}

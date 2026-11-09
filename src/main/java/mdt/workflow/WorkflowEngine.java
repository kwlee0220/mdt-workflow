package mdt.workflow;

/**
 * 워크플로우 엔진 어댑터가 구현하는 SPI.
 * <p>
 * 다음 세 가지를 정의한다.
 * <ul>
 *   <li>배포: {@link #deploy(WorkflowModel)} / {@link #undeploy(String)}. 모델을 엔진이 실행할 수 있게 올리고 내린다.</li>
 *   <li>렌더링: {@link #render(WorkflowModel)}. 모델을 엔진용 산출물(Airflow는 DAG 파이썬 코드)로 바꾼다.</li>
 *   <li>인스턴스 관리: 상속한 {@link WorkflowInstanceManager}(조회·시작·중지·삭제·로그).</li>
 * </ul>
 * {@link mdt.workflow.service.MDTWorkflowManager}가 모델을 저장한 뒤 {@link #deploy}를 부르고, 이것이 실패하면 저장을
 * 되돌린다. 즉 배포는 알림이 아니라 등록의 필수 단계다. 모델 저장은 이 SPI의 일이 아니다(엔진은 모델 저장소를 모른다).
 * 현재 구현은 {@link mdt.workflow.service.AirflowWorkflowEngine}뿐이다.
 * <p>
 * 2026-11-05 이전 이름은 {@code WorkflowInstanceManagerProvider}였고(구현 클래스는 {@code AirflowWorkflowManager}),
 * 메소드는 {@code onWorkflowModelAdded}/{@code onWorkflowModelRemoved}/{@code getWorkflowScript(String modelId)}였다
 * (별칭 없음).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public interface WorkflowEngine extends WorkflowInstanceManager {
	/**
	 * 모델을 엔진에 배포한다. 같은 id로 이미 배포된 것이 있으면 바꾼다.
	 *
	 * @param wfModel	배포할 워크플로우 모델.
	 * @throws IllegalArgumentException	모델이 엔진 규칙에 맞지 않는 경우(잘못된 id, 지원하지 않는 task 유형 등).
	 * @throws MDTWorkflowInstanceManagerException	배포 중 오류가 난 경우.
	 */
	public void deploy(WorkflowModel wfModel) throws MDTWorkflowInstanceManagerException;
	
	/**
	 * 모델의 배포를 내린다. 배포된 것이 없으면 아무 일도 하지 않는다.
	 *
	 * @param wfModelId	배포를 내릴 워크플로우 모델 id.
	 * @throws MDTWorkflowInstanceManagerException	정리 중 오류가 난 경우.
	 */
	public void undeploy(String wfModelId) throws MDTWorkflowInstanceManagerException;
	
	/**
	 * 모델을 엔진용 산출물 텍스트로 바꾼다. 배포하지 않는다.
	 *
	 * @param wfModel	렌더링할 워크플로우 모델.
	 * @return	엔진용 산출물(예: Airflow DAG 파이썬 코드).
	 * @throws IllegalArgumentException	모델이 엔진 규칙에 맞지 않는 경우.
	 * @throws MDTWorkflowInstanceManagerException	렌더링 중 오류가 난 경우.
	 */
	public String render(WorkflowModel wfModel) throws MDTWorkflowInstanceManagerException;
	
	/**
	 * 주어진 모델로 만든 워크플로우 인스턴스(실행)들을 모두 지운다. 모델이 배포되어 있지 않으면 아무 일도 하지 않는다.
	 * <p>
	 * 일부 인스턴스를 지우지 못해도 나머지는 계속 지우고, 실패는 로그로 남긴다.
	 *
	 * @param wfModelId	워크플로우 모델 id.
	 * @throws MDTWorkflowInstanceManagerException	인스턴스 목록을 얻지 못한 경우.
	 */
	public void removeWorkflowsOfModel(String wfModelId) throws MDTWorkflowInstanceManagerException;
}

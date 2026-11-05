package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mdt.model.ResourceNotFoundException;
import mdt.workflow.MDTWorkflowInstanceManagerException;
import mdt.workflow.WorkflowEngine;
import mdt.workflow.WorkflowModel;
import mdt.workflow.WorkflowModelManager;


/**
 * {@link MDTWorkflowManager#addOrReplaceWorkflowModel(WorkflowModel)}의 후처리 실패 복구 테스트.
 */
public class MDTWorkflowManagerTest {
	/** 저장된 모델(id → description). */
	private final Map<String, WorkflowModel> m_models = new LinkedHashMap<>();
	/** 후처리(DAG 생성)에 성공한 모델의 description 기록. */
	private final List<String> m_processed = new ArrayList<>();
	/** 엔진 쪽 정리(undeploy)를 부른 모델 id 기록. */
	private final List<String> m_cleaned = new ArrayList<>();
	private MDTWorkflowManager m_manager;

	@BeforeEach
	public void setUp() {
		WorkflowModelManager modelMgr = (WorkflowModelManager)Proxy.newProxyInstance(
				getClass().getClassLoader(), new Class<?>[] { WorkflowModelManager.class },
				(proxy, method, args) -> switch ( method.getName() ) {
					case "getWorkflowModel" -> {
						WorkflowModel model = m_models.get((String)args[0]);
						if ( model == null ) {
							throw new ResourceNotFoundException("WorkflowModel", "id=" + args[0]);
						}
						yield model;
					}
					case "addOrReplaceWorkflowModel" -> {
						WorkflowModel model = (WorkflowModel)args[0];
						m_models.put(model.getId(), model);
						yield model;
					}
					case "removeWorkflowModel" -> m_models.remove((String)args[0]);
					case "getWorkflowModelAll" -> new ArrayList<>(m_models.values());
					default -> throw new UnsupportedOperationException(method.getName());
				});
		WorkflowEngine provider = (WorkflowEngine)Proxy.newProxyInstance(
				getClass().getClassLoader(), new Class<?>[] { WorkflowEngine.class },
				(proxy, method, args) -> switch ( method.getName() ) {
					case "deploy" -> {
						WorkflowModel model = (WorkflowModel)args[0];
						if ( "broken".equals(model.getDescription()) ) {
							throw new MDTWorkflowInstanceManagerException("cannot generate DAG: " + model.getId());
						}
						m_processed.add(model.getDescription());
						yield null;
					}
					case "render" -> "dag:" + ((WorkflowModel)args[0]).getDescription();
					case "undeploy" -> {
						m_cleaned.add((String)args[0]);
						if ( "stuck".equals(args[0]) ) {
							throw new MDTWorkflowInstanceManagerException("cannot delete DAG: " + args[0]);
						}
						yield null;
					}
					default -> throw new UnsupportedOperationException(method.getName());
				});
		m_manager = new MDTWorkflowManager(modelMgr, provider);
	}

	@Test
	public void replaceRestoresPreviousModelWhenPostProcessingFails() {
		m_models.put("m1", model("m1", "v1"));

		assertThrows(RuntimeException.class, () -> m_manager.addOrReplaceWorkflowModel(model("m1", "broken")));
		assertEquals("v1", m_models.get("m1").getDescription());
		assertEquals(List.of("v1"), m_processed, "the previous model's DAG must be regenerated");
	}

	@Test
	public void addRemovesNewModelWhenPostProcessingFails() {
		assertThrows(RuntimeException.class, () -> m_manager.addOrReplaceWorkflowModel(model("m2", "broken")));
		assertFalse(m_models.containsKey("m2"));
	}

	@Test
	public void replaceSucceeds() {
		m_models.put("m1", model("m1", "v1"));

		m_manager.addOrReplaceWorkflowModel(model("m1", "v2"));
		assertEquals("v2", m_models.get("m1").getDescription());
		assertEquals(List.of("v2"), m_processed);
	}

	/**
	 * 후처리 실패는 원래 예외 유형 그대로 나간다(mdt-workflow 검토 D7). 2026-11-04 이전에는 RuntimeException으로 감쌌다.
	 */
	@Test
	public void postProcessingFailureKeepsExceptionType() {
		assertThrows(MDTWorkflowInstanceManagerException.class,
					() -> m_manager.addOrReplaceWorkflowModel(model("m3", "broken")));
	}

	/**
	 * 모든 모델 삭제는 모델마다 엔진 쪽 정리를 부르고, 한 모델의 정리가 실패해도 나머지를 계속 지운다(검토 D9).
	 */
	@Test
	public void removeAllCleansUpEveryModel() {
		m_models.put("a", model("a", "v"));
		m_models.put("stuck", model("stuck", "v"));
		m_models.put("b", model("b", "v"));

		m_manager.removeWorkflowModelAll();
		assertTrue(m_models.isEmpty());
		assertEquals(List.of("a", "stuck", "b"), m_cleaned);
	}

	/**
	 * 스크립트 조회는 저장된 모델을 읽어 엔진의 render에 넘긴다(검토 A3: 엔진은 모델 저장소를 모른다).
	 */
	@Test
	public void getWorkflowScriptRendersStoredModel() {
		m_models.put("m1", model("m1", "v1"));
		assertEquals("dag:v1", m_manager.getWorkflowScript("m1"));
		assertThrows(ResourceNotFoundException.class, () -> m_manager.getWorkflowScript("none"));
	}

	private static WorkflowModel model(String id, String description) {
		WorkflowModel model = new WorkflowModel();
		model.setId(id);
		model.setDescription(description);
		return model;
	}
}

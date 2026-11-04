package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mdt.model.ResourceNotFoundException;
import mdt.workflow.MDTWorkflowInstanceManagerException;
import mdt.workflow.WorkflowInstanceManagerProvider;
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
					default -> throw new UnsupportedOperationException(method.getName());
				});
		WorkflowInstanceManagerProvider provider = (WorkflowInstanceManagerProvider)Proxy.newProxyInstance(
				getClass().getClassLoader(), new Class<?>[] { WorkflowInstanceManagerProvider.class },
				(proxy, method, args) -> switch ( method.getName() ) {
					case "onWorkflowModelAdded" -> {
						WorkflowModel model = (WorkflowModel)args[0];
						if ( "broken".equals(model.getDescription()) ) {
							throw new MDTWorkflowInstanceManagerException("cannot generate DAG: " + model.getId());
						}
						m_processed.add(model.getDescription());
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

	private static WorkflowModel model(String id, String description) {
		WorkflowModel model = new WorkflowModel();
		model.setId(id);
		model.setDescription(description);
		return model;
	}
}

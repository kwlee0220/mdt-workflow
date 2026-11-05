package mdt.workflow.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import mdt.model.ResourceNotFoundException;
import mdt.workflow.repository.JpaWorkflowModelRepository;


/**
 * 없는 모델을 삭제하면 {@link ResourceNotFoundException}이다(mdt-workflow 검토 D8).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class JpaWorkflowModelManagerTest {
	@Test
	public void removingMissingModelIsResourceNotFound() {
		JpaWorkflowModelRepository repo = mock(JpaWorkflowModelRepository.class);
		when(repo.deleteByModelId("none")).thenReturn(0L);
		when(repo.deleteByModelId("m1")).thenReturn(1L);
		JpaWorkflowModelManager manager = new JpaWorkflowModelManager(repo);

		manager.removeWorkflowModel("m1");
		assertThrows(ResourceNotFoundException.class, () -> manager.removeWorkflowModel("none"));
	}
}

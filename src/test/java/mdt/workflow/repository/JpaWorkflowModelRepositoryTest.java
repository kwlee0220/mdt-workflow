package mdt.workflow.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;

import mdt.workflow.WorkflowModel;
import mdt.workflow.domain.JpaWorkflowModel;


/**
 * {@link JpaWorkflowModel}의 저장 형식(json_model text)과 파생 쿼리를 인메모리 H2로 확인한다(mdt-workflow 검토 I9).
 *
 * @author Kang-Woo Lee (ETRI)
 */
@DataJpaTest
public class JpaWorkflowModelRepositoryTest {
	@Autowired private JpaWorkflowModelRepository m_repo;
	@Autowired private JdbcTemplate m_jdbc;

	@Test
	public void modelIsStoredAsJsonTextAndFoundById() {
		WorkflowModel model = new WorkflowModel();
		model.setId("wf1");
		model.setDescription("설명");
		m_repo.saveAndFlush(new JpaWorkflowModel(model));

		String json = m_jdbc.queryForObject("select json_model from workflow_models where id = 'wf1'", String.class);
		assertTrue(json.contains("\"설명\""), json);

		assertTrue(m_repo.existsByModelId("wf1"));
		assertEquals("설명", m_repo.findByModelId("wf1").get().asWorkflowModel().getDescription());
		assertFalse(m_repo.findByModelId("none").isPresent());

		assertEquals(0L, m_repo.deleteByModelId("none"));
		assertEquals(1L, m_repo.deleteByModelId("wf1"));
		assertFalse(m_repo.existsByModelId("wf1"));
	}
}

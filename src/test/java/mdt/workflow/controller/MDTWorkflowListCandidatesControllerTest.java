package mdt.workflow.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;

import mdt.workflow.WorkflowStatus;
import mdt.workflow.service.MDTWorkflowManager;


/**
 * 셸 자동완성용 워크플로우 id 목록이 {@code ?status=}로 상태를 거르는지 확인한다(mdt-workflow 검토 A6).
 * 모르는 상태는 400이다(검토 D16, 2026-11-04 이전에는 NullPointerException(500)이었다).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MDTWorkflowListCandidatesControllerTest {
	@Test
	public void unknownStatusIsBadRequest() {
		MDTWorkflowListCandidatesController controller = new MDTWorkflowListCandidatesController(null);
		ResponseStatusException e = assertThrows(ResponseStatusException.class,
												() -> controller.listWorkflowIds("finished"));
		assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
	}

	@Test
	public void statusQueryFiltersWorkflowIds() throws Exception {
		MDTWorkflowManager manager = mock(MDTWorkflowManager.class);
		when(manager.listWorkflowIds()).thenReturn(List.of("w1", "w2", "w3"));
		when(manager.getWorkflowStatus("w1")).thenReturn(WorkflowStatus.RUNNING);
		when(manager.getWorkflowStatus("w2")).thenReturn(WorkflowStatus.COMPLETED);
		when(manager.getWorkflowStatus("w3")).thenReturn(WorkflowStatus.FAILED);
		MockMvc mvc = MockMvcBuilders.standaloneSetup(new MDTWorkflowListCandidatesController(manager)).build();

		String nl = System.lineSeparator();
		mvc.perform(get("/list/workflows")).andExpect(content().string("w1" + nl + "w2" + nl + "w3"));
		mvc.perform(get("/list/workflows").param("status", "running")).andExpect(content().string("w1"));
		mvc.perform(get("/list/workflows").param("status", "stopped")).andExpect(content().string("w2" + nl + "w3"));
		mvc.perform(get("/list/workflows").param("status", "finished")).andExpect(status().isBadRequest());
		mvc.perform(get("/list/workflows/running")).andExpect(status().isNotFound());
	}
}

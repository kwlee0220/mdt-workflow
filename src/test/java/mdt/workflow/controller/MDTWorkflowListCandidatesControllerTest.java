package mdt.workflow.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;


/**
 * 모르는 type이면 400이다(mdt-workflow 검토 D16). 2026-11-04 이전에는 NullPointerException(500)이었다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MDTWorkflowListCandidatesControllerTest {
	@Test
	public void unknownTypeIsBadRequest() {
		MDTWorkflowListCandidatesController controller = new MDTWorkflowListCandidatesController(null);
		ResponseStatusException e = assertThrows(ResponseStatusException.class,
												() -> controller.listWorkflowIds("finished"));
		assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
	}
}

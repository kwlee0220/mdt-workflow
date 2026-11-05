package mdt.workflow.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import mdt.model.ResourceNotFoundException;


/**
 * 컨트롤러 예외 처리의 상태 코드(mdt-workflow 검토 D7).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MDTWorkflowManagerControllerTest {
	private final MDTWorkflowManagerController m_controller = new MDTWorkflowManagerController(null, null);

	@Test
	public void exceptionsMapToStatusCodes() {
		assertEquals(HttpStatus.NOT_FOUND,
					m_controller.handleException(new ResourceNotFoundException("Workflow", "x")).getStatusCode());
		assertEquals(HttpStatus.NOT_IMPLEMENTED,
					m_controller.handleException(new UnsupportedOperationException("suspend")).getStatusCode());
		assertEquals(HttpStatus.BAD_REQUEST,
					m_controller.handleException(new IllegalArgumentException("bad")).getStatusCode());
		assertEquals(HttpStatus.INTERNAL_SERVER_ERROR,
					m_controller.handleException(new IllegalStateException("boom")).getStatusCode());
	}
}

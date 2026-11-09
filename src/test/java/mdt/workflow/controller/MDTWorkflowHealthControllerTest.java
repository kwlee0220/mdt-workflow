package mdt.workflow.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;


/**
 * Docker 이미지의 HEALTHCHECK가 쓰는 {@code GET /health}를 확인한다(mdt-workflow Docker 검토 I5).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class MDTWorkflowHealthControllerTest {
	@Test
	public void healthAnswersOk() throws Exception {
		MockMvc mvc = MockMvcBuilders.standaloneSetup(new MDTWorkflowHealthController()).build();
		mvc.perform(get("/health")).andExpect(status().isOk()).andExpect(content().string("ok"));
	}
}

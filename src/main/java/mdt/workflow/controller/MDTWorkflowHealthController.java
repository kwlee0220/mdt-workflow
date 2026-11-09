package mdt.workflow.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;


/**
 * 서버가 요청을 받을 수 있는지만 알리는 상태 확인 API({@code GET /health}).
 * <p>
 * Docker 이미지의 {@code HEALTHCHECK}가 쓴다. DB·Airflow 연결은 확인하지 않는다(그 상태에 따라 컨테이너를
 * 비정상으로 표시하면, 외부 서비스 장애 때 이 서버까지 재시작될 수 있다). mdt-manager의 {@code GET /health}와
 * 같은 형태다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Tag(name = "상태 확인 API")
@RestController
public class MDTWorkflowHealthController {
	@Operation(summary = "서버가 동작 중인지 확인한다.")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "성공",
					content = {@Content(mediaType = "text/plain", schema = @Schema(type = "string"))})
	})
	@GetMapping("/health")
	public String health() {
		return "ok";
	}
}

package mdt.workflow.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;


import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import lombok.RequiredArgsConstructor;

import utils.Throwables;
import utils.http.RESTfulErrorEntity;
import utils.http.RESTfulException;

import mdt.model.ResourceAlreadyExistsException;
import mdt.model.ResourceNotFoundException;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowModel;
import mdt.workflow.service.ExecutionTimeEstimator;
import mdt.workflow.service.MDTWorkflowManager;


/**
 * 워크플로우 관리 REST API({@code /workflow-manager}).
 * <p>
 * 워크플로우 모델 등록·조회·삭제·DAG 스크립트 조회, 워크플로우 인스턴스 시작·중지·조회·삭제, 실행시간 예측을
 * 제공한다. 예외는 상태 코드로 바꾼다: 대상 없음 404, 중복 409, 잘못된 인자 400, 엔진이 지원하지 않는 기능 501,
 * 하위 서비스(Airflow·예측 서비스) 오류 502, 그 밖 500(서버 로그에 남김).
 *
 * @author Kang-Woo Lee (ETRI)
 */
@RestController
@RequestMapping(value={"/workflow-manager"})
@RequiredArgsConstructor
public class MDTWorkflowManagerController {
	private static final Logger s_logger = LoggerFactory.getLogger(MDTWorkflowManagerController.class);
	
	private final MDTWorkflowManager m_wfManager;
	private final ExecutionTimeEstimator m_estimator;

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "식별자에 해당하는 워크플로우 모델을 반환한다.")
    @Parameters({
    	@Parameter(name = "id", description = "검색할 워크플로우 모델 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = WorkflowModel.class), mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우가 등록되지 않은 경우.")
    })
    @GetMapping("/models/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<WorkflowModel> getWorkflowModel(@PathVariable("id") String id) {
    	WorkflowModel wfModel = m_wfManager.getWorkflowModel(id);
		return ResponseEntity.ok(wfModel);
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "등록된 모든 워크플로우 모델들을 반환한다.")
    @Parameters()
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
    		content = {
    			@Content(mediaType = "application/json",
    					array = @ArraySchema(schema=@Schema(implementation = WorkflowModel.class)))
    		}
    	)
    })
    @GetMapping("/models")
    @ResponseStatus(HttpStatus.OK)
    public List<WorkflowModel> getWorkflowModelAll() {
    	return m_wfManager.getWorkflowModelAll();
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "워크플로우 관리자에 주어진 워크플로우 모델을 등록시킨다.")
    @Parameters({
    	@Parameter(name = "updateIfExists",
    				description = "동일 식별자의 워크플로우 모델이 이미 등록된 경우 갱신할지 여부. (기본값: false)")
    })
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
    	required = true,
    	description = "Json 형식으로 작성된 등록시킬 워크플로우 모델",
		content = {
			@Content(schema = @Schema(implementation = WorkflowModel.class), mediaType = "application/json")
		}
	)
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "201", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = WorkflowModel.class),
						mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "409",
    				description = "동일 식별자의 워크플로우 모델이 이미 등록되어 있는 경우."
    							+ " (updateIfExists가 false인 경우)")
    })
    @PostMapping({"/models"})
    @ResponseStatus(HttpStatus.CREATED)
    public WorkflowModel addWorkflowModel(@RequestBody WorkflowModel wfDesc,
										@RequestParam(name="updateIfExists", defaultValue="false") boolean updateIfExists)
    	throws ResourceAlreadyExistsException {
    	WorkflowModel wfModel = (updateIfExists)
    							? m_wfManager.addOrReplaceWorkflowModel(wfDesc)
    							: m_wfManager.addWorkflowModel(wfDesc);
    	return wfModel;
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "식별자에 해당하는 워크플로우 모델을 삭제한다.")
    @Parameters({
    	@Parameter(name = "id", description = "삭제할 워크플로우 등록 정보 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공"),
    	@ApiResponse(responseCode = "404",
    				description = "식별자에 해당하는 워크플로우 등록 정보가 등록되어 있지 않습니다.")
    })
    @DeleteMapping("/models/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeWorkflowModel(@PathVariable("id") String id) {
    	m_wfManager.removeWorkflowModel(id);
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "등록된 모든 워크플로우 모델을 삭제한다.")
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공")
    })
    @DeleteMapping("/models")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeWorkflowModelAll() {
    	m_wfManager.removeWorkflowModelAll();
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "식별자에 해당하는 워크플로우 정보를 Airflow DAG 스크립트(Python)로 변환하여 반환한다.")
    @Parameters({
    	@Parameter(name = "id", description = "변환시킬 워크플로우 등록 정보 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "text/x-python")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우가 등록되지 않은 경우.")
    })
    @GetMapping("/models/{id}/script")
    @ResponseStatus(HttpStatus.OK)
    public String renderWorkflowScript(@PathVariable("id") String id) {
		return m_wfManager.renderWorkflowScript(id);
    }
    

    @Tag(name = "실행시간 예측 API")
    @Operation(summary = "AI 또는 Simulation 작업의 실행시간을 예측한다.")
    @Parameters({
    	@Parameter(name = "smRef", description="예측 대상 AI 또는 Simulation 서브모델 참조 표현식. ",
    				example = "test:AddAndSleep")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode="200", description="성공. 예측된 실행시간(초).",
    		content = {
    			@Content(schema = @Schema(implementation = Double.class), mediaType="application/json")
    		}
    	),
    	@ApiResponse(responseCode = "404", description = "서브모델 참조 표현식에 해당하는 서브모델이 존재하지 않은 경우.")
    })
    @PostMapping("/execution-times/{smRef}")
    public Double estimateTaskExecutionTime(@PathVariable("smRef") String smRefString) {
    	return m_estimator.estimate(smRefString);
    }


    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "생성된 모든 워크플로우 인스턴스들을 반환한다.")
    @Parameters()
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
    		content = {
    			@Content(mediaType = "application/json",
    					array = @ArraySchema(schema=@Schema(implementation = Workflow.class)))
    		}
    	)
    })
	@GetMapping("/workflows")
    @ResponseStatus(HttpStatus.OK)
	public List<Workflow> getWorkflowAll() {
		return m_wfManager.getWorkflowAll();
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "식별자에 해당하는 워크플로우 인스턴스를 반환한다.")
    @Parameters({
    	@Parameter(name = "id", description = "검색할 워크플로우 인스턴스 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = Workflow.class), mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
	@GetMapping("/workflows/{wfId}")
    @ResponseStatus(HttpStatus.OK)
	public Workflow getWorkflow(@PathVariable("wfId") String wfId) throws ResourceNotFoundException {
		return m_wfManager.getWorkflow(wfId);
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "워크플로우 관리자에 등록된 워크플로우 모델을 이용하여 새로운 워크플로우를 시작시킨다.")
    @Parameters({
    	@Parameter(name = "wfModelId", description = "워크플로우 모델 식별자"),
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = Workflow.class),
						mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 모델이 없는 경우.")
    })
	@PostMapping("/models/{modelId}/start")
    @ResponseStatus(HttpStatus.OK)
	public Workflow startWorkflow(@PathVariable("modelId") String wfModelId) throws ResourceNotFoundException {
		return m_wfManager.startWorkflow(wfModelId);
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "동작 중인 워크플로우를 종료시킨다.")
    @Parameters({
    	@Parameter(name = "wfId", description = "워크플로우 인스턴스 식별자"),
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공적으로 종료됨"),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
    @PutMapping("/workflows/{wfId}/stop")
    @ResponseStatus(HttpStatus.NO_CONTENT)
	public void stopWorkflow(@PathVariable("wfId") String wfId) throws ResourceNotFoundException {
		m_wfManager.stopWorkflow(wfId);
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "워크플로우 인스턴스를 삭제시킨다.")
    @Parameters({
    	@Parameter(name = "wfId", description = "삭제시킬 워크플로우 인스턴스 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공"),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
	@DeleteMapping("/workflows/{wfId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
	public void removeWorkflow(@PathVariable("wfId") String wfId) {
		m_wfManager.removeWorkflow(wfId);
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "모든 워크플로우 인스턴스를 삭제시킨다.")
    @Parameters({
    	@Parameter(name = "model",
    				description = "삭제 대상을 한정할 워크플로우 모델 식별자. "
    							+ "지정하면 해당 모델로 생성된 워크플로우 인스턴스만 삭제한다.")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공")
    })
	@DeleteMapping("/workflows")
    @ResponseStatus(HttpStatus.NO_CONTENT)
	public void removeWorkflowAll(@RequestParam(name="model", required=false) String modelId) {
    	// 2026-11-07 이전 파라미터 이름은 'modelFilter'였다(별칭 없음, 검토 A5).
    	if ( modelId != null ) {
    		m_wfManager.removeWorkflowsOfModel(modelId);
    	}
    	else {
    		m_wfManager.removeWorkflowAll();
    	}
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "워크플로우 인스턴스에서 한 task의 최신 시도 로그를 조회한다.",
    			description = "Airflow 엔진에서는 경로의 podName을 task id로 해석한다(이전 엔진의 pod 이름 자리).")
    @Parameters({
    	@Parameter(name = "wfId", description = "로그를 조회할 워크플로우 인스턴스 식별자"),
    	@Parameter(name = "podName", description = "로그를 조회할 task id")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "text/plain")
			}),
    	@ApiResponse(responseCode = "404", description = "워크플로우 인스턴스나 task가 없거나, task가 아직 실행되지 않은 경우.")
    })
	@GetMapping("/workflows/{wfId}/log/{podName}")
    @ResponseStatus(HttpStatus.OK)
	public String getWorkflowLog(@PathVariable("wfId") String wfId, @PathVariable("podName") String podName)
		throws ResourceNotFoundException {
		return m_wfManager.getWorkflowLog(wfId, podName);
	}
    
    @ExceptionHandler()
    public ResponseEntity<RESTfulErrorEntity> handleException(Exception e) {
		Throwable cause = Throwables.unwrapThrowable(e);
    	if ( cause instanceof ResourceNotFoundException ) {
    		return ResponseEntity.status(HttpStatus.NOT_FOUND)
    							.contentType(MediaType.APPLICATION_JSON)
    							.body(RESTfulErrorEntity.of(cause));
    	}
    	else if ( cause instanceof ResourceAlreadyExistsException ) {
    		return ResponseEntity.status(HttpStatus.CONFLICT)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    	else if ( cause instanceof IllegalArgumentException ) {
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    	else if ( cause instanceof RESTfulException ) {
    		// 워크플로우 엔진(Airflow)·예측 서비스 같은 하위 서비스의 오류. 대상 없음(404)은 이미
    		// ResourceNotFoundException으로 바뀌어 위에서 처리된다.
    		s_logger.warn("upstream service failed: {}", "" + cause);
    		return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    	else if ( cause instanceof UnsupportedOperationException ) {
    		// 워크플로우 엔진이 지원하지 않는 기능.
    		return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    	else {
    		s_logger.error("failed to handle a workflow-manager request", cause);
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    }
}

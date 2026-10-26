package mdt.workflow.controller;

import java.util.Collections;
import java.util.List;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

import com.fasterxml.jackson.core.JsonProcessingException;

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
import lombok.extern.slf4j.Slf4j;

import utils.Throwables;
import utils.http.HttpRESTfulClient;
import utils.http.RESTfulErrorEntity;

import mdt.client.HttpMDTManager;
import mdt.client.instance.HttpMDTInstanceManager;
import mdt.model.MDTModelSerDe;
import mdt.model.ResourceAlreadyExistsException;
import mdt.model.ResourceNotFoundException;
import mdt.model.expr.MDTExpressionParser;
import mdt.model.sm.ref.MDTSubmodelReference;
import mdt.workflow.Workflow;
import mdt.workflow.WorkflowModel;
import mdt.workflow.config.ArgoWorkflowManagerConfiguration;
import mdt.workflow.service.MDTWorkflowManager;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
@RestController
@RequestMapping(value={"/workflow-manager"})
@Slf4j
@RequiredArgsConstructor
public class MDTWorkflowManagerController {
	private static final Logger s_logger = LoggerFactory.getLogger(MDTWorkflowManagerController.class);
	
	@Autowired private ArgoWorkflowManagerConfiguration m_conf;
	@Autowired private MDTWorkflowManager m_wfManager;

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
    	for ( var wfmodel: m_wfManager.getWorkflowModelAll() ) {
    		try {
    			m_wfManager.removeWorkflowModel(wfmodel.getId());
    		}
    		catch ( Exception e ) {
    			s_logger.warn("failed to remove workflow model: " + wfmodel.getId(), e);
    		}
    	}
    }

    @Tag(name = "워크플로우 모델 관리 API")
    @Operation(summary = "식별자에 해당하는 워크플로우 정보를 Argo 워크플로우 구동 스크립트로 변환하여 반환한다.")
    @Parameters({
    	@Parameter(name = "id", description = "변환시킬 워크플로우 등록 정보 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "application/yaml")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우가 등록되지 않은 경우.")
    })
    @GetMapping("/models/{id}/script")
    @ResponseStatus(HttpStatus.OK)
    public String getArgoWorklfowScript(@PathVariable("id") String id) throws JsonProcessingException {
		return m_wfManager.getWorkflowScript(id);
    }
    
	private static final okhttp3.MediaType JSON_TYPE = okhttp3.MediaType.parse("application/json; charset=utf-8");

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
    	System.out.println("estimateTaskExecutionTime: smRef=" + smRefString);
    	
    	HttpMDTManager mdt = HttpMDTManager.connect(m_conf.getMdtUrl());
    	HttpMDTInstanceManager manager = mdt.getInstanceManager();
    	
    	MDTSubmodelReference smRef = MDTExpressionParser.parseSubmodelReference(smRefString).evaluate();
    	smRef.activate(manager);
    	Submodel sm = smRef.get().getSubmodel();
		String smJsonStr = MDTModelSerDe.toJsonString(sm);
		
		okhttp3.RequestBody reqBody = okhttp3.RequestBody.create(smJsonStr, JSON_TYPE);
    	HttpRESTfulClient client = HttpRESTfulClient.newDefaultClient();
    	String respBody = client.post(m_conf.getExecutionTimeEstimatorEndpoint(), reqBody,
    									HttpRESTfulClient.STRING_DESER);
    	
    	return Double.valueOf(respBody);
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
		try {
			return m_wfManager.getWorkflowAll();
		}
		catch ( IllegalArgumentException e ) {
			String msg = e.getMessage();
			if ( msg.equals("Expected the field `items` to be an array in the JSON string but got `null`") ) {
				// argo-java-client 라이브러리에서는 버그 때문에 workflow가 하나도 없는 경우 오류가 발생.
				return Collections.emptyList();
			}
			else {
				throw e;
			}
		}
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
    @Operation(summary = "식별자에 해당하는 워크플로우 인스턴스를 수행 중지시킨다.")
    @Parameters({
    	@Parameter(name = "wfId", description = "중지시킬 워크플로우 인스턴스 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = Workflow.class), mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
	@PutMapping("/workflows/{wfId}/suspend")
    @ResponseStatus(HttpStatus.OK)
	public Workflow suspendWorkflow(@PathVariable("wfId") String wfId) throws ResourceNotFoundException {
		return m_wfManager.suspendWorkflow(wfId);
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "수행 중지된 워크플로우 인스턴스를 재개시킨다.")
    @Parameters({
    	@Parameter(name = "wfId", description = "재개시킬 워크플로우 인스턴스 식별자")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = Workflow.class), mediaType = "application/json")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
	@PutMapping("/workflows/{wfId}/resume")
    @ResponseStatus(HttpStatus.OK)
	public Workflow resumeWorkflow(@PathVariable("wfId") String wfId) throws ResourceNotFoundException {
		return m_wfManager.resumeWorkflow(wfId);
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
    	@Parameter(name = "modelFilter",
    				description = "삭제 대상을 한정할 워크플로우 모델 식별자. "
    							+ "지정하면 해당 모델로 생성된 워크플로우 인스턴스만 삭제한다.")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "204", description = "성공")
    })
	@DeleteMapping("/workflows")
    @ResponseStatus(HttpStatus.NO_CONTENT)
	public void removeWorkflowAll(@RequestParam(name="modelFilter", required=false) String modelFilter) {
    	if ( modelFilter != null ) {
			m_wfManager.getWorkflowAll().stream()
						.filter(wf -> wf.getModelId().equals(modelFilter))
						.forEach(wf -> {
							try {
								m_wfManager.removeWorkflow(wf.getName());
							}
							catch ( ResourceNotFoundException e ) { }
						});
    	}
    	else {
    		m_wfManager.removeWorkflowAll();
    	}
	}

    @Tag(name = "워크플로우 인스턴스 관리 API")
    @Operation(summary = "주어진 POD에서 수행 중인 워크플로우 인스턴스의 로그 정보를 조회한다.")
    @Parameters({
    	@Parameter(name = "wfId", description = "로그를 조회할 워크플로우 인스턴스 식별자"),
    	@Parameter(name = "podName", description = "수행 중인 POD 이름")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "text/plain")
			}),
    	@ApiResponse(responseCode = "404", description = "식별자에 해당하는 워크플로우 인스턴스가 없는 경우.")
    })
	@GetMapping("/workflows/{wfId}/log/{podName}")
    @ResponseStatus(HttpStatus.OK)
	public String log(@PathVariable("wfId") String wfId, @PathVariable("podName") String podName)
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
    	else {
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
								.contentType(MediaType.APPLICATION_JSON)
								.body(RESTfulErrorEntity.of(cause));
    	}
    }
}

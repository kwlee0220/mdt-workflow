package mdt.workflow.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import utils.Utilities;
import utils.stream.FStream;

import mdt.workflow.WorkflowStatus;
import mdt.workflow.service.MDTWorkflowManager;


/**
*
* @author Kang-Woo Lee (ETRI)
*/
@Tag(name = "워크플로우 식별자 목록 조회 API")
@RestController
@RequestMapping(value={"/list"})
public class MDTWorkflowListCandidatesController {
	@Autowired private MDTWorkflowManager m_wfManager;

    @Operation(summary = "등록된 모든 워크플로우 모델의 식별자 목록을 반환한다.")
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공. 줄 단위로 구분된 워크플로우 모델 식별자 목록.",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "text/plain")
			})
    })
    @GetMapping("/models")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> listCandidateModels() {
    	String output = FStream.from(m_wfManager.getWorkflowModelAll())
								.map(model -> model.getId())
								.join(Utilities.getLineSeparator());
    	return ResponseEntity.ok(output);
    }

    @Operation(summary = "주어진 상태에 해당하는 워크플로우 인스턴스의 식별자 목록을 반환한다.")
    @Parameters({
    	@Parameter(name = "type",
    				description = "조회할 워크플로우 인스턴스의 상태. "
    							+ "'all'(전체), 'running'(수행 중), 'stopped'(완료 또는 실패) 중 하나.",
    				example = "running")
    })
    @ApiResponses(value = {
    	@ApiResponse(responseCode = "200", description = "성공. 줄 단위로 구분된 워크플로우 인스턴스 식별자 목록.",
			content = {
				@Content(schema = @Schema(implementation = String.class), mediaType = "text/plain")
			})
    })
    @GetMapping("/workflows/{type}")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<?> listWorkflowIds(@PathVariable("type") String type) {
    	List<String> wfIdList = null;
    	if ( type.equals("all") ) {
    		wfIdList = m_wfManager.listWorkflowIds();
    	}
    	else if ( type.equals("running") ) {
    		wfIdList = FStream.from(m_wfManager.listWorkflowIds())
								.filter(wfId -> m_wfManager.getWorkflowStatus(wfId) == WorkflowStatus.RUNNING)
								.toList();
    	}
    	else if ( type.equals("stopped") ) {
    		wfIdList = FStream.from(m_wfManager.listWorkflowIds())
								.filter(wfId -> {
									WorkflowStatus status = m_wfManager.getWorkflowStatus(wfId);
                                    return status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED;
								})
								.toList();
    	}
		String output = FStream.from(wfIdList)
								.join(Utilities.getLineSeparator());
    	return ResponseEntity.ok(output);
    }
}

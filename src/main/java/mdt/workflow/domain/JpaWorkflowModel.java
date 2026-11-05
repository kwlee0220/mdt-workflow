package mdt.workflow.domain;

import java.io.IOException;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import utils.InternalException;

import mdt.model.MDTModelSerDe;
import mdt.workflow.WorkflowModel;


/**
 * 워크플로우 모델 하나를 저장하는 JPA 엔티티(테이블 {@code workflow_models}).
 * <p>
 * 모델은 JSON 문자열로 {@code json_model}(text) 컬럼에 저장하고, 모델 식별자는 {@code id} 컬럼(유일)에 둔다.
 * 기본키 {@code row_id}는 DB가 붙이는 일련번호다.
 * <p>
 * 2026-11-05 이전에는 JSON을 UTF-8 바이트로 PostgreSQL 전용 {@code bytea} 컬럼({@code json_model_bytes})에 저장했고,
 * 모델 식별자 필드 이름이 {@code id}여서 리포지토리가 {@code @Query}를 직접 써야 했다. 기존 DB는
 * {@code docs/migration/2026-11-05-workflow-models-json-text.sql}로 옮긴 뒤 이 버전을 배포해야 한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Entity
@Table(name="workflow_models")
public class JpaWorkflowModel {
	@Id @GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name="row_id") private Long rowId;

	@Column(name="id", length=64, unique=true) private String modelId;
	@Column(name="json_model", columnDefinition = "text", nullable = false)
	private String jsonModel;
	
	@SuppressWarnings("unused")
	private JpaWorkflowModel() { }
	
	public JpaWorkflowModel(WorkflowModel wfModel) {
		this.modelId = wfModel.getId();
		this.jsonModel = MDTModelSerDe.toJsonString(wfModel);
	}
	
	public Long getRowId() {
		return rowId;
	}
	
	public void setRowId(Long rowId) {
		this.rowId = rowId;
	}
	
	public String getModelId() {
		return modelId;
	}
	
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	
	public String getJsonModel() {
		return jsonModel;
	}
	
	public void setJsonModel(String jsonModel) {
		this.jsonModel = jsonModel;
	}
	
	public WorkflowModel asWorkflowModel() {
		try {
			return MDTModelSerDe.readValue(jsonModel, WorkflowModel.class);
		}
		catch ( IOException e ) {
			throw new InternalException(e);
		}
	}
}
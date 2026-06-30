package mdt.workflow.domain;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.core.JsonProcessingException;

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
 *
 * @author Kang-Woo Lee (ETRI)
 */
@Entity
@Table(name="workflow_models")
public class JpaWorkflowModel {
	@Id @GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name="row_id") private Long rowId;

	@Column(name="id", length=64, unique=true) private String id;
	@Column(columnDefinition = "bytea", nullable = false)
	private byte[] jsonModelBytes;
	
	@SuppressWarnings("unused")
	private JpaWorkflowModel() { }
	
	public JpaWorkflowModel(WorkflowModel wfModel) {
		this.id = wfModel.getId();
		this.jsonModelBytes = MDTModelSerDe.toJsonString(wfModel).getBytes(StandardCharsets.UTF_8);
	}
	
	public JpaWorkflowModel(String wfModelJson) throws JsonProcessingException {
		WorkflowModel wfDesc = WorkflowModel.parseJsonString(wfModelJson);
		
		this.id = wfDesc.getId();
		this.jsonModelBytes = wfModelJson.getBytes(StandardCharsets.UTF_8);
	}
	
	public Long getRowId() {
		return rowId;
	}
	
	public void setRowId(Long rowId) {
		this.rowId = rowId;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public byte[] getJsonModelBytes() {
		return jsonModelBytes;
	}
	
	public void setJsonModelBytes(byte[] jsonModelBytes) {
		this.jsonModelBytes = jsonModelBytes;
	}
	
	public WorkflowModel asWorkflowModel() {
		try {
			String jsonModel = new String(jsonModelBytes, StandardCharsets.UTF_8);
			WorkflowModel wfModel =  MDTModelSerDe.readValue(jsonModel, WorkflowModel.class);
			return  wfModel;
		}
		catch ( IOException e ) {
			throw new InternalException(e);
		}
	}
}
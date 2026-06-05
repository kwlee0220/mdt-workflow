package mdt.workflow.airflow;

import java.util.List;

import utils.Preconditions;
import utils.stream.FStream;

import mdt.workflow.WorkflowModel;


/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class DagSpec {
	private final String m_dagId;
	private final String m_description;
	private final List<TaskSpec> m_tasks;

	public DagSpec(WorkflowModel wfDesc) {
		m_dagId = wfDesc.getId();
		Preconditions.checkArgument(m_dagId != null && !m_dagId.isEmpty() && !m_dagId.contains("__"),
				                    "invalid DAG id: %s (null, empty, or contains '__')", m_dagId);
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(m_dagId),
				                    "invalid DAG id: %s (invalid Python identifier)", m_dagId);
		m_description = wfDesc.getDescription();
		
		m_tasks = FStream.from(wfDesc.getTaskDescriptors())
										.map(TaskSpec::from)
										.toList();
	}

	public String getId() {
		return m_dagId;
	}
	
	public String getDescription() {
		return m_description;
	}

	public List<TaskSpec> getTasks() {
		return m_tasks;
	}
}
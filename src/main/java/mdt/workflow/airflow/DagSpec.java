package mdt.workflow.airflow;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

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
	private final List<OperatorSpec> m_tasks;

	public DagSpec(WorkflowModel wfDesc) {
		m_dagId = wfDesc.getId();
		// "__"는 AirflowWorkflowId가 dagId와 dagRunId를 결합하는 구분자이기 때문에,
		// dagId에 포함되면 문자열 표현의 라운드트립이 깨진다.
		Preconditions.checkArgument(m_dagId != null && !m_dagId.isEmpty() && !m_dagId.contains("__"),
				                    "invalid DAG id: %s (null, empty, or contains '__')", m_dagId);
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(m_dagId),
				                    "invalid DAG id: %s (invalid Python identifier)", m_dagId);
		m_description = wfDesc.getDescription();
		
		m_tasks = FStream.from(wfDesc.getTaskDescriptors())
										.map(OperatorSpec::from)
										.toList();

		// Task가 하나도 없으면 템플릿이 본문 없는 'def _dag():'를 생성하여 Airflow 파싱 시점에
		// IndentationError가 나기 때문에, 생성 시점에 실패시킨다.
		Preconditions.checkArgument(!m_tasks.isEmpty(), "DAG has no task: %s", m_dagId);

		// Task id가 중복되거나 존재하지 않는 task를 dependency로 참조하면 문법적으로는 유효하지만
		// 의미가 틀리거나 Airflow 파싱 시점에 NameError를 내는 DAG가 만들어지기 때문에,
		// 생성 시점에 실패시킨다.
		Set<String> taskIds = Sets.newHashSet();
		for ( OperatorSpec task: m_tasks ) {
			Preconditions.checkArgument(taskIds.add(task.getTaskId()),
										"duplicate task id: %s", task.getTaskId());
		}
		for ( OperatorSpec task: m_tasks ) {
			for ( String dep: task.getDependencies() ) {
				Preconditions.checkArgument(taskIds.contains(dep),
											"unknown dependency task id: %s (referenced by task: %s)",
											dep, task.getTaskId());
			}
		}
	}

	public String getId() {
		return m_dagId;
	}
	
	public String getDescription() {
		return m_description;
	}

	public List<OperatorSpec> getTasks() {
		return Collections.unmodifiableList(m_tasks);
	}
}
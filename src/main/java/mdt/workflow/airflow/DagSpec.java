package mdt.workflow.airflow;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import utils.Preconditions;
import utils.stream.FStream;

import mdt.workflow.WorkflowModel;


/**
 * DAG 템플릿에 넘기는 DAG 하나의 정보(DAG id, 설명, task spec 목록).
 * <p>
 * 생성할 때 다음을 검증해, Airflow가 읽지 못하거나 의미가 틀린 DAG 파일이 만들어지지 않게 한다: DAG id가 Python
 * 식별자이고 {@code "__"}를 포함하지 않을 것({@link AirflowWorkflowId}의 구분자), task가 하나 이상일 것, task id가
 * 중복되지 않을 것, 의존 task가 모두 존재할 것, task-output 입력이 가리키는 task가 존재하고 직접 또는 간접 선행
 * task일 것.
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
		Preconditions.checkArgument(PythonIdentifiers.isValid(m_dagId),
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

		// task-output 입력은 출력을 내는 task가 먼저 끝나 있어야 값을 읽을 수 있으므로, 그 task가 존재하고
		// 선행 task(직접 또는 간접)일 것을 요구한다. 출력 인자 이름은 연산 실행 결과로 정해지므로 검사하지 않는다.
		Map<String,OperatorSpec> taskMap = FStream.from(m_tasks).tagKey(OperatorSpec::getTaskId).toMap();
		for ( OperatorSpec task: m_tasks ) {
			Set<String> ancestors = null;
			for ( DagArgument input: task.getInputs() ) {
				if ( input instanceof DagArgument.TaskOutput taskOut ) {
					String srcId = taskOut.getTaskId();
					Preconditions.checkArgument(taskIds.contains(srcId),
												"unknown task-output source task: %s (input '%s' of task: %s)",
												srcId, input.getId(), task.getTaskId());
					if ( ancestors == null ) {
						ancestors = collectAncestors(task, taskMap);
					}
					Preconditions.checkArgument(ancestors.contains(srcId),
												"task-output source task is not upstream: %s (input '%s' of task: %s); "
												+ "add it to the dependencies", srcId, input.getId(), task.getTaskId());
				}
			}
		}
	}

	// task의 직접·간접 선행 task 식별자들. 의존 관계에 순환이 있어도 끝나도록 방문한 task는 다시 보지 않는다.
	private static Set<String> collectAncestors(OperatorSpec task, Map<String,OperatorSpec> taskMap) {
		Set<String> ancestors = Sets.newHashSet();
		Deque<String> remains = new ArrayDeque<>(task.getDependencies());
		while ( !remains.isEmpty() ) {
			String id = remains.pop();
			if ( ancestors.add(id) ) {
				OperatorSpec dep = taskMap.get(id);
				if ( dep != null ) {
					remains.addAll(dep.getDependencies());
				}
			}
		}
		return ancestors;
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
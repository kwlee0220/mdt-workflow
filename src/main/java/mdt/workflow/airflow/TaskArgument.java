package mdt.workflow.airflow;

import utils.Preconditions;

/**
 *
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class TaskArgument {
	private final String m_argId;
	
	public abstract String getPythonStatement();
	
	protected TaskArgument(String argId) {
		Preconditions.checkNotNullArgument(argId, "argId should not be null");
		Preconditions.checkArgument(PythonIdentifierUtil.isValidPythonIdentifier(argId),
									"argId should be a valid Python identifier: %s", argId);
		m_argId = argId;
	}
	
	public String getId() {
		return m_argId;
	}
	
	public static final class ReferenceArgument extends TaskArgument {
		private final String m_refString;

		public ReferenceArgument(String argId, String refString) {
			super(argId);
			Preconditions.checkNotNullArgument(refString, "refString should not be null");
			
			m_refString = refString;
		}

		public String getReference() {
			return m_refString;
		}

		@Override
		public String getPythonStatement() {
			return String.format("\"%s\": reference(\"%s\")", getId(), m_refString);
		}
	}
	
	public static final class LiteralArgument extends TaskArgument {
		private final String m_literal;

		public LiteralArgument(String argId, String literal) {
			super(argId);
			Preconditions.checkNotNullArgument(literal, "literal should not be null");

			m_literal = literal;
		}

		@Override
		public String getPythonStatement() {
			return String.format("\"%s\": literal(%s)", getId(), m_literal);
		}
	}
	
	public static final class TaskOutputArgument extends TaskArgument {
		private final String m_taskId;
		private final String m_outputArgumentName;

		public TaskOutputArgument(String argId, String taskId, String outputArgumentName) {
			super(argId);
			Preconditions.checkNotNullArgument(taskId, "taskId should not be null");
			Preconditions.checkNotNullArgument(outputArgumentName, "outputName should not be null");

			m_taskId = taskId;
			m_outputArgumentName = outputArgumentName;
		}

		@Override
		public String getPythonStatement() {
			return String.format("\"%s\": task_output(\"%s\", \"%s\")", getId(), m_taskId, m_outputArgumentName);
		}
	}
}
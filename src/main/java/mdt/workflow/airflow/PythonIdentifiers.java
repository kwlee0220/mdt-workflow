package mdt.workflow.airflow;

import java.util.Set;

/**
 * 문자열이 Python 3 식별자(ASCII 글자·숫자·밑줄, 첫 글자는 숫자가 아님, 예약어 아님)인지 확인한다.
 * <p>
 * DAG id·task id·인자 id처럼 생성된 Python 코드나 DAG 파일 이름에 그대로 쓰이는 값을 검사할 때 쓴다.
 * <p>
 * 2026-11-05 이전 이름은 {@code PythonIdentifierUtil.isValidPythonIdentifier}였다(별칭 없음).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class PythonIdentifiers {
	private PythonIdentifiers() {
		throw new AssertionError("Should not be called: class=" + getClass().getName());
	}
	
    // Python 3 기준 예약어 목록
    private static final Set<String> PYTHON_KEYWORDS = Set.of(
        "False", "None", "True",
        "and", "as", "assert", "async", "await",
        "break", "class", "continue",
        "def", "del",
        "elif", "else", "except",
        "finally", "for", "from",
        "global",
        "if", "import", "in", "is",
        "lambda",
        "nonlocal", "not",
        "or",
        "pass",
        "raise", "return",
        "try",
        "while", "with",
        "yield"
    );

    /**
     * 문자열이 파이썬 식별자 규칙을 만족하는지 검사한다.
     *
     * @param value 검사할 문자열
     * @return 파이썬 식별자이면 true, 아니면 false
     */
    public static boolean isValid(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        // 예약어 검사
        if (PYTHON_KEYWORDS.contains(value)) {
            return false;
        }

        // 첫 글자 검사: [A-Za-z_]
        char first = value.charAt(0);
        if (!isLetter(first) && first != '_') {
            return false;
        }

        // 나머지 글자 검사: [A-Za-z0-9_]
        for (int i = 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!isLetter(c) && !isDigit(c) && c != '_') {
                return false;
            }
        }

        return true;
    }

    private static boolean isLetter(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }

    private static boolean isDigit(char c) {
        return (c >= '0' && c <= '9');
    }
}

package mdt.workflow.airflow;

/**
 * 임의의 문자열을 Python 소스의 문자열 리터럴 내부에 안전하게 삽입할 수 있도록 이스케이프한다.
 * <p>
 * 작은따옴표와 큰따옴표를 모두 이스케이프하기 때문에, 결과는 {@code '...'}와 {@code "..."} 중
 * 어느 리터럴 안에 넣어도 안전하다. (Python에서는 {@code \'}와 {@code \"} 모두 두 인용 스타일에서 유효하다.)
 * <p>
 * 생성되는 Python 파일은 UTF-8로 기록되고 Python 3의 기본 소스 인코딩도 UTF-8이기 때문에,
 * ASCII 범위를 벗어난 문자는 그대로 둔다. 반면 리터럴을 깨뜨리거나 의미를 바꿀 수 있는 문자들
 * (백슬래시, 따옴표, 개행 등)과 제어 문자는 escape sequence로 변환한다.
 *
 * @author Kang-Woo Lee (ETRI)
 */
public final class PythonStringEscaper {
	private PythonStringEscaper() {
		throw new AssertionError("Should not be called: class=" + getClass().getName());
	}

	/**
	 * 주어진 문자열을 Python 문자열 리터럴의 내용으로 사용할 수 있도록 이스케이프한다.
	 * <p>
	 * 반환값은 따옴표를 포함하지 않기 때문에, 호출자가 {@code '...'} 또는 {@code "..."} 사이에 넣어 사용한다.
	 *
	 * @param value	이스케이프할 문자열. {@code null}이면 빈 문자열을 반환한다.
	 * @return	이스케이프된 문자열.
	 */
	public static String escape(String value) {
		if ( value == null ) {
			return "";
		}

		StringBuilder builder = new StringBuilder(value.length() + 16);
		for ( int i = 0; i < value.length(); ++i ) {
			char c = value.charAt(i);
			switch ( c ) {
				// 백슬래시를 가장 먼저 처리해야 한다. 뒤에 처리하면 앞서 삽입한 escape sequence의
				// 백슬래시까지 다시 이스케이프되어 리터럴이 깨진다.
				case '\\' -> builder.append("\\\\");
				case '"' -> builder.append("\\\"");
				case '\'' -> builder.append("\\'");
				case '\n' -> builder.append("\\n");
				case '\r' -> builder.append("\\r");
				case '\t' -> builder.append("\\t");
				case '\b' -> builder.append("\\b");
				case '\f' -> builder.append("\\f");
				default -> {
					// 그 외 제어 문자는 \xNN 형식으로 변환한다. (DEL 포함)
					if ( c < 0x20 || c == 0x7F ) {
						builder.append(String.format("\\x%02x", (int)c));
					}
					else {
						builder.append(c);
					}
				}
			}
		}

		return builder.toString();
	}

	/**
	 * 주어진 문자열을 큰따옴표로 감싼 Python 문자열 리터럴로 변환한다.
	 *
	 * @param value	리터럴로 변환할 문자열. ({@code null} 가능)
	 * @return	{@code "..."} 형식의 Python 문자열 리터럴.
	 */
	public static String quote(String value) {
		return "\"" + escape(value) + "\"";
	}
}

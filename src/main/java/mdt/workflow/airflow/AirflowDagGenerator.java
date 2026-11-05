package mdt.workflow.airflow;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.loader.ClasspathLoader;
import io.pebbletemplates.pebble.template.PebbleTemplate;

import mdt.workflow.WorkflowModel;

/**
 * 워크플로우 모델을 Airflow DAG Python 소스로 만드는 생성기.
 * <p>
 * 모델을 {@link DagSpec}으로 바꿔(여기서 DAG id·task id·의존 관계를 검증한다) Pebble 템플릿
 * {@code templates/airflow_dag.peb}에 넣는다. 템플릿은 HTML 이스케이프를 끄고 존재하지 않는 속성을 오류로
 * 처리한다({@code strictVariables}).
 *
 * @author Kang-Woo Lee (ETRI)
 */
public class AirflowDagGenerator {
	// PebbleEngine은 스레드 세이프하고 파싱된 템플릿을 내부적으로 캐시하기 때문에
	// 매 호출마다 생성하지 않고 공유한다.
	private static final PebbleEngine ENGINE = buildEngine();

	private static PebbleEngine buildEngine() {
        ClasspathLoader loader = new ClasspathLoader();
        loader.setPrefix("templates");
        loader.setCharset(StandardCharsets.UTF_8.name());

        // Python 소스를 생성하기 때문에 Pebble의 HTML autoescaping은 끄고,
        // 대신 'py' 필터와 'fail' 함수({@link AirflowTemplateExtension})를 제공한다.
        //
        // strictVariables를 켜지 않으면 템플릿이 존재하지 않는 속성을 참조할 때 Pebble이 예외 대신
        // 빈 문자열을 채워 넣는다. 그 결과 문법적으로는 멀쩡하지만 의미가 틀린 DAG가 조용히 생성된다.
        // (예: instanceId를 갖지 않는 OperatorSpec에 대해 instance="" 가 생성됨)
        // 잘못된 DAG를 Airflow에 넘기는 것보다 생성 시점에 실패하는 편이 낫다.
        return new PebbleEngine.Builder()
					                .loader(loader)
					                .autoEscaping(false)
					                .strictVariables(true)
					                .extension(new AirflowTemplateExtension())
					                .build();
	}

	public static DagSpec generate(WorkflowModel wfDesc, Writer writer) throws IOException {
        PebbleTemplate template = ENGINE.getTemplate("airflow_dag.peb");

		DagSpec dagSpec = new DagSpec(wfDesc);
        template.evaluate(writer, Map.of("dag", dagSpec));
        writer.flush();

        return dagSpec;
	}
}

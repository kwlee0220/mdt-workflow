# 새 Airflow Operator 추가 가이드

워크플로 모델에 새 task 유형을 추가하고, 그것이 Airflow DAG 안의 새 mdtpy Operator 호출로 이어지도록
연결할 때 따라야 할 작업 목록과 순서를 정리한다.

**참고 구현은 `PythonScriptOperator`다.** 아래 파일들이 이 문서의 절차대로 만들어졌으므로, 막히면 그대로 비교한다.
(2026-10-07 기준 GUI 노드만 미구현)

| 단계 | 참고 파일 |
|---|---|
| mdtpy operator | `~/development/mdtpy/mdtpy/src/mdtpy/airflow/operator/python_script.py`, `tests/test_airflow.py`의 `TestPythonScriptOperator` |
| task type (Java 클래스 없음) | `mdt-client/src/main/java/mdt/workflow/model/TaskDescriptors.java`의 `PYTHON_SCRIPT_TASK_TYPE` |
| spec | `mdt-workflow/src/main/java/mdt/workflow/airflow/PythonScriptSpec.java`, `PythonScriptSpecTest`, `OperatorCallTest` |
| 워크플로 예 | `mdt-client/misc/test-workflows/test_workflow.json`의 `dec_by_5` task |

## 전체 경로

```
mdt-gui (워크플로 편집) ──JSON──▶ mdt-workflow REST (워크플로 등록)
                                        │
WorkflowModel JSON                      │
  └ TaskDescriptor (type, options, inputArguments, outputArguments, dependencies)
        │  mdt-client: 모델 / 직렬화
        ▼
mdt-workflow: DagSpec → OperatorSpec.from() → XxxSpec
        │  getOperatorCall() + inputs/outputs Python 구문
        ▼
airflow_dag.peb 렌더링 → <dagsFolder>/<dagId>.py
        │  Airflow worker에 설치된 mdtpy를 import
        ▼
mdtpy: XxxOperator(..., inputs=..., outputs=...).run()
```

| 저장소 | 위치 | 역할 |
|---|---|---|
| mdtpy | `~/development/mdtpy/mdtpy` | Operator 구현, PyPI 배포 |
| mdt-client | `~/development/mdt/mdt-client` | task type 식별자, `TaskDescriptor`/`ArgumentSpec` 모델 |
| mdt-workflow | `~/development/mdt/mdt-workflow` | DAG 생성기(`OperatorSpec`, 템플릿), 워크플로 등록 서비스 |
| mdt-gui | `~/mdt/mdt-gui/dtfront-main` | 워크플로 편집기의 노드·속성 패널·JSON 변환 |
| mdt-manager (조건부) | `~/development/mdt/mdt-manager` | GUI가 인자 JSON을 받는 `/argumentSpec/$json`. `ArgumentSpec` 모델이 바뀔 때만 손댄다 |
| mdt-cli (선택) | `~/development/mdt/mdt-cli` | task 단독 실행 명령 |

## 작업 순서 요약

0. [설계 결정](#0-설계-결정-착수-전)
1. [mdtpy: Operator 구현·테스트·문서](#1-mdtpy--operator-구현)
2. [mdtpy 배포](#2-mdtpy-배포)
3. [mdt-client: task type 등록](#3-mdt-client--task-type-등록)
4. [mdt-workflow: Spec·템플릿·테스트](#4-mdt-workflow--dag-생성기)
5. [mdt-gui: 노드와 JSON 변환](#5-mdt-gui--노드와-json-변환)
6. [통합 검증](#6-통합-검증)
7. [배포 순서](#7-배포-순서)

mdtpy를 먼저 하는 이유: Java 쪽 spec은 mdtpy operator의 생성자 시그니처를 그대로 렌더링하므로,
그 시그니처가 먼저 확정되어야 한다.

---

## 0. 설계 결정 (착수 전)

아래 항목을 정한 뒤 시작한다. 출력 관련 결정은 mdtpy `operator/README.md`의 비교표에 그대로 기록한다.

| 항목 | 질문 | 기존 operator 참고 |
|---|---|---|
| task type 방식 | Java `MDTTask`로도 실행해야 하는가? | 기존 5종은 Java 클래스가 있다 · `PythonScript`는 Airflow 전용이라 type 문자열 상수만 둔다 ([§3.1](#31-task-type-방식-고르기)) |
| 고유 인자 | 생성자 키워드 인자의 이름과 타입은? 어느 option에서 오는가? | `RestfulRpc`: `op_url`(문자열), `timeout`(초, 숫자) ← option `opUrl`, `timeout` · `PythonScript`: `script` ← option `script` |
| 출력 전달 범위 | 결과 전부를 전달하는가, 선언된 출력만 전달하는가? | 전부: `OperationSubmodel`/`AASOperation`/`RestfulAsyncRpc`, `PythonScript`(script가 `outputs` dict에 넣은 값 전부) · 선언된 것만: `RestfulRpc` |
| 선언된 출력 누락 | `KeyError`인가, 건너뛰는가? | `RestfulRpc`: `KeyError` · `PythonScript`: 기록을 건너뜀 |
| 저장 위치 `None` | 기록 없이 후속 task에만 넘기는 출력을 허용하는가? | `RestfulRpc`은 허용하지만 워크플로 모델로는 표현할 수 없다([§9](#9-알려진-한계와-주의점)) · `PythonScript`는 허용하나 효과 없음 |
| File 입력 | 값으로 받는가, `ElementReference`로 받는가? | `OperationSubmodel`: reference 그대로 전달 · `SetElement`: 값으로 다시 읽음 |

**출력 전달 범위는 "전부 전달"을 기본으로 고려한다.** 워크플로 모델의 `outputArguments`는 저장 위치(reference)가
필수라서, "선언된 것만 전달" 계약에서 기록하지 않고 넘기기만 할 출력은 모델로 표현할 수 없다. 두 task 사이를
메우는 연결용 로직처럼 출력을 기록하지 않는 operator라면, 후속 task가 별도 선언 없이 출력을 받을 수 있는
"전부 전달"이 모델 변경 없이 동작한다(`PythonScript`가 이 이유로 이 방식을 택했다).

---

## 1. mdtpy — Operator 구현

### 1.1 구현 파일: `src/mdtpy/airflow/operator/<module>.py`

- `class XxxOperator(Operator)`, `__all__ = ['XxxOperator']`
- 생성자: 고유 인자 + `*, inputs: Mapping[str, DagTaskInputArgument], outputs: Mapping[str, Optional[ElementReference]] = {}`.
  고유 인자는 dict로 묶지 않고 키워드 인자로 직접 받는다. 생성되는 DAG가 이 키워드 이름을 그대로 쓴다.
- `run(self, context: Optional[DagContext]=None)`: `context`가 `None`이면 `AirflowDagContext()`를 만든다.
- `__repr__`를 구현하고, `logger.info`로 호출 대상과 `inputs`/`outputs` 명세를 남긴다.

출력 처리는 **모두 변환 → 기록 → 같은 값 전달** 순서를 따른다. "전부 전달" 계약의 골격:

```python
def run(self, context: Optional[DagContext]=None) -> None:
    if context is None:
        context = AirflowDagContext()

    in_args = { arg_id: arg.get(context) for arg_id, arg in self.inputs.items() }
    results = ...   # operator 고유 동작 (출력 이름 → 값)

    # 1) 모든 결과를 ElementValue로 변환한다. (여기서 실패하면 아무것도 기록되지 않는다)
    task_outputs = { key: v if isinstance(v, ElementValue) else mdt_value(v) for key, v in results.items() }
    # 2) 저장 위치가 지정된 값만 기록한다. (지정됐는데 결과에 없으면 건너뛴다)
    for key, ref in self.outputs.items():
        if ref is not None and key in task_outputs:
            ref.update_value(task_outputs[key])
    # 3) 변환된 값 전부를 후속 task에 넘긴다.
    context.set_task_outputs(task_outputs)
```

"선언된 것만 전달" 계약이면 1)을 `for key in self.outputs`로 순회해 선언된 출력만 변환하고(누락이면 `KeyError` 또는
건너뜀), 3)에 그 결과만 넘긴다(`restful_rpc.py` 참고).

### 1.2 구현 규칙 (리뷰에서 반복해서 지적된 사항)

- **task 출력은 `ElementValue`만 허용된다.** 원시 값을 `set_task_outputs()`에 넘기면 `TypeError`가 난다
  (XCom에 JSON으로 저장하기 위한 계약). 변환한 값을 기록과 전달에 함께 쓴다. 기록에만 변환값을 쓰고
  전달에는 원본을 넘기면, 외부 값은 이미 갱신됐는데 task는 실패하는 상태가 된다.
- **모두 변환한 뒤에 기록한다.** 출력마다 변환하고 바로 기록하면, 뒤쪽 출력의 변환이 실패했을 때
  앞쪽 출력만 기록된 채로 끝난다.
- **`mdt_value()`는 스칼라(str/bool/int/float/timedelta)만 받는다.** "전부 전달" 계약에서는 넘기는 값이 모두 변환
  가능해야 하므로, 리스트·dict 같은 값이 섞이면 task 전체가 `ValueError`로 실패한다.
- **쓰기 대상은 실제 `ElementReference`여야 한다.** `mdtpy.reference()`나 airflow `sink()`로 만든다.
  airflow의 `reference()`는 입력 명세이지 쓰기 대상이 아니다.
- **생성 시점에 검증할 수 있는 오류는 생성자에서 던진다.** 그러면 task 실행이 아니라 DAG 파싱 단계에서 드러난다.
- **Airflow import는 지연시킨다.** `airflow.sdk`를 모듈 최상위에서 import하지 않는다.
- **마지막에 docstring, README, 샘플을 코드와 대조한다.** 출력 방식이나 누락 정책이 문서마다 다르게
  적히는 일이 실제로 있었다.

### 1.3 등록

- `operator/__init__.py`에 `from .<module> import *`를 추가한다. 그러면 `mdtpy.airflow.operator`와
  `mdtpy.airflow` 양쪽에서 import할 수 있다.
- `operator/operator.py` 모듈 docstring에 있는 모듈 목록을 갱신한다.

### 1.4 테스트: `tests/test_airflow.py`에 `TestXxxOperator` 추가

최소한 다음을 확인한다.

| 테스트 | 확인 내용 |
|---|---|
| 원시 값 출력 | 변환된 값이 기록과 task 출력 양쪽에 전달된다. `local_ctx` fixture를 써서 실제 `_check_task_output`을 거친다 |
| 선언되지 않은 출력 | 계약대로 전달되거나 무시된다 |
| 저장 위치가 지정된 출력의 누락 | 정책대로 `KeyError`가 나거나 기록을 건너뛴다 |
| 변환 실패 | 앞쪽 출력의 `update_value`와 `set_task_outputs`가 호출되지 않는다 |
| 고유 인자 | 외부 호출(mock)에 인자가 그대로 전달된다 |

- MagicMock 컨텍스트로 `context.set_task_outputs.assert_called_once_with({...})`를 검증해도 된다.
  `PropertyValue.__eq__`가 타입까지 비교하므로 원시 값과 혼동되지 않는다.
- **operator 모듈 안의 이름을 patch할 때 문자열 경로를 쓰지 않는다.** `mdtpy.airflow`의 star import 때문에
  `mdtpy.airflow.operator` 속성이 패키지가 아니라 `operator.py` 모듈로 가려져서,
  `patch("mdtpy.airflow.operator.<module>.X")`는 해석에 실패한다. 모듈을 import해서 `patch.object`를 쓴다:
  `from mdtpy.airflow.operator import restful_async_rpc` → `patch.object(restful_async_rpc, "RESTfulAsyncRpcClient")`.
- 실행: `make test` (= `uv run --env-file .env pytest`) 또는 `uv run pytest tests/test_airflow.py`.

### 1.5 문서

- `src/mdtpy/airflow/operator/README.md`: operator 표, 출력 동작 비교표(선언 외 출력 / `None`), 누락·변환 실패
  설명, operator 전용 절(시그니처와 예제), 주의사항.
- `src/mdtpy/airflow/CLAUDE.md`: operator 표, "생성된 DAG와의 계약"(템플릿이 쓰는 import와 생성자 키워드 이름) 목록.
- `src/samples/sample_<module>.py` (선택).

### 1.6 로컬 Airflow에서 바로 확인하기

로컬 Airflow(`~/mdt/airflow`)는 `.env`의 `_PIP_ADDITIONAL_REQUIREMENTS="-e /opt/mdtpy"`로 mdtpy 개발 소스를
editable 설치해 쓴다. 따라서 소스를 고치면 PyPI 배포 없이 다음 task 실행부터 반영된다. 반대로 PyPI 배포판을
쓰는 환경에는 [§2](#2-mdtpy-배포)를 거쳐야 반영된다.

---

## 2. mdtpy 배포

1. PyPI 최신 버전과 로컬 `__version__`을 비교한다:
   `curl -s https://pypi.org/pypi/mdtpy/json | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['version'])"`.
2. `src/mdtpy/__init__.py`의 `__version__`을 올린다. `pyproject.toml`은 이 값을 동적으로 읽는다(버전의 단일 출처).
3. 전체 테스트(`make test`)를 돌린 뒤 `uv build`로 `dist/`에 sdist와 wheel을 만든다.
4. 업로드 전에 확인한다. 업로드한 버전은 다시 올릴 수 없다.
   - wheel에 수정한 코드가 들어 있는지: `unzip -p dist/mdtpy-<ver>-py3-none-any.whl mdtpy/airflow/operator/<module>.py`
   - `uvx twine check dist/mdtpy-<ver>-py3-none-any.whl dist/mdtpy-<ver>.tar.gz`
5. 업로드: `uvx twine upload --non-interactive dist/mdtpy-<ver>-py3-none-any.whl dist/mdtpy-<ver>.tar.gz`.
   `dist/`에 예전 빌드가 남아 있으므로 해당 버전 파일만 지정한다. 시스템 twine(3.8)은 Metadata 2.4를 읽지 못해 실패한다.
6. `https://pypi.org/pypi/mdtpy/<ver>/json`으로 올라간 것을 확인한다.
7. PyPI 배포판을 쓰는 Airflow worker 환경의 mdtpy를 새 버전으로 갱신한다.

7번이 끝나기 전에 그 환경을 대상으로 mdt-workflow를 배포하면 안 된다 ([§7](#7-배포-순서)).

---

## 3. mdt-client — task type 등록

### 3.1 task type 방식 고르기

| 방식 | 언제 | 할 일 |
|---|---|---|
| **A. Java task 클래스** | Java `MDTTask`로도 실행해야 할 때 (기존 5종) | `src/main/java/mdt/task/XxxTask.java` 작성. type은 클래스 FQCN(`XxxTask.class.getName()`) |
| **B. type 문자열 상수** | Airflow에서만 실행할 때 (`PythonScript`) | Java 클래스 없이 type 상수만 `TaskDescriptors`에 둔다 |

**A. Java task 클래스**

- `implements MDTTask, LoggerSettable`. 생성자 `XxxTask(TaskDescriptor)`, `getTaskDescriptor()`,
  `run(MDTInstanceManager)`(반환 타입 `Map<String,SubmodelElement>`)를 구현한다.
- option 이름은 클래스에 상수로 둔다(예: `RESTfulRpcTask.OPTION_URL = "opUrl"`). 공통 option은
  `MDTTask.OPTION_TIMEOUT`(`"timeout"`), `OPTION_POLL_INTERVAL`(`"poll"`), `OPTION_LOG_LEVEL`(`"loglevel"`)을 쓴다.
- 참고 구현: `RESTfulRpcTask`(option, 입력 읽기, 출력 기록), `SetTask`.

**B. type 문자열 상수** (`TaskDescriptors.java`의 실제 코드)

```java
/**
 * Python script를 실행하는 task의 type 식별자 ({@link TaskDescriptor#getType()} 값).
 * <p>
 * 이 type은 Airflow DAG의 mdtpy {@code PythonScriptOperator}로만 실행되며, 대응하는 Java
 * {@link mdt.task.MDTTask} 구현 클래스는 <b>없다</b>. ...
 */
public static final String PYTHON_SCRIPT_TASK_TYPE = "mdt.task.PythonScriptTask";
```

- 형식은 다른 type과 같은 클래스 이름 꼴로 둔다(GUI의 `TaskType` 상수와 기존 워크플로 JSON이 모두 이 형식이다).
- type 문자열에 의존하는 곳은 `OperatorSpec.from()`과 `TaskDescriptors.CLASS_TO_SHORT_ID_MAP` 둘뿐이고,
  type 문자열로 클래스를 로드하는 코드는 없으므로 상수만으로 충분하다.
- option 이름 상수는 유일한 사용처인 mdt-workflow의 spec에 둔다(`PythonScriptSpec.OPTION_SCRIPT`).

### 3.2 짧은 type ID 등록 (빠뜨리기 쉬움)

`src/main/java/mdt/workflow/model/TaskDescriptors.java`의 `CLASS_TO_SHORT_ID_MAP`에 등록한다(A는 클래스 이름, B는 상수).
빠뜨리면 `TaskDescriptor.toSignatureString()`과 `toString()`이 `IllegalArgumentException("unregistered task class")`를
던지고, `mdt get workflow` 출력 같은 곳에서 실패한다.

### 3.3 워크플로 JSON 형식

`test_workflow.json`의 `dec_by_5`(연결용 Python script task):

```json
{
  "id": "dec_by_5",
  "type": "mdt.task.PythonScriptTask",
  "dependencies": [ "get_heater_cycle_time" ],
  "inputArguments": [
    { "@type": "mdt:task_arg:task-output", "id": "value",
      "taskId": "get_heater_cycle_time", "argumentName": "target" }
  ],
  "outputArguments": [],
  "options": [
    { "name": "script", "value": "value = inputs['value'].to_raw_object()\noutputs['result'] = value - 5" }
  ],
  "labels": []
}
```

후속 task는 `{ "@type": "mdt:task_arg:task-output", "id": "Data", "taskId": "dec_by_5", "argumentName": "result" }`로
결과를 받는다. `PythonScript`는 script가 넣은 출력을 전부 넘기므로 `outputArguments`에 선언하지 않는다.

- `inputArguments`는 세 종류다: `reference`(참조 문자열), `literal`(`value`는 `@type`이 포함된 ElementValue JSON),
  `task-output`(`taskId` + `argumentName`).
- `outputArguments`는 `reference`만 가능하다 ([§9](#9-알려진-한계와-주의점)).
- `options`는 `name`/`value` 문자열 쌍의 목록이다. spec이 읽지 않는 option(예: `SetTask`·`AASOperationTask`의
  `timeout`/`loglevel`)은 Airflow 실행에 적용되지 않고 Java task 실행(mdt-cli)에서만 쓰인다.
- task `id`와 인자 `id`는 Python 식별자여야 한다. DAG를 생성할 때 검증한다.
- **literal의 `@type`은 등록된 식별자만 쓴다.** `mdt:value:` 뒤에 `string`, `integer`, `long`, `short`, `float`,
  `double`, `decimal`, `boolean`, `dateTime`, `duration`, `collection`, `list`, `file`, `range`, `mlprop`.
  `mdt:value:int`처럼 등록되지 않은 식별자가 하나라도 있으면 워크플로 **전체**가
  `Unregistered ElementValue type`으로 파싱에 실패한다.
- **literal 타입은 받는 연산 변수의 타입에 맞춘다**(`xs:int` → `integer`, `xs:float` → `float`). mdtpy는 연산 변수에
  들어 있는 기본값의 타입으로 변환해 주지만, 기본값이 없는 변수에서는 변환되지 않는다.

### 3.4 (선택) CLI 실행 명령

A 방식에서 task를 단독으로 실행하는 명령이 필요하면 `mdt-cli/src/main/java/mdt/cli/run/`에 명령 클래스를 만들고
`RunCommands`의 `subcommands`에 등록한다. 현재 등록된 것은 AASOperation, OperationSubmodel, RESTfulAsyncRpc,
Set 네 가지이고, `RESTfulRpcTask`에는 명령이 없다.

---

## 4. mdt-workflow — DAG 생성기

### 4.1 Spec 클래스: `src/main/java/mdt/workflow/airflow/XxxSpec.java`

- `extends OperatorSpec`. builder는 `OperatorSpec.AbstractBuilder<Builder, XxxSpec>`를 상속하고
  `static Builder builder()`를 제공한다.
- `getOperatorCall()`의 규칙:
  - operator 이름과 `(\n`으로 시작한다.
  - 인자 한 줄은 `kwargLine(name, value)`로 만든다. 문자열을 `PythonStringEscaper`로 이스케이프하며,
    개행·따옴표도 이스케이프하므로 여러 줄 script도 하나의 문자열 리터럴로 안전하게 들어간다.
  - 숫자나 `None`처럼 문자열이 아닌 값은 `rawKwargLine(name, literal)`을 쓴다. 이스케이프하지 않으므로
    신뢰할 수 있는 리터럴에만 쓴다.
  - **반드시 개행으로 끝나야 한다.** Pebble은 표현식 태그만 있는 줄의 후행 개행을 제거하므로, 반환값이
    스스로 개행으로 끝나야 뒤따르는 `inputs = {`가 다음 줄에 온다.
  - `inputs`/`outputs`와 닫는 괄호는 템플릿이 붙인다. 반환값을 템플릿에서 `| py`로 다시 이스케이프하지 않는다.
- **생성 메소드는 `of()` + `from()`으로 나눈다** (`PythonScriptSpec` 방식, 새 spec에 권장):
  - `static XxxSpec of(taskId, <고유 인자>, inputs, outputs, dependencies)`: 실제 생성. 테스트나 코드에서 직접 만들 때도 쓴다.
  - `static XxxSpec from(TaskDescriptor task)`: option과 인자를 꺼내 `of()`에 넘기기만 하는 얇은 어댑터.
    - option: `task.getOptions().get(OPTION_...)`. 필수 option이 없으면 task id를 담아 `IllegalArgumentException`.
    - 입력: `fromTaskArgument(arg.getId(), arg)`. reference·literal·task-output을 모두 처리하고, literal은
      `@type`을 보존해 `mdtpy.value.parse_json_node()`로 복원하는 식을 만든다.
    - 출력: `fromOutputVariable(arg.getId(), arg)` → `sink(...)`.
    - 의존성: `task.getDependencies()`.
  - 실행해도 의미 없는 값(예: 빈 script)은 생성자에서 거부해, 후속 task의 엉뚱한 오류 대신 생성 시점에 드러나게 한다.
- 참고 구현: `PythonScriptSpec`(`of()`/`from()`, option 상수, 빈 값 거부), `RESTfulRpcSpec`(숫자 timeout을
  `rawKwargLine`으로), `SetElementSpec`(고유 인자 없음).

```java
@Override
public String getOperatorCall() {
	// kwargLine이 script를 Python 문자열 리터럴로 이스케이프하므로(개행 포함) 여러 줄 script도 안전하다.
	return "PythonScriptOperator(\n"
			+ kwargLine("script", m_script);
}

public static PythonScriptSpec from(TaskDescriptor task) {
	var scriptOpt = task.getOptions().get(OPTION_SCRIPT);
	if ( scriptOpt == null || scriptOpt.getValue() == null ) {
		throw new IllegalArgumentException("'" + OPTION_SCRIPT + "' option is required ...: " + task.getId());
	}
	List<OperatorArgumentSpec> inputs = FStream.from(task.getInputArguments())
												.map(arg -> fromTaskArgument(arg.getId(), arg)).toList();
	List<SinkArgument> outputs = FStream.from(task.getOutputArguments())
												.map(arg -> fromOutputVariable(arg.getId(), arg)).toList();
	return of(task.getId(), scriptOpt.getValue(), inputs, outputs, task.getDependencies());
}
```

### 4.2 분기 등록

`OperatorSpec.from()`에 분기를 추가한다.

```java
// A. Java task 클래스가 있는 경우
else if ( XxxTask.class.getName().equals(task.getType()) ) {
	return XxxSpec.from(task);
}
// B. Airflow 전용 type은 task 클래스 대신 type 문자열 상수와 비교한다.
else if ( TaskDescriptors.PYTHON_SCRIPT_TASK_TYPE.equals(task.getType()) ) {
	return PythonScriptSpec.from(task);
}
```

### 4.3 템플릿 import

`src/main/resources/templates/airflow_dag.peb` 상단의 `from mdtpy.airflow.operator import (...)` 목록에 추가한다.

> **주의**: 생성되는 모든 DAG가 모든 operator를 import한다. 따라서 Airflow에 그 operator가 없는 mdtpy가 깔려 있으면,
> 새 operator를 쓰지 않는 DAG까지 파싱 단계에서 `ImportError`로 실패한다. [§7](#7-배포-순서)의 순서를 지킨다.

### 4.4 테스트

- `OperatorCallTest`:
  - 정확한 호출 문자열을 검증한다(여러 줄 문자열 인자가 있으면 이스케이프 결과까지).
  - `everyCallStartsWithOperatorNameAndEndsWithNewline`의 대상 목록에 추가한다.
- `XxxSpecTest`(`PythonScriptSpecTest` 참고):
  - `OperatorSpec.from()`이 새 type으로 이 spec을 고르는지(`assertInstanceOf`).
  - `from()`이 option·입력·의존성을 제대로 꺼내는지, `of()`로 직접 만들 수 있는지.
  - 필수 option 누락, 잘못된 값 거부.
- 실행: `gradle :test --tests 'mdt.workflow.airflow.*'`.
  **`:` 접두어가 필요하다.** mdt-workflow와 mdt-client 모두 다른 빌드(utils 등)를 포함하므로, 접두어가 없으면
  포함된 빌드의 test 작업에도 필터가 걸려 "No tests found"로 실패한다(대상 테스트가 통과했어도 빌드가 실패로 표시된다).

### 4.5 워크플로 등록 서비스의 동작 (알고 있어야 할 것)

- **등록할 때만 DAG 파일을 만든다.** `AirflowWorkflowManager.onWorkflowModelAdded()`가 `<dagsFolder>/<dagId>.py`를 쓴다
  (`dagsFolder` = `mdt.workflow-manager.airflow.dagsFolder`, 기본 `${MDT_HOME}/airflow/dags`). 생성기를 고친 뒤에는
  워크플로를 **다시 등록해야** 새 형식의 DAG가 만들어진다.
- **같은 id의 파일이 있어도 확인 없이 덮어쓴다.** 워크플로 id가 기존 DAG와 겹치면 그 DAG가 조용히 바뀌고, Airflow 실행
  이력도 같은 dag_id 아래 섞인다. 테스트용 워크플로에는 고유한 id를 쓴다.
- **배포된 jar는 빌드 결과물을 가리키는 링크다.** `~/mdt/mdt-workflow/mdt-workflow-all.jar` →
  `~/development/mdt/mdt-workflow/build/libs/mdt-workflow-<ver>-all.jar`. 서비스가 실행 중일 때 다시 빌드하면 실행 중인
  JVM 아래에서 jar가 바뀌므로, **서비스를 멈춘 뒤에 빌드**하고 다시 띄운다.

---

## 5. mdt-gui — 노드와 JSON 변환

GUI에서 새 task를 만들 수 있게 하려면 아래 지점을 손댄다(`~/mdt/mdt-gui/dtfront-main/src/apps/` 기준, 확인한 지점).

| 위치 | 역할 |
|---|---|
| `datas/definedData.js`의 `TaskType` | type 문자열 상수. **mdt-client의 type과 정확히 같아야 한다** |
| `datas/definedData.js`의 type별 초기값 | 새 노드의 기본 입력/출력 인자(`Get_Origin_VariableData`) |
| `components/Workflow/nodes/Node*.js` | 노드 컴포넌트(`NodeAas`, `NodeRestful`, `NodeHttp`, `NodeCopy` 등). 출력 핸들은 `outputArguments`로 만든다(`CONVERT_HANDLE_DATA`) |
| `components/Workflow/comps/PropertyPanel.js` | type별 속성 패널 |
| `utils/useDataConvert.js`의 `uDC_UIData_To_*Task` | UI 데이터 → `TaskDescriptor` JSON. **type과 option 이름이 spec이 읽는 이름과 같아야 한다** |
| `utils/useDataConvert.js`의 `uDC_fetchJSONConnect` | 노드 연결선 → 후속 입력의 `task-output`(`<앞 task id>:<출력 이름>`) |
| mdt-manager `GET .../instance-manager/argumentSpec/$json?(ref│value│taskOutput)=...&id=...` | GUI가 인자 JSON을 받아 오는 곳(`REQ_GET_ArgumentSpec_JSON`). `ArgumentSpec` 모델이 바뀌면 함께 고친다 |

- **type·option 불일치는 DAG 생성 단계에서야 드러난다.** 실제 사례로, GUI의 AI 노드는 `type: "mdt.task.AITask"`와 option
  `baseUrl`/`endpoint`/`timeout`을 내보내지만 백엔드는 `mdt.task.RESTfulRpcTask`와 `opUrl`을 기대한다. 그래서
  GUI에서 만든 AI task는 "unsupported task type"으로 실패한다(미해결).
- **"전부 전달" operator는 출력을 `outputArguments`로 선언하지 않는다.** 그래서 GUI 노드가 후속 연결용 출력 핸들 이름을
  스스로 관리하고 다시 불러올 수 있게 저장해야 한다. 저장 위치(노드 UI 데이터, 워크플로의 `gui` 영역 등)는
  `PythonScript` 노드를 만들 때 정한다.

---

## 6. 통합 검증

1. **렌더링:** 새 task가 들어간 `WorkflowModel`(예: `test_workflow.json`)로 `AirflowDagGenerator.generate(model, writer)`를
   호출해 DAG 파일을 만든다. 임시 테스트 클래스로 충분하고, 검증 후 지운다.
2. **문법:** `python -c "import ast; ast.parse(open('dag.py').read())"`.
3. **호출식 실행:** 생성된 operator 호출식을 잘라 mdtpy 환경에서 `eval`하고, `get_task_output`이 앞 task 값을 돌려주는
   MagicMock 컨텍스트로 `run()`해 본다. 템플릿과 같은 import 구성(`import mdtpy`, `from mdtpy.airflow.operator import ...`)을 쓴다.
   이스케이프된 인자(예: script)가 원래대로 복원되는지, literal이 의도한 타입으로 복원되는지 확인한다.
4. **실제 Airflow 실행** (로컬 docker compose, `~/mdt/airflow`):
   - MDT 주소 확인: `docker exec airflow-airflow-scheduler-1 airflow variables get mdt_manager_url`. 호스트 IP가 바뀌면
     이 값이 낡아서 모든 task가 접속 시간 초과로 실패한다.
   - DAG 파일 반영: 워크플로를 mdt-workflow에 등록하거나(정식) 생성한 파일을 dags 폴더에 둔 뒤,
     `docker exec airflow-airflow-dag-processor-1 airflow dags reserialize`.
   - 새 DAG는 일시정지 상태로 만들어진다: `docker exec airflow-airflow-scheduler-1 airflow dags unpause <dag_id>`.
   - 실행·상태: `... airflow dags trigger <dag_id>`, `... airflow dags list-runs <dag_id>`.
   - 로그: `~/mdt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=1.log`(JSON 한 줄씩).
   - 실행하면 sink 대상의 실제 디지털 트윈 데이터가 바뀐다는 점에 유의한다.

---

## 7. 배포 순서

1. mdtpy 새 버전을 PyPI에 배포한다([§2](#2-mdtpy-배포)).
2. PyPI 배포판을 쓰는 Airflow worker 환경의 mdtpy를 갱신한다.
3. mdt-client를 빌드하고, 이를 쓰는 mdt-workflow, mdt-cli를 다시 빌드한다. mdt-manager는 GUI용 `ArgumentSpec` 처리 등
   mdt-client의 바뀐 API를 쓸 때 다시 빌드한다.
4. mdt-workflow를 배포한다: 서비스 중지 → 빌드 → 재시작([§4.5](#45-워크플로-등록-서비스의-동작-알고-있어야-할-것)).
   생성기가 바뀌었으면 기존 워크플로를 다시 등록해 DAG를 새로 만든다.
5. mdt-gui를 배포한다. 백엔드가 모두 새 type을 이해한 뒤에 GUI가 그 type을 내보내야 한다.

4번을 2번보다 먼저 하면 새로 생성되는 모든 DAG가 Airflow 파싱 단계에서 `ImportError`로 실패한다.
또 이미 생성된 DAG 파일은 Airflow 쪽에 남으므로, mdt-workflow만 롤백해서는 새 operator를 import하는
DAG가 사라지지 않는다는 점도 고려한다.

---

## 8. 완료 체크리스트

- [ ] 설계 결정(task type 방식, 출력 계약 등)을 정하고 mdtpy README 비교표에 기록
- [ ] mdtpy: operator 모듈, `operator/__init__.py` 등록, `operator.py` docstring
- [ ] mdtpy 테스트: 원시 값 / 선언 외 출력 / 누락 / 변환 실패 / 고유 인자 (patch는 `patch.object`)
- [ ] mdtpy 문서: README(표·비교표·전용 절), `airflow/CLAUDE.md`(operator 표, 생성된 DAG와의 계약), 샘플. 코드와 대조
- [ ] mdtpy 버전 올림, 확인(wheel 내용·`twine check`), PyPI 배포, Airflow 환경 갱신
- [ ] mdt-client: A) `XxxTask`와 `OPTION_*` 상수, 또는 B) `TaskDescriptors`의 type 상수
- [ ] mdt-client: `TaskDescriptors.CLASS_TO_SHORT_ID_MAP` 등록
- [ ] (선택) mdt-cli: `run` 명령과 `RunCommands` 등록
- [ ] mdt-workflow: `XxxSpec`(`of()` + `from()`), `OperatorSpec.from()` 분기, 템플릿 import
- [ ] mdt-workflow 테스트: `OperatorCallTest`, `XxxSpecTest`
- [ ] mdt-gui: `TaskType`, 노드, 속성 패널, `uDC_UIData_To_*Task`(type·option 이름 일치)
- [ ] 통합 검증: 렌더링 → `ast.parse` → mdtpy에서 호출식 실행 → (가능하면) 실제 Airflow 실행
- [ ] 배포 순서 준수, 생성기가 바뀌었으면 워크플로 재등록

---

## 9. 알려진 한계와 주의점

- **저장 위치 없는 출력을 표현할 수 없다.** `TaskDescriptor.getOutputArguments()`가 `ReferenceArgument` 목록이므로
  `outputs={'x': None}`을 생성할 수 없다. "선언된 것만 전달"하는 `RestfulRpcOperator`의 "기록 없이 전달만" 출력이
  여기에 걸린다. "전부 전달" operator(`PythonScript` 등)는 선언 없이 넘기므로 해당하지 않는다. 이 기능이 꼭 필요하면
  mdt-client 출력 모델에 이름만 있는 출력 선언을 추가해야 한다(설계안은 보류 중).
- **literal 입력은 `@type`이 포함된 JSON으로 전달된다.** 생성 코드는 `mdtpy.value.parse_json_node()`로 값을 복원한다.
  새 ElementValue 종류를 literal로 쓰려면 Java `ElementValues`와 mdtpy `value/codec.py`·`value/factory.py`가 모두
  그 종류를 지원해야 한다. 현재 Java의 `reference`, `opvars`는 mdtpy가 지원하지 않는다.
- **식별자 제약.** task id와 인자 id는 Python 식별자여야 하고, DAG id에는 `__`를 쓸 수 없다. task가 없는 DAG는 거부된다.
- **Pebble 공백 규칙.** 표현식 태그로 끝나는 줄은 후행 개행이 제거된다. `getOperatorCall()`이 개행으로 끝나야 하고,
  템플릿의 의존성 줄을 괄호로 감싸는 것도 이 때문이다.
- **값 타입 변환으로 인한 조용한 손실.** 실수 출력이 `xs:int` 연산 변수로 들어가면 mdtpy가 정수로 변환하면서 소수점이
  잘린다(예: 12.7 → 12). 연결용 script나 literal에서 타입을 의도적으로 맞춘다.

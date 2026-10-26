# OperatorSpec 사용 가이드

`OperatorSpec`은 워크플로 모델의 task(`TaskDescriptor`)를 Airflow DAG 안의 mdtpy Operator 호출로 변환한다.
이 문서는 현재 지원되는 Spec 6종의 선택 기준, 워크플로 JSON 작성 규칙, task 유형별 설정과 생성 결과,
Java에서의 사용법, 오류 확인 방법을 정리한다. 워크플로 JSON을 직접 작성하거나 Java 코드로 DAG를
생성할 때 참고한다.

새 task 유형(operator)을 **추가하는** 절차는 [adding-operator.md](adding-operator.md)를 본다.

## 목차

1. [개요](#1-개요)
2. [작업에 맞는 Spec 고르기](#2-작업에-맞는-spec-고르기)
3. [첫 워크플로 작성하기](#3-첫-워크플로-작성하기)
4. [공통 규칙: 워크플로와 task JSON](#4-공통-규칙-워크플로와-task-json)
5. [task 유형별 사용법](#5-task-유형별-사용법)
6. [전체 예: 워크플로 JSON → DAG](#6-전체-예-워크플로-json--dag)
7. [Java에서 직접 사용하기](#7-java에서-직접-사용하기)
8. [검증 시점과 흔한 오류](#8-검증-시점과-흔한-오류)

---

## 1. 개요

```
WorkflowModel JSON
  └ TaskDescriptor (type, options, inputArguments, outputArguments, dependencies)
        │  OperatorSpec.from(task)  ── type으로 Spec 클래스 선택
        ▼
XxxSpec (OperatorSpec 하위 클래스)
        │  getOperatorCall()            → "XxxOperator(" + 고유 인자
        │  getInputs() / getOutputs()   → inputs = {...}, outputs = {...}
        ▼
DagSpec + airflow_dag.peb 렌더링 → <dagsFolder>/<dagId>.py
        ▼
Airflow worker: task 본문에서 mdtpy XxxOperator(..., inputs=..., outputs=...).run()
```

- **`OperatorSpec`** (`mdt.workflow.airflow`): task 하나를 Python operator 호출 하나로 옮기는 추상 클래스.
  task id, 입력 인자, 출력 인자, 선행 task 목록을 공통으로 갖는다.
- **하위 Spec 6종**: task 유형마다 하나씩 있고, 그 operator의 고유 인자(연산 위치, URL, script 등)를 담는다.
- **`DagSpec`**: 워크플로 전체를 담는다. 모든 task를 `OperatorSpec.from()`으로 변환하고 DAG 수준의 검증을 한다.
- **`AirflowDagGenerator.generate()`**: `DagSpec`을 만들어 템플릿 `templates/airflow_dag.peb`로 Python 파일을 쓴다.

**Spec 생성은 연산을 실행하지 않는다.** 참조 값 읽기, 원격 연산 호출, 출력 저장은 Airflow worker가 task
본문을 실행할 때 일어난다. `getOperatorCall()`도 완성된 DAG가 아니라 operator 생성자 호출의 시작 부분
(operator 이름과 고유 인자)만 반환하고, 공통 `inputs`, 선택적 `outputs`, 닫는 괄호와 `.run()`은 템플릿이 붙인다.

워크플로를 mdt-workflow 서비스에 등록하면 이 과정이 자동으로 실행되어 DAG 파일이 생성된다.

---

## 2. 작업에 맞는 Spec 고르기

| 하려는 작업 | task `type` | Spec 클래스 | 생성되는 mdtpy Operator | 필수 option | 그 밖에 읽는 option |
|---|---|---|---|---|---|
| 값 읽기 또는 복사 | `mdt.task.SetTask` | `SetElementSpec` | `SetElementOperator` | 없음 | 없음 |
| AAS `Operation` 요소를 경로로 지정하여 호출 | `mdt.task.AASOperationTask` | `AASOperationSpec` | `AASOperationOperator` | `operation` | 없음 |
| MDT Operation submodel 호출 | `mdt.task.OperationSubmodelTask` | `OperationSubmodelSpec` | `OperationSubmodelOperator` | `operationSubmodel` | 없음 |
| 원격 연산 시작 후 세션 상태 폴링 | `mdt.task.RESTfulAsyncRpcTask` | `RESTfulAsyncRpcSpec` | `RestfulAsyncRpcOperator` | `baseUrl`, `endpoint` | 없음 |
| AI 모델의 동기 RESTful RPC 호출 | `mdt.task.RESTfulRpcTask` | `RESTfulRpcSpec` | `RestfulRpcOperator` | `opUrl` | `timeout` |
| Python으로 값 가공 | `mdt.task.PythonScriptTask` | `PythonScriptSpec` | `PythonScriptOperator` | `script` | 없음 |

- `type`에는 표의 문자열 전체를 넣는다. `AASOperationSpec` 같은 Spec 클래스명이나 `RestfulRpcOperator` 같은
  Python 클래스명을 넣으면 `IllegalArgumentException("unsupported task type: ...")`으로 DAG 생성이 실패한다.
- `PythonScriptTask`는 type 식별자일 뿐 대응하는 Java 실행 클래스가 없는 **Airflow 전용** 유형이다.

> **Spec이 읽지 않는 option은 Airflow 실행에 적용되지 않는다.** 예를 들어 `SetTask`·`AASOperationTask`의
> `timeout`/`loglevel`, `RESTfulAsyncRpcTask`의 `poll`/`timeout`은 mdt-cli로 Java task를 실행할 때만 쓰인다.
> Airflow에서 실행하면 `RestfulAsyncRpcOperator`의 기본값(폴링 3초, 대기 시간 제한 없음)을 따른다.

출력 처리 방식은 operator마다 다르다(mdtpy `operator/README.md`의 비교표 요약).

| Spec / Operator | 후속 task에 전달되는 출력 | 선언한 출력이 결과에 없으면 | 저장 위치 설정 시 주의점 |
|---|---|---|---|
| `SetElementSpec` / `SetElementOperator` | 항상 `target` 하나 | 해당 없음 | 출력 이름을 `target`으로 지정한다 |
| `AASOperationSpec` / `AASOperationOperator` | 연산이 반환한 **모든** 결과 | 기록되지 않음 | 선언한 이름과 일치하는 결과만 기록한다 |
| `OperationSubmodelSpec` / `OperationSubmodelOperator` | `invoke()`가 반환한 **모든** 결과 | 기록되지 않음 | 입력과 출력 참조를 함께 `invoke()`에 전달한다 |
| `RESTfulAsyncRpcSpec` / `RestfulAsyncRpcOperator` | RPC가 반환한 **모든** 결과 | 기록되지 않음 | 선언한 이름과 일치하는 결과만 기록한다 |
| `RESTfulRpcSpec` / `RestfulRpcOperator` | `outputArguments`에 **선언한 것만** | `KeyError` | 필요한 출력은 반드시 선언한다 |
| `PythonScriptSpec` / `PythonScriptOperator` | script가 `outputs` dict에 넣은 **모든** 값 | 기록을 건너뜀 | MDT 요소에 저장할 값만 선언한다 |

특히 `RESTfulRpcSpec`에서 출력을 전혀 선언하지 않으면 모델이 결과를 반환해도 후속 task에는 전달되지 않는다.
Python `RestfulRpcOperator`를 직접 쓸 때는 저장 위치 `None`으로 "기록 없이 전달만"을 표현할 수 있지만, 워크플로
JSON과 `SinkArgument`는 이 방식을 표현하지 못한다. 이 Spec에서는 필요한 출력에 유효한 저장 참조를 지정한다.

---

## 3. 첫 워크플로 작성하기

다음 JSON을 `operator_spec_demo.json`으로 저장한다. `seed`가 정수 10을 `target`으로 내보내고, `scale`이 그 값을
두 배로 만들어 `result`로 내보낸다. MDT 요소에 결과를 저장하지 않으므로 등록된 인스턴스 없이도 생성 결과를
확인할 수 있다.

```json
{
  "id": "operator_spec_demo",
  "name": "OperatorSpec 사용 예제",
  "description": "정수 값을 전달하고 Python으로 가공한다",
  "taskDescriptors": [
    {
      "id": "seed",
      "type": "mdt.task.SetTask",
      "dependencies": [],
      "inputArguments": [
        {
          "@type": "mdt:task_arg:literal",
          "id": "source",
          "value": { "@type": "mdt:value:integer", "value": 10 }
        }
      ],
      "outputArguments": [],
      "options": []
    },
    {
      "id": "scale",
      "type": "mdt.task.PythonScriptTask",
      "dependencies": ["seed"],
      "inputArguments": [
        {
          "@type": "mdt:task_arg:task-output",
          "id": "value",
          "taskId": "seed",
          "argumentName": "target"
        }
      ],
      "outputArguments": [],
      "options": [
        {
          "name": "script",
          "value": "value = inputs['value'].to_raw_object()\noutputs['result'] = value * 2"
        }
      ]
    }
  ]
}
```

이 워크플로로 생성되는 DAG의 task 본문은 다음과 같다(import와 DAG 선언부는 [§6](#6-전체-예-워크플로-json--dag) 참고).

```python
  @task(task_id="seed")
  def _t0() -> None:
    SetElementOperator(
      inputs = {
        "source": mdtpy.airflow.literal(mdtpy.value.parse_json_node({'@type': 'mdt:value:integer', 'value': 10})),
      }
    ).run()

  @task(task_id="scale")
  def _t1() -> None:
    PythonScriptOperator(
      script = "value = inputs[\'value\'].to_raw_object()\noutputs[\'result\'] = value * 2",
      inputs = {
        "value": mdtpy.airflow.task_output("seed", "target"),
      }
    ).run()

  t_seed = _t0()
  t_scale = _t1()

  (t_seed >> t_scale)
```

`scale`의 기대 출력은 `result = 20`이다. 다음 task가 이 값을 쓰려면 입력에 `taskId: "scale"`,
`argumentName: "result"`를 지정하고, `dependencies`에도 `"scale"`을 추가한다.

---

## 4. 공통 규칙: 워크플로와 task JSON

### 4.1 필드와 이름 규칙

```json
{
  "id": "test_workflow",
  "name": "테스트 시뮬레이션",
  "description": "...",
  "taskDescriptors": [
    {
      "id": "add_and_sleep",
      "type": "mdt.task.AASOperationTask",
      "dependencies": [ "dec_by_5" ],
      "inputArguments":  [ ... ],
      "outputArguments": [ ... ],
      "options": [
        { "name": "operation", "value": "test:AddAndSleep:Operation" }
      ],
      "labels": []
    }
  ]
}
```

| 필드 | 의미와 생성되는 DAG에서의 쓰임 |
|---|---|
| 워크플로 `id` | `@dag(dag_id=...)`. 비어 있으면 안 되고 `__`를 포함할 수 없다 (DAG id와 run id의 구분자). |
| `name`, `description` | `description`은 `@dag(description=...)`로 들어간다. |
| `taskDescriptors` | task 배열. 최소 하나가 필요하다. |
| task `id` | `@task(task_id=...)`와 의존 관계 변수 이름(`t_<id>`). 같은 워크플로 안에서 유일해야 한다. |
| `type` | Spec 클래스 선택([§2](#2-작업에-맞는-spec-고르기)). |
| `dependencies` | 먼저 완료되어야 하는 task `id` 배열 → `(t_<선행 id> >> t_<id>)` 줄. 같은 워크플로 안의 task만 쓸 수 있다. `taskDescriptors`의 나열 순서는 실행 순서와 무관하다. |
| `triggerRule` | 선택. 직전 task들의 종료 상태에 따라 이 task를 실행할지 정한다([§4.4](#44-실행-조건-triggerrule)). 생략하면 `all_success`이며, 그 외의 값은 `@task(..., trigger_rule="...")`로 출력된다. |
| `inputArguments` | operator의 `inputs = {...}`. |
| `outputArguments` | operator의 `outputs = {...}`. 연산 결과를 기록할 MDT 참조이며, 후속 task에 전달되는 출력과는 구분한다. 비어 있으면 `outputs` 인자 자체를 생략한다. |
| `options` | `{ "name": "옵션명", "value": "문자열" }` 배열. 옵션명은 대소문자를 구분한다. Spec이 고유 인자를 만들 때 읽는다. |
| `labels` | 선택. DAG 생성에는 쓰이지 않는다. |

**식별자 규칙.** 워크플로 `id`, task `id`, 입출력 인자 `id`는 Python 식별자 규칙 `[A-Za-z_][A-Za-z0-9_]*`를 따라야
하고, `class`, `for`, `None` 같은 Python 예약어는 쓸 수 없다. 예를 들어 `predict_result`는 유효하지만
`predict-result`, `1st_task`, `예측`은 거부된다. 한 task의 입력 목록(또는 출력 목록) 안에서 인자 이름이 중복되는지는
검사하지 않으며, 생성된 Python dict에서는 뒤의 항목이 앞의 항목을 덮어쓴다. 중복 없이 작성한다.

### 4.2 입력 인자 (`inputArguments`)

입력 인자는 세 종류이고, 각각 아래 Python 구문으로 바뀐다. 인자 `id`는 operator가 받는 입력 이름이다.

| `@type` | JSON 필드 | 생성되는 구문 | 실행 시 값 |
|---|---|---|---|
| `mdt:task_arg:reference` | `reference` | `mdtpy.airflow.reference("<참조>")` | 참조 대상에서 읽은 값 |
| `mdt:task_arg:literal` | `value` (`@type` 포함 ElementValue JSON) | `mdtpy.airflow.literal(mdtpy.value.parse_json_node({...}))` | 고정 값 |
| `mdt:task_arg:task-output` | `taskId`, `argumentName` | `mdtpy.airflow.task_output("<task>", "<출력 이름>")` | 선행 task가 남긴 출력 |

**reference** — MDT 요소의 현재 값을 읽는다.

```json
{ "@type": "mdt:task_arg:reference", "id": "Data", "reference": "param:machine:Temperature" }
```

참조 표현식은 MDT 참조 문법을 따른다. 자주 쓰는 형태는 다음과 같다.

| 형태 | 예 |
|---|---|
| 파라미터 | `param:<instance>:<parameter>` (예: `param:heater:CycleTime`) |
| 요소 경로 | `<instance>:<submodel>:<idShort 경로>` (예: `inst:Data:DataInfo.Equipment.EquipmentID`) |
| 연산 인자 | `oparg:<instance>:<operation>:in|out:<index>` |
| AAS 연산 변수 | `opvar:<instance>:<submodel>:<path>:in|out:<index>` |
| 시계열 | `timeseries:<instance>:<submodel>[#last=<n|기간>][|<컬럼>,...]` (예: `timeseries:Welder:NozzleProductionLog#last=50s|Time,QuantityProduced`) |

일반적인 참조는 실행 시 `ElementValue`로 읽히지만, File 참조는 파일을 전달할 수 있도록 `ElementReference` 자체가,
`timeseries:` 참조는 `SubmodelElement`가 전달된다. Python script에서 모든 입력에 무조건 `to_raw_object()`를
호출하지 말고 실제 입력 종류를 확인한다.

**literal** — 고정 값. `value` 안에 ElementValue 타입(`@type`)을 함께 넣는다.

```json
{ "@type": "mdt:task_arg:literal", "id": "Threshold", "value": { "@type": "mdt:value:double", "value": 0.8 } }
```

`@type`은 등록된 식별자만 쓴다. `mdt:value:` 뒤에 `string`, `integer`, `long`, `short`, `float`, `double`, `decimal`,
`boolean`, `dateTime`, `duration`, `collection`, `list`, `file`, `range`, `mlprop` 중 하나를 쓴다. `mdt:value:int`처럼
등록되지 않은 식별자가 하나라도 있으면 워크플로 **전체**의 파싱이 `Unregistered ElementValue type`으로 실패한다.
타입은 그 값을 받는 연산 변수의 타입(`xs:int` → `integer`, `xs:float` → `float`)에 맞춘다.

> literal은 `@type`이 붙은 JSON 그대로 DAG에 들어가고 실행 시 `parse_json_node()`로 복원된다. 그래서
> `dateTime`, `duration`, `long`, 컬렉션 값도 타입이 바뀌지 않고 전달된다.

**task-output** — 선행 task의 출력.

```json
{ "@type": "mdt:task_arg:task-output", "id": "Data", "taskId": "scale", "argumentName": "result" }
```

`id`는 현재 task의 입력 이름이고 `argumentName`은 선행 task의 출력 이름이다. 두 이름이 같을 필요는 없다.
다음 두 가지에 유의한다.

- **`task-output`은 실행 순서를 자동으로 추가하지 않는다.** 이 입력을 받는 task의 `dependencies`에도 `"scale"`을 넣는다.
- **생성기는 `taskId`가 가리키는 task나 `argumentName`의 존재 여부를 검사하지 않는다.** 오타는 task 실행 시점에
  "출력을 찾을 수 없음"으로 드러난다. 선행 operator가 그 이름을 실제로 출력하는지는 [§2](#2-작업에-맞는-spec-고르기)의
  출력 처리 표로 확인한다.

### 4.3 출력 인자 (`outputArguments`)

출력 인자는 **결과를 기록할 위치**이고, `reference` 형태만 쓸 수 있다.

```json
"outputArguments": [
  { "@type": "mdt:task_arg:reference", "id": "Output", "reference": "param:test:Data" }
]
```

→ `outputs = { "Output": mdtpy.airflow.sink("param:test:Data") }`

- 인자 `id`는 operator가 반환하는 출력 이름이다.
- 기록할 위치 없이 후속 task에 넘기기만 하는 출력은 선언할 수 없다. "모든 결과를 전달"하는 operator는
  선언하지 않아도 후속 task가 `task-output`으로 받을 수 있다([§2](#2-작업에-맞는-spec-고르기)의 출력 처리 표).

### 4.4 실행 조건 (`triggerRule`)

Airflow는 직전(직접 upstream) task들의 종료 상태를 보고 task를 실행할지 정한다. 조건이 맞지 않으면 task는
실행되지 않고 `skipped` 또는 `upstream_failed`로 끝나며, 기본값에서는 이 상태가 뒤로 이어진다.
`triggerRule`은 모든 task 유형에서 쓸 수 있고, 다음 네 값만 허용한다.

| 값 | 실행 조건 | 주 용도 |
|---|---|---|
| `all_success` (기본값) | 직전 task가 모두 성공 | 일반적인 순차 실행 |
| `none_failed_min_one_success` | 실패가 없고 하나 이상 성공 (`skipped` 허용) | 분기 뒤 경로가 합류하는 task |
| `all_done` | 직전 task가 상태와 관계없이 모두 끝남 | 정리·마무리 task (예: 실패해도 홈 복귀) |
| `one_failed` | 하나라도 실패하면 나머지를 기다리지 않고 즉시 | 실패 처리 task (예: 안전 정지) |

```json
{
  "id": "return_home",
  "type": "mdt.task.AASOperationTask",
  "dependencies": [ "place" ],
  "triggerRule": "all_done",
  "options": [ { "name": "operation", "value": "panda01:Data:Operations.MoveToNamedTarget" } ]
}
```

→ `@task(task_id="return_home", trigger_rule="all_done")`

- 생략하거나 `null`이면 `all_success`이며, 이때는 `trigger_rule` 인자를 출력하지 않으므로 기존 워크플로의 DAG는
  바뀌지 않는다. 그 밖의 값은 워크플로 JSON을 읽을 때 `unsupported trigger rule`로 거부된다.
- **실패가 성공으로 가려질 수 있다.** Airflow는 DAG 실행의 최종 상태를 마지막(leaf) task들의 상태로 정한다.
  `all_done`/`one_failed` task가 마지막에 성공하면 중간 task가 실패했어도 DAG 실행이 `success`가 되어
  MDT에서는 `COMPLETED`로 보인다. 작업의 성패를 알려야 하면 마무리 뒤에 앞 단계 결과를 확인해 실패시키는 task를 둔다.
- 현재 GUI 편집기는 이 필드를 다루지 않으므로, GUI에서 저장하면 `triggerRule`이 사라진다.

---

## 5. task 유형별 사용법

각 절의 "생성되는 호출"은 `getOperatorCall()`이 만드는 부분이다. `inputs`/`outputs`와 닫는 괄호, `.run()`은
템플릿이 붙인다. 예제의 인스턴스, 파라미터, 연산 이름, URL은 예시이므로 실제 등록된 리소스로 바꾼다.
컨테이너 환경에서는 Airflow worker가 접근할 수 있는 주소를 쓴다.

### 5.1 SetTask → `SetElementSpec` → `SetElementOperator`

값 하나를 읽어 후속 task에 넘기거나, 다른 위치에 복사한다. 연산을 호출하지 않는다.

- **option**: 없음
- **입력**: `source` 하나가 반드시 있어야 한다. reference뿐 아니라 literal([§3](#3-첫-워크플로-작성하기))도 된다.
- **출력**: 지정한다면 이름이 `target`이어야 한다. 지정하지 않아도 읽은 값은 언제나 `target` 이름으로 후속 task에 전달된다.

```json
{
  "id": "copy_cycle_time",
  "type": "mdt.task.SetTask",
  "dependencies": [],
  "inputArguments": [
    { "@type": "mdt:task_arg:reference", "id": "source", "reference": "param:heater:CycleTime" }
  ],
  "outputArguments": [
    { "@type": "mdt:task_arg:reference", "id": "target", "reference": "param:other:CycleTime" }
  ],
  "options": []
}
```

생성되는 호출:

```python
SetElementOperator(
  inputs = {
    "source": mdtpy.airflow.reference("param:heater:CycleTime"),
  },
  outputs = {
    "target": mdtpy.airflow.sink("param:other:CycleTime"),
  }
).run()
```

> Java 쪽에서는 `source`/`target` 이름을 검사하지 않는다. 이름이 틀리면 DAG 파일은 만들어지고 Airflow 파싱도
> 통과하지만, worker가 task 본문에서 `SetElementOperator`를 생성할 때 `ValueError`가 난다.

### 5.2 AASOperationTask → `AASOperationSpec` → `AASOperationOperator`

submodel 안의 AAS `Operation` 요소를 idShort 경로로 직접 지정해 호출한다.

- **option `operation`**: `<instanceId>:<submodelIdShort>:<path>`
  - 앞의 두 `:`로만 나누므로, `path`에는 `:`가 들어가도 된다.
  - 세그먼트가 3개보다 적으면 `IllegalArgumentException`.
- **입력/출력**: 인자 `id`는 AAS `Operation`의 입력/출력 변수 idShort와 맞춘다.
- `timeout`, `poll`, `loglevel` option은 이 Spec이 읽지 않는다.

```json
"options": [ { "name": "operation", "value": "test:AddAndSleep:Operation" } ]
```

```python
AASOperationOperator(
  instance = "test",
  submodel = "AddAndSleep",
  path = "Operation",
  inputs = { ... },
  outputs = { ... }
).run()
```

> submodel의 종류는 따지지 않는다. Data submodel 안의 `Operation` 요소(예: 로봇 트윈 이동 연산
> `panda01:Data:Operations.MoveLinear`)도 호출할 수 있으며, Java `AASOperationTask`(mdt-cli `run operation`)와
> 같은 동작이다. `path`가 가리키는 요소가 `Operation`이 아니면 task 실행 시 `ValueError`가 난다.
> mdtpy 0.2.13 이하는 `OperationSubmodelService`가 아니면 `ValueError`로 거부하므로, Airflow worker에 설치된
> mdtpy가 이 변경을 포함한 버전인지 확인한다.

### 5.3 OperationSubmodelTask → `OperationSubmodelSpec` → `OperationSubmodelOperator`

MDT Operation submodel을 등록정보(descriptor)에 따라 호출한다. 지정하지 않은 입력이나 출력은 연산 등록정보에
정의된 기본 참조로 채워진다.

- **option `operationSubmodel`**: `<instanceId>:<submodelIdShort>`
  - `:`로 나눈 결과가 **정확히 2개**여야 한다. 아니면 `IllegalArgumentException`.
- 별도 `path`는 받지 않는다.

```json
"options": [ { "name": "operationSubmodel", "value": "inspector:ProcessSimulation" } ]
```

```python
OperationSubmodelOperator(
  instance = "inspector",
  submodel = "ProcessSimulation",
  inputs = { ... },
  outputs = { ... }
).run()
```

> 실행 시 해석된 입력 값과 출력 참조를 하나의 kwargs로 병합해 `invoke()`에 넘긴다. 입력과 출력에 같은 이름을
> 지정하면 병합 과정에서 **출력 참조가 우선**하므로 이름 충돌을 피한다.

### 5.4 RESTfulAsyncRpcTask → `RESTfulAsyncRpcSpec` → `RestfulAsyncRpcOperator`

원격 연산을 비동기 RESTful RPC로 호출한다. POST로 연산을 시작하고, 끝날 때까지 세션 상태를 폴링한다.
`mdt-operation-http`의 연산 서버가 대표적인 호출 대상이며, 대상은 시작 응답과 세션 상태 조회를 제공하는
MDT RESTful RPC 프로토콜을 지원해야 한다.

- **option `baseUrl`**: 연산 서버의 기본 URL (예: `http://localhost:12987`). 끝의 `/`는 생략한다.
- **option `endpoint`**: 연산 경로 (예: `/api/v1/operations/UpdateDefectList`). **`/`로 시작해야 한다.**
  - mdtpy가 `base_url + op_endpoint`로 단순 연결하기 때문이다. Java 클라이언트는 `/`를 보정해 주므로,
    mdt-cli에서는 동작하던 값이 Airflow에서만 실패할 수 있다.
- `poll`, `timeout` option은 이 Spec이 읽지 않는다. 폴링 간격 3초, 전체 완료 대기 제한 없음이라는 mdtpy 클라이언트의
  기본값이 적용된다. 개별 HTTP 요청의 제한 시간은 별도 설정이므로 전체 완료 대기 제한과 구분한다.

```json
"options": [
  { "name": "baseUrl",  "value": "http://localhost:12987" },
  { "name": "endpoint", "value": "/api/v1/operations/UpdateDefectList" }
]
```

```python
RestfulAsyncRpcOperator(
  base_url = "http://localhost:12987",
  op_endpoint = "/api/v1/operations/UpdateDefectList",
  inputs = { ... },
  outputs = { ... }
).run()
```

### 5.5 RESTfulRpcTask → `RESTfulRpcSpec` → `RestfulRpcOperator`

AI 모델 연산을 동기 RESTful RPC로 호출한다. 폴링 없이 POST 한 번의 응답에 결과가 실려 오므로, URL을 나누지
않고 모델 URL 전체를 받는다. 원격 서버는 mdtpy의 동기 RESTful RPC 요청·응답 형식을 지원해야 하며, 임의의 JSON
API를 URL만 바꿔 호출하는 범용 HTTP task는 아니다.

- **option `opUrl`**: 연산 호출 URL 전체
- **option `timeout`** (선택): 응답 대기 제한. `30s`, `10m`, `60`, `1.5`처럼 쓰고 단위가 없으면 초로 읽는다.
  - 생성된 DAG에는 초 단위 숫자로 들어간다(예: `timeout = 30.0,`).
  - 생략하면 인자 자체가 빠져 **제한 없이** 기다린다. 추론이 오래 걸릴 수 있어서 기본 제한을 두지 않는다.
  - 이 값은 모델 HTTP 호출의 응답 대기에만 쓰이며, Airflow task 전체의 `execution_timeout`은 설정하지 않는다.
- **출력**: `outputArguments`에 선언한 출력만 후속 task에 전달되고, 연산 결과에 그 출력이 없으면 `KeyError`가 난다.
  결과를 쓰려면 반드시 선언한다.

```json
"options": [
  { "name": "opUrl", "value": "http://localhost:9000/api/v1/models/defect-detector" },
  { "name": "timeout",   "value": "10m" }
]
```

```python
RestfulRpcOperator(
  op_url = "http://localhost:9000/api/v1/models/defect-detector",
  timeout = 600.0,
  inputs = { ... },
  outputs = { ... }
).run()
```

### 5.6 PythonScriptTask → `PythonScriptSpec` → `PythonScriptOperator`

두 task 사이의 입출력 형식을 맞추는 연결용 로직처럼, 짧은 Python script를 실행한다. 이 type에는 대응하는
Java task 클래스가 없어서 **Airflow에서만** 실행된다.

- **option `script`**: script 본문. 비어 있거나 공백뿐이면 `IllegalArgumentException`.
  - JSON 안의 줄바꿈은 `\n`으로 쓴다. 여러 줄 script도 이스케이프되어 하나의 Python 문자열 리터럴로 들어간다.
- **script 안에서 쓰는 전역 변수**
  - `inputs`: 입력 이름 → 해석된 값. 일반 값은 `ElementValue`이므로 원시 값이 필요하면 `to_raw_object()`를 쓴다
    (File·시계열 입력은 [§4.2](#42-입력-인자-inputarguments) 참고).
  - `outputs`: 빈 dict. 여기에 넣은 값이 **모두** 후속 task에 전달된다. `alarm = ...`처럼 같은 이름의 변수에
    대입하는 것만으로는 출력되지 않는다. 원시 불리언·숫자·문자열은 operator가 `ElementValue`로 변환하고,
    dict는 컬렉션 값, list는 리스트 값으로 멤버까지 변환한다. 그래서 계산한 자세를
    `{'Position': {...}, 'Orientation': {...}}`로 넣으면 후속 `AASOperationTask`의 SMC 입력(예: `TargetPose`)으로
    넘길 수 있다. (mdtpy 0.2.13 이하는 dict/list 출력을 `ValueError`로 거부하므로 `ElementCollectionValue`를
    직접 만들어 넣어야 한다.)
- **출력 인자**: 값을 SubmodelElement에도 기록해야 할 때만 선언한다. 후속 task에 넘기기만 할 값은 선언하지 않는다.
- script가 쓰는 외부 패키지는 Airflow worker 실행 환경에 설치되어 있어야 한다.

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
  ]
}
```

후속 task는 `{ "@type": "mdt:task_arg:task-output", "id": "Data", "taskId": "dec_by_5", "argumentName": "result" }`로
결과를 받는다.

> script는 Airflow worker에서 `exec()`로 실행된다. 외부 입력으로 script 본문을 조립하지 않는다.

---

## 6. 전체 예: 워크플로 JSON → DAG

`mdt-client/misc/test-workflows/test_workflow.json`의 task 세 개는 다음과 같이 연결된다.
SetTask가 값을 읽고, PythonScript가 그 값을 바꾼 뒤, AASOperation이 결과를 받아 연산을 호출한다.

```
get_heater_cycle_time (SetTask)
   └─ target ─▶ dec_by_5 (PythonScriptTask)
                   └─ result ─▶ add_and_sleep (AASOperationTask) ─▶ param:test:Data
```

이 워크플로로 생성한 실제 DAG 파일이다.

```python
import mdtpy

from airflow.sdk import dag, task
from mdtpy.airflow.operator import (
  AASOperationOperator,
  RestfulAsyncRpcOperator,
  SetElementOperator,
  OperationSubmodelOperator,
  RestfulRpcOperator,
  PythonScriptOperator
)


@dag(
  dag_id="test_workflow",
  description="본 워크플로우는 시뮬레이션 연동을 확인하기 위한 테스트 목적으로 작성됨.",
  tags=["mdt"]
)
def _dag():
  @task(task_id="get_heater_cycle_time")
  def _t0() -> None:
    SetElementOperator(
      inputs = {
        "source": mdtpy.airflow.reference("param:heater:CycleTime"),
      }
    ).run()

  @task(task_id="dec_by_5")
  def _t1() -> None:
    PythonScriptOperator(
      script = "value = inputs[\'value\'].to_raw_object()\noutputs[\'result\'] = value - 5",
      inputs = {
        "value": mdtpy.airflow.task_output("get_heater_cycle_time", "target"),
      }
    ).run()

  @task(task_id="add_and_sleep")
  def _t2() -> None:
    AASOperationOperator(
      instance = "test",
      submodel = "AddAndSleep",
      path = "Operation",
      inputs = {
        "Data": mdtpy.airflow.task_output("dec_by_5", "result"),
        "IncAmount": mdtpy.airflow.literal(mdtpy.value.parse_json_node({'@type': 'mdt:value:integer', 'value': 11})),
        "SleepTime": mdtpy.airflow.literal(mdtpy.value.parse_json_node({'@type': 'mdt:value:float', 'value': 1.5})),
      },
      outputs = {
        "Output": mdtpy.airflow.sink("param:test:Data"),
      }
    ).run()


  t_get_heater_cycle_time = _t0()
  t_dec_by_5 = _t1()
  t_add_and_sleep = _t2()

  (t_get_heater_cycle_time >> t_dec_by_5)
  (t_dec_by_5 >> t_add_and_sleep)

_dag()
```

알아 둘 점:

- **모든 DAG가 6개 operator를 모두 import한다.** Airflow에 설치된 mdtpy에 그중 하나라도 없으면, 그 operator를
  쓰지 않는 DAG도 파싱 단계에서 `ImportError`로 실패한다.
- **operator 객체는 task 함수 본문 안에서 만들어진다.** 따라서 operator 생성자의 검증(`source`/`target` 이름,
  빈 URL 등)은 DAG 파싱이 아니라 task 실행 시점에 일어난다.
- **함수 이름은 `_t0`, `_t1`, …로 고정된다.** task id는 `task_id=` 인자로만 쓰이므로, task id가 `task`나 `dag`여도
  데코레이터 이름을 가리지 않는다.
- **`AASOperationTask`의 `timeout`/`loglevel` option은 DAG에 나타나지 않는다**([§2](#2-작업에-맞는-spec-고르기)).

---

## 7. Java에서 직접 사용하기

`mdt-workflow`와 의존 라이브러리가 포함된 Java 21 환경에서 사용한다.

### 7.1 워크플로 파일에서 DAG 생성

프로그램 인자는 순서대로 입력 JSON 경로와 출력 Python 경로다(예: `operator_spec_demo.json`,
`generated_dags/operator_spec_demo.py`).

```java
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import mdt.workflow.WorkflowModel;
import mdt.workflow.airflow.AirflowDagGenerator;
import mdt.workflow.airflow.DagSpec;

public class GenerateOperatorSpecDemo {
	public static void main(String[] args) throws Exception {
		if ( args.length != 2 ) {
			throw new IllegalArgumentException("usage: input.json output.py");
		}
		WorkflowModel model = WorkflowModel.parseJsonFile(Path.of(args[0]).toFile());

		// 메모리에서 렌더링을 마친 뒤 파일을 쓰므로, 검증이나 렌더링이 실패해도 기존 출력 파일은 그대로 남는다.
		StringWriter rendered = new StringWriter();
		DagSpec dag = AirflowDagGenerator.generate(model, rendered);

		Path output = Path.of(args[1]).toAbsolutePath();
		Files.createDirectories(output.getParent());
		Files.writeString(output, rendered.toString(), StandardCharsets.UTF_8);
		System.out.println("generated: " + output + ", tasks=" + dag.getTasks().size());
	}
}
```

`generate()`는 `DagSpec`을 만들면서 모든 검증을 먼저 하므로, 잘못된 워크플로는 아무것도 쓰기 전에 예외로 끝난다.
파일 생성과 Airflow 등록·실행은 별도 단계다. 생성 결과의 Python 문법은 다음과 같이 확인할 수 있다.

```bash
python3 -m py_compile generated_dags/operator_spec_demo.py
```

문법 검사만으로 mdtpy import, 참조 대상, 원격 연산 호출까지 검증되지는 않는다. 생성된 DAG는 `airflow.sdk`와
6개 mdtpy operator를 모두 import하므로, DAG 파싱 환경과 worker에 해당 모듈이 있어야 한다.

### 7.2 task 하나를 Spec으로 변환

```java
for ( TaskDescriptor task: model.getTaskDescriptors() ) {
	OperatorSpec spec = OperatorSpec.from(task);		// type에 맞는 하위 Spec을 고른다
	System.out.println(spec.getTaskId());
	System.out.print(spec.getOperatorCall());			// "AASOperationOperator(\n      instance = ...\n"
	spec.getInputs().forEach(arg -> System.out.println(arg.getPythonStatement()));
}
```

`OperatorSpec.from()`은 task 하나의 type·option·인자만 검증한다. 워크플로 전체의 중복 task id와 dependency 존재
여부는 `DagSpec`이 검사하므로, Spec 하나를 만들었다고 워크플로 전체 검증이 끝난 것은 아니다.

### 7.3 builder로 Spec 직접 만들기

모든 하위 Spec은 `builder()`를 제공하고, 공통 메소드(`taskId`, `inputs`/`addInput`, `outputs`/`addOutput`,
`dependencies`/`addDependency`)를 `OperatorSpec.AbstractBuilder`에서 물려받는다. 입력·출력·의존 관계의 기본값은
빈 컬렉션이다.

```java
AASOperationSpec spec = AASOperationSpec.builder()
							.taskId("add_and_sleep")
							.instanceId("test")
							.submodelId("AddAndSleep")
							.path("Operation")
							.addInput(new OperatorArgumentSpec.TaskOutputArgument("Data", "dec_by_5", "result"))
							.addInput(new OperatorArgumentSpec.ReferenceArgument("SleepTime", "param:test:SleepTime"))
							.addOutput(new OperatorArgumentSpec.SinkArgument("Output", "param:test:Data"))
							.addDependency("dec_by_5")
							.build();
```

| Spec | 고유 builder 메소드 | 필수 |
|---|---|---|
| `SetElementSpec` | 없음 | 없음 (실행에는 `source` 입력이 필요) |
| `AASOperationSpec` | `instanceId`, `submodelId`, `path` | 셋 다 |
| `OperationSubmodelSpec` | `instanceId`, `submodelId` | 둘 다 |
| `RESTfulAsyncRpcSpec` | `baseUrl`, `endpoint` | 둘 다 |
| `RESTfulRpcSpec` | `modelUrl`, `timeout(Duration)` | `modelUrl` (`timeout`이 `null`이면 제한 없음) |
| `PythonScriptSpec` | `script` | `script` (공백 불가) |

- `PythonScriptSpec`은 builder 대신 `PythonScriptSpec.of(taskId, script, inputs, outputs, dependencies)`로도 만들 수 있다.
- `AirflowDagGenerator.generate()`는 `WorkflowModel`만 받는다. builder로 만든 Spec 목록을 직접 넘겨 DAG를 만드는
  API는 없으므로, builder는 테스트나 Spec 단위 확인에 쓴다.
- `OperatorArgumentSpec.LiteralArgument`의 두 번째 인자는 값 자체가 아니라 이미 Python 리터럴로 변환된 표현식이며,
  이스케이프 없이 그대로 삽입된다. 워크플로 JSON의 literal 값을 쓸 때는 직접 만들지 말고 `OperatorSpec.from()`이
  하는 변환(`ElementValue.toJsonString()` → `JsonToPythonLiteralHybrid`)을 거치게 한다.

---

## 8. 검증 시점과 흔한 오류

오류는 네 단계 중 하나에서 드러난다. 앞 단계에서 잡힐수록 원인을 찾기 쉽다.

| 단계 | 언제 | 예 |
|---|---|---|
| **JSON 파싱** (mdt-client) | 워크플로 JSON을 읽을 때 | literal `@type` 오류(`Unregistered ElementValue type`), JSON 구조 오류 |
| **DAG 생성** (mdt-workflow) | 워크플로 등록 또는 `generate()` 호출 | 아래 표 |
| **DAG 파싱** (Airflow) | Airflow가 DAG 파일을 읽을 때 | mdtpy에 operator가 없어 생긴 `ImportError`, 순환 의존 |
| **task 실행** (Airflow worker) | task가 실행될 때 | operator 생성자 검증(`source`/`target` 이름), 연산 호출 실패, 선행 task 출력 누락, `RestfulRpcOperator`의 선언 출력 누락(`KeyError`), 잘못된 `op_endpoint` |

DAG 생성 단계의 오류 메시지:

| 메시지 | 원인 | 조치 |
|---|---|---|
| `unsupported task type: ...` | [§2](#2-작업에-맞는-spec-고르기) 표에 없는 type. 예를 들어 Spec/Operator 클래스명을 넣었거나, GUI의 AI 노드가 내보내는 `mdt.task.AITask` | type을 표의 문자열로 맞춘다(AI 모델은 `mdt.task.RESTfulRpcTask` + option `opUrl`) |
| `<option> option is required ...: <task id>` | 필수 option 누락 | [§5](#5-task-유형별-사용법)의 option 이름을 확인한다(대소문자 구분) |
| `invalid operation option value: ...` | `operation` 값의 세그먼트가 3개 미만 | `<instance>:<submodel>:<path>` |
| `invalid operationSubmodel option value: ...` | `operationSubmodel` 값이 정확히 2개 세그먼트가 아님 | `<instance>:<submodel>` |
| `script must not be blank` | 빈 script | script 본문을 채운다 |
| `invalid Task id: ...` / `argId should be a valid Python identifier` | task id나 인자 id가 Python 식별자가 아니거나 예약어 | [§4.1](#41-필드와-이름-규칙)의 식별자 규칙을 따른다 |
| `invalid DAG id: ...` | 워크플로 id가 비었거나, Python 식별자가 아니거나, `__`를 포함 | `__`는 DAG id와 run id의 구분자라 쓸 수 없다 |
| `DAG has no task` | task가 하나도 없음 | task를 추가한다 |
| `duplicate task id` / `unknown dependency task id` | task id 중복, 또는 `dependencies`가 없는 task를 가리킴 | id와 의존 관계의 오탈자를 확인한다 |

task 실행 단계에서 흔한 증상:

| 증상 | 확인할 설정 |
|---|---|
| `SetElementOperator`의 `source`/`target` 관련 `ValueError` | SetTask의 입력 이름이 `source`, 출력 이름이 `target`인지 확인한다 |
| 선행 task 출력을 찾지 못함 | `taskId`, `argumentName`의 오탈자, `dependencies` 누락, 선행 operator의 출력 전달 방식([§2](#2-작업에-맞는-spec-고르기))을 확인한다 |
| 모델 호출 후 출력 관련 `KeyError` | `RESTfulRpcSpec`에 선언한 출력 이름을 서버 응답과 맞춘다 |
| 설정한 `timeout`이나 `poll`이 반영되지 않음 | 해당 Spec이 실제로 읽는 option인지 확인한다([§2](#2-작업에-맞는-spec-고르기)) |
| RPC 호출 URL이 잘못됨 | `baseUrl` 끝의 `/`를 빼고 `endpoint`가 `/`로 시작하는지 확인한다 |

그 밖에 알아 둘 동작:

- **`DagSpec`은 순환 의존과 자기 자신에 대한 의존을 검사하지 않는다.** 이런 DAG는 Airflow 파싱 단계에서야 거부된다.
  task 사이의 연결은 순환 없이 작성한다.
- **DAG 파일은 워크플로를 등록할 때만 만들어진다.** mdt-workflow를 새 버전으로 바꿨다면 워크플로를 다시 등록해야
  새 형식의 DAG가 생성된다.
- **같은 id의 DAG 파일은 확인 없이 덮어쓴다.** 테스트용 워크플로에는 기존 DAG와 겹치지 않는 id를 쓴다.
- **새 DAG는 Airflow에서 일시정지 상태로 만들어진다.** `airflow dags unpause <dag_id>`로 풀어야 실행된다.
- 실행 순서, 참조 리소스, 연산 인자 이름은 실제 worker 환경에서도 확인한다. 실행하면 sink 대상의 실제 디지털 트윈
  데이터가 바뀐다.

---

## 관련 구현과 문서

- [OperatorSpec.java](../src/main/java/mdt/workflow/airflow/OperatorSpec.java) — type별 Spec 선택과 공통 인자 변환
- [OperatorArgumentSpec.java](../src/main/java/mdt/workflow/airflow/OperatorArgumentSpec.java) — 입력과 출력의 Python 표현식
- [DagSpec.java](../src/main/java/mdt/workflow/airflow/DagSpec.java) — 워크플로 수준 검증
- [airflow_dag.peb](../src/main/resources/templates/airflow_dag.peb) — 생성되는 Python 구조
- [adding-operator.md](adding-operator.md) — 새 task 유형/operator 추가 절차, 배포 순서, 통합 검증 방법
- mdtpy `src/mdtpy/airflow/operator/README.md` — 각 operator의 실행 시 동작, 출력 처리 계약
- `mdt-client/misc/test-workflows/test_workflow.json` — [§6](#6-전체-예-워크플로-json--dag)의 예제 워크플로

# mdt-workflow

MDT 워크플로우 관리 서버. 워크플로우 모델(task와 task 사이의 입출력 연결)을 등록·보관하고, Airflow를 실행 엔진으로
써서 모델을 DAG로 배포·실행·모니터링한다. Spring Boot 애플리케이션이며(`mdt.workflow.MDTWorkflowManagerApplication`),
REST API는 `/workflow-manager`(관리)와 `/list`(식별자 목록) 아래에 있다.

## 문서

- [Docker 이미지 만들기](docker/README.md) — JAR 빌드부터 이미지 빌드, compose 실행, 레지스트리 업로드, DAG 폴더 조건, 문제 해결까지.
- [OperatorSpec 설정과 실행 사용 설명서](docs/operator-spec-guide.md) — 지원 Spec 6종의 선택 기준,
  JSON 설정과 데이터 전달, Java DAG 생성 예제, 오류 확인 방법.
- [새 Airflow Operator 추가 가이드](docs/adding-operator.md) — 새 task 유형을 워크플로 모델에서 Airflow DAG의
  mdtpy Operator 호출까지 연결할 때의 작업 목록과 순서 (mdtpy, mdt-client, mdt-workflow, mdt-gui).
  참고 구현은 `PythonScriptOperator`(`PythonScriptSpec`).
- [DB 마이그레이션](docs/migration/) — 배포된 DB를 새 버전에 맞추는 SQL. 파일 이름의 날짜 순으로, 그 버전을 시작하기 전에 실행한다.

## 동작 개요

1. **모델 등록** (`POST /workflow-manager/models`): 모델을 DB(`workflow_models`)에 저장하고 Airflow DAG 파일
   `<dagsFolder>/<모델 id>.py`를 만든다. DAG 생성이 실패하면(잘못된 id, task가 없음, 지원하지 않는 task 유형 등)
   저장을 되돌리고 400으로 응답한다. 모델 id는 Python 식별자여야 하고 `__`를 포함할 수 없다.
2. **실행** (`POST /workflow-manager/models/{id}/start`): DAG를 활성화하고 DagRun을 만든다. 워크플로우 인스턴스 id는
   `<모델 id>__<DagRun id>`이다. 모델을 막 등록했다면 Airflow가 DAG 파일을 아직 읽지 않아 404가 날 수 있다.
   `?runId=<실행 id>`(영문자·숫자·`.`·`_`·`-`, 1~200자, 형식이 틀리면 400)를 주면 그 값이 DagRun id가 된다. 같은 모델에
   같은 `runId`의 실행이 이미 있으면 새로 만들지 않고 그 실행을 돌려준다. 그래서 응답을 받지 못한 시작 요청을 같은
   `runId`로 다시 보내도 실행은 하나만 생긴다. `runId`가 없으면 서버가 시작 시각과 무작위 접미어로 만든다.
3. **조회·중지·삭제** (`/workflow-manager/workflows/...`): Airflow REST API v2로 DagRun과 TaskInstance를 읽어
   워크플로우 상태로 바꾼다. 태그 `mdt`가 붙은 DAG(이 서버가 만든 DAG)만 다룬다.
   Airflow에 대응 기능이 없는 일시정지·재개 API는 없다(2026-11-05에 `PUT .../suspend`·`/resume`을 지웠다).
   로그 조회(`GET /workflow-manager/workflows/{wfId}/log/{taskId}`)는 경로의 마지막 값(이전 이름 `podName`)을 task id로
   해석해, 그 task의 최신 시도 로그를 한 줄에 한 항목(`<시각> <레벨> <logger>: <메시지>`)으로 돌려준다. task가 없거나
   아직 실행되지 않았으면 404다.
4. **모델 삭제** (`DELETE /workflow-manager/models/{id}`): DB 행, DAG 파일, Airflow DAG를 지운다.

오류 응답: 대상 없음 404, 중복 409, 잘못된 요청 400, 지원하지 않는 기능 501, Airflow·예측 서비스 오류 502, 그 밖 500.

## 설정

`config/application.properties`(Spring 설정)와 `config/env.file`(변수 정의)을 쓴다.

| 키 | 필수 | 설명 |
|---|---|---|
| `mdt.workflow-manager.airflow.airflowBaseUrl` | 예 | Airflow 주소(예: `http://localhost:8080`). REST API는 `<주소>/api/v2`를 쓴다. |
| `mdt.workflow-manager.airflow.username` / `password` | 예 | Airflow REST API 계정(JWT 발급). 없으면 시작할 때 실패한다. |
| `mdt.workflow-manager.airflow.dagsFolder` | 예 | Airflow가 읽는 DAG 폴더. 이 서버가 DAG 파일을 쓴다. |
| `mdt.workflow-manager.airflow.mdtUrl` | 예 | MDT 서버 주소. Airflow 변수 `mdt_manager_url`(`<주소>/instance-manager`)로 등록되어 DAG의 mdtpy operator가 쓴다. |
| `mdt.workflow-manager.airflow.insecureTls` | 아니오 | `true`면 Airflow 서버 인증서를 검증하지 않는다(기본 `false`). 시험 환경에서만 쓴다. |
| `mdt.workflow-manager.executionTimeEstimatorEndpoint` | 아니오 | 실행시간 예측 서비스 주소. 없으면 `/workflow-manager/execution-times/...`가 501이다. |

**env 파일**: 경로는 시스템 속성 `env.file`, 환경 변수 `ENV_FILE`, 기본값 `config/env.file` 순으로 정한다. 정의한
변수는 설정 파일에서 `${이름}`으로 쓸 수 있고, 값에 `${env:이름:-기본값}`으로 OS 환경 변수를 넣을 수 있다. 비밀번호는
설정 파일에 직접 쓰지 말고 env 파일의 변수(예: `AIRFLOW_PASSWORD=${env:AIRFLOW_PASSWORD:-airflow}`)로 넘긴다. 시작 로그에는
변수 이름과 값이 나오지만, 이름에 `PASS`/`SECRET`/`TOKEN`/`CREDENTIAL`이 든 값과 URL 안의 비밀번호는 가려진다.

## 빌드와 테스트

```bash
gradle bootJar                                     # build/libs/mdt-workflow-<버전>-all.jar
gradle test --rerun -x :utils:test -x :mdt-client:test
```

테스트는 실행 중인 Airflow나 DB 없이 돈다. Airflow 호출은 OkHttp MockWebServer로, JPA는 인메모리 H2로 확인한다.

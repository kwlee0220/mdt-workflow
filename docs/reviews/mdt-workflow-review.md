# mdt-workflow 코드 검토

- 검토 범위: `mdt-workflow/src` 전체(Java 28개 + 테스트 8개, 약 4,200줄), `src/main/resources/templates/airflow_dag.peb`, `README.md`·`docs/`
- 검토일: 2026-11-04. 검토 시점 테스트: 47개 모두 통과.
- 상태: **대기**(미처리) · **진행** · **완료** · **보류**(나중에) · **기각**(하지 않음). 항목을 처리하면 요약 표의 상태와 항목의 "결과" 줄을 함께 고친다.
- 분류: **결함**(동작이 틀림·위험) / **개선**(중복·죽은 코드·문서) / **인터페이스**(API 모양) / **이름**
- 링크의 줄 번호는 검토 시점 기준이다.

## 진행 현황

| 분류 | 항목 | 완료 | 보류·기각 | 남음 |
|---|---|---|---|---|
| 결함 (D) | 20 | 20 | 0 | 0 |
| 개선 (I) | 10 | 10 | 0 | 0 |
| 인터페이스 (A) | 6 | 4 | 0 | 2 |
| 이름 (N) | 10 | 10 | 0 | 0 |
| 합계 | 46 | 44 | 0 | 2 |

## 항목 요약

| # | 분류 | 심각도 | 제목 | 상태 |
|---|---|---|---|---|
| D1 | 결함 | 상 | Airflow 계정이 `airflow`/`airflow`로 하드코딩되고 로그인 요청 JSON을 문자열로 조립 | 완료 |
| D2 | 결함 | 상 | JWT를 한 번만 받고 갱신하지 않음 — 만료 뒤 모든 Airflow 호출 실패 | 완료 |
| D3 | 결함 | 상 | `getRestfulClient()` 초기화: 실패하면 인증 없는 클라이언트가 남고, 동기화가 없음 | 완료 |
| D4 | 결함 | 상 | `removeWorkflowAll()`이 `mdt` 태그로 거르지 않아 MDT가 아닌 DAG의 실행 기록까지 삭제 | 완료 |
| D5 | 결함 | 중 | Airflow 목록 API의 페이지를 처리하지 않아 일부만 조회·삭제 | 완료 |
| D6 | 결함 | 중 | `AirflowWorkflowManager.toString()`이 항상 `MissingFormatArgumentException` | 완료 |
| D7 | 결함 | 중 | Airflow 오류(없는 DAG·run 등)가 404가 아니라 500으로 나감, 미지원 기능도 500 | 완료 |
| D8 | 결함 | 중 | `removeWorkflowModel`이 없는 모델에도 성공(계약은 `ResourceNotFoundException`) | 완료 |
| D9 | 결함 | 중 | 모델 삭제 경로의 DAG 정리: 서비스의 `removeWorkflowModelAll`은 DAG 파일을 안 지우고, 개별 삭제는 정리 실패를 조용히 무시 | 완료 |
| D10 | 결함 | 중 | env 파일의 변수 값을 그대로 로그에 출력(비밀번호 노출) | 완료 |
| D11 | 결함 | 중 | DAG 파일 쓰기가 원자적이지 않고, 삭제는 API → 파일 순서라 DAG가 다시 나타날 수 있음 | 완료 |
| D12 | 결함 | 중 | Airflow 상태 매핑 누락·오분류, `queued_at`이 null이면 예외, `System.out` | 완료 |
| D13 | 결함 | 중 | `startWorkflow`: run id 충돌 가능, DAG id 미인코딩, 새로 등록한 DAG는 아직 파싱 전 | 완료 |
| D14 | 결함 | 하 | `stopWorkflow`: queued run은 중지 못 함, 확인-갱신 경합, task PATCH 본문 형식 확인 필요 | 완료 |
| D15 | 결함 | 하 | 실행시간 예측 API: 요청마다 클라이언트 생성, `System.out`, 설정·응답 오류 처리 없음 | 완료 |
| D16 | 결함 | 하 | `/list/workflows/{type}`: 모르는 type이면 NPE(500), 워크플로우마다 상태 조회(N+1) | 완료 |
| D17 | 결함 | 하 | `JsonToPythonLiteralHybrid`가 `Infinity`/`NaN`을 잘못된 Python 코드로 출력 | 완료 |
| D18 | 결함 | 하 | Airflow 연결이 TLS 인증서 검증을 끈 trust-all 클라이언트 | 완료 |
| D19 | 결함 | 하 | `onWorkflowModelRemoved`가 검증하지 않은 모델 id로 파일 경로를 만듦 | 완료 |
| D20 | 결함 | 하 | `timeout`·`poll` 옵션이 Airflow 실행에서 조용히 무시됨(Java 태스크와 동작이 다름) | 완료 |
| I1 | 개선 | 중 | 6개 Spec의 `from()`이 입력·출력·의존·trigger rule 변환을 반복 | 완료 |
| I2 | 개선 | 중 | 옵션 키를 문자열로 하드코딩(mdt-client 상수 미사용) | 완료 |
| I3 | 개선 | 중 | `OperatorSpec.fromTaskType`의 if-else 연쇄 → 등록표 | 완료 |
| I4 | 개선 | 중 | 운영 소스의 데모 `main()`과 개발자 로컬 경로 | 완료 |
| I5 | 개선 | 하 | 쓰지 않는 코드·중복 선언 정리 | 완료 |
| I6 | 개선 | 중 | dagRun 조회 중복(run당 GET 2회) 및 상태·시각 변환 코드 중복 | 완료 |
| I7 | 개선 | 하 | `OperatorSpec`이 builder의 가변 컬렉션을 그대로 공유·노출 | 완료 |
| I8 | 개선 | 중 | 문서: README의 없는 링크, 대부분 클래스의 빈 Javadoc, 패키지 CLAUDE.md 없음 | 완료 |
| I9 | 개선 | 하 | `JpaWorkflowModel`: PostgreSQL 전용 `bytea`, JSON을 바이트로 저장, `id`/`rowId` 이름 | 완료 |
| I10 | 개선 | 하 | `AirflowWorkflowManager`가 `JpaWorkflowModelManager` 구체 타입에 의존 | 완료 |
| A1 | 인터페이스 | 중 | REST 동작 메소드의 HTTP 동사 불일치(start는 POST, stop/suspend/resume은 PUT) | 대기 |
| A2 | 인터페이스 | 중 | Airflow가 지원하지 않는 suspend/resume/log(podName)를 그대로 노출 | 완료 |
| A3 | 인터페이스 | 중 | `WorkflowInstanceManagerProvider`가 인스턴스 관리 + 모델 이벤트 + 스크립트 생성을 한데 묶음 | 완료 |
| A4 | 인터페이스 | 중 | `POST /models?updateIfExists=`의 boolean 플래그, 실패를 `RuntimeException`으로 감싸 400/409 구분 소실 | 대기 |
| A5 | 인터페이스 | 하 | `DELETE /workflows?modelFilter=`: 이름과 서버 측 전체 조회 | 완료 |
| A6 | 인터페이스 | 하 | `/list/workflows/{type}`: 상태 필터를 경로 변수로 받음, `text/plain` 줄 목록 | 완료 |
| N1 | 이름 | 중 | `OperatorArgumentSpec`의 중첩 클래스가 mdt-client `ArgumentSpec`과 이름이 같음 | 완료 |
| N2 | 이름 | 하 | `getPythonStatement()`는 문장이 아니라 dict 항목 | 완료 |
| N3 | 이름 | 중 | `AASOperationSpec`/`OperationSubmodelSpec`의 `submodelId`는 실제로 idShort | 완료 |
| N4 | 이름 | 하 | `JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid` | 완료 |
| N5 | 이름 | 하 | `AirflowWorkflowManager` / `WorkflowInstanceManagerProvider` | 완료 |
| N6 | 이름 | 하 | `getWorkflowScript` | 완료 |
| N7 | 이름 | 하 | `PythonIdentifierUtil.isValidPythonIdentifier` | 완료 |
| N8 | 이름 | 하 | `StartWorkflowRequest.getDagRunIdJackson`, `m_runIdEncoded` | 완료 |
| N9 | 이름 | 하 | `TaskOutputArgument.m_outputArgumentName`(생성 쪽은 `outputArgumentId`) | 완료 |
| N10 | 이름 | 하 | 컨트롤러 메소드 `log(...)` | 완료 |

---

## 1. 결함

### D1. Airflow 계정 하드코딩과 로그인 요청 조립 — 상
- 위치: [AirflowWorkflowManager.java:369-375](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L369), [:472](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L472)
- 문제:
  - `getJwtToken(m_restfulClient, "airflow", "airflow")`로 계정이 코드에 박혀 있다. 운영 Airflow의 계정을 바꾸면 연동이 끊기고, 기본 계정을 쓰도록 강제한다.
  - 요청 본문을 `String.format("{\"username\": \"%s\", \"password\": \"%s\"}", …)`로 만든다. 비밀번호에 `"`나 `\`가 있으면 JSON이 깨진다.
- 제안: `AirflowWorkflowManagerConfiguration`에 `username`/`password`를 추가하고(env 파일·환경 변수로 주입), 본문은 `ObjectNode`/`Map`을 Jackson으로 직렬화한다.
- 결과: 완료(2026-11-04).
  - `AirflowWorkflowManagerConfiguration`에 `username`/`password`(`mdt.workflow-manager.airflow.username/password`)를 추가했다. 필수 설정이며(사용자 결정), 없으면 `afterPropertiesSet()`이 Airflow에 접속하기 전에 설정 키를 담은 `IllegalArgumentException`으로 실패한다.
  - 로그인 본문은 `Map.of(...)`를 `MDTModelSerDe.MAPPER`로 직렬화한다(따옴표·백슬래시가 든 비밀번호도 안전).
  - 배포 설정 두 곳(`~/mdt/mdt-workflow/config`, `~/mdt/docker-compose/mdt-workflow-data/config`)의 `application.properties`에 `username=${AIRFLOW_USERNAME}`, `password=${AIRFLOW_PASSWORD}`를, `env.file`에 `AIRFLOW_USERNAME=${env:AIRFLOW_USERNAME:-airflow}`, `AIRFLOW_PASSWORD=${env:AIRFLOW_PASSWORD:-airflow}`를 추가했다. 기본값이 이전 고정값과 같아 지금 배포는 그대로 동작하고, 컨테이너·셸 환경 변수로 바꿀 수 있다.
  - 주의: D10이 처리되기 전까지는 env 파일의 `AIRFLOW_PASSWORD` 값이 시작 로그에 출력된다.
  - 테스트: `AirflowWorkflowManagerConfigTest`(2개). mdt-workflow 테스트 49개 통과.

### D2. JWT를 갱신하지 않음 — 상
- 위치: [AirflowWorkflowManager.java:461-503](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L461)
- 문제: 토큰은 첫 호출 때 한 번 받아 `Authorization` 헤더에 고정한다. Airflow의 JWT는 만료 시간이 있으므로, 만료 뒤에는 서버를 재시작할 때까지 모든 호출이 401로 실패한다.
- 제안: 401을 받으면 한 번 재로그인하고 다시 시도한다. 또는 토큰의 만료 시각(`exp`)을 기억해 만료 전에 갱신한다. 헤더를 고정하지 말고 OkHttp `Authenticator`/`Interceptor`로 붙이면 갱신이 쉽다.
- 결과: 완료(2026-11-04).
  - 토큰을 클라이언트 헤더에 고정하지 않는다. OkHttp `Interceptor`(`addBearerToken`)가 요청마다 현재 토큰(`volatile m_jwtToken`)을 붙이고, `Authenticator`(`reauthenticate`)가 401을 받으면 토큰을 다시 받아 그 요청을 한 번 재시도한다. 재시도도 401이면 더 시도하지 않고 401을 그대로 전달한다(계정 오류 등).
  - 토큰 발급은 인증 처리를 붙이지 않은 별도 클라이언트(`m_loginClient`)로 한다. `refreshJwtToken(staleToken)`은 `synchronized`이고, 401을 받은 요청의 토큰이 아직 현재 토큰일 때만 새로 받으므로 동시에 여러 요청이 401을 받아도 한 번만 발급한다.
  - 만료 시각(`exp`) 기반 선제 갱신은 하지 않았다(401 재시도로 충분).
  - 남은 것(D3): 첫 토큰 발급이 실패하면 `m_restfulClient`가 대입되지 않아 다음 호출에서 다시 초기화하지만, 토큰 발급 뒤 `mdt_manager_url` 변수 등록이 실패하면 클라이언트가 등록되지 않은 채 남는다. 초기화 동기화도 아직 없다.
  - 테스트: `AirflowWorkflowManagerJwtTest`(MockWebServer, 2개) — 만료 토큰 → 재발급 → 재시도와 이후 요청의 새 토큰, 재발급 뒤에도 401이면 재시도하지 않음, 로그인 본문의 비밀번호 이스케이프(D1). `build.gradle`에 `testImplementation libs.okhttp.mockwebserver` 추가. mdt-workflow 테스트 51개 통과.

### D3. `getRestfulClient()` 초기화 결함 — 상
- 위치: [AirflowWorkflowManager.java:461-503](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L461)
- 문제:
  - 인증 없는 클라이언트를 먼저 `m_restfulClient` 필드에 넣은 뒤 토큰을 받는다(L466 → L472). 토큰 발급이나 이어지는 변수 등록(L482-491)이 실패하면 필드에는 **인증 없는 클라이언트가 남고**, 다음 호출부터는 `m_restfulClient != null`이라 다시 초기화하지 않는다. 그 뒤로는 모든 호출이 401이 된다.
  - 동기화가 없다. Spring 싱글턴이라 첫 요청이 동시에 들어오면 로그인과 변수 삭제·등록이 여러 번 겹쳐 실행된다.
  - `afterPropertiesSet()`은 초기화 실패를 로그만 남기고 넘어간다(L92-98). 실패 원인은 첫 API 호출 때 다른 예외로 다시 나타난다.
- 제안: 지역 변수로 클라이언트를 만들고 모든 단계가 성공했을 때만 필드에 대입한다. 초기화는 `synchronized`(또는 `Lazy`)로 한 번만 한다. D2와 함께 처리한다.
- 결과: 완료(2026-11-04).
  - 초기화를 `connect()`로 분리했다. 토큰 발급 → 인증 클라이언트 생성 → `mdt_manager_url` 변수 삭제·재등록을 모두 지역 변수로 진행하고, 성공했을 때만 `getRestfulClient()`가 `volatile m_restfulClient`에 대입한다. 실패하면 예외를 던지고 다음 호출에서 처음부터 다시 초기화한다.
  - `getRestfulClient()`는 더블 체크 잠금(`synchronized (this)`)으로 초기화를 한 번만 실행한다. `refreshJwtToken`(D2)도 같은 모니터를 쓴다.
  - 변수 등록 본문도 문자열 템플릿(`ADD_VARIABLE_BODY` + `StrSubstitutor`) 대신 `Map`을 Jackson으로 직렬화한다. 상수와 `StrSubstitutor` import를 지웠다.
  - `afterPropertiesSet()`은 지금처럼 초기화 실패를 로그만 남기고 넘어간다(Airflow가 늦게 떠도 서버는 시작되고, 첫 호출 때 다시 시도한다).
  - 테스트: `AirflowWorkflowManagerInitTest` — 시작 때 변수 등록이 실패하면 다음 호출이 토큰부터 다시 받고 새 토큰으로 변수를 등록·요청하며, 그 뒤에는 다시 초기화하지 않는다. D2 테스트가 `Map.of`의 순서에 기대어 로그인 본문을 문자열로 비교하던 것을 필드 비교로 고쳤다(순서는 JVM 실행마다 달라 불안정했다). mdt-workflow 테스트 52개 통과(3회 반복).

### D4. `removeWorkflowAll()`이 MDT가 아닌 DAG의 실행 기록도 삭제 — 상
- 위치: [AirflowWorkflowManager.java:168-185](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L168)
- 문제: `listWorkflowIds()`·`getWorkflowAll()`은 `mdt` 태그가 붙은 DAG만 다루지만(L107, L139), `removeWorkflowAll()`은 태그를 거르지 않는다. `DELETE /workflow-manager/workflows`를 부르면 같은 Airflow에 있는 다른 DAG의 실행 기록까지 지운다.
- 제안: 같은 `existsTag(dagNode, "mdt")` 필터를 적용한다. 태그 이름 `"mdt"`는 템플릿(`tags=["mdt"]`)과 공유하는 상수로 둔다.
- 결과: 완료(2026-11-04). `removeWorkflowAll()`이 `listMdtDagIds()`(서버에서 `tags=mdt`로 거르고 응답도 `existsTag`로 다시 확인)를 쓴다. 태그 이름은 상수 `MDT_DAG_TAG`로 두었다(템플릿의 `tags=["mdt"]`와 같아야 한다는 주석 포함). `listWorkflowIds()`·`getWorkflowAll()`도 같은 메소드를 쓴다. 테스트: `AirflowWorkflowManagerApiTest.removeAllDeletesOnlyMdtRunsAcrossPages`.

### D5. Airflow 목록 API의 페이지 처리 없음 — 중
- 위치: [AirflowWorkflowManager.java:101-143](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L101), [:306-326](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L306), [:405-411](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L405)
- 문제: `/dags`, `/dags/{id}/dagRuns`, `/taskInstances`는 페이지 단위(`limit`/`offset`, 응답의 `total_entries`)로 결과를 준다. 지금은 첫 페이지만 읽으므로, DAG나 실행 기록이 기본 limit(Airflow 설정값, 보통 수십~100개)을 넘으면 목록·상태 조회·전체 삭제가 일부만 처리된다.
- 제안: `total_entries`까지 `offset`을 늘려 가며 읽는 공통 메소드를 둔다. DAG 목록은 `tags=mdt` 질의 파라미터로 서버에서 거른다.
- 결과: 완료(2026-11-04). `listAll(url, field)`가 `limit=100&offset=n`을 붙여 `total_entries`에 이를 때까지(없으면 빈 페이지·짧은 페이지까지) 읽는다. 서버 최대 limit이 더 작아도 `total_entries`로 계속 읽는다. DAG 목록·DagRun 목록(두 곳)·TaskInstance 목록에 적용했다. 테스트: 위와 같음(한 페이지에 1개씩 주는 서버로 두 페이지 읽기 확인).

### D6. `toString()`이 항상 예외 — 중
- 위치: [AirflowWorkflowManager.java:300-304](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L300)
- 문제: 형식 문자열 `"%s[%s, dag-folder=%s]"`에 자리 표시자가 3개인데 인자는 2개다. 로그나 디버거에서 `toString()`을 부르면 `MissingFormatArgumentException`이 난다.
- 제안: `"%s[airflow=%s, dag-folder=%s]"`에 `m_airflowUrl`을 넣는다.
- 결과: 완료(2026-11-04). `"%s[airflow=%s, dag-folder=%s]"`에 `m_airflowUrl`을 넣었다. 테스트: `AirflowWorkflowManagerApiTest.unsupportedOperationsAndToString`.

### D7. 오류가 HTTP 상태로 옮겨지지 않음 — 중
- 위치: [AirflowWorkflowManager.java:114-246](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L114), [MDTWorkflowManagerController.java:399-422](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L399)
- 문제:
  - 인터페이스(`WorkflowInstanceManager`)는 없는 워크플로우에 `ResourceNotFoundException`을 던지도록 정의한다. 그런데 구현은 Airflow의 404 응답을 `RESTfulRemoteException`으로 그대로 올린다. 컨트롤러의 `@ExceptionHandler`는 이를 500으로 응답한다.
  - `suspendWorkflow`/`resumeWorkflow`/`getWorkflowLog`는 `RuntimeException`을 던져 500이 되고, 세 메시지가 모두 `"suspendWorkflow is not supported"`다(L235, L240, L245).
  - `addWorkflowModel`은 후처리 실패를 `RuntimeException`으로 감싼다([MDTWorkflowManager.java:54-59](../../src/main/java/mdt/workflow/service/MDTWorkflowManager.java#L54)). 그래서 잘못된 모델(DagSpec 검증 실패, IAE)이 400이 아니라 500이 된다.
  - 500 응답을 만들 때 로그를 남기지 않는다.
- 제안: Airflow 404는 `ResourceNotFoundException`으로 바꾼다. 미지원 기능은 `UnsupportedOperationException`을 던지고 501로 매핑한다. 후처리 실패는 원래 예외 유형을 유지한다(IAE는 그대로 던진다). 500은 서버 로그에 남긴다.
- 결과: 완료(2026-11-04).
  - `getDagRunNode`·`removeWorkflow`·`startWorkflow`(DAG 조회)가 Airflow 404(`RESTfulException.getHttpStatusCode() == 404`)를 `ResourceNotFoundException`으로 바꾼다(`toNotFoundIf404`). `getWorkflow`·`getWorkflowStatus`·`stopWorkflow`도 `getDagRunNode`를 거친다.
  - suspend/resume/log는 `UnsupportedOperationException`(메시지도 각 메소드 이름)을 던지고, 컨트롤러가 501로 응답한다. 500 응답은 서버 로그(ERROR)에 남긴다.
  - `MDTWorkflowManager`의 후처리 실패는 원래 예외 유형을 그대로 던진다(`toPostProcessFailure`, checked 예외만 감싼다). 잘못된 모델(IAE)은 400이 된다.
  - 테스트: `AirflowWorkflowManagerApiTest.missingDagRunIsResourceNotFound`, `MDTWorkflowManagerControllerTest`, `MDTWorkflowManagerTest.postProcessingFailureKeepsExceptionType`.

### D8. `removeWorkflowModel`이 없는 모델에도 성공 — 중
- 위치: [JpaWorkflowModelManager.java:73-75](../../src/main/java/mdt/workflow/service/JpaWorkflowModelManager.java#L73), [JpaWorkflowModelRepository.java:40-43](../../src/main/java/mdt/workflow/repository/JpaWorkflowModelRepository.java#L40)
- 문제: `deleteByModelId`는 지운 행 수를 돌려주지 않는다. 그래서 없는 id를 지워도 예외 없이 끝나고, `DELETE /models/{id}`는 204를 응답한다. 인터페이스와 Swagger 문서는 404를 약속한다.
- 제안: `@Modifying` 쿼리의 반환형을 `int`로 바꾸고 0이면 `ResourceNotFoundException`을 던진다.
- 결과: 완료(2026-11-04). `deleteByModelId`가 지운 행 수(`int`)를 돌려주고, `JpaWorkflowModelManager.removeWorkflowModel`은 0이면 `ResourceNotFoundException`을 던진다(컨트롤러 404). 테스트: `JpaWorkflowModelManagerTest`.

### D9. 모델 삭제 경로의 DAG 정리 — 중
- 위치: [MDTWorkflowManager.java:103-114](../../src/main/java/mdt/workflow/service/MDTWorkflowManager.java#L103), [JpaWorkflowModelManager.java:77-79](../../src/main/java/mdt/workflow/service/JpaWorkflowModelManager.java#L77), [MDTWorkflowManagerController.java:163-174](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L163)
- 문제:
  - 서비스의 `removeWorkflowModelAll()`은 DB 행만 지우고 `onWorkflowModelRemoved`를 부르지 않는다. 그래서 DAG 파일과 Airflow DAG가 남는다. 이 메소드는 `Try.run(m_repo::deleteAll)`로 오류도 삼킨다. 컨트롤러는 이 메소드를 쓰지 않고 모델별로 지우므로 지금은 드러나지 않지만, 같은 이름의 API가 서로 다르게 동작한다.
  - 개별 삭제는 `Unchecked.acceptOrIgnore(..., onWorkflowModelRemoved)`로 DAG 정리 실패를 로그도 없이 무시한다(L108). 그러면 DB에는 없는데 Airflow에는 DAG가 남는다.
- 제안: 서비스 `removeWorkflowModelAll()`이 모델마다 `removeWorkflowModel`을 부르게 하고, 컨트롤러는 그것을 호출한다. 정리 실패는 최소한 WARN 로그로 남긴다.
- 결과: 완료(2026-11-04). `MDTWorkflowManager.removeWorkflowModelAll()`이 모델마다 `removeWorkflowModel`을 부르고(한 모델이 실패해도 계속, WARN 로그), 컨트롤러의 `DELETE /models`는 이 메소드를 부른다. 개별 삭제의 엔진 쪽 정리 실패는 무시하지 않고 WARN 로그로 남긴다(모델 삭제는 되돌리지 않음). `JpaWorkflowModelManager.removeWorkflowModelAll()`(DB만 지움)은 그대로 두었다. 테스트: `MDTWorkflowManagerTest.removeAllCleansUpEveryModel`.

### D10. env 파일 변수 값을 로그에 출력 — 중
- 위치: [EnvironmentVariablePostProcessor.java:41-56](../../src/main/java/mdt/workflow/EnvironmentVariablePostProcessor.java#L41)
- 문제:
  - 읽은 모든 변수를 `EnvVar: KEY=VALUE` 형식으로 INFO 로그에 남긴다. env 파일에는 DB·Airflow 비밀번호가 들어가므로 로그에 그대로 노출된다(D1을 설정으로 옮기면 Airflow 비밀번호도 여기 들어간다).
  - 기본 경로(`config/env.file`)에 파일이 없어도 매번 스택 트레이스가 붙은 ERROR 로그를 남긴다.
- 제안: 값은 출력하지 않고 키만 출력하거나, `PASSWORD`/`SECRET`/`TOKEN`이 들어간 키의 값은 가린다. 기본 경로에 파일이 없으면 INFO 한 줄로 낮춘다(명시한 경로가 없을 때만 ERROR).
- 결과: 완료(2026-11-04). 로그에 키 이름에 `PASS`/`SECRET`/`TOKEN`/`CREDENTIAL`이 들어간 값은 `****`로, 그 밖의 값은 `JdbcUtils.maskPassword`로 URL 안의 비밀번호를 가려 출력한다(`maskValue`). 경로를 명시하지 않았고 기본 경로(`config/env.file`)에 파일이 없으면 INFO 한 줄만 남긴다. 클래스 Javadoc을 썼다. 테스트: `EnvironmentVariablePostProcessorTest`. D1에서 추가한 `AIRFLOW_PASSWORD`도 이제 가려진다. mdt-workflow 테스트 60개 통과.

### D11. DAG 파일의 쓰기·삭제 순서 — 중
- 위치: [AirflowWorkflowManager.java:248-282](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L248)
- 문제:
  - DAG 파일을 제자리에 바로 쓴다(`IOUtils.toFile`). Airflow DAG processor는 폴더를 계속 훑으므로, 쓰는 중인 파일을 읽어 일시적인 import 오류를 기록할 수 있다.
  - 삭제는 Airflow API로 DAG를 먼저 지우고(L271) 그다음 파일을 지운다. 그 사이에 processor가 파일을 다시 읽으면 DAG가 다시 등록된다. API 삭제 실패도 무시한다.
- 제안: 같은 폴더의 임시 파일에 쓴 뒤 `Files.move(..., ATOMIC_MOVE)`로 바꾼다. 삭제는 파일 → API 순서로 하고, API 실패는 로그로 남긴다.
- 결과: 완료(2026-11-04). DAG 파일을 같은 폴더의 `.<id>.py.tmp`(`.py`로 끝나지 않아 Airflow가 읽지 않음)에 쓴 뒤 `Files.move(REPLACE_EXISTING, ATOMIC_MOVE)`로 바꾼다(실패하면 임시 파일을 지운다). 삭제는 파일 → Airflow API 순서이고, API 실패는 404(Airflow가 아직 읽지 않은 DAG)를 빼고 WARN 로그로 남긴다. 테스트: `AirflowWorkflowManagerApiTest.dagFileIsWrittenAtomicallyAndRemovedBeforeApiDelete`(API 삭제가 도착했을 때 파일이 이미 없는지 확인).

### D12. Airflow 상태 매핑과 시각 파싱 — 중
- 위치: [AirflowWorkflowManager.java:121-128](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L121), [:383-397](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L383), [:413-439](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L413)
- 문제:
  - task 상태 중 `upstream_failed`·`skipped`를 `NOT_STARTED`로 본다. 앞 task가 실패해 실행되지 않은 것인데 "아직 시작 안 함"으로 보인다. `up_for_retry`·`up_for_reschedule`·`deferred`·`restarting`·`removed`는 `UNKNOWN`이 되고, 그때마다 `System.out.println`으로 출력한다.
  - DagRun 상태 `"timeout"`은 Airflow의 DagRun 상태가 아니다. 같은 switch가 두 곳(L121, L383)에 복사되어 있다.
  - `queued_at`이 null이면(바로 실행된 run 등) `Instant.parse("null")`로 예외가 난다. 그러면 `getWorkflowAll()`에서는 그 run이 `mapOrIgnore`로 조용히 빠진다(L310).
  - task 상태가 null이면 `asText()`가 `"null"`을 돌려주는 점에 기대어 검사한다(L417). 필드 자체가 없으면 NPE다.
- 제안: 상태 매핑을 한 곳으로 모으고 `upstream_failed`→`FAILED`(또는 별도 `SKIPPED`), 재시도 계열→`RUNNING`으로 정한다. 시각은 `hasNonNull` 검사 후 파싱하는 helper로 통일하고, `System.out`은 logger로 바꾼다.
- 결과: 완료(2026-11-04). 실행 중인 Airflow의 OpenAPI 명세(API v2)로 상태 목록을 확인했다(DagRun: `queued`/`running`/`success`/`failed`, TaskInstance 12가지). `toDagRunStatus`·`toTaskStatus`로 매핑을 한 곳에 모았다: `upstream_failed`→`FAILED`, `restarting`·`up_for_retry`·`up_for_reschedule`·`deferred`→`RUNNING`, `skipped`는 대응 상태가 없어 `NOT_STARTED`(Javadoc에 명시), DagRun 상태가 아닌 `timeout`·`scheduled`는 지웠다. 모르는 상태는 `System.out` 대신 WARN 로그. 시각은 `parseTime`(null·없는 필드는 null)으로 통일해 `queued_at`이 null인 run도 목록에서 빠지지 않는다. 테스트: `airflowStatesMapToWorkflowStatus`, `startAndStopRequests`(`queued_at: null`).

### D13. `startWorkflow`의 run id와 DAG id — 중
- 위치: [AirflowWorkflowManager.java:198-215](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L198), [:347-367](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L347)
- 문제:
  - `dag_run_id`가 epoch 밀리초다. 같은 모델을 같은 밀리초에 두 번 시작하면 409로 실패한다. `logical_date`는 `Instant.now()`를 한 번 더 불러 run id와 다른 시각이 된다.
  - URL에 DAG id를 인코딩하지 않는다(L200, L307, L322). 다른 곳(`enableDag`, `AirflowWorkflowId.toUrl`)은 인코딩한다. 지금은 DagSpec이 Python 식별자만 허용해 문제가 드러나지 않을 뿐이다.
  - 모델을 등록한 직후에는 Airflow가 DAG 파일을 아직 파싱하지 않아 `enableDag`의 GET이 404가 난다. 이 404는 D7 때문에 500으로 나간다.
- 제안: run id는 `mdt__<ISO 시각>__<짧은 UUID>`처럼 충돌하지 않게 만들고, `logical_date`와 같은 시각을 쓴다. URL 조립은 `AirflowWorkflowId`처럼 한 helper로 모은다. DAG가 아직 없으면 "등록 직후라 아직 준비되지 않음"을 알리는 오류(409 또는 503)로 응답한다.
- 결과: 완료(2026-11-04). run id는 `mdt__<시작 시각>_<UUID 8자리>`이고 `logical_date`는 같은 시각이다. DAG URL은 `dagUrl()`(path segment 인코딩)로 한 곳에서 만든다(`startWorkflow`·`enableDag`·DagRun 목록·DAG 삭제). DAG가 없으면(등록 직후 Airflow가 아직 파싱하지 않은 경우 포함) `ResourceNotFoundException("Airflow DAG (not registered, or not parsed by Airflow yet)", id)`로 404를 응답한다(제안의 409/503 대신 404를 유지하고 메시지로 원인을 알린다). 테스트: `startAndStopRequests`.

### D14. `stopWorkflow` — 하
- 위치: [AirflowWorkflowManager.java:217-231](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L217), [:332-345](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L332)
- 문제:
  - 상태가 `running`일 때만 중지한다. `queued` run은 조용히 그대로 둔다.
  - 상태를 읽고 PATCH하기까지의 사이에 상태가 바뀔 수 있다.
  - task instance를 PATCH할 때 본문으로 `{"state": "failed"}`를 보낸다. Airflow 3 API는 task instance 상태 변경에 `new_state` 필드를 쓰는 것으로 알고 있다. **실제 Airflow 버전으로 동작 확인이 필요하다.**
- 제안: `queued`도 중지 대상에 넣는다. DagRun을 `failed`로 바꾸면 Airflow가 남은 task를 정리하는지 확인하고, 필요할 때만 task를 개별 PATCH한다.
- 결과: 완료(2026-11-04). Airflow OpenAPI 명세로 확인한 결과 task instance PATCH 본문 필드는 `new_state`이고 정의되지 않은 필드는 거부한다(`additionalProperties: false`). 그래서 이전의 `{"state": "failed"}`는 거부되고 있었다(결함 확인). `new_state`로 고쳤고, `running`뿐 아니라 `queued` run도 중지한다. task PATCH 실패(사이에 끝난 task 등)는 WARN 로그로 남기고 계속한다. task id도 path segment로 인코딩한다. 확인-갱신 사이의 경합은 그대로 두고 주석으로 남겼다(Airflow는 끝난 run도 바꿀 수 있어 결과는 '실패'로 남는다). 테스트: `startAndStopRequests`.

### D15. 실행시간 예측 API — 하
- 위치: [MDTWorkflowManagerController.java:196-228](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L196)
- 문제:
  - `System.out.println`으로 출력한다.
  - 요청마다 `HttpMDTManager.connect(...)`와 `HttpRESTfulClient.newDefaultClient()`를 새로 만든다.
  - `executionTimeEstimatorEndpoint`가 설정되지 않으면 NPE가 난다.
  - 응답을 `Double.valueOf`로 읽는데 숫자가 아니면 `NumberFormatException`(500)이 난다.
  - MDT URL을 Airflow 설정(`m_airflowConf.getMdtUrl()`)에서 읽는다.
  - 모델 관리와 무관한 기능이 워크플로우 컨트롤러에 들어 있다.
- 제안: 별도 서비스 빈(예: `ExecutionTimeEstimator`)으로 옮기고 클라이언트를 재사용한다. 설정이 없으면 501(또는 503)로 응답하고, 응답 형식 오류는 502로 응답한다. MDT URL은 `WorkflowManagerConfiguration`으로 옮긴다.
- 결과: 완료(2026-11-04). 새 서비스 빈 `ExecutionTimeEstimator`로 옮겼다. HTTP 클라이언트와 MDT 연결은 처음 쓸 때 한 번 만들어 재사용하고, `System.out`은 DEBUG 로그로 바꿨다. 주소가 없으면 `UnsupportedOperationException`(501), 응답이 숫자가 아니면 `RESTfulIOException`(502)이다. 컨트롤러는 하위 서비스 오류(`RESTfulException`, 404 제외)를 502로 응답한다. MDT URL은 아직 Airflow 설정(`mdt.workflow-manager.airflow.mdtUrl`)에서 읽는다(설정 키를 옮기면 배포 설정도 바꿔야 해서 그대로 두었다). 테스트: `ExecutionTimeEstimatorTest.unconfiguredEstimatorIsUnsupported`.

### D16. `/list/workflows/{type}` — 하
- 위치: [MDTWorkflowListCandidatesController.java:68-91](../../src/main/java/mdt/workflow/controller/MDTWorkflowListCandidatesController.java#L68)
- 문제: `type`이 `all`/`running`/`stopped`가 아니면 `wfIdList`가 null로 남아 `FStream.from(null)`에서 NPE(500)가 난다. `running`/`stopped`는 워크플로우 id마다 `getWorkflowStatus`를 부르고, 그 안에서 Airflow를 다시 호출한다(N+1).
- 제안: 모르는 값은 400으로 응답한다. 상태는 `getWorkflowAll()` 한 번으로 얻은 `Workflow.getStatus()`로 거른다. 경로 모양은 A6을 참고한다.
- 결과: 완료(2026-11-04). 모르는 type은 `ResponseStatusException(400)`. 상태별 조회의 N+1 호출은 그대로 두었다: `getWorkflowAll()`로 바꾸면 지금은 run마다 DagRun 재조회와 task 목록 조회를 해서 오히려 요청이 늘어난다. I6·A6에서 함께 정리한다. 테스트: `MDTWorkflowListCandidatesControllerTest`.

### D17. `Infinity`/`NaN` 리터럴 — 하
- 위치: [JsonToPythonLiteralHybrid.java:183-211](../../src/main/java/mdt/workflow/airflow/JsonToPythonLiteralHybrid.java#L183)
- 문제: JSON의 `1e400` 같은 범위 밖 값이나 `NaN`을 `numberValue().toString()`으로 출력하면 `Infinity`/`NaN`이 된다. Python에서는 정의되지 않은 이름이라 DAG import가 `NameError`로 실패한다.
- 제안: `float('inf')`/`float('nan')`으로 출력하거나 생성 시점에 거부한다(DagSpec의 다른 검증과 같이).
- 결과: 완료(2026-11-04). 유한하지 않은 실수는 `float('inf')`/`float('-inf')`/`float('nan')`으로 출력한다. 테스트: `AirflowSpecReviewTest.nonFiniteNumbersBecomeValidPython`.

### D18. trust-all TLS — 하
- 위치: [AirflowWorkflowManager.java:464](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L464)
- 문제: `OkHttpClientUtils.newTrustAllOkHttpClientBuilder()`를 쓰므로 Airflow 서버 인증서를 검증하지 않는다. D1의 계정·JWT가 중간자에게 노출될 수 있다.
- 제안: 기본은 검증하고, 사설 인증서 환경은 설정(`insecure-tls: true` 또는 trust store 경로)으로만 끈다.
- 결과: 완료(2026-11-04). `mdt.workflow-manager.airflow.insecureTls`(기본 `false`)를 추가했다. 기본은 `OkHttpClientUtils.newClient()`로 인증서를 검증하고, `true`일 때만 trust-all 클라이언트를 쓰며 시작 시 WARN 로그를 남긴다. 두 배포 설정은 `http://` 주소라 바꾸지 않았다. 테스트: `ExecutionTimeEstimatorTest.tlsVerificationIsOnByDefault`, 기존 MockWebServer 테스트(검증하는 기본 클라이언트로 동작).

### D19. 삭제 경로의 모델 id 검증 — 하
- 위치: [AirflowWorkflowManager.java:266-282](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L266)
- 문제: 생성할 때는 `DagSpec`이 모델 id가 Python 식별자인지 확인하지만, 삭제할 때는 URL 경로 변수의 id를 그대로 `new File(dagsFolder, id + ".py")`에 쓴다. Spring이 경로 변수에서 `/`를 막아 주므로 지금 위험은 작지만, 생성·삭제가 같은 규칙을 쓰지 않는다.
- 제안: `PythonIdentifierUtil.isValidPythonIdentifier`로 확인한 뒤 경로를 만든다. 결과 경로가 `dagsFolder` 안인지도 확인한다.
- 결과: 완료(2026-11-04). DAG 파일 경로를 `dagFile(id)`로 한 곳에서 만들고, id가 Python 식별자인지와 결과 경로가 DAG 폴더 바로 아래인지 확인한다(생성·삭제 공통). 테스트: `dagFileIsWrittenAtomicallyAndRemovedBeforeApiDelete`(`../x` 거부).

### D20. `timeout`·`poll` 옵션이 Airflow 실행에서 무시됨 — 하
- 위치: [AASOperationSpec.java](../../src/main/java/mdt/workflow/airflow/AASOperationSpec.java), [RESTfulAsyncRpcSpec.java](../../src/main/java/mdt/workflow/airflow/RESTfulAsyncRpcSpec.java), [OperationSubmodelSpec.java](../../src/main/java/mdt/workflow/airflow/OperationSubmodelSpec.java)
- 문제: mdt-client의 Java 태스크(`RESTfulAsyncRpcTask` 등)는 `MDTTask.OPTION_TIMEOUT`·`OPTION_POLL_INTERVAL`을 쓴다. 하지만 Airflow로 실행할 때는 `RESTfulRpcSpec`을 빼고 이 옵션을 버린다. 대응하는 mdtpy Operator(`RestfulAsyncRpcOperator`, `AASOperationOperator`, `OperationSubmodelOperator`)도 이 인자를 받지 않는다. 같은 모델이 실행 엔진에 따라 다르게 동작하는데, 이를 알리는 경고도 없다.
- 제안: mdtpy Operator에 `timeout`(필요하면 `poll_interval`)을 추가하고 Spec이 전달한다. 당장 지원하지 않는다면, 옵션이 있을 때 DAG 생성 로그에 경고를 남긴다. mdtpy와 함께 바꿔야 한다([adding-operator.md](../adding-operator.md)의 순서를 따른다).
- 결과: 완료(2026-11-04, 경고만). mdtpy operator를 바꾸지 않고, DAG를 만들 때 task에 `timeout`·`poll` 옵션이 있는데 그 spec이 전달하지 못하면 WARN 로그를 남긴다(`OperatorSpec.getIgnoredExecutionOptions`, spec별 `getSupportedExecutionOptions()`; 현재는 `RESTfulRpcSpec`의 `timeout`만 지원). 옵션을 실제로 전달하려면 mdtpy `RestfulAsyncRpcOperator`·`AASOperationOperator`·`OperationSubmodelOperator`에 인자를 추가한 뒤 spec이 넘기도록 바꿔야 한다(남은 일). 테스트: `AirflowSpecReviewTest.ignoredExecutionOptionsAreDetected`. mdt-workflow 테스트 68개 통과.

---

## 2. 개선

### I1. Spec `from()`의 반복 — 중
- 위치: `AASOperationSpec`·`SetElementSpec`·`RESTfulRpcSpec`·`RESTfulAsyncRpcSpec`·`OperationSubmodelSpec`·`PythonScriptSpec`의 `from(TaskDescriptor)`
- 문제: 여섯 곳 모두 다음 일을 같은 코드로 반복한다. 새 유형을 추가할 때 하나를 빠뜨리기 쉽다. trigger rule을 빠뜨리는 실수는 `OperatorSpec.from`이 사후에 검사하고 있다.
  - 입력 인자를 `fromArgumentSpec`으로 변환
  - 출력 인자를 `fromOutputVariable`로 변환
  - `taskId`·`dependencies`·`triggerRule` 설정
- 제안: `AbstractBuilder`에 `B fromTask(TaskDescriptor task)`(공통 다섯 항목을 채움)를 두고, 각 Spec의 `from()`은 고유 옵션만 처리한다. 그러면 `OperatorSpec.from`의 trigger rule 사후 검사도 필요 없어진다.
- 결과: 완료(2026-11-04). `OperatorSpec.AbstractBuilder.fromTask(task)`가 task id·입력·출력·의존 task·trigger rule을 채우고, 여섯 spec의 `from()`은 `builder().fromTask(task)` 뒤에 고유 option만 처리한다(각 3~15줄). `OperatorSpec.from`의 trigger rule 사후 검사는 남겼다(`fromTask`를 쓰지 않는 새 spec에 대한 안전장치). 쓰지 않게 된 import를 정리했다. 테스트: `AirflowSpecReviewTest.fromTaskCopiesCommonFields`, 기존 `TriggerRuleTest`·`OperatorCallTest` 통과.

### I2. 옵션 키 하드코딩 — 중
- 위치: [AASOperationSpec.java:54](../../src/main/java/mdt/workflow/airflow/AASOperationSpec.java#L54)(`"operation"`), [OperationSubmodelSpec.java:46](../../src/main/java/mdt/workflow/airflow/OperationSubmodelSpec.java#L46)(`"operationSubmodel"`), [RESTfulAsyncRpcSpec.java:46-52](../../src/main/java/mdt/workflow/airflow/RESTfulAsyncRpcSpec.java#L46)(`"baseUrl"`, `"endpoint"`)
- 문제: mdt-client에 같은 값의 상수(`TaskDescriptor.OPTION_OPERATION`, `OPTION_OPERATION_SUBMODEL`, `RESTfulAsyncRpcTask.OPTION_BASE_URL`, `OPTION_OPERATION_ENDPOINT`)가 있다. `RESTfulRpcSpec`만 상수(`RESTfulRpcTask.OPTION_URL`)를 쓴다.
- 제안: 모두 상수로 바꾼다. 옵션 값 읽기·필수 검사도 I1의 공통 helper(`requireOption(task, key)`)로 모은다.
- 결과: 완료(2026-11-04). option 키를 mdt-client 상수(`TaskDescriptor.OPTION_OPERATION`, `OPTION_OPERATION_SUBMODEL`, `RESTfulAsyncRpcTask.OPTION_BASE_URL`, `OPTION_OPERATION_ENDPOINT`)로 바꿨다. 필수 option은 `OperatorSpec.requireOption(task, key, specClass)`로 읽고, 메시지는 `'<key>' option is required for <Spec>: <taskId>`로 통일했다. 테스트: `AirflowSpecReviewTest.registryAndRequiredOptions`.

### I3. `fromTaskType`의 if-else 연쇄 — 중
- 위치: [OperatorSpec.java:140-163](../../src/main/java/mdt/workflow/airflow/OperatorSpec.java#L140)
- 문제: task type 문자열을 if-else로 비교한다. 새 유형을 넣을 때마다 이 메소드를 고쳐야 하고, 지원하는 유형 목록을 코드에서 바로 얻을 수 없다.
- 제안: `Map<String, Function<TaskDescriptor, OperatorSpec>>` 등록표로 바꾼다(`static` 초기화). 지원 유형 목록은 오류 메시지와 문서에 쓸 수 있다.
- 결과: 완료(2026-11-04). if-else 연쇄를 `FACTORIES` 등록표(`Map<String, Function<TaskDescriptor, ? extends OperatorSpec>>`)로 바꾸고 `getSupportedTaskTypes()`를 추가했다. 모르는 유형의 오류 메시지에 지원 목록을 붙였다(`unsupported task type: Foo (supported: [...])`). [adding-operator.md](../adding-operator.md) §4.1·§4.2를 새 방식(`fromTask`, `requireOption`, 등록표)으로 고쳤다. mdt-workflow 테스트 70개 통과.

### I4. 운영 소스의 데모 코드 — 중
- 위치: [AirflowDagGenerator.java:55-65](../../src/main/java/mdt/workflow/airflow/AirflowDagGenerator.java#L55)(`/home/kwlee/mdt/models/...` 경로), [JsonToPythonLiteralHybrid.java:232-243](../../src/main/java/mdt/workflow/airflow/JsonToPythonLiteralHybrid.java#L232), [SampleAirflowDagGenerator.java](../../src/main/java/mdt/workflow/airflow/SampleAirflowDagGenerator.java)(159줄 전체)
- 문제: 개발자 로컬 경로와 데모 `main()`이 bootJar에 들어간다.
- 제안: 테스트(`src/test`)로 옮기거나 지운다. `SampleAirflowDagGenerator`는 DAG 생성 예제로서 테스트 리소스 + 테스트 케이스로 바꾸면 회귀 검증도 된다.
- 결과: 완료(2026-11-05). `AirflowDagGenerator.main`(로컬 경로 하드코딩)과 `JsonToPythonLiteralHybrid.main`(데모)을 지웠고, `SampleAirflowDagGenerator`는 실행 가능한 예제로 `src/test/java`로 옮겼다(bootJar에 들어가지 않는다). 쓰지 않게 된 import를 정리했다.

### I5. 쓰지 않는 코드·중복 선언 — 하
- 위치와 내용:
  - [MDTWorkflowManagerController.java:61-70](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L61): `@Slf4j`와 직접 선언한 `s_logger`가 겹친다. `@RequiredArgsConstructor`를 붙였지만 필드는 `@Autowired`이고 `final`이 아니다.
  - [JpaWorkflowModelRepository.java:34-38](../../src/main/java/mdt/workflow/repository/JpaWorkflowModelRepository.java#L34): `findModelIdAll`, `existsByModelId`는 사용처가 0이다.
  - [JpaWorkflowModel.java:43-48](../../src/main/java/mdt/workflow/domain/JpaWorkflowModel.java#L43): `JpaWorkflowModel(String)` 생성자는 사용처가 0이다.
  - [AirflowWorkflowManager.java:351-356](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L351): 주석 처리된 코드와 쓰지 않는 지역 변수 `now`.
  - [MDTWorkflowManagerController.java:190](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L190): `getWorkflowScript`의 `throws JsonProcessingException`(던지지 않음).
  - `JpaWorkflowModelManager`의 구현 메소드에 `@Override`가 없고 탭·공백 들여쓰기가 섞여 있다.
- 제안: 지우거나 정리한다.
- 결과: 완료(2026-11-05). 두 컨트롤러를 생성자 주입(`final` 필드 + `@RequiredArgsConstructor`)으로 바꾸고 중복 `@Slf4j`를 지웠다. `findModelIdAll`, `JpaWorkflowModel(String)` 생성자, `getWorkflowScript`의 `throws JsonProcessingException`을 지웠다(`StartWorkflowRequest`의 주석·지역 변수는 D13에서 정리). `existsByModelId`는 A4에서 쓸 예정이라 남겼다. `JpaWorkflowModelManager`에 `@Override`를 붙이고 들여쓰기를 탭으로 통일했다.

### I6. dagRun 조회 중복과 변환 코드 중복 — 중
- 위치: [AirflowWorkflowManager.java:377-403](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L377)
- 문제: `getWorkflowFromDagRun(dagId, dagRunNode)`은 이미 받은 dagRun 노드에서 id만 꺼낸 뒤 같은 dagRun을 다시 GET한다. 그래서 `getWorkflowAll()`은 run 하나당 GET을 2번(dagRun + taskInstances) 하고, `getWorkflow()`·`startWorkflow()`도 같은 dagRun을 두 번 읽는다. 상태 switch·시각 파싱(`LocalDateTimes.fromInstant(Instant.parse(...))`)도 여러 곳에 복사되어 있다.
- 제안: 받은 노드를 그대로 쓰고(필요할 때만 다시 읽기), 상태·시각 변환을 private helper로 모은다(D12와 함께).
- 결과: 완료(2026-11-05). `getWorkflowFromDagRun`이 넘겨받은 DagRun 노드(`state`가 있으면)를 그대로 쓴다. 목록·조회·실행 응답이 모두 `DAGRunResponse`이므로, run 하나당 GET이 한 번 줄었다(`getWorkflow`는 2→1, `getWorkflowAll`은 run마다 1회 감소). 상태·시각 변환은 D12에서 helper로 모았다. 테스트: `startAndStopRequests`(실행 뒤 DagRun 재조회 없이 taskInstances만 조회).

### I7. `OperatorSpec`의 가변 컬렉션 공유 — 하
- 위치: [OperatorSpec.java:34-47](../../src/main/java/mdt/workflow/airflow/OperatorSpec.java#L34), [:96-110](../../src/main/java/mdt/workflow/airflow/OperatorSpec.java#L96)
- 문제: builder의 `List`/`Set`을 복사하지 않고 그대로 필드에 넣고 getter로 노출한다. builder를 재사용하거나 getter 결과를 수정하면 이미 만든 spec이 바뀐다(`DagSpec`은 `getTasks()`를 불변으로 감싸는데 하위 객체는 그렇지 않다).
- 제안: 생성자에서 `List.copyOf`/`Set.copyOf`(순서 유지가 필요하면 `Collections.unmodifiableSet(new LinkedHashSet<>(…))`).
- 결과: 완료(2026-11-05). `OperatorSpec` 생성자가 `List.copyOf`·`Collections.unmodifiableSet(new LinkedHashSet<>(…))`로 복사한다(의존 task 순서 유지). 테스트: `AirflowSpecReviewTest.specCollectionsAreImmutableCopies`.

### I8. 문서 — 중
- 위치: [README.md](../../README.md), 대부분의 클래스 Javadoc
- 문제:
  - README가 가리키는 `docs/workflow-user-interface-guide.md`가 없다.
  - `AirflowWorkflowManager`, `MDTWorkflowManager`, 컨트롤러 두 개, Spec 5개, `OperatorSpec`, `DagSpec`, `AirflowDagGenerator`, `JsonToPythonLiteralHybrid`, `WorkflowInstanceManagerProvider`의 클래스 Javadoc이 빈 `@author`뿐이다.
  - 패키지 CLAUDE.md가 없다(utils 패키지들과 달리).
  - 설정 키(`mdt.workflow-manager.*`, `mdt.workflow-manager.airflow.*`)와 env 파일 규칙을 설명한 곳이 없다.
- 제안: 깨진 링크를 고치거나 문서를 복구한다. 핵심 클래스 Javadoc을 쓴다. 설정 키·Airflow 연동 방식(DAG 파일 배포, `mdt_manager_url` 변수, 인증)을 README에 정리하고, 구현 메모는 CLAUDE.md에 둔다.
- 결과: 완료(2026-11-05).
  - README: 없는 `docs/workflow-user-interface-guide.md` 링크를 지웠다(디스크 어디에도 없다). 동작 개요, 오류 코드, 설정 키 표(`mdt.workflow-manager.*`·`airflow.*`), env 파일 규칙(경로 순서, `${env:..}`, 로그 마스킹), 빌드·테스트, `docs/migration/` 안내를 추가했다.
  - Javadoc: 빈 `@author`뿐이던 17개 클래스에 클래스 Javadoc을 썼다(현재 동작만). `OperatorSpec`의 `{@code inputs = {}`(중괄호가 맞지 않는 inline tag)를 고쳐 `gradle javadoc`이 다시 성공한다(이전에는 이 오류로 실패했다). 남은 경고는 원래 있던 "주석 없음" 종류다.
  - 프로젝트 `CLAUDE.md`를 새로 썼다(구조, 핵심 계약, 배포 설정 위치, 테스트 방식). 패키지별 CLAUDE.md 대신 프로젝트 하나로 두었다(패키지가 작다).

### I9. `JpaWorkflowModel` 저장 형식 — 하
- 위치: [JpaWorkflowModel.java:25-41](../../src/main/java/mdt/workflow/domain/JpaWorkflowModel.java#L25)
- 문제:
  - `columnDefinition = "bytea"`는 PostgreSQL 전용이다(H2 테스트나 다른 DB를 쓸 수 없다).
  - JSON 문자열을 UTF-8 바이트로 바꿔 저장해 DB에서 읽거나 질의할 수 없다.
  - 모델 식별자 필드 이름이 `id`이고 기본키가 `rowId`라서 `findByModelId`·`deleteByModelId`를 `@Query`로 직접 써야 한다.
- 제안: `@Lob String`(또는 PostgreSQL `jsonb` + 변환기)으로 저장한다. 필드 이름을 `modelId`로 바꾸면 Spring Data 파생 쿼리를 쓸 수 있다. 기존 데이터가 있으면 마이그레이션이 필요하다.
- 결과: 완료(2026-11-05, 사용자 결정: 저장 형식까지 text로).
  - 필드 `id` → `modelId`(컬럼 이름 `id` 유지), JSON은 `json_model`(text) 컬럼에 문자열로 저장한다(`@Lob`은 PostgreSQL에서 oid가 되어 쓰지 않았다). 리포지토리는 `@Query` 대신 파생 쿼리(`findByModelId`, `existsByModelId`, `deleteByModelId`(반환 `long`))를 쓴다.
  - 마이그레이션: [docs/migration/2026-11-05-workflow-models-json-text.sql](../migration/2026-11-05-workflow-models-json-text.sql) (`json_model_bytes` → `json_model` 이름 변경 + `convert_from(..., 'UTF8')`로 text 변환). `ddl-auto=update`는 기존 컬럼을 바꾸지 않으므로 **새 버전을 시작하기 전에** 배포 DB에서 실행해야 한다. 이 SQL은 PostgreSQL 전용 함수를 써서 실제 DB에서는 실행해 보지 않았다.
  - 테스트: `JpaWorkflowModelRepositoryTest`(`@DataJpaTest`, H2를 `testRuntimeOnly`로 추가) — `json_model`에 JSON 텍스트가 저장되고 파생 쿼리가 동작하는지 확인.

### I10. 구체 타입 의존 — 하
- 위치: [AirflowWorkflowManager.java:73-82](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L73)
- 문제: `JpaWorkflowModelManager`를 직접 주입받는다. 쓰는 메소드는 `getWorkflowModel` 하나뿐이다.
- 제안: `WorkflowModelManager` 인터페이스로 받는다.
- 결과: 완료(2026-11-05). `AirflowWorkflowManager`가 `WorkflowModelManager` 인터페이스를 주입받는다. mdt-workflow 테스트 72개 통과.

---

## 3. 인터페이스

### A1. 동작 API의 HTTP 동사 — 중
- 위치: [MDTWorkflowManagerController.java:279-334](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L279)
- 문제: 시작은 `POST /models/{modelId}/start`이고, 중지·일시정지·재개는 `PUT /workflows/{wfId}/stop|suspend|resume`이다. 이들은 멱등한 자원 교체가 아니라 동작 호출이므로 `PUT`이 어울리지 않는다. 시작만 모델 경로 아래에 있다.
- 제안: 동작은 모두 `POST`로 통일한다. 예: `POST /workflows`(본문 또는 `?model=`에 모델 id) → 201 + 새 워크플로우, `POST /workflows/{wfId}/stop`. 클라이언트(mdt-client의 워크플로우 HTTP 클라이언트, mdt-cli, mdt-gui)를 함께 바꿔야 하므로, 옮기는 동안 기존 경로를 남길지 정한다.
- 결과:

### A2. 지원하지 않는 기능의 노출 — 중
- 위치: [AirflowWorkflowManager.java:233-246](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L233), [MDTWorkflowManagerController.java:300-334](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L300), [:379-397](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L379)
- 문제: suspend/resume/log는 Airflow 구현에서 항상 실패한다. `GET /workflows/{wfId}/log/{podName}`의 `podName`은 이전 엔진(Kubernetes/Argo 계열)의 흔적이다. 클라이언트는 호출해 보기 전까지 지원 여부를 알 수 없다.
- 제안: 다음 중 하나를 고른다.
  - (a) 엔드포인트를 지우고, 인터페이스 메소드는 `default`로 `UnsupportedOperationException`을 던지게 한다.
  - (b) 501로 응답하고, 지원 기능을 알려 주는 `GET /capabilities` 같은 엔드포인트를 둔다.

  Airflow에서 로그는 `taskInstances/{task}/logs/{try}`로 얻을 수 있으므로, 필요하면 `GET /workflows/{wfId}/tasks/{taskId}/log`로 다시 설계한다.
- mdt-gui 사용(2026-11-05 확인): 워크플로우 표·목록 모달이 suspend/resume을 쓴다. 표의 "재시작" 버튼(NOT_STARTED·COMPLETED)이 resume을 부르는데 의미상 `start`가 맞다. log 래퍼는 있으나 화면에서 부르지 않는다. mdt-cli `GetWorkflowLogCommand`는 log를 쓴다.
- 결과: **완료**(2026-11-05). suspend/resume은 (a)로 처리했다: 서버 엔드포인트 `PUT /workflows/{id}/suspend`·`/resume`과 `MDTWorkflowManager`·`AirflowWorkflowEngine`의 구현을 지우고, mdt-client `WorkflowInstanceManager.suspendWorkflow`/`resumeWorkflow`를 `UnsupportedOperationException`을 던지는 `default` 메소드로 바꿨다. mdt-client `HttpWorkflowManager`의 두 구현과 README 표 행도 지웠다. mdt-cli `SuspendWorkflowCommand`/`ResumeWorkflowCommand`는 그대로 두어 이제 `UnsupportedOperationException`으로 끝나고, mdt-gui의 일시정지·"재시작" 버튼(`PUT .../suspend`·`/resume`)은 404/405를 받는다(GUI는 고치지 않음). 로그는 방식 (1)로 구현했다: 경로·시그니처(`getWorkflowLog(wfId, podName)`)는 그대로 두고 `podName`을 task id로 해석해, TaskInstance의 `try_number`(최신 시도) 로그를 `?full_content=true`로 읽어 구조화 항목 하나를 한 줄(`<timestamp> <LEVEL> <logger>: <event> <나머지 필드 JSON>`)로 바꾼다(`AirflowWorkflowEngine.toLogText`). task가 없거나 `try_number`가 0이면 404. 실제 Airflow(`stack_box_on_cylinder`/`read_scene`)에서 로그 API 응답 형식을 확인했다. mdt-client·mdt-cli·mdt-gui는 바꾸지 않았다(mdt-client `WorkflowInstanceManager.getWorkflowLog` Javadoc은 아직 pod 기준). 테스트 3개 추가(76개 통과). 

### A3. `WorkflowInstanceManagerProvider`의 역할 — 중
- 위치: [WorkflowInstanceManagerProvider.java](../../src/main/java/mdt/workflow/WorkflowInstanceManagerProvider.java)
- 문제: 하나의 인터페이스가 세 가지 역할을 한다. 그래서 다른 엔진을 붙일 때 경계가 흐리다. Javadoc도 없다.
  - 워크플로우 인스턴스 관리(`WorkflowInstanceManager`)
  - 모델 변경 알림(`onWorkflowModelAdded`/`Removed`)
  - 엔진별 산출물 생성(`getWorkflowScript`)
- 제안: 엔진 SPI를 `WorkflowEngine` 하나로 정의한다(`deploy(WorkflowModel)`, `undeploy(String modelId)`, `render(WorkflowModel) : String`, 인스턴스 조회·시작·중지). "on…"은 이벤트 리스너 이름이라, 실패하면 등록 자체를 되돌리는 지금의 의미(필수 단계)와 맞지 않는다.
- 결과: **완료**(2026-11-05). `WorkflowEngine`(N5에서 이름 변경)을 `deploy(WorkflowModel)`/`undeploy(String modelId)`/`render(WorkflowModel) : String` + 상속한 `WorkflowInstanceManager`(조회·시작·중지·삭제·로그)로 바꾸고 Javadoc을 썼다(별칭 없음). 엔진은 이제 모델 저장소를 모른다: `AirflowWorkflowEngine` 생성자에서 `WorkflowModelManager`를 빼고, `MDTWorkflowManager.getWorkflowScript(id)`가 모델을 읽어 `render`에 넘긴다. REST 경로·mdt-client 공개 인터페이스는 그대로라 mdt-cli·mdt-gui는 영향 없다. `MDTWorkflowManager`의 필드 `m_instanceManager`→`m_engine`. 테스트 `MDTWorkflowManagerTest.getWorkflowScriptRendersStoredModel` 추가(77개 통과), `CLAUDE.md`·`docs/adding-operator.md` 갱신.

### A4. 모델 등록 API — 중
- 위치: [MDTWorkflowManagerController.java:131-140](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L131), [MDTWorkflowManager.java:47-92](../../src/main/java/mdt/workflow/service/MDTWorkflowManager.java#L47)
- 문제:
  - `POST /models?updateIfExists=true|false` 같은 boolean 플래그 API다.
  - 실패를 `RuntimeException`으로 감싸 409(이미 있음)와 400(잘못된 모델)의 구분을 잃는다(D7).
  - `JpaWorkflowModelManager.addWorkflowModel`은 `DataIntegrityViolationException`이면 모두 `ResourceAlreadyExistsException`으로 바꾼다. 다른 제약 위반(예: null JSON)도 "이미 있음"이 된다.
- 제안: `POST /models`(생성, 있으면 409)와 `PUT /models/{id}`(생성 또는 교체)로 나눈다. 중복 여부는 `existsByModelId`로 먼저 확인한다. 등록 전에 `DagSpec` 검증을 먼저 해서, 잘못된 모델은 저장하기 전에 400으로 거른다.
- 결과:

### A5. `DELETE /workflows?modelFilter=` — 하
- 위치: [MDTWorkflowManagerController.java:297](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L297), [AirflowWorkflowEngine.java:221](../../src/main/java/mdt/workflow/service/AirflowWorkflowEngine.java#L221), [WorkflowEngine.java:58](../../src/main/java/mdt/workflow/WorkflowEngine.java#L58)
- 문제:
  - 컨트롤러가 `getWorkflowAll()`로 모든 워크플로우의 상세(run마다 task 목록 포함)를 받아 모델 id로 거른다(I6 때문에 비용이 크다).
  - 삭제 중 예외를 빈 `catch`로 삼킨다.
  - 파라미터 이름 `modelFilter`는 필터 표현식처럼 보이지만 실제로는 모델 id다.
- 제안: `?model=<id>`로 이름을 바꾸고, 서비스에 `removeWorkflowsOfModel(modelId)`를 둔다. Airflow 구현은 그 DAG의 dagRuns만 조회해 삭제한다. 실패는 로그로 남긴다.
- 결과: **완료**(2026-11-07). 파라미터를 `?model=<id>`로 바꿨다(별칭 없음). mdt-client, mdt-cli, mdt-gui, mdtpy 어디에서도 `modelFilter`를 쓰지 않아 클라이언트 수정은 없다. 엔진 SPI `WorkflowEngine.removeWorkflowsOfModel`과 `MDTWorkflowManager.removeWorkflowsOfModel`을 두었고, Airflow 구현은 그 DAG의 dagRuns만 조회해 지운다. 하나를 지우지 못해도 나머지는 계속 지우고 ERROR 로그를 남긴다. 배포되지 않은 모델(DAG 404)은 아무 일도 하지 않는다(이전처럼 204). `removeWorkflowAll`도 같은 삭제 루프를 쓴다. 테스트 `AirflowWorkflowEngineApiTest`의 두 사례.

### A6. `/list/workflows/{type}` — 하
- 위치: [MDTWorkflowListCandidatesController.java:84](../../src/main/java/mdt/workflow/controller/MDTWorkflowListCandidatesController.java#L84), [mdt_completion:279](../../../mdt-cli/sbin/mdt_completion#L279)
- 문제: 상태 필터를 경로 변수로 받는다(`/workflows/running`). 같은 정보를 주는 `/workflow-manager/workflows`와 별도의 `/list` 경로·`text/plain` 줄 목록 형식이 따로 있다. 사용처(셸 자동완성 등)가 문서화되어 있지 않다.
- 제안: 용도가 셸 자동완성이라면 Javadoc에 적고 `?status=running|finished|all`로 바꾼다. 아니라면 `GET /workflow-manager/workflows?status=`로 합친다.
- 결과: **완료**(2026-11-07). 용도가 셸 자동완성(mdt-cli `sbin/mdt_completion`)임을 확인해 클래스 Javadoc에 적고, `GET /list/workflows/{type}`을 `GET /list/workflows?status=all|running|stopped`(기본값 `all`, 별칭 없음)로 바꿨다. 상태 값은 자동완성이 쓰는 `stopped`를 그대로 두었다(제안의 `finished`는 쓰지 않음). `mdt_completion`의 호출과 주석(지원하지 않는 `suspended` 삭제)도 고쳤다. mdt-gui, mdt-client, mdtpy는 이 경로를 쓰지 않는다. 테스트 `MDTWorkflowListCandidatesControllerTest`.

---

## 4. 이름

### N1. `OperatorArgumentSpec`의 중첩 클래스 이름 — 중
- 위치: [OperatorArgumentSpec.java](../../src/main/java/mdt/workflow/airflow/OperatorArgumentSpec.java), [OperatorSpec.java:165-189](../../src/main/java/mdt/workflow/airflow/OperatorSpec.java#L165)
- 문제: `OperatorArgumentSpec.ReferenceArgument`/`LiteralArgument`/`TaskOutputArgument`가 mdt-client `ArgumentSpec`의 중첩 클래스와 이름이 같다. `OperatorSpec.fromArgumentSpec`처럼 한 메소드에서 둘을 다 쓸 때 정규화된 이름으로만 구분된다. 또 `SinkArgument`(출력)가 입력 인자와 같은 sealed 계층에 있다.
- 제안: 계층 이름을 `DagArgument`(또는 `PyArgument`)로 바꾸고, 출력은 별도 타입 `DagSink`로 나눈다. 중첩 클래스 이름은 `DagArgument.Reference`/`Literal`/`TaskOutput`처럼 짧게 한다.
- 결과: **완료**(2026-11-05). `OperatorArgumentSpec`을 지우고 입력 인자 sealed 계층 `DagArgument`(`Reference`/`Literal`/`TaskOutput`)와 출력 전용 final 클래스 `DagSink`로 나눴다(별칭 없음). 생성되는 Python 코드와 id 검증(`DagArgument.checkArgId`, `DagSink`도 같이 씀)은 그대로다. `OperatorSpec`(`getInputs()`→`List<DagArgument>`, `getOutputs()`→`List<DagSink>`, `fromArgumentSpec`/`fromOutputVariable`), `PythonScriptSpec.of`, `JsonToPythonLiteralHybrid` Javadoc, `docs/operator-spec-guide.md` 예제·링크를 고쳤다. 템플릿은 `arg.pythonStatement`만 쓰므로 바꾸지 않았다. 테스트 `OperatorArgumentSpecTest`→`DagArgumentTest`. 테스트 72개 통과, javadoc 성공.

### N2. `getPythonStatement()` — 하
- 위치: [OperatorArgumentSpec.java:14](../../src/main/java/mdt/workflow/airflow/OperatorArgumentSpec.java#L14)
- 문제: 돌려주는 것은 문장이 아니라 `inputs = {…}` dict의 한 항목(`"key": expr`)이다.
- 제안: `toPythonDictEntry()`(템플릿에서는 `arg.pythonDictEntry`).
- 결과: **완료**(2026-11-05). `DagArgument`·`DagSink`의 `getPythonStatement()`를 `toPythonDictEntry()`로 바꿨다(별칭 없음). Pebble은 `to` 접두어 메소드를 속성으로 찾지 않으므로 템플릿은 `{{ arg.toPythonDictEntry() }}`로 직접 부른다. 템플릿이 입력·출력 항목을 렌더링하는지 확인하는 테스트 `PythonScriptSpecTest.dagTemplateRendersInputAndOutputEntries`를 추가했다(전에는 인자가 있는 DAG 렌더링 테스트가 없었다).

### N3. `submodelId` → `submodelIdShort` — 중
- 위치: [AASOperationSpec.java:17](../../src/main/java/mdt/workflow/airflow/AASOperationSpec.java#L17), [OperationSubmodelSpec.java:17](../../src/main/java/mdt/workflow/airflow/OperationSubmodelSpec.java#L17)
- 문제: 필드·getter·builder 메소드 이름은 `submodelId`인데, 실제로는 옵션 `<instanceId>:<submodelIdShort>`의 idShort가 들어간다(`from()`의 지역 변수 이름은 `submodelIdShort`). AAS에서 Submodel id와 idShort는 다른 값이다.
- 제안: `submodelIdShort`로 바꾼다(Python kwarg `submodel`은 mdtpy 쪽 이름이라 그대로 둔다).
- 결과: **완료**(2026-11-05). `AASOperationSpec`·`OperationSubmodelSpec`의 필드·getter·builder 메소드를 `submodelIdShort`/`getSubmodelIdShort()`/`submodelIdShort(...)`로 바꿨다(별칭 없음). Python kwarg `submodel`은 그대로다. `OperatorCallTest`, `docs/operator-spec-guide.md`(예제·표)를 고쳤다.

### N4. `JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid` — 하
- 위치: [JsonToPythonLiteralHybrid.java:19](../../src/main/java/mdt/workflow/airflow/JsonToPythonLiteralHybrid.java#L19), [:40](../../src/main/java/mdt/workflow/airflow/JsonToPythonLiteralHybrid.java#L40)
- 문제: "Hybrid"(짧으면 한 줄, 길면 여러 줄)는 구현 방식이다. 메소드 이름이 클래스 이름을 되풀이한다. 클래스는 인스턴스를 만들지 않는데 `final`과 private 생성자가 없다.
- 제안: `PythonLiterals.fromJson(String json)`(유틸 클래스, `PythonStringEscaper`와 짝).
- 결과: **완료**(2026-11-05). `JsonToPythonLiteralHybrid.convertToPythonLiteralHybrid`를 `PythonLiterals.fromJson`으로 바꾸고(별칭 없음), 클래스를 `final`로 하고 private 생성자를 넣었다. 호출처 `OperatorSpec.fromArgumentSpec`, `DagArgument` Javadoc, `AirflowSpecReviewTest`, `CLAUDE.md`, `docs/operator-spec-guide.md`를 고쳤다.

### N5. `AirflowWorkflowManager` / `WorkflowInstanceManagerProvider` — 하
- 위치: [AirflowWorkflowManager.java](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java), [WorkflowInstanceManagerProvider.java](../../src/main/java/mdt/workflow/WorkflowInstanceManagerProvider.java)
- 문제: 실제 "워크플로우 관리자"는 `MDTWorkflowManager`다. `AirflowWorkflowManager`는 그 아래의 엔진 어댑터인데 이름이 같은 층처럼 보인다. "Provider"는 무엇을 제공하는지 드러내지 않는다.
- 제안: A3과 함께 `WorkflowEngine` / `AirflowWorkflowEngine`.
- 결과: **완료**(2026-11-05). 이름만 바꿨다(별칭 없음): `WorkflowInstanceManagerProvider`→`WorkflowEngine`, `AirflowWorkflowManager`→`AirflowWorkflowEngine`, 테스트 `AirflowWorkflowManager{Config,Jwt,Init,Api}Test`→`AirflowWorkflowEngine*Test`. 메소드 구성(`deploy`/`undeploy`/`render`)은 A3에서 다룬다. 설정 클래스 `AirflowWorkflowManagerConfiguration`과 속성 접두어 `mdt.workflow-manager.airflow`는 배포 설정과 맞물려 있어 그대로 두었다. 배포된 설정 파일에는 옛 클래스 이름이 없다(로그 파일에만 있음).

### N6. `getWorkflowScript` — 하
- 위치: [MDTWorkflowManager.java:164](../../src/main/java/mdt/workflow/service/MDTWorkflowManager.java#L164), [MDTWorkflowManagerController.java:176](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L176), [WorkflowEngine.java:48](../../src/main/java/mdt/workflow/WorkflowEngine.java#L48)
- 문제: 저장된 스크립트를 읽는 것처럼 보이지만, 호출할 때마다 모델에서 새로 생성한다. 그래서 배포된 DAG 파일과 다를 수 있다(템플릿이 바뀐 경우).
- 제안: `renderWorkflowScript`(또는 엔진 SPI의 `render`). 배포된 파일을 돌려줘야 한다면 별도 메소드로 둔다.
- 결과: **완료**(2026-11-07). `MDTWorkflowManager.getWorkflowScript`와 같은 이름의 컨트롤러 메소드를 `renderWorkflowScript`로 바꿨다(별칭 없음). 엔진 SPI는 이미 `WorkflowEngine.render`였다. Javadoc에 호출할 때마다 새로 만들어 배포된 DAG 파일과 다를 수 있다고 적었다. REST 경로 `GET /models/{id}/script`와 mdt-client·mdt-cli의 `getWorkflowScript`는 그대로다. 배포된 파일을 돌려주는 별도 메소드는 필요가 확인되지 않아 만들지 않았다.

### N7. `PythonIdentifierUtil.isValidPythonIdentifier` — 하
- 위치: [PythonIdentifierUtil.java](../../src/main/java/mdt/workflow/airflow/PythonIdentifierUtil.java)
- 문제: 클래스 이름과 메소드 이름에 "Python"·"Identifier"가 겹친다.
- 제안: `PythonIdentifiers.isValid(String)`.
- 결과: **완료**(2026-11-05). `PythonIdentifierUtil.isValidPythonIdentifier`를 `PythonIdentifiers.isValid`로 바꿨다(별칭 없음). 호출처 `DagSpec`·`OperatorSpec`·`DagArgument`·`AirflowWorkflowEngine`을 고쳤다.

### N8. `StartWorkflowRequest.getDagRunIdJackson` — 하
- 위치: [AirflowWorkflowManager.java:347-367](../../src/main/java/mdt/workflow/service/AirflowWorkflowManager.java#L347)
- 문제: getter 이름에 "Jackson"이 붙어 있고, 필드 `m_runIdEncoded`는 인코딩하지 않은 값이다.
- 제안: `record StartDagRunRequest(@JsonProperty("dag_run_id") String dagRunId, @JsonProperty("logical_date") String logicalDate)`.
- 결과: **완료**(2026-11-05). `StartWorkflowRequest`를 `private record StartDagRunRequest(@JsonProperty("dag_run_id") String dagRunId, @JsonProperty("logical_date") String logicalDate)`와 팩토리 `now()`로 바꿨다. 만드는 run id 형식과 본문 JSON은 같다(`AirflowWorkflowEngineApiTest`가 확인).

### N9. `TaskOutputArgument.m_outputArgumentName` — 하
- 위치: [OperatorArgumentSpec.java:81-101](../../src/main/java/mdt/workflow/airflow/OperatorArgumentSpec.java#L81)
- 문제: 값은 `ArgumentSpec.TaskOutputArgument.getOutputArgumentId()`에서 오는데 필드·생성자 인자 이름은 `outputArgumentName`이다(검사 메시지는 `outputName`).
- 제안: `outputArgumentId`로 통일한다.
- 결과: **완료**(2026-11-05). `DagArgument.TaskOutput`의 필드·생성자 인자를 `outputArgumentId`로, 검사 메시지를 `"outputArgumentId should not be null"`로 바꿨다.

### N10. 컨트롤러 메소드 `log(...)` — 하
- 위치: [MDTWorkflowManagerController.java:392-397](../../src/main/java/mdt/workflow/controller/MDTWorkflowManagerController.java#L392)
- 문제: 다른 핸들러는 서비스 메소드와 같은 이름(`getWorkflow`, `stopWorkflow` 등)인데 이것만 `log`다.
- 제안: `getWorkflowLog` (A2에서 엔드포인트를 유지하는 경우).
- 결과: **완료**(2026-11-05). 컨트롤러 메소드 `log(...)`를 `getWorkflowLog(...)`로 바꿨다. 엔드포인트(`GET /workflows/{wfId}/log/{podName}`, 지금은 501)는 그대로다. 엔드포인트를 둘지는 A2에서 정한다.

---

## 권장 순서

1. **보안·운영 장애: D1 → D2 → D3 → D18 → D10.** Airflow 연결과 인증을 한 번에 정리한다(설정 추가, 토큰 갱신, 초기화 수정, TLS).
2. **데이터 손상 위험: D4 → D9 → D11.**
3. **정확성: D6 → D7 → D8 → D12 → D13 → D5 → D14 → D16 → D17 → D19.** D7(오류 매핑)을 먼저 하면 나머지의 오류 응답이 바로 맞춰진다.
4. **Spec 정리: I1 → I2 → I3 → I7 → N1 → N2 → N3 → N9**(같은 파일들을 한 번에 손본다). D20은 mdtpy와 함께.
5. **서비스·컨트롤러 정리: I6 → I10 → I5 → D15 → A4 → A5 → A2 → A6 → A1 → A3/N5.** REST 경로를 바꾸는 A1·A2·A5·A6은 mdt-client·mdt-cli·mdt-gui 클라이언트와 같이 바꾼다.
6. **나머지: I4, I9, N4, N7, N8, N10, I8(문서는 마지막에 한 번 더).**
7. 모든 항목을 마치면 이 문서를 지우고, 남길 내용은 README·CLAUDE.md로 옮긴다.

# mdt-workflow

## 문서

- [워크플로우 사용자 인터페이스 가이드](docs/workflow-user-interface-guide.md) — 모델 편집, 입출력 연결,
  저장·실행·모니터링 흐름, 화면별 API와 현재 Airflow 구현의 제약.
- [OperatorSpec 설정과 실행 사용 설명서](docs/operator-spec-guide.md) — 지원 Spec 6종의 선택 기준,
  JSON 설정과 데이터 전달, Java DAG 생성 예제, 오류 확인 방법.
- [새 Airflow Operator 추가 가이드](docs/adding-operator.md) — 새 task 유형을 워크플로 모델에서 Airflow DAG의
  mdtpy Operator 호출까지 연결할 때의 작업 목록과 순서 (mdtpy, mdt-client, mdt-workflow, mdt-gui).
  참고 구현은 `PythonScriptOperator`(`PythonScriptSpec`).

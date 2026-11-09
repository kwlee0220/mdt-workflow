# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

`mdt-workflow` is the MDT workflow manager: a Spring Boot 3.5 service (Java 21, entry point `mdt.workflow.MDTWorkflowManagerApplication`) that stores workflow models (task graphs from `mdt-client`'s `mdt.workflow.model`) in PostgreSQL and runs them on **Airflow 3** (REST API v2) by rendering each model into a DAG file. User-facing documentation (Korean): [README.md](README.md) (behavior, settings, error codes), [docs/operator-spec-guide.md](docs/operator-spec-guide.md), [docs/adding-operator.md](docs/adding-operator.md) (adding a task type across mdtpy / mdt-client / mdt-workflow / mdt-gui), [docs/migration/](docs/migration/) (DB SQL to run before starting a given version).

Composite build: `settings.gradle` includes `../../common/utils` (`:utils`) and `../mdt-client` (`:mdt-client`); versions come from `../mdt-client/gradle/libs.versions.toml`.

Docker image: `docker/build_image.sh` / `push_image.sh` (procedure in [docker/README.md](docker/README.md)). `GET /health` (`controller.MDTWorkflowHealthController`) exists only for the image `HEALTHCHECK`.

```bash
gradle test --rerun -x :utils:test -x :mdt-client:test   # this project's tests only
gradle javadoc -x :utils:javadoc -x :mdt-client:javadoc
gradle bootJar
```

## Structure

- `service.MDTWorkflowManager` — the `WorkflowManager` façade. Model add/replace = save (`JpaWorkflowModelManager`) then deploy (`WorkflowEngine.deploy`); a deploy failure rolls the save back (replace restores and re-deploys the previous model) and rethrows the original exception type. Model removal deletes the row (`ResourceNotFoundException` if absent) then cleans up the engine side (failures are WARN-logged, not rolled back). `removeWorkflowModelAll` removes models one by one through the same path.
- `WorkflowEngine` — engine SPI: `deploy(model)` / `undeploy(modelId)` / `render(model)` plus the inherited `WorkflowInstanceManager` methods. The engine does not know the model store; `MDTWorkflowManager.renderWorkflowScript(id)` loads the model and calls `render`.
- `service.AirflowWorkflowEngine` — the only engine adapter (`WorkflowEngine`, constructor takes only the Airflow configuration). Key behaviors:
  - **Client init** (`getRestfulClient()` → `connect()`): double-checked lock; logs in (`/auth/token`, credentials from config — required), builds the client, re-registers Airflow variable `mdt_manager_url`; the field is assigned only if every step succeeded. `afterPropertiesSet()` only logs an init failure (Airflow may come up later).
  - **Auth**: an OkHttp `Interceptor` adds the current JWT to every request; an `Authenticator` re-logs in once on 401 (`refreshJwtToken` is `synchronized` and only refreshes if the failed request used the current token). The token is never baked into headers.
  - **Lists** go through `listAll(url, field)` (`limit`/`offset` until `total_entries`). DAG lists are filtered by tag `mdt` (`MDT_DAG_TAG`, must match `tags=["mdt"]` in `templates/airflow_dag.peb`) both server-side and client-side — never act on non-MDT DAGs.
  - **Errors**: Airflow 404 → `ResourceNotFoundException` (`toNotFoundIf404`); suspend/resume have no endpoints (removed 2026-11-05) and use the `WorkflowInstanceManager` default methods, which throw `UnsupportedOperationException`. `getWorkflowLog(wfId, podName)` treats `podName` as the **task id** (interface name kept from the old pod-based engine) and returns the latest try's log (`try_number` from the TaskInstance, `.../logs/{try}?full_content=true`) as one text line per structured entry (`toLogText`); a missing or never-run task → `ResourceNotFoundException`.
  - **DAG files**: `dagFile(id)` validates the id (Python identifier, path directly under `dagsFolder`); written via a `.<id>.py.tmp` file + atomic move; removal deletes the file first, then the Airflow DAG.
  - **Status mapping** lives only in `toDagRunStatus` / `toTaskStatus` (checked against the live Airflow OpenAPI spec; `skipped` → `NOT_STARTED` because `WorkflowStatus` has no SKIPPED). Task-instance PATCH bodies use `new_state` (Airflow rejects unknown fields); DagRun PATCH uses `state`.
  - Run ids are `mdt__<instant>_<8 hex>`; workflow ids are `<dagId>__<dagRunId>` (`airflow.AirflowWorkflowId`, split at the first `__`, so DAG ids must not contain `__`).
- `airflow.*` — DAG rendering. `AirflowDagGenerator` → `DagSpec` (validates ids/deps) → Pebble template `templates/airflow_dag.peb` (autoescape off, `strictVariables` on; `py` filter and `fail()` from `AirflowTemplateExtension`). `OperatorSpec.from(task)` picks a spec from the `FACTORIES` registry (task type → factory); each spec's `from()` is `builder().fromTask(task)` (common fields) + its own options read with `requireOption(...)` using mdt-client option constants. `OperatorSpec.getIgnoredExecutionOptions` warns when `timeout`/`poll` cannot be passed to the mdtpy operator (only `RESTfulRpcSpec` passes `timeout`). Spec collections are immutable copies. All Python string escaping goes through `PythonStringEscaper`; JSON literals through `PythonLiterals`.
- `service.ExecutionTimeEstimator` — posts a submodel's JSON to the configured estimator; 501 if not configured, `RESTfulIOException` (→ 502) for a non-numeric reply.
- Controllers use constructor injection. `MDTWorkflowManagerController.handleException` maps 404/409/400/501/502(`RESTfulException`)/500 (500 is ERROR-logged).
- `domain.JpaWorkflowModel` — table `workflow_models`: `row_id` PK, `id` = model id (field `modelId`), `json_model` text. Changed on 2026-11-05 from `json_model_bytes bytea`; `spring.jpa.hibernate.ddl-auto=update` will not convert existing columns, so deployed DBs must run `docs/migration/2026-11-05-workflow-models-json-text.sql` first.
- `EnvironmentVariablePostProcessor` (registered in `META-INF/spring.factories`) loads the env file into the first property source and logs variables with secrets masked (`maskValue`).

## Conventions and cautions

- Deployed configs live outside the repo: `~/mdt/mdt-workflow/config/` and `~/mdt/docker-compose/mdt-workflow-data/config/` (`application.properties` + `env.file`). Never print or commit their secret values.
- Tests need no Airflow/DB: `AirflowWorkflowEngine*Test` use OkHttp MockWebServer (`libs.okhttp.mockwebserver`), `JpaWorkflowModelRepositoryTest` is a `@DataJpaTest` on H2 (`testRuntimeOnly`). Don't assert JSON bodies built from `Map.of` as strings — iteration order differs per JVM run; compare fields.
- A review of this project is tracked in `docs/reviews/mdt-workflow-review.md` while it is in progress (deleted when done).

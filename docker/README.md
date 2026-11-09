# mdt-workflow Docker 이미지 만들기

이 문서는 mdt-workflow Docker 이미지를 빌드하고, compose로 실행하고, 레지스트리에 올리는 절차를 설명한다.
mdt-workflow는 워크플로 모델을 Airflow DAG 파일로 만들어 DAG 폴더에 쓰고, Airflow REST API로 실행을 관리한다.
Airflow는 이 이미지에 들어 있지 않고 따로 실행한다.

아래 예의 경로는 다음과 같다. 환경에 맞게 바꿔 읽는다.

| 표기 | 의미 | 예 |
| --- | --- | --- |
| `~/development/mdt` | MDT 소스를 받아 둔 위치 | `mdt-workflow`, `mdt-client`가 이 아래에 있다 |
| `$MDT_HOME` | MDT 설치 디렉터리 | `/home/kwlee/mdt` |
| `~/mdt/docker-compose` | compose 파일과 마운트 데이터가 있는 배포 디렉터리 | `docker-compose.yml`, `mdt-workflow-data/` |
| `1.7.2` | 빌드할 버전 | |

## 1. 준비

- Docker, Gradle(래퍼 없음, 시스템 `gradle` 사용), Java 21
- 형제 프로젝트: mdt-workflow는 `../../common/utils`와 `../mdt-client`를 함께 빌드하므로 둘 다 받아 두어야 한다.
- 환경 변수 `MDT_HOME`: `build_image.sh`는 `$MDT_HOME/mdt-workflow/`에서 JAR을 가져온다.

## 2. JAR 빌드와 연결

```bash
export MDT_BUILD_VERSION=1.7.2
cd ~/development/mdt/mdt-workflow && gradle bootJar      # build/libs/mdt-workflow-1.7.2-all.jar

ln -sfn ~/development/mdt/mdt-workflow/build/libs/mdt-workflow-${MDT_BUILD_VERSION}-all.jar \
        $MDT_HOME/mdt-workflow/mdt-workflow-all.jar
```

심볼릭 링크로 연결해 두면 같은 버전을 다시 빌드할 때는 링크를 다시 걸 필요가 없다.

## 3. 이미지 빌드

```bash
cd ~/development/mdt/mdt-workflow/docker
./build_image.sh --tag 1.7.2                 # 이미지 안의 mdt 사용자: UID/GID 1000
./build_image.sh --host-user --tag 1.7.2     # 현재 호스트 사용자의 UID/GID
```

결과는 `mdt-workflow:1.7.2` 이미지다. 옵션은 `./build_image.sh --help`로 볼 수 있다.

| 옵션·환경 변수 | 설명 |
| --- | --- |
| `--tag`, `-t <버전>` | 이미지 태그. 생략하면 `MDT_BUILD_VERSION` |
| `--host-user`, `-H` | 이미지의 `mdt` 사용자를 현재 호스트 사용자의 UID/GID로 만든다. |
| `--uid <UID>`, `--gid <GID>` | UID/GID를 직접 지정한다. `--host-user`와 함께 쓸 수 없다. |
| `MDT_IMAGE_NAME` | 이미지 이름(기본값 `mdt-workflow`). `push_image.sh`에도 같은 값을 쓴다. |

- **JAR 버전 확인:** 빌드 전에 JAR의 버전(매니페스트의 `Implementation-Version`, 없으면 링크가 가리키는 파일 이름)이 태그와 같은지 확인하고, 다르면 빌드하지 않는다.
- **빌드 위치:** 임시 디렉터리를 빌드 컨텍스트로 쓰므로 `docker/` 폴더의 파일은 바뀌지 않는다.
- **이전 이미지 정리:** 빌드 후에는 라벨 `mdt.image=mdt-workflow`가 붙은 dangling 이미지만 정리한다.
- **UID 주의:** Airflow 컨테이너는 UID 1000(그룹 0)으로 실행된다. DAG 폴더를 함께 쓰려면 이미지의 `mdt` 사용자도 UID 1000이 편하다. `--host-user`는 호스트 사용자가 1000일 때 같은 결과다.

이미지 구성은 [Dockerfile](Dockerfile)에 있다.

- **기반 이미지:** `eclipse-temurin:21-jre-jammy`를 mdt-manager와 같은 digest로 고정했다. 갱신 방법은 첫 주석에 있다.
- **헬스체크:** 30초마다 `GET /health`를 확인한다. 서버가 요청을 받는지만 보고, DB나 Airflow 연결은 확인하지 않는다.

## 4. 실행

### 4.1 처음 한 번: 마운트 디렉터리 만들기

compose는 `config`, `logs`를 `~/mdt/docker-compose/mdt-workflow-data/` 아래에, DAG 폴더를 `$MDT_HOME/airflow/dags`(4.2장)에 바인드 마운트한다.
호스트에 없는 디렉터리는 Docker가 root 소유로 만들어 컨테이너의 `mdt` 사용자가 쓰지 못하므로, 미리 만든다.
디렉터리 소유자의 UID/GID는 이미지를 빌드할 때 쓴 값과 같아야 한다.

```bash
mkdir -p ~/mdt/docker-compose/mdt-workflow-data/{config,logs}
```

`config/`에는 `application.properties`, `env.file`, `logback.xml`을 둔다. 설정 키는 [프로젝트 README의 설정 절](../README.md#설정)을 본다.

### 4.2 DAG 폴더는 Airflow와 같아야 한다

컨테이너 안의 mdt-workflow는 `mdt.workflow-manager.airflow.dagsFolder`(배포 설정: `${MDT_HOME}/airflow/dags`, 즉 `/mdt/airflow/dags`)에 DAG 파일을 쓴다.
compose가 이 위치에 마운트한 호스트 폴더를 Airflow도 자신의 DAG 폴더(`/opt/airflow/dags`)로 마운트해야, 등록한 모델이 Airflow에 나타난다.

| 쪽 | 설정 위치 | 마운트하는 호스트 폴더 |
| --- | --- | --- |
| mdt-workflow | `~/mdt/docker-compose/docker-compose.yml`: `${MDT_HOME:?...}/airflow/dags:/mdt/airflow/dags` | `$MDT_HOME/airflow/dags` (예: `/home/kwlee/mdt/airflow/dags`) |
| Airflow | `~/mdt/airflow/docker-compose.yaml`: `${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags` | `~/mdt/airflow/dags` (`AIRFLOW_PROJ_DIR`을 정하지 않은 경우) |

- **`MDT_HOME`이 필요하다:** compose는 호스트 경로를 `docker compose`를 실행하는 셸의 `MDT_HOME`으로 만든다. 비어 있으면 compose가 "MDT_HOME must be set"으로 실패하므로, 잘못된 폴더가 마운트되지 않는다.
- **권한:** Airflow 컨테이너는 UID 1000(그룹 0)으로 실행되고, `~/mdt/airflow/dags`는 UID 1000 소유다. 이미지의 `mdt` 사용자도 UID 1000(기본값)으로 빌드해야 서로 읽고 쓸 수 있다.
- **두 위치를 바꿀 때:** Airflow의 `AIRFLOW_PROJ_DIR`이나 `MDT_HOME`을 바꾸면 두 경로가 여전히 같은지 확인한다.

### 4.3 compose로 띄우기

```bash
cd ~/mdt/docker-compose
export MDT_HOME=/home/kwlee/mdt        # DAG 폴더 마운트에 필요(4.2장)
MDT_BUILD_VERSION=1.7.2 docker compose up -d mdt-workflow
docker ps                     # STATUS에 (healthy)가 나오면 정상
docker logs -f mdt-workflow
```

### 4.4 포트와 메모리

- **포트:** REST 포트는 12989다. 바꾸려면 `application.properties`의 `server.port`, compose의 포트 매핑, 환경 변수 `MDT_WORKFLOW_PORT`(헬스체크가 쓴다)를 함께 바꾼다.
- **메모리:** JVM 옵션은 환경 변수 `JAVA_OPTS`(기본값 `-XX:MaxRAMPercentage=75.0`)로 정한다. compose에 메모리 제한(`mem_limit`)이 없으면 이 비율은 호스트 메모리를 기준으로 계산되므로, 제한을 두거나 `JAVA_OPTS=-Xmx1g`처럼 힙을 직접 정한다.

## 5. 레지스트리에 올리기(선택)

```bash
cd ~/development/mdt/mdt-workflow/docker
./push_image.sh --repo <레지스트리/경로> --tag 1.7.2            # <레지스트리/경로>/mdt-workflow:1.7.2
./push_image.sh --repo <레지스트리/경로> --tag 1.7.2 --latest   # :latest도 함께
```

레지스트리에 미리 `docker login` 해 둔다. 값이 빠진 옵션, 모르는 옵션, 빈 태그는 거부하고, `docker tag`가 실패하면 푸시하지 않는다.

## 6. 문제 해결

| 증상 | 원인과 조치 |
| --- | --- |
| `오류: $MDT_HOME 환경변수가 설정되어 있어야 합니다.` (이미지 빌드) | `export MDT_HOME=...`을 설정한다. |
| `required variable MDT_HOME is missing a value: MDT_HOME must be set` (`docker compose`) | compose가 DAG 폴더 경로를 만들 때 쓴다. `docker compose`를 실행하는 셸에서 `export MDT_HOME=...`을 설정한다(4.2장). |
| `오류: JAR이 없습니다: ...` | 2장을 다시 확인한다. 링크가 가리키는 파일이 없어도 이 오류가 난다. |
| `오류: JAR 버전(x)이 이미지 태그(y)와 다릅니다` | `--tag`를 JAR 버전과 맞추거나, JAR을 다시 빌드하고 링크를 바꾼다. |
| 빌드 중 `오류: GID ... 가 이미 사용 중입니다` 또는 `UID ...` | `--uid`·`--gid`로 비어 있는 값을 지정한다. |
| 컨테이너 로그에 권한 오류 | 마운트 디렉터리의 소유자가 이미지의 UID/GID와 다르다. 4.1장을 확인한다. |
| 모델을 등록했는데 Airflow에 DAG가 없음 | 4.2장의 DAG 폴더가 맞는지 확인한다. 막 등록했다면 Airflow가 아직 파일을 읽지 않았을 수 있다. |
| `docker ps`에 `(unhealthy)` | 서버가 `GET /health`에 응답하지 않는다. `docker logs mdt-workflow`로 기동 오류를 확인한다. |

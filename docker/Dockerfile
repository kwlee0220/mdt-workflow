# 기반 이미지를 digest로 고정해, 같은 소스로 빌드하면 같은 JRE·OS 패키지가 들어가게 한다(검토 I4).
# mdt-manager 이미지와 같은 digest를 쓴다. 대신 보안 갱신을 자동으로 받지 않으므로 주기적으로 갱신한다:
#   docker buildx imagetools inspect eclipse-temurin:21-jre-jammy   # 'Digest:' 값으로 아래를 바꾼다
FROM eclipse-temurin:21-jre-jammy@sha256:1eac8b8fee11ca7d6725a48bc5d41992ed1561504ebae25490d2d062847e8269

# build_image.sh 의 'docker image prune --filter' 가 이 라벨로 자기 dangling 이미지만 정리한다.
LABEL mdt.image=mdt-workflow

# UID/GID 설정 (빌드 시 --build-arg UID=1000 형태로 오버라이드 가능)
# Airflow 컨테이너는 UID 1000(그룹 0)으로 실행되므로, DAG 폴더를 함께 쓰려면 기본값 1000을 유지한다.
ARG UID=1000
ARG GID=1000

ENV MDT_HOME=/mdt

# curl: 아래 HEALTHCHECK에서 쓴다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# mdt 사용자 생성 + 작업 디렉토리 준비 + 권한 부여 (단일 layer)
# GID/UID가 베이스 이미지에 이미 점유돼 있으면(예: --host-user로 GID 100=users 전달)
# 조용히 재사용하지 않고 충돌 원인을 명확히 알리며 빌드를 중단한다.
RUN if getent group "${GID}" >/dev/null; then \
        echo "오류: GID ${GID}가 이미 사용 중입니다 ($(getent group "${GID}" | cut -d: -f1)). --gid 로 비어 있는 GID를 지정하세요." >&2; \
        exit 1; \
    fi \
 && if getent passwd "${UID}" >/dev/null; then \
        echo "오류: UID ${UID}가 이미 사용 중입니다 ($(getent passwd "${UID}" | cut -d: -f1)). --uid 로 비어 있는 UID를 지정하세요." >&2; \
        exit 1; \
    fi \
 && groupadd -g ${GID} mdt \
 && useradd -u ${UID} -g mdt -m -s /bin/bash mdt \
 && mkdir -p /mdt/mdt-workflow/config \
             /mdt/airflow/dags \
             /mdt/mdt-workflow/logs \
 && chown -R mdt:mdt /mdt

# 아래 JAR은 빌드 컨텍스트에 있어야 한다. docker/build_image.sh는 임시 디렉터리를 컨텍스트로 만들어
# 이 Dockerfile과 $MDT_HOME/mdt-workflow/의 JAR(버전 확인 후)을 복사해 빌드하고, 끝나면 그 디렉터리만 지운다.
# docker build를 직접 쓰려면 이 Dockerfile과 JAR을 한 디렉터리에 두고 그 디렉터리를 컨텍스트로 지정한다.
COPY --chown=mdt:mdt ./mdt-workflow-all.jar /mdt/mdt-workflow/

WORKDIR /mdt/mdt-workflow
USER mdt

# REST 포트. application.properties의 server.port를 바꾸면 MDT_WORKFLOW_PORT도 같이 바꾼다(검토 I5).
ENV MDT_WORKFLOW_PORT=12989
EXPOSE 12989

# GET /health가 응답하는지 확인한다. DB·Airflow 연결은 확인하지 않는다.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS "http://localhost:${MDT_WORKFLOW_PORT}/health" > /dev/null || exit 1

# JVM 옵션. 'docker run -e JAVA_OPTS=...'이나 compose의 environment로 바꾼다(검토 I6).
# sh가 exec로 java를 실행하므로 java가 PID 1이 되어 종료 신호를 직접 받는다.
# (이 서비스는 자식 프로세스를 띄우지 않아 tini가 필요 없다)
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
CMD ["sh", "-c", "exec java $JAVA_OPTS -jar /mdt/mdt-workflow/mdt-workflow-all.jar"]

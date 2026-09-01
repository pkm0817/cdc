# ── 1단계: 빌드 ──────────────────────────────────────────────────────────────
FROM golang:1.25 AS build
WORKDIR /workspace

# 의존성 캐시를 살리기 위해 모듈 파일을 먼저 복사한다.
# 소스가 바뀌어도 go.mod/go.sum 이 그대로면 이 층이 재사용된다.
COPY go.mod go.sum ./

# 실패해도 넘어간다. vendor/ 가 함께 커밋돼 있으면 모듈 프록시가 필요 없고,
# 사내망처럼 proxy.golang.org 로 나가지 못하는 환경에서도 이미지가 빌드되어야 한다.
# 정말로 의존성이 없으면 아래 build 단계에서 분명하게 실패한다.
RUN go mod download || echo "모듈 내려받기 실패 — vendor/ 로 빌드를 시도한다"

COPY cmd ./cmd
COPY internal ./internal
COPY vendor ./vendor

# CGO 를 끈다 — 순수 Go 바이너리라야 최소 이미지에서 그대로 돈다.
# pgx 는 순수 Go 드라이버라 libpq 가 필요 없다.
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -trimpath -ldflags="-s -w" \
    -o /out/cdc-service ./cmd/cdc-service

# ── 2단계: 실행 ──────────────────────────────────────────────────────────────
FROM debian:bookworm-slim
WORKDIR /app

# compose healthcheck 용
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/cdc-service /app/cdc-service

# Debezium 판과 달리 오프셋 파일이 없다 — 진행 지점은 원천의 복제 슬롯과
# 수신 측 cdc_checkpoint 에만 남는다. 그래서 마운트할 볼륨도 없다.
EXPOSE 8080
ENTRYPOINT ["/app/cdc-service"]

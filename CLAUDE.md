# Plumb — Adaptive Scheduler Agent

Kubernetes 워크로드를 위한 벤더 중립 적응형 스케줄링 에이전트.
Karpenter·KEDA를 대체하지 않고, 그 위에서 System 1 결정 모델(Laya)로 빠르고 적응적인 판단을 더한다.

## 해결하려는 문제

- 특정 리전에 GPU가 정적으로 확보돼 있는데도 불필요하게 동적 확장하거나 다른 리전으로 보내는 문제
- ICE(Insufficient Capacity Error) 발생 시 재시도/대기/리전 이동 판단이 고정 휴리스틱에 묶여 있는 문제
- 기존 LLM 에이전트는 이벤트 대응 경로에 넣기엔 너무 느리고 출력이 비구조적인 문제

## 핵심 설계 원칙 (반드시 지킬 것)

1. **정적 용량 우선**: 기존 노드(예약 용량 포함)로 수요를 채울 수 있으면 동적 프로비저닝·리전 이동을 하지 않는다. 이건 모델 판단이 아니라 결정론적 규칙으로 구현한다.
2. **컨트롤러와 싸우지 않는다**: 노드·파드를 직접 생성/삭제하지 않는다. Karpenter NodePool, KEDA ScaledObject 등 CRD 설정만 조정하고 실행은 컨트롤러에 맡긴다.
3. **핫패스에서 모델 동기 호출 금지**: 결정은 비동기로 계산해 캐시/CRD status에 기록하고, 스케일러·어댑터는 캐시를 읽기만 한다.
4. **에이전트 없이도 안전**: 에이전트·모델 장애 시 마지막 안전한 설정으로 고정되고 Karpenter·KEDA 기본 동작으로 계속 운영된다.
5. **진동 방지**: 모든 결정에 쿨다운과 히스테리시스를 적용한다 (변경하려면 현재 선택보다 명확히 나아야 함).
6. **섀도 모드 우선**: 기본 모드는 shadow(결정만 기록, 적용 안 함). auto 모드는 명시적 opt-in.

## 아키텍처

3개 루프, 시간 스케일로 분리:

- **컨트롤러 루프 (ms~s)**: kube-scheduler, Karpenter, KEDA. 건드리지 않는다.
- **System 1 (수십~수백 ms)**: Laya 기반 결정. ICE 분류, 대기 vs 이동, 리전 선택, 스케일 목표 보정.
- **System 2 (분~시간)**: LLM 에이전트. 정책 튜닝, 결정 사후 검토, 낮은 확신도 에스컬레이션. **MVP 범위 밖.**

### 중립 코어 + CSP 어댑터

- `core/`: 정책 CRD, 상태 요약 계층, 결정 엔진 클라이언트, 규칙 기반 가드레일, KEDA external scaler, 결정 로그
- `adapters/<csp>/`: 노드 프로비저너, 용량 신호(ICE 정규화), 트래픽, 비용 어댑터
- 코어는 어떤 CSP SDK에도 의존하지 않는다. CSP별 코드는 반드시 어댑터 인터페이스 뒤에 둔다.

## MVP 범위

- **AWS만**: EKS + Karpenter v1 (NodePool / NodeClaim / EC2NodeClass)
- **워크로드**: 여러 리전에 같은 Deployment가 떠 있는 추론 서비스. 파드를 옮기지 않고 "부하를 옮긴다"(리전별 KEDA 스케일 목표 조정).
- **모드**: shadow 모드 완성이 1차 목표. 결정·입력 상태·실제 결과를 로그로 남겨 Laya 파인튜닝 데이터로 쓴다.
- **범위 밖**: 학습/배치 잡의 리전 간 제출, 트래픽 가중치 자동 변경, System 2, GCP/Azure 어댑터

## Laya 사용 시 제약 (공식 문서 기준)

- 베이스 모델 제로샷 성능은 거의 랜덤 수준 → 도메인 파인튜닝 전제. 그 전까지는 규칙 기반 결정이 실제 결정이고 Laya는 섀도로만 비교한다.
- 컨텍스트 512 / 1024 토큰 → 원시 메트릭을 넣지 말고 상태 요약 계층에서 압축한 JSON만 전달.
- choice 선택지 20개 이하. 넘으면 계층적으로 분리 (리전 → 존 → 인스턴스 타입).
- 질문 유형별 temperature 스칼라 보정 필요.
- 확신도는 학습 분포 밖 입력에서 경고를 주지 않음 → 모델 호출 전에 규칙으로 OOD 체크 (알 수 없는 인스턴스 타입, 처음 보는 에러 형식 등이면 모델 호출 없이 폴백).
- 결정 스키마: `choice`, `score`, `noul` 3종.

## 기술 스택 (제안, 착수 전 확인)

- 코어·어댑터·오퍼레이터: Go, controller-runtime (kubebuilder 레이아웃)
- KEDA external scaler: Go gRPC (KEDA externalscaler proto)
- 결정 서비스: Python 사이드카 (gRPC). 모델은 플러그인(`plumb_decision/plugins`)으로 붙인다: `laya`(로컬), `jev`(TypeSafe Jev 및 `/v1/systemone` 호환 서버), `uniform`(테스트용). Laya가 Python 전용이므로 분리.
- 테스트: Go unit test + envtest, 결정 서비스는 pytest. 로컬 통합은 kind + Karpenter CRD만 설치해 mock.

## 디렉터리 구조 (제안)

```
api/v1alpha1/            # 정책 CRD 타입 (AdaptivePolicy 등)
cmd/plumb-agent/         # 오퍼레이터 엔트리포인트
cmd/plumb-scaler/        # KEDA external scaler
internal/core/
  state/                 # 상태 요약 (원시 메트릭 → 압축 JSON)
  guardrail/             # 정적 우선, 쿼터, 예산, OOD 체크
  decision/              # 결정 서비스 클라이언트, 캐시
  hysteresis/            # 쿨다운·히스테리시스
  decisionlog/           # 결정·결과 로그 (파인튜닝 데이터)
internal/adapters/
  interfaces.go          # 어댑터 인터페이스 정의
  aws/
    provisioner/         # Karpenter NodePool 조정
    capacity/            # NodeClaim 상태·이벤트 기반 ICE 감지·정규화
decision-service/        # Python 결정 모델 사이드카 (모델 플러그인)
  questions/             # 질문 세트 정의
config/                  # CRD, RBAC, 샘플
charts/                  # Helm 차트
```

## 어댑터 인터페이스 초안

```go
type NodeProvisioner interface {
    // 정적/동적 용량을 분리해서 보고한다
    Capacity(ctx context.Context, req ResourceRequest) (CapacityReport, error)
    // 동적 프로비저닝 허용 범위를 조정한다 (limits, weight, capacity-type)
    ApplyProvisioningHint(ctx context.Context, hint ProvisioningHint) error
}

type CapacitySignalSource interface {
    // CSP별 에러를 공통 이벤트로 정규화: capacity / quota / config / unknown
    Watch(ctx context.Context) (<-chan CapacityEvent, error)
}
```

## 결정 질문 세트 초안 (ICE 이벤트)

- `ice_kind` (choice): capacity / quota / config / unknown
- `transient` (noul): 짧은 대기 후 재시도로 해결될 가능성
- `action` (choice): wait_and_retry / fallback_in_region / shift_to_other_region / escalate
- `urgency` (score): 0~3

확신도가 임계값 미만이거나 OOD면 규칙 기반 기본값으로 폴백.

## 작업 규칙

- 새 기능은 섀도 모드에서 먼저 동작해야 한다.
- 모든 결정은 입력 상태, 모델 출력, 가드레일 결과, 최종 행동을 decisionlog에 남긴다.
- CSP SDK import는 `internal/adapters/<csp>/` 밖에서 금지.
- Karpenter 에러 reason·condition 문자열은 추측하지 말고 사용 중인 Karpenter 버전 소스로 확인 후 상수로 정의.
- 새 결정 모델은 `ModelPlugin`을 구현해 플러그인으로 추가한다. 에이전트는 인스턴스마다 독립적인 타임아웃·서킷 브레이커로 섀도 호출한다.
- 상태를 클러스터 밖으로 보내는 플러그인(Jev 호스팅 API 등)은 기본 비활성, 명시적 opt-in.

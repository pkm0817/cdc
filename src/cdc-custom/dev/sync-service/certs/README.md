# 사내 TLS 검사 프록시 인증서

이 폴더의 `*.crt` 는 **빌드 환경 전용**이다. 사내망이 TLS 를 가로채 검사하는 경우
(여기서는 Somansa Root CA) 컨테이너 안에서 Maven 저장소로 가는 HTTPS 가 신뢰 실패로 끊긴다.
호스트 Windows 는 인증서 저장소에 이미 갖고 있지만 컨테이너는 아무것도 모르기 때문이다.

Dockerfile 이 이 폴더를 읽어 OS 트러스트와 JDK 트러스트 양쪽에 넣는다.
비어 있어도 빌드는 그대로 진행된다 — 사내망이 아닌 곳에서는 필요 없다.

## 다시 뽑는 법 (PowerShell)

```powershell
Get-ChildItem Cert:\LocalMachine\Root |
  Where-Object { $_.Subject -match 'Somansa' } |
  ForEach-Object {
    "-----BEGIN CERTIFICATE-----`n" +
    [Convert]::ToBase64String($_.RawData,'InsertLineBreaks') +
    "`n-----END CERTIFICATE-----" | Set-Content certs/Somansa-Root-CA.crt
  }
```

공개 인증서라 비밀은 아니지만 조직 환경에 종속되므로 저장소에는 커밋하지 않는다
(`.gitignore` 참고).

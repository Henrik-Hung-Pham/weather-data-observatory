# Security Policy

## Supported Versions

This project is an actively developed demonstration platform. Security fixes
are applied to the latest `main`.

| Version | Supported          |
| ------- | ------------------ |
| `main`  | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

Instead, report privately via GitHub's
[Private Vulnerability Reporting](https://github.com/Henrik-Hung-Pham/weather-data-observatory/security/advisories/new)
("Security" tab → "Report a vulnerability"). Include:

- a description of the issue and its impact,
- steps to reproduce (or a proof of concept),
- any known mitigations.

### What to expect

- **Acknowledgement** within 3 business days.
- A triage assessment and severity rating shortly after.
- A fix (or a documented mitigation) and a coordinated disclosure once a patch
  is available. We'll credit reporters who wish to be named.

## Scope & Hardening

Automated scanning runs in CI (see
[`.github/workflows/security.yml`](.github/workflows/security.yml)):

- **gitleaks** — secret scanning
- **pip-audit** — dependency CVEs against the pinned lock file
- **bandit** — Python static analysis
- **Trivy** — filesystem / IaC / secret scanning

Dependencies are pinned and hash-locked (`requirements.lock`), and
[Dependabot](.github/dependabot.yml) keeps them current.

> Note: the credentials committed in `.env.example` and `docker-compose.yml`
> (e.g. `observatory_secret`, the LocalStack `test` keys) are **local-development
> placeholders only**. Never use them in a real deployment.

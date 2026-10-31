# UHP contract

| File | What it is |
| --- | --- |
| `uhp-2026-10-04.openapi.yaml` | Unified Harness Protocol 2026-10-04, byte-for-byte the normalized copy in BeeOS Cloud `backend/openapi/uhp/` (beeos-cloud-backend `53299f06`), which BMA's UHP gateway also uses. sha256 `52111b18c1b067b04e2e3bd9600332a7a8ad093cedea69b9ddea8ae6fcc7ec1c`. |
| `SOURCE.json` | Upstream HarnessRouter commit and the normalizations applied to the upstream file. |

`go/uhp/generated.go` is generated from it by `tools/generate.sh`
(oapi-codegen v2.8.0 directly on OpenAPI 3.1, models only, Memories
excluded). The BMA extensions (`bma.*`) are hand-written in
`go/uhp/extensions.go`; their normative description is
`docs/architecture/uhp-harness-integration.md` in beeos-managed-agents.

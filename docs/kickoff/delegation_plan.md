# Delegation Plan

**Date:** 20 October 2026 (M0)
**Format:** AI Fluency Build Brief, kickoff deliverable 2. One decision per capability.

| Capability | Mode for this project | What that means here |
|---|---|---|
| 1. Empathy | **Human-led**, AI assists research | Sylvester plays OSRS and owns the persona definitions. AI supplied market research (discovery report). The M4 manual-trade log is the reality check on what players experience. |
| 2. Design | **Augmentation** | AI drafts layouts, copy and chart choices against the accessibility and design floors; Sylvester decides. Real-device checks (iPhone Safari, Mac Safari) are Sylvester's. |
| 3. Architecture | **Augmentation** | AI proposes options and ADRs with evidence; Sylvester decides (done for the 12 kickoff decisions). New ADRs follow the same route. |
| 4. Implementation | **Automation, moving to agency** as tests mature | AI writes code test-first; every change lands with tests and passes CI. Autonomy grows only after the guard rails (contract tests, parity checks, the pre-registered gate) have caught real failures. |
| 5. Judgement | **Human only** | Sylvester owns: the ML gate thresholds and test weeks (fixed before results are seen), what ships, every public claim, what data is published, and whether a result is "good enough". |
| 6. Shipping | **Automation with approval gates** | AI prepares deploys and PRs. Sylvester approves every state-changing step: creating the GitHub repo, merges to `main`, Cloudflare and Oracle account changes, token creation, the Discord post, and any production deploy. |

## Delegation criteria applied

| Task type | Verifiable? | Reversible? | Stakes | Mode |
|---|---|---|---|---|
| Pure functions (tax, fills, grades) | Yes, unit and property tests | Yes | Medium (wrong numbers mislead players) | Automate, verify against tests |
| Ingestion and storage code | Yes, contract, idempotency and parity tests | Mostly (raw data retained 90 days) | Medium | Automate with review |
| Backfill runs against the Wiki API | Partly (completeness checks) | No (load on a community service) | High for the relationship | Sylvester starts it, after the Discord post |
| Evaluation design and gate thresholds | No (judgement) | No once results are seen | High (credibility) | Human only |
| Public copy, claims, case study | Partly | Yes, but reputational | High | Augmentation, Sylvester signs off |
| Credentials, accounts, tokens | n/a | Varies | High | Sylvester only; agents never handle secret values in chat |

## Standing rules for any agent on this build

- Read `docs/decisions/` and `docs/adr/` before proposing tools or structure. The decision log overrides older documents.
- Verify, don't assert: quote metrics only from committed artefacts, with their date.
- No commits, pushes, deploys or account changes without Sylvester's explicit go for that action.
- Never automate the game client. Never show real-money values.
- Australian English and no em dashes in public copy.

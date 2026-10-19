# ADR-0010: Pre-registered ML promotion gate

- **Status:** Accepted
- **Date:** 2026-10-20
- **Decided by:** Sylvester (kickoff decision 7)

## Context

After a 2% tax and a typical 1.5% to 2% spread, a model can look good on error metrics and still lose money. Looking at test results and then choosing the rule invites self-deception. About 20 days of post-switch data exist at kickoff; meaningful post-switch evaluation needs roughly 8 to 12 weeks.

## Decision

For each policy (ADR-0008) separately, a model replaces that policy's baseline on the dashboard only if **both** hold on post-switch walk-forward evaluation:

1. The lower end of the 90% confidence interval for the model's gain over the baseline in **net GP per slot-day** is above zero. Intervals come from a day-block bootstrap (2,000 resamples, blocks of whole UTC days).
2. Its Brier score for P(net > 0) is no worse than the baseline's calibrated score.

Walk-forward rules: weekly retraining, testing on the following week, dropping training rows whose label window overlaps the test week, plus a 24-hour embargo.

**Pre-registration.** Before any post-switch evaluation runs for a policy, a registration file is committed at `docs/registrations/<policy_id>.md` containing:

```markdown
- Policy:
- Candidate model(s) and baseline:
- Training window rule:
- Test weeks (first and last, UTC):
- Primary metric and gate (as above):
- Secondary metrics reported (precision at 10, calibration plot, interval coverage):
- Feature list frozen at commit:
- Registered by / date:
```

The commit's timestamp, earlier than the evaluation run's, is the evidence. Changing anything after results are seen requires a new registration with new, later test weeks.

If the gate fails, the baseline stays and the negative result is published on the methodology page.

## Consequences

- ML may never reach the dashboard. That is an acceptable, publishable outcome.
- A model that passes still runs a shadow week (scores logged, not shown) before display, and keeps a one-step rollback to the previous artefact.

## Alternatives considered

- Ship ML with a disclaimer regardless: breaks the reliability promise.
- No ML in the product: loses the ML-lifecycle evidence.

## References

- Promotion guard, shadow evaluation and rollback practice carried over from an earlier delay-prediction model.
- Discovery report section 5.5.

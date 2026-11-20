# ADR 0001: Tapstate-managed AI for Pipeline Builder

Status: Accepted for implementation on 2026-11-20

## Decision

When managed AI is enabled for a Cloud deployment, workspaces without a saved
provider use Amazon Bedrock. The initial route is Kimi K3 Standard
(`global.moonshotai.kimi-k3`) through `bedrock-runtime` in `ap-southeast-1`.
The source Region and inference profile are operator configuration. Bedrock's
Chat Completions API shares the assistant's existing streaming and tool-call
parser. A separate transport signs requests with AWS SDK SigV4 and workload
credentials. The AWS destination is derived from deployment configuration.

Keep the existing workspace-owned OpenAI-compatible Chat Completions and
Responses routes. An authorized workspace AI manager can switch between managed and BYOK without re-entering
the saved BYOK key. Cloud sessions currently carry workload read/write scope
without an organization-admin claim. Configure the authorized managers by
verified Cloud user ID per deployment. Existing saved
providers stay selected until deliberately changed. On-premises installations
continue to use BYOK. Resolve the route once per request. Operator changes
can be rolled back by restoring the previous configuration or disabling
managed AI. A disabled route does not implicitly activate saved BYOK.

## Data, failure and usage

Pass the existing authorized schema and draft context to the chosen provider.
Continue to validate the complete proposal and require an explicit revision-
checked apply. AWS may process requests outside Singapore under a Global
inference profile. Keep Bedrock invocation content logging disabled for the
deployment and verify the account's AWS retention settings before enabling
customer traffic. BYOK data goes to the endpoint selected by that workspace.

Keep credential authorities separate. The browser receives route metadata only.
A transient failure may be retried on the same route before any model output
has reached the user. A partial response, access denial or exhausted allowance
ends the request. An attempted request never transfers prompts or billing to a
second provider automatically.

Set a managed input and output limit, a low reasoning default and bounded
admission per serving process. Record attempt completion, duration, known token
usage and errors without prompts, tool arguments or secrets. These safeguards
are operational limits. Commercial entitlements and shared billing policy are
tracked in #773. Verify customer access, workload identity, model access and
credit attribution separately before a production rollout.

## Acceptance

Verify that a new eligible Cloud workspace can stream a K3 response, continue
a conversation and generate a validated, unapplied DSL proposal. Exercise
managed to BYOK to managed switching, including saved-key isolation, on-prem
behavior and a midstream failure. Record live model and billing evidence
separately from tests against a simulated provider.

## References

- [Issue #1003](https://github.com/tapstate/tapstate/issues/1003)
- [Kimi K3 on Bedrock](https://docs.aws.amazon.com/bedrock/latest/userguide/model-card-moonshot-ai-kimi-k3.html)
- [Chat Completions API](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-chat-completions.html)
- [Bedrock data retention](https://docs.aws.amazon.com/bedrock/latest/userguide/data-retention.html)

## Deployment settings

Set `TAPSTATE_AI_BEDROCK_ENABLED=true` on a Cloud deployment with a verified
workload identity. Defaults for `TAPSTATE_AI_BEDROCK_REGION` and
`TAPSTATE_AI_BEDROCK_MODEL` are the Singapore endpoint and K3 Global profile
above. `TAPSTATE_AI_BEDROCK_REQUESTS_PER_MINUTE` defaults to 30 upstream
attempts per server process. Each process allows four concurrent managed
streams. A managed request sends up to 128 KiB of JSON and requests at most
4,096 completion tokens. The workload's AWS policy must permit the Bedrock
streaming action for the selected profile and its routing destinations.

Set `TAPSTATE_AI_BEDROCK_PROVIDER_MANAGERS` to the comma-separated verified Cloud
user IDs allowed to edit and activate the workspace route on that deployment.
Keep this list during operator rollback so a manager can reactivate saved BYOK.
Upgrade every serving process before enabling the managed feature. Before a
binary downgrade, managers must reactivate saved BYOK wherever it was parked,
so old binaries can read the existing credential record format.

Run the opt-in live smoke with a temporary AWS credential source available to
the Java SDK. It uses synthetic data for two real K3 calls and compiles the
returned edit to DSL.

```sh
TAPSTATE_BEDROCK_LIVE_TEST=true ./mvnw -pl app -am \
  -Dtest=ManagedBedrockLiveTest -Dsurefire.failIfNoSpecifiedTests=false test
```

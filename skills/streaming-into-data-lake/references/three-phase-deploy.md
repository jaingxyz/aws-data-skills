# Two-Phase CloudFormation Deploy (collapsed from three)

Defining the Firehose stream, the Firehose IAM role, and the Lake Formation grants in a single CloudFormation template creates a race. CloudFormation evaluates the Firehose role's effective permissions at stream-create time, before the Lake Formation grants apply. The stream creation fails with `AccessDeniedException` even though every grant in the template is correct.

The fix is to gate the Firehose stream resource on a CloudFormation `Condition` and deploy in **two effective phases** controlled by `EnableFirehose` (always `true` after first deploy) and `EnableFirehoseStream` (toggled from `false` to `true` once grants exist).

> **Note on the historical 3-phase shape.** An earlier version of this skill prescribed three phases: `EnableFirehose=false / Stream=false` (base), `EnableFirehose=true / Stream=false` (role + grants), then `Stream=true`. That collapsed in the companion repo to two effective phases because Phase A already creates the namespace and bucket; a separate "before role exists" pre-phase is redundant and re-running it tears down the error bucket / role / log group on subsequent runs (CFN parameter-inheritance footgun, below). Keep `EnableFirehose=true` from first deploy onward and only toggle `EnableFirehoseStream`.

## Template structure

```yaml
Parameters:
  EnableFirehose:
    Type: String
    Default: "false"
    AllowedValues: ["true", "false"]
    Description: Phase 2 onward. Creates the Firehose IAM role and grants.
  EnableFirehoseStream:
    Type: String
    Default: "false"
    AllowedValues: ["true", "false"]
    Description: Phase 3 only. Creates the delivery stream.

Conditions:
  CreateFirehoseRole: !Equals [!Ref EnableFirehose, "true"]
  CreateFirehoseStream: !And
    - !Equals [!Ref EnableFirehose, "true"]
    - !Equals [!Ref EnableFirehoseStream, "true"]

Resources:
  FirehoseRole:
    Type: AWS::IAM::Role
    Condition: CreateFirehoseRole
    # ...

  LFGrantOnTable:
    Type: AWS::LakeFormation::PrincipalPermissions
    Condition: CreateFirehoseRole
    # ...

  DeliveryStream:
    Type: AWS::KinesisFirehose::DeliveryStream
    Condition: CreateFirehoseStream
    # ...
```

## Deploy sequence

```bash
# Phase A: bucket + namespace + Firehose IAM role + transform Lambda + error
# bucket. Stream is held back. After CFN finishes, apply Lake Formation
# grants to the role using the lf_grant helper (see lake-formation-grants.md).
aws cloudformation deploy \
  --stack-name <stack> \
  --template-file template.yaml \
  --parameter-overrides \
    EnableFirehose=true \
    EnableFirehoseStream=false \
  --capabilities CAPABILITY_NAMED_IAM

# Apply LF grants here (out-of-CFN; see lake-formation-grants.md for the
# lf_grant helper). The grants need a real IAM principal to exist (which
# Phase A just created), and CFN cannot represent the loud-failure
# semantics the lf_grant helper enforces.

# Phase B: turn on the Firehose delivery stream. With grants now in place
# and propagated, CFN's stream-create call passes.
aws cloudformation deploy \
  --stack-name <stack> \
  --template-file template.yaml \
  --parameter-overrides \
    EnableFirehose=true \
    EnableFirehoseStream=true \
  --capabilities CAPABILITY_NAMED_IAM
```

Wait at least 30 seconds between Phase A grant application and Phase B for Lake Formation grant propagation.

## The parameter-inheritance footgun

`aws cloudformation deploy --parameter-overrides` does NOT inherit values from the previous deploy. Any parameter omitted from the CLI invocation reverts to its template default. With `Default: "false"` on the gate parameters, Phase 3 deploys that omit `EnableFirehose=true` will set it back to `false`, evaluate `CreateFirehoseRole` as false, and tear down the Firehose role and Lake Formation grants the previous phase created.

You MUST pass every gate parameter explicitly on every `aws cloudformation deploy` call after Phase 1, even ones whose value did not change. Wrap the deploy in a shell script that always exports both flags so this cannot be forgotten:

```bash
deploy_phase() {
  local enable_firehose="$1"
  local enable_stream="$2"
  aws cloudformation deploy \
    --stack-name "${STACK_NAME}" \
    --template-file template.yaml \
    --parameter-overrides \
      "EnableFirehose=${enable_firehose}" \
      "EnableFirehoseStream=${enable_stream}" \
    --capabilities CAPABILITY_NAMED_IAM
}
```

`--no-fail-on-empty-changeset` is safe to add. It does not affect parameter inheritance.

## Why this is not solvable with DependsOn

`DependsOn` makes CloudFormation wait until the listed resource finishes creating before starting the next. It does not wait for IAM policy or Lake Formation grant propagation, which is asynchronous and not visible to CloudFormation. Even with `DependsOn: [FirehoseRole, LFGrantOnTable]` on the delivery stream, the create-stream call from CloudFormation can race ahead of grant propagation.

The three-phase pattern works because each phase ends with a stable stack state, and the gap between phases (the time the operator takes to invoke the next `deploy`) gives Lake Formation time to settle.

## Drift after Phase 3

After Phase 3 succeeds, the stack is in its target state with both flags `true`. Subsequent deploys to fix unrelated resources MUST keep both flags `true`. Add a guardrail comment in the deploy script.

## Teardown

Reverse the order. Set `EnableFirehoseStream=false` first (Phase 3 reverse), wait, then `EnableFirehose=false` (Phase 2 reverse), then delete the stack. Tearing down all at once is safe but produces noisy CloudFormation events as the Firehose stream tries to write while its IAM role is being destroyed.

# Atlantis Operations

This context names the durable operational history added around Atlantis without changing how Atlantis executes Terraform.

## Language

**Run**:
One accepted logical Atlantis operation against a repository and, when applicable, a pull request and commit.
_Avoid_: Job, task, execution

**Project Run**:
One Terraform project's participation in a Run, including the plan and policy-check stages that belong to the same logical plan operation.
_Avoid_: Project job, step run

**Output Chunk**:
An ordered block of command output belonging to a Project Run.
_Avoid_: Log line, terminal row

**Audit Event**:
An immutable record of a significant operational or security action associated with a repository and, when applicable, a Run.
_Avoid_: Domain event, source event

**Plan Artifact**:
An opaque Terraform or OpenTofu plan stored outside durable history and referenced from a Project Run.
_Avoid_: Plan output, plan record

**Drift Result**:
The durable outcome of checking one project against one resolved commit for infrastructure drift.
_Avoid_: Drift status, drift cache entry

**Remediation**:
A recorded attempt to produce or execute a fresh plan in response to one or more Drift Results.
_Avoid_: Auto-fix, drift apply

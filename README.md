# Domain State Event Logging (DSEL)

A pragmatic persistence pattern for platform engineering that stores complete domain object state in events—eliminating replay complexity while maintaining full temporal history.

## Why Read This?

If you're building platform engineering systems (internal developer platforms, Kubernetes-style APIs, deployment orchestration) and struggling with:

- **Event Sourcing complexity**: Tired of rebuilding state through event replay?
- **CQRS overhead**: Don't want to maintain separate read/write models?
- **Platform evolution**: Need to rapidly add new workflows, domain objects, and capabilities without downtime or breaking existing functionality?
- **Audit requirements**: Need complete temporal history for compliance?
- **Global distribution**: Want multi-region consistency without coordination complexity?

**DSEL might be your answer.** This pattern has been battle-tested in multiple mission-critical production systems serving thousands of users globally, with proven implementations on both MongoDB and DynamoDB.

## Key Innovation

Instead of storing event deltas (traditional Event Sourcing) or maintaining separate projections (CQRS), DSEL stores **complete domain object state snapshots within event envelopes**. This trades storage cost for operational simplicity—a worthwhile tradeoff in 2025 when storage is cheap but engineer time is expensive.

## What You'll Learn

📄 **[Read the Full Paper](DomainStateEventLogging.md)**

The paper covers:

- **Full-State Event Envelopes**: How to eliminate replay complexity while maintaining temporal history
- **Multi-Resolution Event Streams**: Fine-grained domain events + coarse-grained aggregates for efficient queries
- **Multi-Tier Read Architecture**: DAX/DynamoDB/OpenSearch with graceful fallback for optimal latency
- **Global Distribution**: Multi-region consistency using DynamoDB Global Tables, Cassandra, or Spanner
- **Platform Evolution Made Easy**: Add new workflows, event types, and domain objects without downtime, migrations, or breaking existing queries
- **Production Track Record**: Real metrics from systems serving thousands of users

## Quick Example

```yaml
# Traditional Event Sourcing stores deltas:
kind: ArtifactUpdatedEvent
changes:
  - field: version
    old: "1.0"
    new: "1.1"

# DSEL stores complete state:
kind: ArtifactAppliedEvent
spec:
  artifact:
    name: terraform-vpc-module
    version: "1.1"
    regions: [us-east-1, us-west-2]
    bundles: [...]
    # Complete domain object state
```

**Result**: Query latest state directly from events—no replay needed.

## Platform Evolution Without Breaking Changes

DSEL's key-based isolation design means you can evolve your platform rapidly:

**Add New Workflows**:
```yaml
# Day 1: Only have artifact deployment
kind: ArtifactAppliedEvent

# Day 30: Add approval workflow - existing queries unaffected
kind: ApprovalRequestedEvent
kind: ApprovalGrantedEvent

# Day 60: Add canary deployment - still no migration needed
kind: CanaryDeploymentStartedEvent
kind: CanaryDeploymentValidatedEvent
```

**Why This Matters**:
- **No database migrations**: New event types use different partition keys
- **No downtime**: Existing queries continue working unchanged
- **No code changes required**: Old workflows keep functioning
- **Rapid iteration**: Ship new platform capabilities in hours, not weeks

This isn't just schema evolution—it's **platform capability evolution**. Add entire new workflows, approval processes, deployment strategies, or domain objects without coordinating changes across your codebase.

## Who Should Read This

- Platform engineers building internal developer platforms
- Architects designing event-driven systems
- Teams maintaining Kubernetes-style declarative APIs
- Organizations requiring audit trails and temporal queries
- Anyone who found Event Sourcing too complex but CRUD too limiting

## Implementation Status

**Production-Validated**: Multiple mission-critical systems, thousands of users, multi-year operational history

**Proven Datastores**:
- MongoDB (earlier implementations)
- DynamoDB (current generation)
- Pattern works with Cassandra, Spanner, or any globally-distributed database

## Author

Trevor Samaroo

## License

CC BY 4.0

---

**[Start Reading →](DomainStateEventLogging.md)**

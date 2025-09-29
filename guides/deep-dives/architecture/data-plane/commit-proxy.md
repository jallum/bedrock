# Commit Proxy

The [Commit Proxy](../../../glossary.md#commit-proxy) serves as the central orchestrator of Bedrock's [transaction](../../../glossary.md#transaction) commit process, transforming individual client transaction requests into efficiently processed batches while maintaining strict consistency guarantees. It coordinates the complete lifecycle from submission to durable persistence, managing [conflict resolution](../../../glossary.md#conflict), [durability](../../../glossary.md#durability-guarantee), and client notification in a carefully orchestrated pipeline.

**Location**: [`lib/bedrock/data_plane/commit_proxy/server.ex`](../../../lib/bedrock/data_plane/commit_proxy/server.ex)

## System Role and Context

The Commit Proxy occupies the critical junction between client transaction requests and the distributed storage infrastructure. It receives prepared transactions from [Transaction Builders](../../../glossary.md#transaction-builder) and orchestrates their processing through multiple specialized components: [Resolvers](../../../glossary.md#resolver) for conflict detection, [Log servers](../../../glossary.md#log) for durable persistence, and the [Sequencer](../../../glossary.md#sequencer) for version ordering.

This central coordination role enables sophisticated optimizations impossible with direct client-to-storage interactions. The component aggregates transactions into batches that amortize processing overhead, implements intra-batch conflict detection[^1], and ensures atomic all-or-nothing durability guarantees across the distributed log infrastructure.

## Core Concepts

### Batching Strategy

The fundamental design principle underlying the Commit Proxy is **batch aggregation**—collecting multiple individual transactions and processing them together as a cohesive unit. This approach creates a critical trade-off between latency and efficiency:

- **Efficiency Gains**: Fixed costs of conflict resolution, logging coordination, and version assignment are amortized across multiple transactions
- **Latency Impact**: Individual transactions must wait for batch completion, increasing response times
- **Conflict Detection**: Batches enable detection of conflicts both with previously committed data and within the current batch itself
- **Ordering Preservation**: Transaction arrival order is maintained within batches to ensure deterministic results

Batching policies are configurable through size limits (`max_per_batch`) and time limits (`max_latency_in_ms`), allowing operators to balance system throughput against responsiveness based on workload characteristics.

### Transaction Format Integration

The Commit Proxy operates on Bedrock's optimized [Transaction binary format](../../../glossary.md#transaction-format), which provides tagged section-based encoding. This structured format enables selective data access throughout the processing pipeline:

- **Conflict Resolution**: Extract only READ_CONFLICTS and WRITE_CONFLICTS sections
- **Logging**: Utilize complete transactions with MUTATIONS and COMMIT_VERSION sections  
- **Validation**: Verify integrity through embedded CRC32 checksums

The format's self-describing architecture with selective section access dramatically reduces processing overhead by avoiding unnecessary data transfer and deserialization costs.

## Transaction Processing Flow

The Commit Proxy implements a sophisticated **finalization pipeline**—a precise state machine that transforms transaction batches from initial aggregation through final client notification. This pipeline ensures atomicity through careful coordination of distributed operations while maintaining comprehensive safety mechanisms.

### Finalization Pipeline: Linear State Progression

Each batch advances through a precisely defined sequence of eight states managed by the `FinalizationPlan` struct. The pipeline is inherently linear, with each step building on the previous state's output:

---

#### Step 1: Batch Initialization

**State Transition**: `:initialized → :created`

**Purpose**: Capture batch metadata and establish [version](../../../glossary.md#version) context

**Actions Performed**:

- Assign a unique commit version to the batch
- Initialize the `FinalizationPlan` structure
- Record batch creation timestamp and metadata

**Output**: Prepared batch ready for transaction processing

---

#### Step 2: Transaction Preparation  

**State Transition**: `:created → :ready_for_resolution`

**Purpose**: Transform transactions for conflict detection processing

**Actions Performed**:

- Extract conflict information from transaction payloads
- Organize read and write conflicts by key ranges
- Prepare transaction summaries for resolver coordination

**Output**: Structured conflict data organized for efficient resolver distribution

---

#### Step 3: Conflict Resolution

**State Transition**: `:ready_for_resolution → :conflicts_resolved`

**Purpose**: Coordinate with [Resolvers](../../../glossary.md#resolver) to detect conflicts

**Actions Performed**:

- Distribute transactions to appropriate resolvers based on key ranges
- Execute parallel conflict detection across all affected resolvers
- Collect conflict resolution results and retry on transient failures

**Output**: Complete conflict analysis determining which transactions can proceed

**Implementation Details:**

The Commit Proxy coordinates conflict detection across the resolver infrastructure through sophisticated range-based filtering and parallel processing:

**Range-Based Distribution**

```elixir
# Transaction filtering by resolver key ranges
filter_transactions_for_resolver(transactions, resolver_key_range)

# Multi-range transaction coordination  
coordinate_across_multiple_resolvers(transaction, affected_resolvers)
```

- **Key Range Mapping**: Determines which resolvers need each transaction based on key range intersections
- **Multi-Range Transactions**: Coordinates with all affected resolvers for transactions spanning multiple ranges
- **Efficient Fan-out**: Transmits only relevant transaction summaries to each resolver to minimize network overhead
- **Conflict Deduplication**: Automatically removes duplicate read conflicts before resolution processing

**Retry Logic and Error Handling**

```elixir
# Exponential backoff configuration
default_timeout_fn(attempts_used) -> 500 * (1 <<< attempts_used)

# Telemetry-tracked retry execution
:telemetry.execute(
  [:bedrock, :commit_proxy, :resolver, :retry],
  %{attempts_remaining: attempts_remaining - 1},
  %{reason: reason}
)
```

The system implements exponential backoff with configurable retry limits (default 2 attempts) to handle transient resolver failures without overwhelming the system. All retry attempts emit [telemetry](../../../glossary.md#telemetry) events for operational monitoring.

---

#### Step 4: Abort Notification

**State Transition**: `:conflicts_resolved → :aborts_notified`

**Purpose**: Separate successful transactions from aborted ones, send immediate abort responses

**Actions Performed**:

- Identify transactions that failed conflict resolution
- Send immediate error responses to clients of aborted transactions
- Update reply tracking to prevent duplicate notifications
- Filter successful transactions for logging phase

**Output**: Clean set of conflict-free transactions ready for durability processing

---

#### Step 5: Logging Preparation

**State Transition**: `:aborts_notified → :ready_for_logging`

**Purpose**: Organize mutations by [storage team](../../../glossary.md#storage-team) tags for efficient distribution

**Actions Performed**:

- Map transaction keys to storage team tags
- Group mutations by responsible log servers
- Optimize data distribution to minimize network overhead

**Output**: Mutations organized by target log servers for parallel transmission

**Implementation Details:**

**Tag-Based Mutation Distribution**

```elixir
# Determine storage team tags for each mutation
key_or_range_to_tags(key_or_range, storage_teams)

# Find logs responsible for these tags
find_logs_for_tags(affected_tags, logs_by_id)

# Distribute mutations to appropriate logs
Map.update!(mutations_acc, log_id, &[mutation | &1])
```

The distribution algorithm ensures each log server receives exactly the mutations relevant to its storage responsibility:

- **Key-to-Tag Mapping**: Efficiently maps keys and ranges to storage team tags
- **Tag Intersection Logic**: Determines log server requirements based on tag coverage
- **Range Operation Handling**: Special logic for `{:clear_range, start_key, end_key}` operations spanning multiple teams
- **Mutation Deduplication**: Prevents redundant data transfer by filtering mutations per log

---

#### Step 6: Durable Persistence

**State Transition**: `:ready_for_logging → :logged`

**Purpose**: Push transactions to all required [log servers](../../../glossary.md#log) and await acknowledgments

**Actions Performed**:

- Transmit mutations to all required log servers in parallel
- Wait for acknowledgment from every required log server
- Enforce all-or-nothing durability guarantee

**Output**: Durably persisted transactions with universal log server confirmation

**Implementation Details:**

Bedrock's absolute durability guarantee requires acknowledgment from **all** required log servers before considering any transaction committed. The Commit Proxy orchestrates this through sophisticated mutation distribution and parallel coordination.

**Universal Acknowledgment Requirements**

The logging system enforces strict all-or-nothing durability through parallel push operations:

- **Parallel Transmission**: All required log servers receive transactions simultaneously
- **Universal Acknowledgment**: Every required log must acknowledge before commit success
- **Failure Aggregation**: Comprehensive collection and reporting of any log server failures
- **Latency Characteristics**: Overall durability latency is bounded by the slowest required log server

---

#### Step 7: Version Coordination

**State Transition**: `:logged → :sequencer_notified`

**Purpose**: Report successful commits to [Sequencer](../../../glossary.md#sequencer) for version tracking

**Actions Performed**:

- Notify sequencer of successful commit version advancement
- Update global version state for future transaction processing
- Maintain version consistency across the system

**Output**: Updated global version state reflecting committed transactions

---

#### Step 8: Client Notification

**State Transition**: `:sequencer_notified → :completed`

**Purpose**: Send success responses to clients and complete processing

**Actions Performed**:

- Send success responses to all remaining clients
- Update reply tracking to mark all transactions as notified
- Complete the finalization plan and clean up batch state

**Output**: Fully processed batch with all clients notified and system state updated

---

### Pipeline Safety Mechanisms

The linear progression incorporates comprehensive safety mechanisms at each step to prevent inconsistent system states:

- **Reply Tracking**: The `replied_indices` MapSet prevents double-replies and ensures all clients receive appropriate responses
- **Immediate Abort Notification**: Failed transactions receive immediate error responses at Step 4, enabling fast client retries
- **Fail-Fast Design**: Pipeline failures at any step trigger clean abortion of all unreplied transactions with proper client notification
- **Error Recovery**: Transient failures are handled through exponential backoff retry logic with configurable limits
- **State Persistence**: Each state transition is atomic, ensuring the pipeline can recover from failures without data loss

## Operational Considerations

### Performance Characteristics and Tuning

The Commit Proxy's performance is governed by fundamental trade-offs in batching configuration:

#### Batching Parameters

- **Maximum Batch Size**: Larger batches improve throughput but increase memory usage and conflict probability
- **Maximum Batch Latency**: Shorter timeouts reduce transaction latency but may prevent optimal batch sizes
- **Empty Transaction Timeout**: Maintains read version advancement during low-activity periods

#### Workload Optimization

- **High-Throughput Workloads**: Benefit from larger batch sizes and longer timeout values
- **Low-Latency Workloads**: Achieve better results with smaller batches and shorter timeout intervals
- **Pipeline Latency Floor**: The finalization pipeline introduces inherent processing latency that cannot be eliminated through parameter tuning

### Monitoring and Observability

The system provides comprehensive operational visibility through structured [telemetry](../../../glossary.md#telemetry) events:

#### Key Performance Indicators

```elixir
# Batch lifecycle tracking
[:bedrock, :data_plane, :commit_proxy, :start]
%{n_transactions: count}, %{commit_version: version, started_at: timestamp}

[:bedrock, :data_plane, :commit_proxy, :stop]  
%{n_oks: successes, n_aborts: aborts, duration_μs: duration}
```

Essential metrics for operational health monitoring:

- **Batch Processing Latency**: End-to-end processing time indicating system responsiveness
- **Transaction Throughput**: Transactions per second measuring system capacity
- **Abort Rates**: Ratio revealing conflict patterns and system efficiency
- **Resolver Retry Frequency**: Infrastructure health indicator
- **Batch Size Distribution**: Workload characteristics and tuning effectiveness

#### Common Operational Issues

1. **High Abort Rates**: Investigate conflict resolution telemetry for hot key patterns
2. **Resolver Unavailability**: Monitor retry events for infrastructure connectivity issues  
3. **Log Server Failures**: Track acknowledgment timeouts and push failures
4. **Batching Inefficiency**: Analyze size distribution and adjust configuration parameters

### Error Recovery and Resilience

The Commit Proxy employs a **fail-fast recovery model** consistent with other Bedrock components. When unrecoverable errors occur—such as persistent resolver unavailability or insufficient log acknowledgments—the component exits immediately, triggering [Director](../../../glossary.md#director)-coordinated system recovery.

This approach prioritizes consistency over availability by avoiding complex partial recovery scenarios. Failed instances are replaced with fresh components that begin operation with clean, well-defined initial state. In-flight transactions are lost during recovery, requiring client-side retry logic as part of Bedrock's consistency-first design philosophy.

## System Integration

The Commit Proxy coordinates with multiple specialized components throughout the transaction lifecycle:

- **[Transaction Builders](../../../glossary.md#transaction-builder)**: Receive prepared transaction data for batch processing
- **[Resolvers](../../../glossary.md#resolver)**: Coordinate conflict detection with range-based filtering and parallel processing
- **[Log Servers](../../../glossary.md#log)**: Ensure durable persistence through universal acknowledgment requirements
- **[Sequencer](../../../glossary.md#sequencer)**: Report successful commits for version consistency maintenance
- **[Director](../../../glossary.md#director)**: Coordinate recovery operations and cluster layout updates

These integration points are designed for both resilience and optimal performance, implementing appropriate timeout mechanisms to prevent resource leaks and providing comprehensive telemetry for system-wide optimization.

> **Complete Transaction Flow**: For the full transaction processing sequence showing the Commit Proxy's role in context, see **[Transaction Processing Deep Dive](../../../deep-dives/transactions.md)**.

## Related Components

- **[Transaction Builder](../infrastructure/transaction-builder.md)**: Prepares and submits transaction data
- **[Sequencer](sequencer.md)**: Provides commit version assignment and ordering
- **[Resolver](resolver.md)**: Performs MVCC conflict detection for batches
- **[Log](log.md)**: Provides durable transaction persistence
- **[Director](../control-plane/director.md)**: Manages component recovery and coordination

## Related Guides

- **[Transaction Format](../../../quick-reads/transaction-format.md)**: Binary format specification and encoding details
- **[Transactions Overview](../../../quick-reads/transactions.md)**: Complete transaction processing and ACID guarantees

[^1]: Intra-batch conflict detection examines transactions within the same batch for conflicts with each other, preventing incompatible transactions from being committed together while maximizing batch success rates.

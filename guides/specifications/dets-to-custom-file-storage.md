# Technical Specification: Replace DETS with Custom File-Based Storage

Replace DETS storage with a custom file-based storage system to eliminate write lag that blocks reads while maintaining data durability and integrity.

## Summary
- **Type:** optimization
- **Complexity:** complex
- **Breaking Change:** no
- **Confidence:** 85%

## Current Implementation

### Affected Components
| File Path | Functions | Current Behavior |
|-----------|-----------|------------------|
| lib/bedrock/data_plane/storage/olivine/database.ex | open(), store_value(), load_value(), advance_durable_version() | Uses DETS for persistent storage with ETS lookaside buffer |
| lib/bedrock/data_plane/storage/olivine/logic.ex | startup(), shutdown() | Opens DETS file during startup |

### Detailed Current Behavior
The current system uses DETS (Disk-based Erlang Term Storage) for persistent storage with a two-tier architecture:

1. **Hot Storage (ETS)**: Recent versions stored in lookaside buffer as `{{version, key}, value}`
2. **Cold Storage (DETS)**: Durable versions persisted to disk as `{key, value}`
3. **Window Advancement**: Periodic flush from ETS to DETS with full sync operations

**Performance Issues:**
- DETS write operations block all reads during sync
- Large batch writes cause significant latency spikes
- Sync operations are atomic and block the entire table

## Required Changes

### Behavior Modifications
Replace DETS with a custom file format that supports:
- Non-blocking reads during writes
- Incremental data storage without full table locks
- Recovery from file corruption
- Backward pointer traversal for efficient recovery

### File Format Specification

#### Value File Structure
```
[DATA_BLOCK_1][DATA_BLOCK_2]...[DATA_BLOCK_N][FOOTER]
```

#### Data Block Format
```
[VALUE_1][VALUE_2]...[VALUE_M]
```

#### Individual Value Format
```
[KEY_LENGTH:4][KEY:KEY_LENGTH][VALUE_LENGTH:4][VALUE:VALUE_LENGTH]
```

#### 64-bit Locator Format
```
<<offset::big-47, size::big-17>>
```
- **offset**: 47 bits = 128TB max file size
- **size**: 17 bits = 128KB max value size

#### Footer Format
```
[MAGIC_NUMBER:8][LAST_PAGE_BLOCK_POINTER:8]
```
- **MAGIC_NUMBER**: `<<"OLIVINE">>`
- **LAST_PAGE_BLOCK_POINTER**: 64-bit offset to most recent page block

### Implementation Steps

#### Step 1: Create Custom Storage Module
- **File:** lib/bedrock/data_plane/storage/olivine/file_storage.ex
- **Purpose:** Replace DETS operations with custom file I/O

#### Step 2: Implement Pre-allocation in Database.store_value()
- **File:** lib/bedrock/data_plane/storage/olivine/database.ex
- **Function:** store_value/3
- **Change:** Add internal offset tracking for immediate locator generation
- **Implementation:**
  ```elixir
  def store_value(database, key, value) do
    # Pre-allocate space and generate locator
    locator = allocate_space(database, key, value)

    # Store in ETS with locator for immediate access
    :ets.insert(database.buffer, {locator, value})

    # Queue for background persistence
    enqueue_for_persistence(database, key, value, locator)

    {:ok, locator}
  end
  ```

#### Step 3: Update ETS Schema
- **File:** lib/bedrock/data_plane/storage/olivine/database.ex
- **Current Schema:** `{{version, key}, value}`
- **New Schema:** `{locator, value}`
- **Change:** Replace version+key composite keys with 64-bit locators

#### Step 4: Implement Background Writer Process
- **File:** lib/bedrock/data_plane/storage/olivine/background_writer.ex
- **Purpose:** Asynchronous persistence without blocking reads
- **Functions:**
  - `start_link/1` - Initialize background writer
  - `enqueue_write/3` - Queue write operations
  - `flush_batch/1` - Write accumulated data

#### Step 5: Implement Recovery System
- **File:** lib/bedrock/data_plane/storage/olivine/recovery.ex
- **Functions:**
  - `recover_from_file/1` - Read file from footer backward
  - `validate_footer/1` - Check magic number and structure
  - `rebuild_index/1` - Reconstruct page index from data blocks

#### Step 6: Update IndexManager Integration
- **File:** lib/bedrock/data_plane/storage/olivine/index_manager.ex
- **Change:** Update to work with locators instead of version+key pairs
- **Functions to modify:**
  - `apply_transactions/3` - Generate locators for new values
  - `page_for_key/3` - Return pages with locator references

#### Step 7: Implement File Operations
- **File:** lib/bedrock/data_plane/storage/olivine/file_ops.ex
- **Functions:**
  - `append_data_block/2` - Write new data blocks
  - `update_footer/2` - Update footer with new page block pointer
  - `read_at_offset/3` - Read value at specific locator
  - `fsync/1` - Force data to disk

## Edge Cases & Error Handling

| Scenario | Current Behavior | New Behavior |
|----------|------------------|--------------|
| File corruption | DETS repair or failure | Recovery from valid data blocks, skip corrupted |
| Disk full during write | DETS write failure | Graceful degradation, maintain read access |
| Process crash during write | DETS inconsistency | Background writer restart, replay from ETS |
| Concurrent reads during write | Blocked by DETS sync | Continue uninterrupted |
| Large value storage | DETS memory pressure | Stream to file with progress tracking |
| File size limits | DETS file size limits | Support up to 128TB with 47-bit offsets |

## Testing Requirements

### Unit Tests
- Test locator generation and parsing
- Test file format writing/reading
- Test recovery from various corruption scenarios
- Test background writer queue management
- Test offset tracking accuracy

### Integration Tests
- Test complete startup/shutdown cycle
- Test concurrent read/write operations
- Test recovery after process crash
- Test large file operations
- Test disk space exhaustion handling

### Performance Tests
- Benchmark read latency during writes
- Compare write throughput vs DETS
- Test recovery time with large files
- Validate memory usage patterns

## Dependencies & Impacts

### Direct Dependencies
- Erlang file I/O primitives
- ETS for in-memory indexing
- Background process supervision
- No new external packages required

### Downstream Impacts
- Storage interface remains unchanged (no breaking changes)
- Improved read performance during writes
- Potential increase in memory usage for background queues
- Need to monitor disk space usage patterns

## Implementation Checklist

- [ ] Custom file storage module with locator management
- [ ] Background writer process with queue management
- [ ] File format implementation (data blocks + footer)
- [ ] Recovery system with backward traversal
- [ ] ETS schema migration from {version+key} to {locator}
- [ ] IndexManager integration with locator generation
- [ ] Error handling for file I/O operations
- [ ] Performance telemetry and monitoring
- [ ] Unit tests for all file operations
- [ ] Integration tests for startup/recovery
- [ ] Performance benchmarks vs DETS
- [ ] Documentation for new file format

## Key Implementation Decisions

1. **64-bit Locator Design**: 47-bit offset + 17-bit size provides optimal balance of file size support (128TB) and value size (128KB) within a single 64-bit integer
2. **Pre-allocation Strategy**: Generate locators immediately in `store_value()` to enable instant ETS insertion and non-blocking reads
3. **Footer-based Recovery**: Store magic number and last page block pointer in footer for efficient backward traversal during recovery
4. **Background Writer**: Decouple persistence from reads using asynchronous background process with batched writes
5. **ETS Schema Change**: Replace `{{version, key}, value}` with `{locator, value}` to align with locator-based access pattern
6. **No Migration Support**: Since this is experimental storage, no backward compatibility with existing DETS files required
7. **Append-only Design**: Simplify implementation and improve crash consistency by always appending new data blocks
8. **Stream-based Large Values**: Handle large values through streaming I/O to prevent memory pressure issues

## Performance Goals

- **Eliminate Read Blocking**: Zero read latency increase during write operations
- **Improved Write Throughput**: Background batching should improve overall write performance
- **Faster Recovery**: Footer-based approach should reduce startup time compared to full DETS scan
- **Memory Efficiency**: Locator-based references should reduce memory overhead compared to full key storage
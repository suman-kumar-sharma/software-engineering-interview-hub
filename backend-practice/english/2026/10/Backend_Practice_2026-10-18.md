# Kafka Concurrent Processing: Commit the Safe Prefix
Date: 2026-10-18 | Java 21; Apache Kafka Java client 4.0 API reference
Level: advanced backend design | 15 minutes: 3 concept, 8 solution, 4 walkthrough
Prerequisites: partitions, consumer groups, durable transactions, offset commits. Builds on retry budgets, but addresses a different correctness boundary.

## Concept question with answer — 3 minutes
Partition P delivers offsets 100, 101, 102. Workers finish 102 first, then 100; 101 remains unfinished. Can you commit 103?
**No.** A committed offset is the resume position, not a receipt for one successful record. Committing 103 could skip unfinished 101 after a crash. After 100 completes, the safe resume boundary is 101. Once all three are durably completed, the batch's next-offset metadata is safe (103 in this example).

Concurrency changes completion order. An offset commit summarizes a prefix of records, so the commit boundary must follow completed delivery order, not max(completed offset). Numeric offsets may have gaps; track actual delivered records rather than waiting for every integer offset.

## Practical exercise — 8 minutes
A consumer sends PostgreSQL writes to workers to keep polling responsive. Design:
- One owner thread uses the KafkaConsumer; workers never share its API.
- Auto commit disabled.
- Bounded pending/in-flight records and bounded completion storage.
- No commit skips a durably unfinished record.
- Rebalances ignore stale completions; duplicates are safe.
- Per-partition ordering of effects is not required for this example; events are independent.
- A failed record is unresolved until retry succeeds or a durable policy hands it off.

## Derivation from requirements
1. Poll position advances when records are returned, before worker effects complete; it cannot be used as completed position.
2. Each partition needs its own ordered delivery ledger. Track record offset, leader epoch, assignment token and durable completion state.
3. A worker completion is a message to the owner thread, not permission to commit directly.
4. Remove only completed entries from the ledger's head. The first remaining entry gives the safe resume offset. When exhausted, use ConsumerRecords.nextOffsets metadata saved for the fully drained batch.
5. Use bounded dispatch and partition pause/resume so continued polling does not create an unlimited backlog.
6. Rebalance callbacks invalidate local assignment tokens. Tokens reject stale local completion notifications, but do not fence external database writes.
7. Make database effects idempotent because process crashes and failed commits still cause replay.

## Complete design pseudocode
```text
poll-owner thread only:
    disable auto commit
    for each assigned partition: create a fresh local epoch token

    loop:
        drain worker completion queue
        ignore completion if partition token != current assignment token
        mark matching delivered entry durablyDone only on durable success

        for each partition:
            while ordered head is durablyDone:
                remove head
            if entries remain:
                candidateCommit = first unfinished delivered record.offset
                candidateLeaderEpoch = that record.leaderEpoch
            else if a batch was delivered:
                candidateCommit = last delivered batch.nextOffsets[partition]
            # No advancement unless a completed prefix was removed.
            # Only commit candidates for partitions currently owned.

        commit explicit candidate map periodically (bounded commitSync)
        # On commit failure do not pretend it succeeded; preserve retry state.
        # No overlapping async commits or blind stale-offset retries in this design.

        dispatch pending delivered records into bounded workers without blocking
        pause partitions near high watermark; resume below low watermark
        records = poll(short timeout)     # continue polling even while paused
        append every returned record in delivery order to its partition ledger
        retain each partition's batch.nextOffsets metadata
        # Register returned records before accepting their worker completions.
        # Capacity must reserve room for a complete max.poll.records batch.

worker:
    process record with idempotent durable effect
    enqueue(partition, assignmentToken, offset, durableSuccessOrFailure)
    # Never call poll/commit/pause/resume/seek on shared consumer.

on revoke(partitions), on poll-owner thread:
    stop dispatch for these partitions
    drain available completions and compute safe prefixes
    optionally await bounded drain within rebalance/time budget
    commit safe explicit offsets while still valid; handle failure
    invalidate tokens and discard local ownership state
    # unfinished or uncommitted work can replay

on lost(partitions):
    invalidate ownership immediately; do not commit lost partitions
```

This is algorithmic pseudocode, not a complete runnable consumer. A ledger includes buffered but not yet dispatched records: dispatch rejection never drops a polled record. Commit metadata should retain the appropriate leader epoch via OffsetAndMetadata/nextOffsets rather than only a naked numeric offset. A candidate must not advance beyond any delivered unfinished entry. Known offset gaps do not require fictitious entries.

### Boundedness and liveness
Example engineering limits: max.poll.records=50, ledger capacity=200 per assigned partition, pause threshold=150, resume threshold=75, 20 globally admitted workers. Limits are illustrative. Reserve at least one poll batch beyond the pause threshold, cap global storage across partitions too, and account for assignment growth. max.poll.records bounds returned records, not all broker fetch-buffer memory or payload bytes; configure fetch/payload limits separately.

Completion storage needs capacity for all outstanding tasks plus a drain strategy; do not let a full queue deadlock bounded shutdown. Pause affects future delivery, not records already returned. Do not sleep/retry indefinitely on the poll-owner thread. Bound commitSync duration and processing in each loop to respect max.poll.interval.ms. Review actual group protocol and rebalance behavior for the deployed client; these examples do not depend on a universal timeout default.

### Durable database effect
In one PostgreSQL transaction, insert an inbox row keyed by (consumer_identity,event_id), and if newly inserted apply the business mutation. Commit the transaction before reporting durable success. Duplicate detection and business effects must be atomic. Deduplicate on a stable business/event key; a Kafka offset alone does not detect the same logical event republished at another offset. Inbox retention must cover the replay horizon. External payment calls need their own idempotency/reconciliation contract.

### Failed records
Retry with finite backoff/budget outside the owner loop. Keep the ledger head unresolved while retrying. If moving to a dead-letter topic, consider it terminal only after confirmed durable publication or an atomic durable outbox handoff. DLT publication can duplicate after a crash; preserve event identity. A DLT policy trades automatic processing for operational repair; retain reason, original location, payload and alert. For ordered business events, skipping a failed record may violate the domain even if offsets are safe.

## Dry runs — 4 minutes
Delivered [100,101,102], saved batch next=103:
1. 102 done: head 100 unfinished; no advancement.
2. 100 done: remove 100; first unfinished=101; commit 101.
3. Crash: resume at 101; 102 may run again; inbox prevents duplicate effects.
4. 101 done before crash: remove 101 then already-done 102; commit saved next=103.

Gap case: delivered [100,104,109], next=110. If only 100 finishes, commit 104. Do not wait for nonexistent offsets 101..103.

Rebalance case: old task token A finishes after P is reassigned under token B. Ignore notification A. Its DB effect may still commit; idempotency, or external fencing where required, handles overlap.

## Mistakes and tradeoffs
- Committing highest completion+1 loses unfinished earlier work.
- Parameterless commitSync can commit consumer positions ahead of workers; use an explicit safe map.
- Blocking poll until a slow worker completes risks liveness.
- A local epoch is not an external fencing token.
- An inbox prevents duplicate effects, not out-of-order business effects.
- Per-partition serial workers simplify this design and preserve partition order. Prefer that if throughput requirements allow; add concurrency only for a demonstrated need.
- More workers than database capacity produces queues, not useful throughput.
- Measure unfinished oldest-record age, ledger size, paused duration, worker utilization, commit failures, replay/dedup rate and lag. Low lag alone can hide premature commits.

## Follow-ups with answers
**Does this give exactly-once delivery?** No: at-least-once processing with idempotent durable effects; replay remains possible.
**What about same-key ordering?** Serialize partition processing, or use carefully bounded per-key lanes with the same partition commit barrier and stale-write protection.
**Why not retry failed commit blindly later?** Assignment may have changed; re-evaluate current ownership and safe position.
**What about onPartitionsLost?** Ownership may already be elsewhere; invalidate and avoid committing those partitions.
**Interview answer:** “I isolate the consumer API on one thread and track per-partition delivered records. Workers report durable completions, and commits advance only over the completed prefix. I bound buffering with pause/resume, reject stale assignment completions, and make downstream effects idempotent because replay and overlapping old workers remain possible.”

## Verification plan and limits
Script completion permutations; commit boundary must never exceed the first unresolved delivered record. Test gaps, worker rejection, DLT failure, DB rollback, crash after DB commit, commit failure, revoke/lost callbacks, late stale completion, partial cooperative revocation and capacity during assignment growth. This report provides a design and test plan; it does not claim broker integration tests were executed.

## Primary references
- [Kafka 4.0 KafkaConsumer API: offsets, threading and manual commits](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Kafka 4.0 ConsumerRebalanceListener API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html)

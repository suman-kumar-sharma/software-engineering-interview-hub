# Kafka Workers Parallel Hain Toh Offset Kab Commit Karein?
Date: 2026-10-18 | Java 21; Kafka Java client 4.0 API reference
Plan: 3 minute concept, 8 minute design, 4 minute walkthrough. Prerequisites: partition, consumer group, DB transaction aur offset basics.

## Concept question aur answer
Partition P se offsets 100,101,102 aaye. Worker ne pehle 102 complete kiya, phir 100. 101 abhi chal raha hai. Kya 103 commit kar sakte hain?
**Nahi.** Socho attendance register mein keh diya: 103 se aage ka kaam baaki hai. Ab crash hua toh Kafka 101 ko dobara kyun bhejega? Tumne usko already finished prefix mein include kar diya.

Offset commit kisi ek successful record ki receipt nahi; restart boundary hai. 100 done aur 101 pending hai toh safe boundary 101 hai. Saare done ho gaye toh saved batch next offset 103 commit kar sakte ho.

## Practical exercise
Consumer polling karta rahe, PostgreSQL writes workers karein. Requirements:
- KafkaConsumer ka owner ek thread.
- Auto commit off.
- Buffer aur workers bounded.
- Unfinished earlier record kabhi skip na ho.
- Rebalance ke baad old completion ignore ho.
- Duplicate processing se duplicate DB effect na ho.
Is example mein events independent hain; business effect ordering required nahi.

## Problem se solution kaise nikla
**Pehla observation:** poll ne record diya matlab processing complete nahi. Consumer position automatically aage ja sakti hai, worker abhi DB mein busy ho sakta hai.

**Isliye ledger:** Har partition ka ordered register rakho: kaunse actual records mile, kaun durable done hai. Highest finished offset se commit decide mat karo.

**Ab ownership:** Worker KafkaConsumer use karega toh thread safety issue. Worker sirf completion message owner ko bheje. Owner register update aur commit kare.

**Ab commit barrier:** Register ke front se done entries hatao. Jo pehli pending entry bachi, wahi restart boundary. Pura batch drain ho toh saved nextOffsets metadata use karo. Offsets mein gaps ho sakte hain; har integer ke liye completion ka wait nahi.

**Ab overload:** Poll chalta rahe aur worker slow ho toh RAM bhar jayegi. High watermark par partition pause, low par resume. Already-returned records phir bhi safely buffer karne honge.

**Ab rebalance:** Purana worker late finish karega. Har assignment ko local token do; naya assignment aaya toh purana token invalid. Lekin token sirf local notification reject karta hai, purane DB write ko magically cancel nahi karta.

## Complete design
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

Ye design pseudocode hai, runnable consumer class nahi. response queue aur ledger ka concrete implementation tumhare framework par depend karega. Ledger mein dispatched ke saath buffered records bhi register karo; executor reject hua toh polled record disappear nahi hona chahiye. Leader epoch metadata preserve karo; fully drained batch ka nextOffsets use karna useful hai.

## Boundedness ka practical example
max.poll.records=50, per-partition ledger capacity=200, pause at 150, resume at 75, globally 20 workers. Ye sample numbers hain. Pause threshold ke upar ek complete poll batch ki jagah reserve karo. Total partitions badhenge toh total memory bhi badhegi: global cap bhi chahiye.

max.poll.records sirf returned record count limit hai; payload bytes aur internal fetch buffer ka complete RAM cap nahi. Fetch size/payload limits separately configure karo. Completion queue outstanding tasks ko handle kar sake aur shutdown par deadlock na ho.

Paused hone par bhi poll karte raho. Owner thread par long retry/sleep mat chalao. Commit timeout aur loop work bounded rakho, max.poll.interval.ms respect karo. Actual deployment ka group protocol aur callback behavior inspect karo; universal default assume mat karo.

## DB duplicate effect kaise rokoge?
Ek transaction mein inbox insert karo: unique key (consumer_identity,event_id). Insert new hai toh business update karo, phir same transaction commit. Already present hai toh duplicate business mutation mat karo.

Ab important baat: inbox insert alag transaction aur business update alag hua toh crash window ban jayegi. Dono atomic hone chahiye. Stable event/business identity use karo; same logical event doosre Kafka offset par republish ho sakta hai. Inbox ko replay horizon se pehle delete mat karo.

External payment ke liye ye DB trick alone enough nahi; provider idempotency aur uncertain result reconciliation chahiye.

## Failure aur DLT
Failed head record retry ho raha hai toh commit barrier wahi rukega. Finite backoff/budget use karo; poll thread free rakho. DLT par bhejne ko success tab bolo jab durable publish confirmed ho, ya durable transactional outbox ne responsibility le li ho.

DLT publish ke baad crash hua toh duplicate DLT possible; original event identity retain karo. Ordered business events mein failed record skip karna business correctness tod sakta hai. DLT ka matlab operation team ko repair responsibility bhi dena: reason, location, payload aur alert preserve karo.

## Dry run
Records [100,101,102], batch next=103:
1. 102 finish: front 100 pending, commit advance nahi.
2. 100 finish: front remove; ab 101 pending, safe commit=101.
3. Crash: 101 se replay; 102 dobara aa sakta hai, DB inbox duplicate effect rokega.
4. Agar 101 bhi finish ho: 101 aur already-done 102 remove, saved next=103 safe.

Gap example [100,104,109], next=110: sirf 100 done hua toh boundary 104. Nonexistent 101,102,103 ka wait kyun karna?

Rebalance example: purana token A, naya B. A ki completion late aayi toh ignore. Old task DB mein phir bhi complete ho sakta hai; idempotency ya external fencing se overlap handle hoga.

## Common mistakes aur tradeoffs
- Highest completed+1 commit karna: earlier pending work lose ho sakta hai.
- Empty-argument commitSync: consumer position worker completion se aage ho sakti hai.
- Local assignment token ko DB fencing samajhna: woh sirf local state guard hai.
- Inbox ko ordering guarantee samajhna: dedup aur order alag requirements hain.
- Bahut workers launch karna: DB limited ho toh queue badhegi.
- Per-partition serial processing simpler hai; business order required ho toh pehle wahi choose karo.
- Monitor oldest pending age, paused duration, ledger size, commit failure, replay rate aur lag. Low lag kabhi-kabhi premature commits ka misleading symptom bhi hai.

## Interview follow-ups with answers
**Exactly-once hai?** Delivery nahi; replay possible hai. Durable side effects idempotent banaye hain.
**Same order ID ke events ordered chahiye?** Partition serialization ya bounded per-key lanes, aur partition commit barrier. Stale updates ka guard bhi chahiye.
**Commit fail hua toh blind retry?** Nahi, ownership aur latest safe position recheck.
**onPartitionsLost?** Ownership already elsewhere ho sakti hai; local state invalidate, lost partition commit mat karo.
**Professional answer:** “I keep KafkaConsumer on one owner thread, track delivered records per partition, and advance commits only across a durably completed prefix. I bound workers and buffers, handle rebalance epochs, and use idempotent effects because replay and old worker overlap remain possible.”

## Verification
Completion order permutations, offset gaps, DB rollback, worker rejection, DLT failure, crash after DB commit, commit failure, late completion, cooperative partial revoke aur assignment growth test karo. Invariant: commit kabhi first unresolved delivered record ke aage nahi. Yahan design/test plan hai; broker integration tests run hone ka claim nahi.

## Primary sources
- [Kafka 4.0 KafkaConsumer API: offsets, threading and manual commits](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Kafka 4.0 ConsumerRebalanceListener API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html)

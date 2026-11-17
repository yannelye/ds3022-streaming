# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

**Name:** Yannely Escalante Ruiz  
**Date:** November 09, 2026

## 1. The source

- **Source and URL:** Bluesky Jetstream — `wss://jetstream2.us-east.bsky.network/subscribe`
- **Method (WebSocket push, REST polling or REST history pull):** WebSocket push. Jetstream converts the Bluesky firehose into JSON messages.
- **Account, key or User-Agent needed:** No account or API key required.
- **Limits I know about (rate limits, paging, reconnects):** The stream sends messages quickly, so the producer needs to keep up. If the connection drops, I will need to reconnect. Jetstream supports limited replay using a cursor, but the exact replay window has not been verified. Deleted posts may arrive without the original record.

## 2. One real message

This is an actual message from my Bluesky probe, with the text shortened.

```json
{
  "did": "did:plc:hryuw4d4vyq26y3sxs3f6bmc",
  "time_us": 1794938305453215,
  "cursor": 27287296488,
  "kind": "commit",
  "commit": {
    "rev": "3n2jmm3emyd2p",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n2jmm3ej3d2p",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-17T17:58:25.322998+00:00",
      "langs": ["en"],
      "text": "folklore Album Cardigan Cable Knit Sweater Pants in size L is back in stock!"
    },
    "cid": "bafyreigysmqdbn4u5jgdrcagmusbwwb3byiobttlt2radzirrempgtuyyy"
  }
}
```

## 3. Volume maths

- **Measured rate (from the probe):** 48.87 messages per second (2,934 messages collected in 60 seconds).
- **Live capture needed for 50,000:** Approximately 1,023 seconds, or 17 minutes.
- **History / backfill available? How many days?** Jetstream supports limited replay using a time cursor. The exact number of days available has not been verified.
- **My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov):** My original plan was to run the producer for approximately 25–30 minutes to allow extra time for changes in message volume or connection interruptions. Since I received an extension, I will complete the collection on November 17 and verify that at least 50,000 raw records have been landed.

## 4. Topic design

- **Topic name:** `bluesky` (created with `-p 3 -r 3` before the producer runs)
- **Kafka key:** `did`
- **Why this key (ordering, spread across partitions):** The DID identifies a Bluesky account. Using it as the Kafka key keeps events from the same account in order within one Kafka partition. Different accounts can be distributed across the three partitions.

## 5. Duplicates

- **What makes two records the same Kafka record? (landed twice by the lander)** Two records with the same Kafka topic, partition, and offset represent the same Kafka message. If the lander processes the same offset again, I should only count it once.
- **What makes two records the same thing (the same trade, post, aircraft position)?** The same Bluesky post can be identified using `did` and `commit.rkey`. The operation also matters because a post can be created and later deleted.
- **If the same thing can arrive in several versions, which one do I keep?** I will keep all raw events for the original dataset. For an analysis requiring one current record per post, I will use the latest event based on its timestamp and operation.

## 6. Research question (draft)

How does Bluesky posting activity vary by hour of day, and which languages have the highest posting activity during the collection period?

I can answer this by grouping posts into hourly time windows using their timestamps and comparing the number of posts by language using `commit.record.langs`. I will focus on post creation events and exclude other event types when calculating posting activity.

## 7. Self-check (checkpoint A)

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [ ] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
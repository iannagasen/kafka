
### Acknowledgements
  - Recommended Replication Factor -> 3
  - Recommended Acknowsledgement Type -> -1 (all)
  - Safest Min InSync Replicas -> 2, 
    - this is to make sure that 1 leader and 1 replica should acknowledge
    - this guarantees 1 replica can get your data
  - Suppose:
    -  3 is the Replication Factor 2 are replicas, and the other 1 is the leader.
    -  Min ISR -> 2
    - What happen if the 2 replicas are down?
      - The broker will send back to Producer "NOT ENOUGH REPLICAS" 
        - means that broker do not want to accept the rights, rather than risk of lossing data

### Kafka Topic Availability
  - Availability: (considering RF -> 3) 
    - `acks=0` || `acks=1`
      - if one partition is up and considered an ISR, the topic will be available for writes
    - `acks=-1` ALL
      - `min.insync.replicas=1` (default)
        - the topic must have atleast 1 partition up as an ISR (including the leader)
        - can tolerate 2 brokers down
      - `min.insync.replicas=2`
        - the topic must have atleast 2 partition up as an ISR (including the leader)
        - can tolerate at most 1 broker going down
        - guarantee that data will be atleast written twice
      - `min.insync.replicas=3`
        - not make sense
        - this could not tolerate if any broker is down
          - producer could not write if any broker is down
    - Summary:
      - when `acks=-1` with `replication.factor=N` and `min.insync.replicas=M`
        - we can tolerate atleast N-M brokers down for topic availability purpose
      - most popular option for durability and availability
        - `acks=-1` & `min.insync.replicas=2`
          - allows to withstand at most the lost of 1 broker, assuming RF=3
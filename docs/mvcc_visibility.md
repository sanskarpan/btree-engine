# MVCC Visibility Rules

A tuple version `(Xmin, Xmax)` is **visible** to transaction `T` with snapshot `S` iff all nine cases below pass.

## Flowchart

```
IsVisible(tuple, T, S):

                 ┌─────────────────────┐
                 │  CHECK XMIN         │
                 │  (creating txn)     │
                 └────────┬────────────┘
                          │
          ┌───────────────▼──────────────────┐
          │  Xmin == T.ID (own write)?       │
          └───────────────┬──────────────────┘
           YES ──────────►│◄──────── NO
                          │                   │
              ┌───────────▼───────────┐       │
              │  Skip to Xmax check   │       │
              └───────────────────────┘       │
                                              │
                          ┌───────────────────▼───────────────────┐
                          │  status(Xmin) == COMMITTED?            │
                          └───────────────────┬───────────────────┘
                               NO ────────────► INVISIBLE
                                              │ YES
                          ┌───────────────────▼───────────────────┐
                          │  Xmin >= S.XmaxSnapshot?              │
                          │  (Xmin is a future txn)               │
                          └───────────────────┬───────────────────┘
                               YES ───────────► INVISIBLE
                                              │ NO
                          ┌───────────────────▼───────────────────┐
                          │  Xmin in S.XipList?                   │
                          │  (was active when snapshot taken)     │
                          └───────────────────┬───────────────────┘
                               YES ───────────► INVISIBLE
                                              │ NO
                                              │ (Xmin committed before snapshot)
                                              │
                 ┌────────────────────────────▼────────────────────┐
                 │  CHECK XMAX                                      │
                 │  (deleting txn)                                  │
                 └────────────────────────────┬────────────────────┘
                                              │
                          ┌───────────────────▼───────────────────┐
                          │  Xmax == 0?  (not deleted)            │
                          └───────────────────┬───────────────────┘
                               YES ───────────► VISIBLE
                                              │ NO
                          ┌───────────────────▼───────────────────┐
                          │  Xmax == T.ID? (we deleted it)        │
                          └───────────────────┬───────────────────┘
                               YES ───────────► INVISIBLE
                                              │ NO
                          ┌───────────────────▼───────────────────┐
                          │  status(Xmax) == ABORTED?             │
                          │  (deletion rolled back)               │
                          └───────────────────┬───────────────────┘
                               YES ───────────► VISIBLE
                                              │ NO
                          ┌───────────────────▼───────────────────┐
                          │  Xmax >= S.XmaxSnapshot?              │
                          │  (deleter is a future txn)            │
                          └───────────────────┬───────────────────┘
                               YES ───────────► VISIBLE
                                              │ NO
                          ┌───────────────────▼───────────────────┐
                          │  Xmax in S.XipList?                   │
                          │  (deleter was active at snapshot)     │
                          └───────────────────┬───────────────────┘
                               YES ───────────► VISIBLE
                                              │ NO
                                              │ (deleter committed before snapshot)
                                              ▼
                                          INVISIBLE
```

## Snapshot Structure

```
Snapshot {
  XminSnapshot  = lowest active TxnID when snapshot was taken
  XmaxSnapshot  = next TxnID to assign (upper bound of visibility)
  XipList       = []TxnID of all in-progress txns at snapshot time
  CreatorTxnID  = T.ID (own writes are always visible)
}
```

## Isolation Level Behavior

| Level              | When snapshot is taken       | Anomalies prevented          | Allowed                   |
|--------------------|------------------------------|------------------------------|---------------------------|
| Read Committed     | Before **each** `Get()` call | Dirty reads                  | Non-repeatable reads      |
| Snapshot Isolation | Once at `BEGIN`              | Dirty reads, non-repeatable  | Write skew                |

## Write Skew (Snapshot Isolation)

Write skew occurs when two concurrent SI transactions each read overlapping data, then each write disjoint keys based on what they read:

```
T1 reads: alice_on_call=true, bob_on_call=true  (snapshot S1)
T2 reads: alice_on_call=true, bob_on_call=true  (snapshot S2)

T1 writes: alice_on_call=false  (no write-write conflict with T2)
T2 writes: bob_on_call=false    (no write-write conflict with T1)

Final state: both clocked out — invariant violated!
```

SI does not detect this because neither transaction conflicts on the same key. Serializable Snapshot Isolation (SSI) would detect the anti-dependency cycle and abort one of them.

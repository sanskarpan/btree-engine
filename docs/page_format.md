# Page Format

Every page is exactly **4096 bytes** with a fixed 32-byte header followed by a slotted record area.

## Page Layout

```
Byte offset   Field           Size   Description
──────────────────────────────────────────────────────────────────
 [0  : 4)     Magic           4      0xB17EE501 — identifies a valid page
 [4  : 5)     Type            1      1=Internal, 2=Leaf, 3=Meta, 4=Free
 [5  : 7)     NumSlots        2      Number of slot entries in the slot array
 [7  : 9)     FreeStart       2      Byte offset of next free slot entry
 [9  : 11)    FreeEnd         2      Byte offset of the top of the data heap
 [11 : 19)    PageLSN         8      LSN of the last WAL record that dirtied this page
 [19 : 20)    Flags           1      Bit 0=IsRoot, Bit 1=IsDirty, Bit 2=HasOverflow
 [20 : 24)    PrevSib         4      Left sibling page ID (leaf: linked list; internal: leftmost child)
 [24 : 28)    NextSib         4      Right sibling page ID (leaf: linked list; internal: rightmost child)
 [28 : 32)    Checksum        4      CRC32 of bytes [0:28]
──────────────────────────────────────────────────────────────────
 [32 : 32+N*4) SlotArray      4*N    N slots of 4 bytes each: [Offset:2][Length:2]
                                     Offset=0 or Length=0 means tombstone
 ...           (free space)          Grows down from FreeEnd; slot array grows up from FreeStart
 [FreeEnd:4096) DataHeap      var    Records stored from high addresses downward
──────────────────────────────────────────────────────────────────
```

## Visual Representation

```
0                              4096
┌──────────────────────────────────────────────────────────────────┐
│  HEADER (32 bytes)                                               │
│  Magic│Type│NumSlots│FreeStart│FreeEnd│PageLSN│Flags│Prev│Next│CRC│
├──────────────────────────────────────────────────────────────────┤
│  Slot[0]  │  Slot[1]  │  Slot[2]  │  ...  │  Slot[N-1]          │
│  [Off:Len]│  [Off:Len]│  [Off:Len]│       │                     │
├──────────────────────────────────────────────────────────────────┤
│                   ← Free Space →                                  │
│  FreeStart points here ↑        ↑ FreeEnd points here            │
├──────────────────────────────────────────────────────────────────┤
│  ...                      │ Record[2] │  Record[1] │  Record[0] │
│                            ↑ Data grows downward from 4096        │
└──────────────────────────────────────────────────────────────────┘
```

## Slot Array

Each 4-byte slot entry:

```
 Bit 31..16   Bit 15..0
┌────────────┬────────────┐
│  Offset    │  Length    │
│  (uint16)  │  (uint16)  │
└────────────┴────────────┘
  0 = tombstone if Length == 0
```

Records are sorted by key within the slot array (maintained via binary-search insertion). The data heap grows downward — records added last have the lowest byte addresses.

## MVCC Tuple Wire Format (Leaf Pages)

Each record in a leaf page is an MVCC tuple:

```
Byte offset   Field           Size   Description
──────────────────────────────────────────────────────────
 [0  : 8)     Xmin            8      TxnID that created this version
 [8  : 16)    Xmax            8      TxnID that deleted this version (0 = alive)
 [16 : 18)    KeyLen          2      Length of the key in bytes
 [18 : 20)    ValLen          2      Length of the value in bytes
 [20 : 21)    Flags           1      Bit 0=Deleted, Bit 1=Updated, Bit 2=Locked
 [21 : 21+K)  Key             K      Raw key bytes
 [21+K: end)  Value           V      Raw value bytes
──────────────────────────────────────────────────────────
```

## Internal Page Separator Format

Each record in an internal page is a separator key → child pointer pair:

```
 [0 : 2)    KeyLen   2   Length of separator key
 [2 : 2+K)  Key      K   Separator key bytes
 [2+K: 2+K+4) ChildID 4  Right-child page ID
```

The leftmost child (for keys less than all separators) is stored in `PrevSib`. The rightmost child (for keys greater than all separators) is stored in `NextSib`.

# LMDB Internals

2 files:
- data file
- lock file

## Page types

- P_BRANCH
- P_LEAF
- P_OVERFLOW
- P_META
- P_LEAF2
- P_SUBP

## Overview

Page 0 and 1 are `P_META` and alternate.

## DBs

There are 2 DBs in LMDB:
- Free pages
- Main

When using named DBs the main DB stores the name -> DB root mapping. 
The `MDB_db` (normally in the Metapage) is stored as the data of the entry.

### Cost of having a high limit of max DBs

The `txn` has to allocate some space for every named DB to hold the data about it. 
This is initialized on `mdb_txn_begin`. On Commit it iterates through all named DBs 
to check if the DB has changed and its new root needs storing.

I think (tbc) roots get lazily read as DBs are touched in a txn. This would mean:
- reading/writing to many DBs in a txn is slower than the same actions all on the same DB
- the speed of main DB vs. 1 named DB should be similar once you do enough ops on it (no repeated cost)

### Overflow pages

They are consecutive pages. This should mean that COW is done over the entire area,
so modifying a small piece of a huge object would be inefficient.

## Questions
- Named DBs
  - how does the max number of DBs affect things
  - how much slower is named DB vs. main DB
  - when do roots get read?
- How is DUP_SORT DB implemented
- How are overflow pages implemented
  - cost of modifying a huge object is huge?
- How is space in a leaf/branch managed when data is variable size
- how is free DB structured
- how does number of readers / max readers affect things?
- does fragmentation in a page get reclaimed other than through page splits/merges?

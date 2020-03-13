# LMDB Internals

2 files:
- data file
- lock file

## Questions
- Named DBs
  - how does the max number of DBs affect things
  - how much slower is named DB vs. main DB
  - when do roots get read?
- How are overflow pages implemented
  - cost of modifying a huge object is huge?
- how is free DB structured
- how does number of readers / max readers affect things?
- does fragmentation in a page get reclaimed other than through page splits/merges?


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

### Space in a Leaf

At the page start is a sorted array of node pointers. At the end of the page (growing to the left) is the nodes.
Inserting a node will append the data into the free space. The pointer list is shifted as required to make space. 
Data is not moved in such situations.

When entries change content they stay where they are. When they grow/shrink they are removed, all other data shifted
to the right and we are re-added in the free space. So there is no fragmentation within pages.

### Overflow pages

They are consecutive pages. This should mean that COW is done over the entire area,
so modifying a small piece of a huge object would be inefficient.

### DUPSORT

This allows storing multiple distinct values per key.

They are stored either:
- Same as normal (if single value)
- On a sub-page (value of the key is a page which can then be iterated over etc.)
- A sub-db (value of the node is a DB object same as for named DBs)

Where multiple are present they are stored as key so I assume size restrictions apply.

I assume we switch from sub-page to sub-db when the value gets too large for the page. A normal value
becomes an overflow page here but this results in a sub-db.

## Structs

#### MDB_Page (mostly common structure)

| Field         | Size          | Notes |
| ------------- |:-------------:|-------|
| p_pgno        | 8             |
| mp_pad        | 2             | key size or DUP_FIXED
| mp_flags      | 2             | 
| pb_lower      | 2             | end of pointer list
| pb_upper      | 2             | start of data space
| node pointers |               | count based on pb_lower - 16 * 2
| data          |               | starting from the right up to pb_upper

Overflow page is a bit different. Always consicutive, only first page has header. count of overflow 
instead of pb_upper and pb_lower.

#### MDB_Meta

| Field         | Size          | Notes |
| ------------- |:-------------:|-------|
| mm_magic      | 4             |
| mm_version    | 4             | 
| mm_address    | 8             | for fixed mmap address env
| mm_mapsize    | 8             | size of mmap region
| Free DB       | 48            | MDB_DB for free DB
| Main DB       | 48            | MDB_DB for main DB
| mm_last_pg    | 8             | last page used in DB
| mm_txnid      | 8             | txn that wrote this meta

#### MDB_Db

| Field             | Size          | Notes |
| ----------------- |:-------------:|-------|
| md_pad            | 4             | whats the difference between this and the field on the pages?
| md_flags          | 2             |
| md_depth          | 2             |
| md_branch_pages   | 8             |
| md_leaf_pages     | 8             |
| md_overflow_pages | 8
| md_entries        | 8             |
| md_root           | 8             | page number of the root page for this DB
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

## Questions
- Named DBs
  - how does the max number of DBs affect things
  - how much slower is named DB vs. main DB
- How is DUP_SORT DB implemented
- How are overflow pages implemented
- How is space in a leaf/branch managed when data is variable size
- how is free DB structured

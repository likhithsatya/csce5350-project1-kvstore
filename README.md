# CSCE 5350 - Project 1: Key-Value Store

## Author
- **Name**: Likhith Satya Neerukonda
- **EUID**: 11800658
- **Date**: October 18, 2025

## Description
Simple persistent key-value store with append-only log storage.

## Features
- SET, GET, EXIT commands
- Persistent storage in data.db
- In-memory indexing
- Crash recovery via log replay
- Last-write-wins semantics

## Running
\\\ash
python kv_store.py
\\\

## Commands
- \SET <key> <value>\ - Store key-value pair
- \GET <key>\ - Retrieve value
- \EXIT\ - Exit program

## Example
\\\
SET user1 Alice
OK
GET user1
Alice
EXIT
\\\

## Implementation
- **Language**: Python 3
- **Index**: Linear array search
- **Storage**: Binary append-only log
- **Persistence**: fsync() for durability

## Test Results
All Gradebot tests passing ✓

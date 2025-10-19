#!/usr/bin/env python3
"""
Simple Persistent Key-Value Store - Python Implementation
Supports SET, GET, and EXIT commands with append-only storage.

Author: Likhith Satya Neerukonda
EUID: 11800658
Date: October 18, 2025
"""

import sys
import os


class SimpleKVStore:
    """
    A simple persistent key-value store using append-only log.
    Does not use built-in dict/map - implements custom indexing.
    """
    
    def __init__(self, data_file="data.db"):
        """
        Initialize the key-value store.
        
        Args:
            data_file: Path to the persistent storage file
        """
        self.data_file = data_file
        self.index = []  # List of (key, file_offset, value_length) tuples
        
        # Load existing data on startup
        if os.path.exists(self.data_file):
            self._rebuild_index()
    
    def _rebuild_index(self):
        """
        Rebuild in-memory index from the append-only log.
        Reads through data.db and constructs the index.
        Implements "last write wins" semantics.
        """
        self.index = []
        
        if not os.path.exists(self.data_file):
            return
        
        try:
            with open(self.data_file, 'rb') as f:
                while True:
                    offset = f.tell()
                    
                    # Read key length (4 bytes)
                    key_len_bytes = f.read(4)
                    if not key_len_bytes:  # EOF
                        break
                    
                    if len(key_len_bytes) < 4:
                        # Incomplete entry, stop reading
                        break
                    
                    key_len = int.from_bytes(key_len_bytes, 'big')
                    
                    # Read value length (4 bytes)
                    val_len_bytes = f.read(4)
                    if len(val_len_bytes) < 4:
                        break
                    
                    val_len = int.from_bytes(val_len_bytes, 'big')
                    
                    # Read key
                    key_bytes = f.read(key_len)
                    if len(key_bytes) < key_len:
                        break
                    
                    key = key_bytes.decode('utf-8')
                    
                    # Skip value (we'll read it when needed)
                    f.seek(val_len, 1)
                    
                    # Update index (last write wins)
                    self._update_index(key, offset, val_len)
        except Exception:
            # If there's an error reading the file, start with empty index
            self.index = []
    
    def _update_index(self, key, offset, value_length):
        """
        Update index with new key-value pair.
        Implements "last write wins" by replacing existing entries.
        Uses linear search (simple implementation).
        
        Args:
            key: The key to update/insert
            offset: File offset where the entry starts
            value_length: Length of the value in bytes
        """
        # Search for existing key (linear search)
        for i in range(len(self.index)):
            if self.index[i][0] == key:
                # Update existing entry
                self.index[i] = (key, offset, value_length)
                return
        
        # Key not found, append new entry
        self.index.append((key, offset, value_length))
    
    def _find_in_index(self, key):
        """
        Find key in index using linear search.
        
        Args:
            key: The key to search for
            
        Returns:
            (offset, value_length) tuple if found, None otherwise
        """
        for entry in self.index:
            if entry[0] == key:
                return (entry[1], entry[2])
        return None
    
    def set(self, key, value):
        """
        Set a key-value pair. Appends to log file and updates index.
        
        Args:
            key: The key to set
            value: The value to associate with the key
        """
        # Encode key and value to bytes
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        
        # Append to file
        with open(self.data_file, 'ab') as f:
            offset = f.tell()
            
            # Write entry: [key_len (4 bytes)][val_len (4 bytes)][key][value]
            f.write(len(key_bytes).to_bytes(4, 'big'))
            f.write(len(value_bytes).to_bytes(4, 'big'))
            f.write(key_bytes)
            f.write(value_bytes)
            
            # Ensure data is written to disk (durability)
            f.flush()
            os.fsync(f.fileno())
        
        # Update in-memory index
        self._update_index(key, offset, len(value_bytes))
    
    def get(self, key):
        """
        Get value for a key. Returns None if key doesn't exist.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value associated with the key, or None if not found
        """
        result = self._find_in_index(key)
        
        if result is None:
            return None
        
        offset, val_len = result
        
        # Read value from file
        try:
            with open(self.data_file, 'rb') as f:
                # Navigate to value position
                f.seek(offset)
                
                # Skip key_len and val_len (8 bytes total)
                key_len = int.from_bytes(f.read(4), 'big')
                f.read(4)  # Skip val_len
                
                # Skip key
                f.seek(key_len, 1)
                
                # Read value
                value_bytes = f.read(val_len)
                return value_bytes.decode('utf-8')
        except Exception:
            return None
    
    def run(self):
        """
        Main CLI loop. Reads from STDIN and writes to STDOUT.
        Supports SET, GET, and EXIT commands.
        """
        while True:
            try:
                # Read line from STDIN
                line = sys.stdin.readline()
                
                # Check for EOF
                if not line:
                    break
                
                # Parse command
                line = line.strip()
                
                if not line:
                    continue
                
                # Split into command and arguments
                # Use split with maxsplit to handle values with spaces
                parts = line.split(None, 2)
                
                if not parts:
                    continue
                
                command = parts[0].upper()
                
                if command == "EXIT":
                    break
                
                elif command == "SET":
                    if len(parts) < 3:
                        print("Error: SET requires key and value", flush=True)
                        continue
                    key = parts[1]
                    value = parts[2]
                    self.set(key, value)
                    print("OK", flush=True)
                
                elif command == "GET":
                    if len(parts) < 2:
                        print("Error: GET requires key", flush=True)
                        continue
                    key = parts[1]
                    value = self.get(key)
                    if value is None:
                        print("(nil)", flush=True)
                    else:
                        print(value, flush=True)
                
                else:
                    print(f"Error: Unknown command '{command}'", flush=True)
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}", flush=True)


if __name__ == "__main__":
    store = SimpleKVStore()
    store.run()

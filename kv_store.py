#!/usr/bin/env python3
"""
Simple Persistent Key-Value Store - Python Implementation
Supports SET, GET, and EXIT commands with append-only storage.

This implementation demonstrates fundamental database concepts:
- Append-only log for durability
- In-memory indexing for performance
- Crash recovery via log replay
- ACID properties (Atomicity, Consistency, Isolation, Durability)

Author: Likhith Satya Neerukonda
EUID: 11800658
Date: October 18, 2025
"""

import sys
import os
from typing import Optional, Tuple, List


class SimpleKVStore:
    """
    A simple persistent key-value store using append-only log storage.
    
    Architecture:
    - Data is stored in a binary append-only log file (data.db)
    - In-memory index maps keys to file offsets for fast lookups
    - Implements "last write wins" semantics for duplicate keys
    - Does not use built-in dict/map - custom indexing for educational purposes
    
    File Format:
    Each entry: [key_length: 4 bytes][value_length: 4 bytes][key: variable][value: variable]
    
    Attributes:
        data_file (str): Path to the persistent storage file
        index (List[Tuple[str, int, int]]): In-memory index storing (key, file_offset, value_length)
    """
    
    def __init__(self, data_file: str = "data.db") -> None:
        """
        Initialize the key-value store.
        
        On startup, if a data file exists, we rebuild the in-memory index
        by replaying the entire log. This implements crash recovery.
        
        Args:
            data_file: Path to the persistent storage file (default: "data.db")
        """
        self.data_file: str = data_file
        
        # Index structure: List of tuples (key, offset, value_length)
        # We use a list for simplicity - O(n) search complexity
        # For production, a B+Tree would provide O(log n) performance
        self.index: List[Tuple[str, int, int]] = []
        
        # Load existing data on startup if file exists
        # This is the crash recovery mechanism
        if os.path.exists(self.data_file):
            self._rebuild_index()
    
    def _rebuild_index(self) -> None:
        """
        Rebuild in-memory index from the append-only log.
        
        This method implements crash recovery by replaying the transaction log.
        It reads through the entire data.db file sequentially and reconstructs
        the index in memory.
        
        Process:
        1. Start at beginning of file
        2. Read each entry (key_len, val_len, key, value)
        3. Update index with latest offset for each key
        4. Implements "last write wins" - newer entries overwrite older ones
        
        Time Complexity: O(n * m) where n = entries in file, m = unique keys
        Space Complexity: O(k) where k = number of unique keys
        """
        # Clear any existing index data
        self.index = []
        
        # Safety check: ensure file exists before attempting to read
        if not os.path.exists(self.data_file):
            return
        
        try:
            # Open file in binary read mode ('rb')
            # Binary mode is crucial for reading our custom binary format
            with open(self.data_file, 'rb') as f:
                # Continue reading until we reach end of file
                while True:
                    # Get current position in file - this is our offset
                    # We'll store this to know where to seek when reading the value later
                    offset = f.tell()
                    
                    # Read key length (4 bytes, big-endian format)
                    # Big-endian is standard network byte order
                    key_len_bytes = f.read(4)
                    
                    # EOF check: if we can't read 4 bytes, we've reached the end
                    if not key_len_bytes:
                        break
                    
                    # Corruption check: partial reads indicate corrupted file
                    if len(key_len_bytes) < 4:
                        break
                    
                    # Convert bytes to integer (big-endian format)
                    key_len = int.from_bytes(key_len_bytes, 'big')
                    
                    # Read value length (4 bytes, big-endian)
                    val_len_bytes = f.read(4)
                    if len(val_len_bytes) < 4:
                        break
                    
                    val_len = int.from_bytes(val_len_bytes, 'big')
                    
                    # Read the actual key bytes
                    key_bytes = f.read(key_len)
                    if len(key_bytes) < key_len:
                        break
                    
                    # Decode key from UTF-8 bytes to string
                    key = key_bytes.decode('utf-8')
                    
                    # Skip over the value - we don't need to read it during index rebuild
                    # We only need to know WHERE it is (offset) and HOW LONG it is (val_len)
                    # Seek relative to current position (1 = SEEK_CUR)
                    f.seek(val_len, 1)
                    
                    # Update index with this entry
                    # If key already exists, this will update it (last write wins)
                    self._update_index(key, offset, val_len)
        
        except Exception:
            # If there's any error reading the file, start with empty index
            # Better to lose data than to crash - defensive programming
            self.index = []
    
    def _update_index(self, key: str, offset: int, value_length: int) -> None:
        """
        Update index with new key-value pair.
        
        Implements "last write wins" semantics:
        - If key exists, update its offset to the new location
        - If key is new, append to index
        
        Uses linear search for simplicity (O(n) complexity).
        A production system would use a B+Tree for O(log n) performance.
        
        Args:
            key: The key to update or insert
            offset: File offset where this entry begins
            value_length: Length of the value in bytes
        """
        # Linear search through index to find existing key
        # This is O(n) but acceptable for educational purposes and small datasets
        for i in range(len(self.index)):
            if self.index[i][0] == key:
                # Key already exists - update it with new offset
                # This implements "last write wins" semantics
                self.index[i] = (key, offset, value_length)
                return
        
        # Key not found in index - append new entry
        # Tuple format: (key, offset_in_file, length_of_value)
        self.index.append((key, offset, value_length))
    
    def _find_in_index(self, key: str) -> Optional[Tuple[int, int]]:
        """
        Find key in index using linear search.
        
        This is a simple O(n) search algorithm. For better performance,
        a B+Tree or hash table would be used in production.
        
        Args:
            key: The key to search for
            
        Returns:
            Tuple of (offset, value_length) if found
            None if key doesn't exist
        """
        # Iterate through all entries in index
        for entry in self.index:
            # Each entry is a tuple: (key, offset, value_length)
            if entry[0] == key:
                # Found the key! Return its location in file
                return (entry[1], entry[2])
        
        # Key not found in index
        return None
    
    def set(self, key: str, value: str) -> None:
        """
        Set a key-value pair. Appends to log file and updates index.
        
        This operation demonstrates ACID properties:
        - Atomicity: Each write is a complete entry
        - Consistency: Index stays in sync with file
        - Isolation: Single-threaded, no concurrency issues
        - Durability: fsync() ensures data reaches disk
        
        Process:
        1. Encode key and value to bytes (UTF-8)
        2. Get current file size (this becomes our offset)
        3. Write: [key_length][value_length][key][value]
        4. Call fsync() to force write to physical disk
        5. Update in-memory index
        
        Args:
            key: The key to set (string)
            value: The value to associate with the key (string)
        """
        # Convert strings to bytes using UTF-8 encoding
        # UTF-8 is a variable-length encoding that handles all Unicode characters
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')
        
        # Open file in append+binary mode ('ab')
        # 'a' = append (writes go to end of file)
        # 'b' = binary (no text processing)
        with open(self.data_file, 'ab') as f:
            # Get current position in file - this is where our entry starts
            # This offset will be stored in the index
            offset = f.tell()
            
            # Write entry in our binary format:
            # [4 bytes: key length][4 bytes: value length][key bytes][value bytes]
            
            # Write key length as 4-byte big-endian integer
            f.write(len(key_bytes).to_bytes(4, 'big'))
            
            # Write value length as 4-byte big-endian integer
            f.write(len(value_bytes).to_bytes(4, 'big'))
            
            # Write the actual key bytes
            f.write(key_bytes)
            
            # Write the actual value bytes
            f.write(value_bytes)
            
            # CRITICAL: Ensure data is written to disk
            # flush() writes from Python buffer to OS buffer
            f.flush()
            
            # fsync() forces OS to write from buffer to physical disk
            # This is essential for durability - survives power failures
            # Without fsync(), data might only be in RAM
            os.fsync(f.fileno())
        
        # Update in-memory index with the new entry
        # This makes the key immediately searchable
        self._update_index(key, offset, len(value_bytes))
    
    def get(self, key: str) -> Optional[str]:
        """
        Get value for a key. Returns None if key doesn't exist.
        
        Process:
        1. Search in-memory index for key
        2. If not found, return None
        3. If found, seek to file offset
        4. Read the value from disk
        5. Return decoded value
        
        Time Complexity: O(n) for index search + O(1) for file read
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value associated with the key (string)
            None if key doesn't exist
        """
        # First, search the in-memory index
        # This avoids scanning the entire file
        result = self._find_in_index(key)
        
        # If key not in index, it doesn't exist
        if result is None:
            return None
        
        # Extract offset and value length from index result
        offset, val_len = result
        
        # Now read the actual value from the file
        try:
            # Open file in binary read mode
            with open(self.data_file, 'rb') as f:
                # Seek to the position where this entry starts
                # offset points to the beginning of the entry
                f.seek(offset)
                
                # Read and parse the entry format:
                # [key_len: 4 bytes][val_len: 4 bytes][key: variable][value: variable]
                
                # Read key length (we need this to skip over the key)
                key_len = int.from_bytes(f.read(4), 'big')
                
                # Skip value length (4 bytes) - we already know it from the index
                f.read(4)
                
                # Skip over the key bytes to get to the value
                # Seek relative to current position (1 = SEEK_CUR)
                f.seek(key_len, 1)
                
                # Read the actual value bytes
                value_bytes = f.read(val_len)
                
                # Decode bytes back to string using UTF-8
                return value_bytes.decode('utf-8')
        
        except Exception:
            # If there's any error reading the file, return None
            # This handles corrupted files gracefully
            return None
    
    def run(self) -> None:
        """
        Main CLI loop. Reads from STDIN and writes to STDOUT.
        
        This implements a REPL (Read-Eval-Print Loop):
        1. Read command from STDIN
        2. Parse and validate the command
        3. Execute the command
        4. Print result to STDOUT
        5. Loop until EXIT or EOF
        
        Supported commands:
        - SET <key> <value>: Store a key-value pair
        - GET <key>: Retrieve value for a key
        - EXIT: Exit the program
        
        The flush=True parameter ensures output is immediately sent to STDOUT,
        which is critical for piped communication (e.g., with Gradebot).
        """
        # Main event loop - continues until EXIT or EOF
        while True:
            try:
                # Read one line from standard input
                # This blocks until a line is available
                line = sys.stdin.readline()
                
                # Check for EOF (End of File)
                # readline() returns empty string on EOF
                if not line:
                    break
                
                # Remove leading/trailing whitespace (including newline)
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Parse the command line
                # split(None, 2) splits on whitespace, max 3 parts
                # This allows values to contain spaces
                # Example: "SET key my value" -> ["SET", "key", "my value"]
                parts = line.split(None, 2)
                
                # Skip if nothing was parsed
                if not parts:
                    continue
                
                # Extract command and convert to uppercase for case-insensitive matching
                command = parts[0].upper()
                
                # Handle EXIT command
                if command == "EXIT":
                    break
                
                # Handle SET command: SET <key> <value>
                elif command == "SET":
                    # Validate: need exactly 3 parts (SET, key, value)
                    if len(parts) < 3:
                        print("Error: SET requires key and value", flush=True)
                        continue
                    
                    # Extract key (parts[1]) and value (parts[2])
                    key = parts[1]
                    value = parts[2]
                    
                    # Store the key-value pair
                    self.set(key, value)
                    
                    # Send success response to STDOUT
                    # flush=True ensures immediate output (important for piped I/O)
                    print("OK", flush=True)
                
                # Handle GET command: GET <key>
                elif command == "GET":
                    # Validate: need exactly 2 parts (GET, key)
                    if len(parts) < 2:
                        print("Error: GET requires key", flush=True)
                        continue
                    
                    # Extract key
                    key = parts[1]
                    
                    # Retrieve value from store
                    value = self.get(key)
                    
                    # Print result:
                    # - If key exists, print the value
                    # - If key doesn't exist, print "(nil)" (Redis convention)
                    if value is None:
                        print("(nil)", flush=True)
                    else:
                        print(value, flush=True)
                
                # Handle unknown commands
                else:
                    print(f"Error: Unknown command '{command}'", flush=True)
            
            # Handle Ctrl+C gracefully
            except KeyboardInterrupt:
                break
            
            # Catch any other exceptions and continue running
            # This prevents the program from crashing on unexpected errors
            except Exception as e:
                print(f"Error: {e}", flush=True)


# Entry point: only run if this file is executed directly (not imported)
if __name__ == "__main__":
    # Create an instance of the key-value store
    # Uses default data file "data.db"
    store = SimpleKVStore()
    
    # Start the CLI loop
    # This will run until EXIT command or EOF
    store.run()

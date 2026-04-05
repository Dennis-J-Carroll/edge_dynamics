#!/usr/bin/env python3
"""
SQLite-backed persistent buffer for edge_dynamics.
Provides a store-and-forward mechanism for telemetry data.
"""

import sqlite3
import os
import time
from typing import List, Tuple, Optional


class DiskBuffer:
    """
    Persistent disk buffer using SQLite.
    
    Stores compressed frames when the network is unavailable and
    supports FIFO eviction to maintain a maximum disk size.
    """
    
    def __init__(self, db_path: str, max_mb: int = 50):
        self.db_path = db_path
        self.max_bytes = max_mb * 1024 * 1024
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    timestamp REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON pending_batches(timestamp)")
            conn.commit()
    
    def push(self, topic: str, payload: bytes):
        """
        Push a batch into the persistent buffer.
        
        Args:
            topic: The topic of the batch
            payload: The binary frame data
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO pending_batches (topic, payload, timestamp) VALUES (?, ?, ?)",
                    (topic, payload, time.time())
                )
                conn.commit()
            
            # Enforce size limit (FIFO eviction)
            self._enforce_limit()
        except Exception as e:
            # Fallback: if disk is full or DB is corrupt, we may still lose data
            # but we should at least log it if possible.
            # In a real edge case, we might try to write to a secondary location.
            pass

    def pop_batch(self, limit: int = 10) -> List[Tuple[int, str, bytes]]:
        """
        Retrieve and remove the oldest batches from the buffer.
        
        Args:
            limit: Maximum number of batches to retrieve
            
        Returns:
            List of (id, topic, payload) tuples
        """
        batches = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT id, topic, payload FROM pending_batches ORDER BY timestamp ASC LIMIT ?",
                    (limit,)
                )
                batches = cursor.fetchall()
                
                if batches:
                    ids = [b[0] for b in batches]
                    conn.execute(
                        f"DELETE FROM pending_batches WHERE id IN ({','.join(['?']*len(ids))})",
                        ids
                    )
                    conn.commit()
        except Exception:
            pass
        return batches

    def get_count(self) -> int:
        """Get the number of pending batches in the buffer."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM pending_batches")
                return cursor.fetchone()[0]
        except Exception:
            return 0

    def _enforce_limit(self):
        """Delete oldest records if database size exceeds limit."""
        try:
            db_size = os.path.getsize(self.db_path)
            if db_size > self.max_bytes:
                # Simple heuristic: delete 10% of oldest records if over limit
                # More robust way: check size after each deletion
                count = self.get_count()
                to_delete = max(1, count // 10)
                
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        f"DELETE FROM pending_batches WHERE id IN (SELECT id FROM pending_batches ORDER BY timestamp ASC LIMIT {to_delete})"
                    )
                    conn.commit()
                
                # Vaccum to reclaim space
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("VACUUM")
        except Exception:
            pass

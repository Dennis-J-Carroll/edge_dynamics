# SPDX-License-Identifier: Apache-2.0
"""
Connection pool implementation for edge_dynamics.

Manages a pool of reusable socket connections to reduce the overhead
of creating new connections for each request.

Example:
    >>> from edge_utils.connection_pool import ConnectionPool
    >>> pool = ConnectionPool("127.0.0.1", 7000, max_size=10)
    >>>
    >>> with pool.get_connection() as conn:
    ...     conn.sendall(data)
"""

import queue
import socket
import threading
import time
from contextlib import contextmanager
from typing import Generator, Optional, Tuple


class ConnectionPool:
    """
    Thread-safe connection pool for socket connections.

    Maintains a pool of reusable connections to avoid the overhead
    of creating new sockets for each request.

    Attributes:
        host: Target host
        port: Target port
        max_size: Maximum number of connections in pool
        timeout: Socket timeout in seconds
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_size: int = 10,
        timeout: float = 2.0,
        max_idle_time: float = 300.0,
    ):
        """
        Initialize connection pool.

        Args:
            host: Target hostname or IP
            port: Target port
            max_size: Maximum connections in pool (default: 10)
            timeout: Socket timeout in seconds (default: 2.0)
            max_idle_time: Max time connection can be idle (default: 300s)

        Example:
            >>> pool = ConnectionPool("collector.example.com", 7000, max_size=5)
        """
        self.host = host
        self.port = port
        self.max_size = max_size
        self.timeout = timeout
        self.max_idle_time = max_idle_time

        self._pool: queue.Queue = queue.Queue(maxsize=max_size)
        self._current_size = 0
        self._lock = threading.Lock()
        self._created_count = 0
        self._reused_count = 0

    def _create_connection(self) -> socket.socket:
        """
        Create a new socket connection.

        Returns:
            Connected socket

        Raises:
            socket.error: If connection fails
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))

        with self._lock:
            self._current_size += 1
            self._created_count += 1

        return sock

    def _is_connection_alive(self, sock: socket.socket) -> bool:
        """
        Check if a connection is still alive.

        Args:
            sock: Socket to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            # Use MSG_PEEK to check without consuming data
            sock.setblocking(False)
            data = sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
            sock.setblocking(True)
            # If we got data, connection is alive but has pending data (unusual)
            return len(data) == 0
        except BlockingIOError:
            # No data available, connection is alive
            sock.setblocking(True)
            return True
        except (socket.error, OSError):
            # Connection is dead
            return False

    def acquire(self) -> socket.socket:
        """
        Acquire a connection from the pool.

        Returns:
            Socket connection

        Raises:
            socket.error: If unable to create connection

        Example:
            >>> conn = pool.acquire()
            >>> try:
            ...     conn.sendall(data)
            >>> finally:
            ...     pool.release(conn)
        """
        # Try to get existing connection from pool
        try:
            sock, timestamp = self._pool.get_nowait()

            # Check if connection is too old or dead
            age = time.time() - timestamp
            if age > self.max_idle_time or not self._is_connection_alive(sock):
                # Connection is stale, close it
                try:
                    sock.close()
                except Exception:
                    pass

                with self._lock:
                    self._current_size -= 1

                # Create new connection
                return self._create_connection()

            # Connection is good, reuse it
            with self._lock:
                self._reused_count += 1

            return sock

        except queue.Empty:
            # No connections available, create new one if under limit
            with self._lock:
                if self._current_size < self.max_size:
                    return self._create_connection()

            # Pool is full, wait for connection to become available
            # This blocks until a connection is released
            sock, timestamp = self._pool.get(timeout=self.timeout)

            # Validate connection
            if not self._is_connection_alive(sock):
                try:
                    sock.close()
                except Exception:
                    pass

                with self._lock:
                    self._current_size -= 1

                return self._create_connection()

            return sock

    def release(self, sock: socket.socket) -> None:
        """
        Return a connection to the pool.

        Args:
            sock: Socket to return

        Example:
            >>> conn = pool.acquire()
            >>> try:
            ...     conn.sendall(data)
            >>> finally:
            ...     pool.release(conn)
        """
        try:
            # Check if connection is still alive
            if self._is_connection_alive(sock):
                # Return to pool with timestamp
                self._pool.put_nowait((sock, time.time()))
            else:
                # Connection is dead, close it
                try:
                    sock.close()
                except Exception:
                    pass

                with self._lock:
                    self._current_size -= 1

        except queue.Full:
            # Pool is full, close the connection
            try:
                sock.close()
            except Exception:
                pass

            with self._lock:
                self._current_size -= 1

    @contextmanager
    def get_connection(self) -> Generator[socket.socket, None, None]:
        """
        Context manager to acquire and release connection.

        Yields:
            Socket connection

        Example:
            >>> with pool.get_connection() as conn:
            ...     conn.sendall(data)
        """
        conn = self.acquire()
        try:
            yield conn
        finally:
            self.release(conn)

    def close_all(self) -> None:
        """
        Close all connections in the pool.

        Example:
            >>> pool.close_all()
        """
        while True:
            try:
                sock, _ = self._pool.get_nowait()
                try:
                    sock.close()
                except Exception:
                    pass
            except queue.Empty:
                break

        with self._lock:
            self._current_size = 0

    def get_stats(self) -> dict:
        """
        Get pool statistics.

        Returns:
            Dictionary with pool statistics

        Example:
            >>> stats = pool.get_stats()
            >>> print(f"Connections: {stats['current_size']}/{stats['max_size']}")
            >>> print(f"Reuse rate: {stats['reuse_rate']:.1%}")
        """
        with self._lock:
            total_requests = self._created_count + self._reused_count
            reuse_rate = self._reused_count / total_requests if total_requests > 0 else 0

            return {
                "host": self.host,
                "port": self.port,
                "current_size": self._current_size,
                "max_size": self.max_size,
                "pool_available": self._pool.qsize(),
                "created_count": self._created_count,
                "reused_count": self._reused_count,
                "reuse_rate": reuse_rate,
            }

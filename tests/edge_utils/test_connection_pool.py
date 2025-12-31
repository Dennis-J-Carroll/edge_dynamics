# SPDX-License-Identifier: Apache-2.0
"""Tests for edge_utils.connection_pool module."""

import pytest
import socket
import threading
import time
from edge_utils.connection_pool import ConnectionPool


class TestConnectionPool:
    """Tests for ConnectionPool class."""

    def test_pool_initialization(self):
        """Test pool initialization."""
        pool = ConnectionPool("127.0.0.1", 7000, max_size=5)
        assert pool.host == "127.0.0.1"
        assert pool.port == 7000
        assert pool.max_size == 5

    def test_get_stats(self):
        """Test getting pool statistics."""
        pool = ConnectionPool("127.0.0.1", 7000, max_size=10)
        stats = pool.get_stats()

        assert stats["host"] == "127.0.0.1"
        assert stats["port"] == 7000
        assert stats["max_size"] == 10
        assert stats["current_size"] == 0
        assert stats["created_count"] == 0
        assert stats["reused_count"] == 0

    def test_close_all(self):
        """Test closing all connections."""
        pool = ConnectionPool("127.0.0.1", 7000, max_size=5)

        # Pool starts empty
        stats = pool.get_stats()
        assert stats["current_size"] == 0

        # Close all (should be no-op on empty pool)
        pool.close_all()

        stats = pool.get_stats()
        assert stats["current_size"] == 0


class TestConnectionPoolIntegration:
    """Integration tests requiring a real server."""

    @pytest.fixture
    def mock_server(self):
        """Create a mock TCP server for testing."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("127.0.0.1", 0))  # Use any available port
        server_socket.listen(5)
        port = server_socket.getsockname()[1]

        # Start server thread
        def server_thread():
            while True:
                try:
                    conn, addr = server_socket.accept()
                    # Echo server
                    data = conn.recv(1024)
                    if data:
                        conn.sendall(data)
                    conn.close()
                except:
                    break

        thread = threading.Thread(target=server_thread, daemon=True)
        thread.start()

        yield port

        server_socket.close()

    def test_acquire_and_release(self, mock_server):
        """Test acquiring and releasing connections."""
        pool = ConnectionPool("127.0.0.1", mock_server, max_size=3)

        # Acquire connection
        conn = pool.acquire()
        assert conn is not None

        stats = pool.get_stats()
        assert stats["current_size"] == 1
        assert stats["created_count"] == 1

        # Release connection
        pool.release(conn)

        stats = pool.get_stats()
        assert stats["pool_available"] == 1

    def test_connection_reuse(self, mock_server):
        """Test that connections are reused."""
        pool = ConnectionPool("127.0.0.1", mock_server, max_size=3)

        # Acquire and release
        conn1 = pool.acquire()
        pool.release(conn1)

        # Acquire again - should reuse
        conn2 = pool.acquire()
        pool.release(conn2)

        stats = pool.get_stats()
        assert stats["created_count"] == 1
        assert stats["reused_count"] == 1
        assert stats["reuse_rate"] > 0

    def test_context_manager(self, mock_server):
        """Test context manager interface."""
        pool = ConnectionPool("127.0.0.1", mock_server, max_size=3)

        # Use context manager
        with pool.get_connection() as conn:
            conn.sendall(b"test")
            data = conn.recv(1024)
            assert data == b"test"

        # Connection should be returned to pool
        stats = pool.get_stats()
        assert stats["pool_available"] == 1

    def test_concurrent_access(self, mock_server):
        """Test concurrent access to pool."""
        pool = ConnectionPool("127.0.0.1", mock_server, max_size=5)
        results = []

        def worker():
            try:
                with pool.get_connection() as conn:
                    conn.sendall(b"test")
                    data = conn.recv(1024)
                    results.append(data)
                    time.sleep(0.01)
            except Exception as e:
                results.append(e)

        # Launch multiple threads
        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed
        assert len(results) == 10
        assert all(r == b"test" for r in results)

    def test_pool_size_limit(self, mock_server):
        """Test that pool respects max_size."""
        pool = ConnectionPool("127.0.0.1", mock_server, max_size=2)

        # Acquire max_size connections
        conn1 = pool.acquire()
        conn2 = pool.acquire()

        stats = pool.get_stats()
        assert stats["current_size"] == 2

        # Release them
        pool.release(conn1)
        pool.release(conn2)

        stats = pool.get_stats()
        assert stats["pool_available"] == 2

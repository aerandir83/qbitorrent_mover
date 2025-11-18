import unittest
from unittest.mock import patch, MagicMock
import time
import threading
from ssh_manager import SSHConnectionPool

class TestSSHConnectionPool(unittest.TestCase):

    @patch('ssh_manager.paramiko.SSHClient')
    def test_basic_reuse(self, mock_ssh_client):
        """Test that connections are reused from the pool."""
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_transport.return_value.is_active.return_value = True
        mock_ssh_client.return_value = mock_ssh_instance

        pool = SSHConnectionPool(
            host='localhost',
            port=22,
            username='testuser',
            password='testpass',
            max_size=3
        )

        connection_ids = []

        with pool.get_connection() as (sftp, ssh):
            connection_ids.append(id(ssh))
        with pool.get_connection() as (sftp, ssh):
            connection_ids.append(id(ssh))

        self.assertEqual(connection_ids[0], connection_ids[1])
        mock_ssh_instance.connect.assert_called_once()
        pool.close_all()

    @patch('ssh_manager.paramiko.SSHClient')
    def test_pool_size_limit(self, mock_ssh_client):
        """Test that pool respects max_size."""
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_transport.return_value.is_active.return_value = True
        mock_ssh_client.return_value = mock_ssh_instance

        pool = SSHConnectionPool(
            host='localhost',
            port=22,
            username='testuser',
            password='testpass',
            max_size=1,
            pool_wait_timeout=0.1
        )

        with pool.get_connection() as (sftp, ssh):
            with self.assertRaises(TimeoutError):
                with pool.get_connection() as (sftp2, ssh2):
                    pass

        pool.close_all()

    @patch('ssh_manager.paramiko.SSHClient')
    def test_dead_connection_removal(self, mock_ssh_client):
        """Test that dead connections are removed from pool."""
        mock_ssh_instance_one = MagicMock()
        mock_ssh_instance_one.get_transport.return_value.is_active.return_value = True

        mock_ssh_instance_two = MagicMock()
        mock_ssh_instance_two.get_transport.return_value.is_active.return_value = True

        mock_ssh_client.side_effect = [mock_ssh_instance_one, mock_ssh_instance_two]

        pool = SSHConnectionPool(
            host='localhost',
            port=22,
            username='testuser',
            password='testpass',
            max_size=1
        )

        with pool.get_connection() as (sftp, ssh):
            conn_id1 = id(ssh)
            # Simulate the connection dying
            ssh.get_transport.return_value.is_active.return_value = False

        with pool.get_connection() as (sftp, ssh):
            conn_id2 = id(ssh)

        self.assertNotEqual(conn_id1, conn_id2)
        self.assertEqual(mock_ssh_client.call_count, 2)
        pool.close_all()

    @patch('ssh_manager.paramiko.SSHClient')
    def test_concurrent_access(self, mock_ssh_client):
        """Test thread safety with multiple threads."""
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_transport.return_value.is_active.return_value = True
        mock_ssh_client.return_value = mock_ssh_instance

        pool = SSHConnectionPool(
            host='localhost',
            port=22,
            username='testuser',
            password='testpass',
            max_size=10
        )

        results = {'success': 0, 'failed': 0}
        lock = threading.Lock()

        def worker():
            try:
                for _ in range(5):
                    with pool.get_connection() as (sftp, ssh):
                        time.sleep(0.01)
                        with lock:
                            results['success'] += 1
            except Exception:
                with lock:
                    results['failed'] += 1

        threads = []
        for _ in range(10):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(results['success'], 50)
        self.assertEqual(results['failed'], 0)
        pool.close_all()

    @patch('ssh_manager.paramiko.SSHClient')
    def test_connection_counter(self, mock_ssh_client):
        """Test that _active_connections counter is accurate using get_stats()."""
        mock_ssh_instance = MagicMock()
        mock_ssh_instance.get_transport.return_value.is_active.return_value = True
        mock_ssh_client.return_value = mock_ssh_instance

        pool = SSHConnectionPool(
            host='localhost',
            port=22,
            username='testuser',
            password='testpass',
            max_size=3
        )

        stats = pool.get_stats()
        self.assertEqual(stats['active_connections'], 0)
        self.assertEqual(stats['in_use'], 0)
        self.assertEqual(stats['in_pool'], 0)

        with pool.get_connection() as (sftp1, ssh1):
            stats1 = pool.get_stats()
            self.assertEqual(stats1['active_connections'], 1)
            self.assertEqual(stats1['in_use'], 1)
            self.assertEqual(stats1['in_pool'], 0)

            with pool.get_connection() as (sftp2, ssh2):
                stats2 = pool.get_stats()
                self.assertEqual(stats2['active_connections'], 2)
                self.assertEqual(stats2['in_use'], 2)
                self.assertEqual(stats2['in_pool'], 0)

        stats_final = pool.get_stats()
        self.assertEqual(stats_final['active_connections'], 2)
        self.assertEqual(stats_final['in_use'], 0)
        self.assertEqual(stats_final['in_pool'], 2)

        pool.close_all()

if __name__ == '__main__':
    unittest.main()
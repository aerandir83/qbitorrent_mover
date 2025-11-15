#!/usr/bin/env python3
"""
Test script for SSHConnectionPool to verify:
1. Connections are reused
2. Dead connections are removed
3. Pool size is respected
4. Thread safety
"""

import time
import threading
from torrent_mover.ssh_manager import SSHConnectionPool
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(threadName)s - %(message)s')

def test_basic_reuse():
    """Test that connections are reused from the pool."""
    print("\n=== Test 1: Basic Connection Reuse ===")
    pool = SSHConnectionPool(
        host='localhost',
        port=22,
        username='jules',
        password='',
        max_size=3
    )

    connection_ids = []

    # Get connection 3 times
    for i in range(3):
        with pool.get_connection() as (sftp, ssh):
            conn_id = id(ssh)
            connection_ids.append(conn_id)
            print(f"  Iteration {i+1}: Connection ID = {conn_id}")
            time.sleep(0.1)

    # Check if we reused connections
    if len(set(connection_ids)) < len(connection_ids):
        print("  ✓ SUCCESS: Connections were reused")
    else:
        print("  ✗ FAIL: No connection reuse detected")

    pool.close_all()


def test_pool_size_limit():
    """Test that pool respects max_size."""
    print("\n=== Test 2: Pool Size Limit ===")
    pool = SSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=2
    )

    connections = []

    try:
        # Try to get 3 connections (should block on the 3rd)
        print("  Acquiring 2 connections...")
        with pool.get_connection() as (sftp1, ssh1):
            print(f"   Got connection 1: {id(ssh1)}")
            with pool.get_connection() as (sftp2, ssh2):
                print(f"   Got connection 2: {id(ssh2)}")

                # This should timeout since pool is full
                print("  Trying to get 3rd connection (should timeout)...")
                start = time.time()
                try:
                    with pool.get_connection() as (sftp3, ssh3):
                        print(f"   ✗ FAIL: Got 3rd connection when pool should be full!")
                except Exception as e:
                    elapsed = time.time() - start
                    print(f"   ✓ SUCCESS: Timed out after {elapsed:.1f}s as expected")

    finally:
        pool.close_all()


def test_dead_connection_removal():
    """Test that dead connections are removed from pool."""
    print("\n=== Test 3: Dead Connection Removal ===")
    pool = SSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=2
    )

    # Get a connection and intentionally kill it
    print("  Getting connection and killing it...")
    with pool.get_connection() as (sftp, ssh):
        conn_id = id(ssh)
        print(f"   Connection ID: {conn_id}")

    # The connection was returned to pool, now kill it
    try:
        dead_sftp, dead_ssh = pool._pool.get_nowait()
        print(f"   Retrieved from pool: {id(dead_ssh)}")
        dead_ssh.close()  # Kill it
        print("   Closed the connection (simulating network failure)")
        # Put it back in the pool (simulating what would happen)
        pool._pool.put_nowait((dead_sftp, dead_ssh))
    except:
        pass

    # Now try to get a connection - should create a new one
    print("  Getting connection again (should create new one)...")
    with pool.get_connection() as (sftp, ssh):
        new_conn_id = id(ssh)
        print(f"   New connection ID: {new_conn_id}")
        if new_conn_id != conn_id:
            print("   ✓ SUCCESS: New connection was created (dead one was removed)")
        else:
            print("   ✗ FAIL: Got the same (dead) connection back")

    pool.close_all()


def test_concurrent_access():
    """Test thread safety with multiple threads."""
    print("\n=== Test 4: Concurrent Access ===")
    pool = SSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=3
    )

    results = {'success': 0, 'failed': 0}
    lock = threading.Lock()

    def worker(worker_id):
        try:
            for i in range(5):
                with pool.get_connection() as (sftp, ssh):
                    # Simulate some work
                    time.sleep(0.01)
                    with lock:
                        results['success'] += 1
        except Exception as e:
            print(f"   Worker {worker_id} failed: {e}")
            with lock:
                results['failed'] += 1

    # Start 10 threads
    threads = []
    print("  Starting 10 threads, each making 5 connections...")
    for i in range(10):
        t = threading.Thread(target=worker, args=(i,), name=f"Worker-{i}")
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"  Results: {results['success']} successful, {results['failed']} failed")
    if results['success'] == 50 and results['failed'] == 0:
        print("  ✓ SUCCESS: All 50 operations completed")
    else:
        print(f"  ✗ FAIL: Expected 50 successful operations")

    pool.close_all()


def test_connection_counter():
    """Test that _active_connections counter is accurate using get_stats()."""
    print("\n=== Test 5: Connection Counter Accuracy ===")
    pool = SSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=3
    )

    stats = pool.get_stats()
    print(f"  Initial stats: {stats}")
    if stats['active_connections'] != 0:
         print(f"  ✗ FAIL: Initial active_connections is not 0")
         pool.close_all()
         return

    # Acquire 3 connections simultaneously to check 'in_use'
    print("  Acquiring 3 connections...")
    try:
        with pool.get_connection() as (sftp1, ssh1):
            stats1 = pool.get_stats()
            print(f"   After connection 1: {stats1}")
            with pool.get_connection() as (sftp2, ssh2):
                stats2 = pool.get_stats()
                print(f"   After connection 2: {stats2}")
                with pool.get_connection() as (sftp3, ssh3):
                    stats3 = pool.get_stats()
                    print(f"   After connection 3: {stats3}")

                    if stats3['active_connections'] == 3 and stats3['in_use'] == 3 and stats3['in_pool'] == 0:
                         print("  ✓ SUCCESS: Stats are correct with 3 connections in use")
                    else:
                         print(f"  ✗ FAIL: Stats incorrect with 3 connections in use: {stats3}")
    except Exception as e:
        print(f"  ✗ FAIL: Error while acquiring 3 connections: {e}")
        pool.close_all()
        return

    # After 'with' blocks exit, all should be in pool
    time.sleep(0.1) # Give connections time to be put back
    stats_final = pool.get_stats()
    print(f"  Final stats (all returned): {stats_final}")
    if stats_final['active_connections'] == 3 and stats_final['in_use'] == 0 and stats_final['in_pool'] == 3:
        print("  ✓ SUCCESS: Counter and pool size match after release")
    else:
        print(f"  ✗ FAIL: Final stats incorrect: {stats_final}")

    pool.close_all()


if __name__ == '__main__':
    print("=== SSH Connection Pool Tests ===")
    print("\nNOTE: These tests require SSH access to localhost.")
    print("Update credentials in the script if needed.\n")

    try:
        test_basic_reuse()
        test_pool_size_limit()
        test_dead_connection_removal()
        test_concurrent_access()
        test_connection_counter()
        print("\n=== All Tests Complete ===")
    except Exception as e:
        print(f"\n!!! Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()

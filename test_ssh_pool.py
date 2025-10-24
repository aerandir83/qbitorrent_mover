#!/usr/bin/env python3
"""
Test script for AsyncSSHConnectionPool to verify:
1. Connections are reused
2. Dead connections are removed
3. Pool size is respected
4. Concurrency safety
"""

import asyncio
import time
from torrent_mover.utils import AsyncSSHConnectionPool
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

async def test_basic_reuse():
    """Test that connections are reused from the pool."""
    print("\n=== Test 1: Basic Connection Reuse ===")
    pool = AsyncSSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=3
    )

    connection_ids = []

    # Get connection 3 times
    for i in range(3):
        async with pool.get_connection() as conn:
            conn_id = id(conn)
            connection_ids.append(conn_id)
            print(f"  Iteration {i+1}: Connection ID = {conn_id}")
            await asyncio.sleep(0.1)

    # Check if we reused connections
    if len(set(connection_ids)) < len(connection_ids):
        print("  ✓ SUCCESS: Connections were reused")
    else:
        print("  ✗ FAIL: No connection reuse detected")

    await pool.close_all()


async def test_pool_size_limit():
    """Test that pool respects max_size."""
    print("\n=== Test 2: Pool Size Limit ===")
    pool = AsyncSSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=2
    )

    try:
        # Try to get 3 connections (should block on the 3rd)
        print("  Acquiring 2 connections...")
        async with pool.get_connection() as conn1:
            print(f"   Got connection 1: {id(conn1)}")
            async with pool.get_connection() as conn2:
                print(f"   Got connection 2: {id(conn2)}")

                # This should timeout since pool is full
                print("  Trying to get 3rd connection (should timeout)...")
                start = time.time()
                try:
                    async with asyncio.timeout(2):
                        async with pool.get_connection() as conn3:
                             print(f"   ✗ FAIL: Got 3rd connection when pool should be full!")
                except TimeoutError:
                    elapsed = time.time() - start
                    print(f"   ✓ SUCCESS: Timed out after {elapsed:.1f}s as expected")

    finally:
        await pool.close_all()


async def test_dead_connection_removal():
    """Test that dead connections are removed from pool."""
    print("\n=== Test 3: Dead Connection Removal ===")
    pool = AsyncSSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=2
    )

    # Get a connection and intentionally kill it
    print("  Getting connection and killing it...")
    async with pool.get_connection() as conn:
        conn_id = id(conn)
        print(f"   Connection ID: {conn_id}")

    # The connection was returned to pool, now kill it
    try:
        dead_conn = await pool._pool.get()
        print(f"   Retrieved from pool: {id(dead_conn)}")
        dead_conn.close()
        print("   Closed the connection (simulating network failure)")
        await pool._pool.put(dead_conn)
    except Exception:
        pass

    # Now try to get a connection - should create a new one
    print("  Getting connection again (should create new one)...")
    async with pool.get_connection() as conn:
        new_conn_id = id(conn)
        print(f"   New connection ID: {new_conn_id}")
        if new_conn_id != conn_id:
            print("   ✓ SUCCESS: New connection was created (dead one was removed)")
        else:
            print("   ✗ FAIL: Got the same (dead) connection back")

    await pool.close_all()


async def test_concurrent_access():
    """Test concurrency safety with multiple coroutines."""
    print("\n=== Test 4: Concurrent Access ===")
    pool = AsyncSSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=3
    )

    results = {'success': 0, 'failed': 0}

    async def worker(worker_id):
        try:
            for i in range(5):
                async with pool.get_connection() as conn:
                    # Simulate some work
                    await asyncio.sleep(0.01)
                    results['success'] += 1
        except Exception as e:
            print(f"   Worker {worker_id} failed: {e}")
            results['failed'] += 1

    # Start 10 workers
    print("  Starting 10 workers, each making 5 connections...")
    tasks = [worker(i) for i in range(10)]
    await asyncio.gather(*tasks)

    print(f"  Results: {results['success']} successful, {results['failed']} failed")
    if results['success'] == 50 and results['failed'] == 0:
        print("  ✓ SUCCESS: All 50 operations completed")
    else:
        print(f"  ✗ FAIL: Expected 50 successful operations")

    await pool.close_all()


async def test_connection_counter():
    """Test that _created counter is accurate."""
    print("\n=== Test 5: Connection Counter Accuracy ===")
    pool = AsyncSSHConnectionPool(
        host='localhost',
        port=22,
        username='testuser',
        password='testpass',
        max_size=3
    )

    print(f"  Initial _created: {pool._created}")

    # Create some connections
    connections = []
    for i in range(3):
         async with pool.get_connection() as conn:
            print(f"   After connection {i+1}: _created = {pool._created}")

    print(f"  Final _created: {pool._created}")
    print(f"  Pool size: {pool._pool.qsize()}")

    if pool._created == 3 and pool._pool.qsize() == 3:
        print("  ✓ SUCCESS: Counter and pool size match")
    else:
        print(f"  ✗ FAIL: Counter={pool._created}, Pool size={pool._pool.qsize()}")

    await pool.close_all()


async def main():
    print("=== Async SSH Connection Pool Tests ===")
    print("\nNOTE: These tests require SSH access to localhost.")
    print("Update credentials in the script if needed.\n")

    try:
        await test_basic_reuse()
        await test_pool_size_limit()
        await test_dead_connection_removal()
        await test_concurrent_access()
        await test_connection_counter()
        print("\n=== All Tests Complete ===")
    except Exception as e:
        print(f"\n!!! Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())

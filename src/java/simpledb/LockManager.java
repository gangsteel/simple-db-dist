package simpledb;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * LockManager manages the relation between transactions and locks
 * The class should be thread-safe.
 */
public class LockManager {
    
    /**
     * A simple Lock implementation by ArrayBlockingQueue supporting lock() and unlock()
     * This Lock is better to use than Java built-in locks because unlock() can be called by
     * any threads.
     */
    private class SimpleLock {
        private final ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
        private final Object obj = new Object();
        
        /**
         * lock the lock
         * @throws TransactionAbortedException if timeout
         */
        private void lock() throws TransactionAbortedException {
            try {
                // queue.put(obj); // This will cause deadlock
                final boolean successFlag = queue.offer(obj, 5, TimeUnit.SECONDS);
                // TODO: tuning parameter here
                if (!successFlag) {
                    // throw exception and abort this xaction: suicidal
                    throw new TransactionAbortedException();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted.");
            }
        }
        
        /**
         * unlock the lock
         * Note: repeately unlock the same lock will result in undefined behavior,
         * but this should not happen in the implementation
         */
        private void unlock() {
            try {
                queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted.");
            }
        }
        
        /**
         * determine if the lock is locked
         * @return true if the lock is locked
         */
        private boolean isLocked() {
            return (queue.peek() != null);
        }
    }
    
    /**
     * Inner class represents a lock of certain Transaction(s)
     */
    private class TransactionLock {
        
        private Permissions perm;
        private final Set<TransactionId> readTid = Collections.synchronizedSet(new HashSet<TransactionId>());
        private final ArrayBlockingQueue<TransactionId> writeTid = new ArrayBlockingQueue<TransactionId>(1);
        private final SimpleLock readLock = new SimpleLock();
        private final SimpleLock writeLock = new SimpleLock();
        
        /**
         * Create a new TransactionLock from tid and perm
         * @param tid the Transaction ID
         * @param perm the Permission type
         * @throws TransactionAbortedException if timeout
         */
        private TransactionLock(TransactionId tid, Permissions perm) throws TransactionAbortedException {
            this.perm = perm;
            
            if (perm.equals(Permissions.READ_ONLY)) {
                readLock.lock();
                readTid.add(tid);
            } else {
                try {
                    writeLock.lock();
                    writeTid.put(tid);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted lock");
                }
            }
        }
        
        /**
         * Determines whether the tid already acquired the lock with permission
         * @param tid the Transaction ID
         * @param perm the Permission type
         * @return true if Transaction tid already acquired the lock with Permission perm, or
         * if Transaction tid already acquire write lock and perm is READ_ONLY
         */
        private synchronized boolean hasAcquired(TransactionId tid, Permissions perm) {
            if (this.perm.equals(Permissions.READ_ONLY)) {
                if (perm.equals(Permissions.READ_ONLY) && readTid.contains(tid)) {
                    return true;
                }
            } else {
                if (tid.equals(writeTid.peek())) {
                    return true;
                }
            }
            return false;
        }
        
        /**
         * Attempt to acquire lock by tid and perm
         * @param tid the Transaction ID
         * @param perm the Permission type
         * @throws TransactionAbortedException if timeout
         */
        private synchronized void acquire(TransactionId tid, Permissions perm) throws TransactionAbortedException {
            if (hasAcquired(tid, perm)) {
                return;
            }
            if (this.perm.equals(Permissions.READ_ONLY)) {
                if (perm.equals(Permissions.READ_ONLY)) {
                    if (!readLock.isLocked()){
                        readLock.lock();
                    }
                    readTid.add(tid);
                } else {
                    // upgrade the lock to read_write if possible
                    release(tid);
                    readLock.lock();
                    writeLock.lock();
                    assert readTid.size() == 0 : "Inconsistency detected";
                    assert writeTid.size() == 0 : "Inconsistency detected";
                    this.perm = Permissions.READ_WRITE;
                    try {
                        writeTid.put(tid);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted lock");
                    }
                    readLock.unlock();
                }
            } else {
                if (perm.equals(Permissions.READ_ONLY)) {
                    writeLock.lock();
                    readLock.lock();
                    assert readTid.size() == 0 : "Inconsistency detected";
                    assert writeTid.size() == 0 : "Inconsistency detected";
                    this.perm = Permissions.READ_ONLY;
                    readTid.add(tid);
                    writeLock.unlock();
                } else {
                    writeLock.lock();
                    try {
                        assert readTid.size() == 0 : "Inconsistency detected";
                        assert writeTid.size() == 0 : "Inconsistency detected";
                        writeTid.put(tid);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted lock");
                    }
                }
            }
        }
        
        /**
         * Release the lock associated with tid. Must be the same thread locking the lock or an
         * exception might be thrown.
         * Note: this is not a synchronized method to avoid deadlock
         * @param tid the Transaction ID
         */
        private void release(TransactionId tid) {
            if (this.perm.equals(Permissions.READ_ONLY)) {
                synchronized(readTid) {
                    if (readTid.contains(tid)) {
                        readTid.remove(tid);
                        if (readTid.size() == 0) {
                            readLock.unlock();
                        }
                    }
                }
            } else {
                synchronized(writeTid) {
                    if (tid.equals(writeTid.peek())) {
                        try {
                            writeTid.take();
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Interrupted lock");
                        }
                        assert writeTid.size() == 0 : "Inconsistency detected";
                        this.perm = Permissions.READ_ONLY;
                        writeLock.unlock();
                    }
                }
            }
            
        }
        
        /**
         * Determines if tid is locking this Lock
         * @param tid the Transaction ID
         * @return true if tid is locking this Lock
         */
        private boolean holdsTransactionLock(TransactionId tid) {
            if (this.perm.equals(Permissions.READ_ONLY)) {
                return readTid.contains(tid);
            } else {
                return tid.equals(writeTid.peek());
            }
        }
    }
    
    private final Map<PageId, TransactionLock> lockMap;
    // The size of this Map should be monotonically increasing. Don't delete elements from this Map.
    
    /**
     * Create a new LockManager instance
     */
    public LockManager() {
        lockMap = Collections.synchronizedMap(new HashMap<PageId, TransactionLock>());
    }
    
    /**
     * Acquire lock for Transaction tid on PageId pid.
     * If the lock of pid is already acquired by other Transaction, this method should block
     * @param tid the Transaction ID
     * @param pid the Page ID
     * @param perm the permission type
     * @throws TransactionAbortedException if timeout
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean flag = addToMap(tid, pid, perm);
        if (!flag) {
            TransactionLock tlock = lockMap.get(pid);
            tlock.acquire(tid, perm);
        }
    }
    
    /**
     * Synchronized method trying to add pid to the Map in order to prevent race condition
     * @param tid the Transaction ID
     * @param pid the Page ID
     * @param perm the permission type
     * @return true if the Map is changed, false if pid already exists in the Map. If multiple
     * threads are using the same pid, only one will return true.
     * @throws TransactionAbortedException if timeout
     */
    private synchronized boolean addToMap(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (lockMap.containsKey(pid)) {
            return false;
        } else {
            lockMap.put(pid, new TransactionLock(tid, perm));
            return true;
        }
    }
    
    /**
     * Release lock of Transaction tid on PageId pid.
     * @param tid the Transaction ID
     * @param pid the Page ID
     */
    public void releaseLock(TransactionId tid, PageId pid) {
        assert lockMap.containsKey(pid) : "try to release a lock that doesn't exist";
        final TransactionLock tlock = lockMap.get(pid);
        tlock.release(tid);
    }
    
    /**
     * Release all the locks Transaction tid holds
     * @param tid the Transaction ID
     */
    public void releaseAllLocks(TransactionId tid) {
        for (final PageId pid : lockMap.keySet()) {
            final TransactionLock tlock = lockMap.get(pid);
            tlock.release(tid);
        }
    }
    
    /**
     * Determines if pid is held by lock with tid
     * @param tid the Transaction ID
     * @param pid the Page ID
     * @return true if pid is held by lock with tid
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (lockMap.containsKey(pid)) {
            final TransactionLock tlock = lockMap.get(pid);
            return tlock.holdsTransactionLock(tid);
        } else {
            return false;
        }
    }

}

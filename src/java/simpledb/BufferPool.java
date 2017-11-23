package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private final int numPages; // Number of pages of the BufferPool
    private Map<PageId, Page> pages; // Initial design: Map structure to maximize random access
    private Queue<PageId> pageQueue; // Queue structure that keeps track of history of PageIds
    
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.pageQueue = new ConcurrentLinkedQueue<>();
        this.lockManager = new LockManager();
    }
    
    /**
     * Checks representation invariant of the class to ensure consistency:
     * 1. pages.size should be always equal to pageQueue.size
     * 2. pages.size should not exceed numPages
     */
    private void checkRep() {
        assert pageQueue.size() == pages.size() : "BufferPool corrupted: inconsistency detected";
        assert pages.size() <= numPages : "BufferPool corrupted: more pages detected";
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    
    /**
     * Add a page to BufferPool consistently.
     * If the page already exists in the Buffer, update the page and move the page
     * to the tail of the queue (mark it as the newest page)
     * If BufferPool reaches maximal number of pages, evict one page
     * @param pid the page id
     * @param page the page
     */
    private synchronized void addPage(PageId pid, Page page) throws DbException {
        if (pages.containsKey(pid)) {
            pages.put(pid, page); // update in the map
            pageQueue.remove(pid);
            pageQueue.add(pid); // move the page to the tail
        } else {
            if (pages.size() == this.numPages) {
                evictPage();
            }
            pages.put(pid, page);
            pageQueue.add(pid);
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     * @throws DbException if page with pid doesn't exist in the database
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        
        lockManager.acquireLock(tid, pid, perm);
        
        if (pages.containsKey(pid)) {
            return pages.get(pid);
        } else {
            Catalog catalog = Database.getCatalog(); // singleton pattern
            try {
                final DbFile file = catalog.getDatabaseFile(pid.getTableId());
                final Page page = file.readPage(pid);
                addPage(pid, page);
                checkRep(); // check consistency
                return page;
            } catch (NoSuchElementException e) {
                throw new DbException("Table ID " + pid.getTableId() + " doesn't exist.");
            } catch (IllegalArgumentException e) {
                throw new DbException("Page ID " + pid + " doesn't exist.");
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        for (final PageId pid : pages.keySet()) {
            final Page page = pages.get(pid);
            final TransactionId dirtyId = page.isDirty();
            if (dirtyId != null) {
                if (commit) {
                    assert tid.equals(dirtyId) : "page dirtied by other transactions!";
                    flushPage(pid);
                } else {
                    if (tid.equals(dirtyId)) {
                        discardPage(pid);
                    }
                }
            }
        }
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            addPage(page.getId(), page);
        }
        checkRep(); // check consistency
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = file.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            addPage(page.getId(), page);
        }
        checkRep(); // check consistency
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pages.keySet()) {
            final Page page = pages.get(pid);
            if (page.isDirty() != null) {
                flushPage(pid);
            }
        }
        checkRep(); // check consistency
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        assert pages.containsKey(pid) && pageQueue.contains(pid) : "Page not in buffer";
        pageQueue.remove(pid);
        pages.remove(pid);
        checkRep(); // check consistency
    }

    /**
     * Flushes a certain page to disk and mark it as not dirty
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        assert pages.containsKey(pid) : "page not existing in the Buffer";
        final DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        final Page page = pages.get(pid);
        file.writePage(page);
        page.markDirty(false, null);
        /*
         * Note: since the second argument is not used if this function is called to mark
         * the page as not dirty (see Page.java), so null is used here.
         */
        checkRep(); // check consistency
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool while satisfying NO STEAL policy
     * 1. the oldest page without updates && is not dirty
     * 2. if all the pages are dirty, DbException is thrown
     */
    private synchronized  void evictPage() throws DbException {
        for (int i = 0; i < pages.size(); i++) {
            final PageId pid = pageQueue.poll();
            assert pages.containsKey(pid) : "Inconsistency detected";
            if (pages.get(pid).isDirty() != null) {
                // the page is dirty, cannot be evicted
                pageQueue.add(pid); // add back to the head of the queue
            } else {
                pages.remove(pid);
                checkRep();
                return;
            }
        }
        throw new DbException("No page can be evicted, all pages are dirty in buffer pool.");
    }

}

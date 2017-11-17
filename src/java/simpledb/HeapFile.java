package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    private final File f;
    private final TupleDesc td;
    private final int fileId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file. (binary format)
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.fileId = f.getAbsoluteFile().hashCode();
        // calculate the number of pages in this HeapFile
        
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return fileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        final int tableid = pid.getTableId();
        if (tableid != fileId) {
            throw new IllegalArgumentException("Not the correct table");
        }
        final int pageno = pid.getPageNumber();
        if (pageno >= numPages() || pageno < 0) {
            throw new IllegalArgumentException("Invalid page number");
        }
        final int pageSize = BufferPool.getPageSize();
        final byte[] data = new byte[pageSize];

        try (BufferedInputStream br = new BufferedInputStream(new FileInputStream(f))) {
            final long skipN = br.skip(pageno*pageSize);
            assert skipN == pageno*pageSize : "InputStream skip() failed";
            br.read(data);
            br.close();
            return new HeapPage(new HeapPageId(tableid, pageno), data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("I/O error detected reading the page");
        }
    }

    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        final int skipIntended = page.getId().getPageNumber() * BufferPool.getPageSize();
        final int skipActual = raf.skipBytes(skipIntended);
        assert skipActual == skipIntended : "IO error: fail to skip bytes";
        raf.write(page.getPageData());
        raf.close();
    }
    
    /**
     * Append a new empty page to the end of the file
     * @throws IOException if there is IO exception
     */
    public synchronized void appendEmptyPage() throws IOException {
        final HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        writePage(page);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public synchronized int numPages() {
        try (BufferedInputStream br = new BufferedInputStream(new FileInputStream(f))) {
            final int numPages = br.available() / BufferPool.getPageSize();
            br.close();
            return numPages;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("I/O error detected reading the file");
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
            final PageId pid = new HeapPageId(getId(), i);
            final HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                ArrayList<Page> pages = new ArrayList<>();
                pages.add(page);
                return pages;
            } else {
                // release the lock on page if there are no empty slot
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        appendEmptyPage(); // We need a new page then
        return insertTuple(tid, t);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        final PageId pid = t.getRecordId().getPageId();
        final HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        
        page.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }
    
    private class HeapFileIterator extends AbstractDbFileIterator {
        
        private final TransactionId tid;
        private boolean activeFlag = false;
        private int currentPage = 0;
        private Iterator<Tuple> currentIterator;
        
        private HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        private Iterator<Tuple> getCurrentIterator() throws DbException, TransactionAbortedException {
            final HeapPageId pid = new HeapPageId(fileId, currentPage);
            return ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            activeFlag = true;
            currentIterator = getCurrentIterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (activeFlag == false) {
                throw new DbException("The iterator has been closed or never opened");
            }
            currentPage = 0;
            open();
        }
        
        @Override
        public void close() {
            super.close();
            activeFlag = false;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (activeFlag == false) {
                return null;
            }
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            } else if (currentPage + 1 < numPages()) {
                currentPage++;
                currentIterator = getCurrentIterator();
                if (currentIterator.hasNext()) {
                    return currentIterator.next();
                }
            }
            return null;
        }
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}


package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNum = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
            f.seek((long) pageNum * BufferPool.getPageSize());
            byte[] bytes = new byte[BufferPool.getPageSize()];
            int read = f.read(bytes);
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException();
            }
            return new HeapPage(new HeapPageId(tableId, pageNum), bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert f != null;
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException();
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNum = page.getId().getPageNumber();
        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            f.seek((long) pageNum * BufferPool.getPageSize());
            f.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // public void writePage(PageId pageId,byte[] bytes) throws IOException {
    //     int pageNum = pageId.getPageNumber();
    //     try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
    //         f.seek((long) pageNum * BufferPool.getPageSize());
    //         f.write(bytes);
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> dirtyList = new ArrayList<>();
        BufferPool bufferPool = Database.getBufferPool();
        int tableId = getId();
        int pgNo = 0;
        for (; pgNo < numPages(); pgNo++) {
            HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                dirtyList.add(page);
                return dirtyList;
            }
        }

        HeapPage page = new HeapPage(new HeapPageId(tableId, pgNo), HeapPage.createEmptyPageData());
        // HeapPage page=(HeapPage)readPage(new HeapPageId(tableId, pgNo));
        // writePage(page.pid,HeapPage.createEmptyPageData());
        // bufferPool.getPage(tid, page.pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        dirtyList.add(page);
        writePage(page);
        return dirtyList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        List<Page> dirtyList = new ArrayList<>();
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        dirtyList.add(page);
        return dirtyList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}


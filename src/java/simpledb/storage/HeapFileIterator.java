package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private final HeapFile heapFile;
    private final TransactionId transactionId;
    private int currentPage;
    private Iterator<Tuple> iterator;

    HeapFileIterator(HeapFile hpf, TransactionId tid) {
        heapFile = hpf;
        transactionId = tid;
    }

    private Iterator<Tuple> getIterator(int pageNum) throws TransactionAbortedException, DbException {
        HeapPageId pageId = new HeapPageId(heapFile.getId(), pageNum);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
        return page.iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        currentPage = 0;
        iterator = getIterator(currentPage);
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator == null) return false;
        if (iterator.hasNext()) return true;
        currentPage++;
        if (currentPage >= heapFile.numPages()) return false;
        iterator = getIterator(currentPage);
        return hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iterator == null || !iterator.hasNext()) {
            throw new NoSuchElementException();
        }
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        iterator = null;
    }
}
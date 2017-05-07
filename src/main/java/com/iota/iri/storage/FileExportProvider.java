package com.iota.iri.storage;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.Transaction;
import com.iota.iri.utils.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static com.iota.iri.controllers.TransactionViewModel.TRINARY_SIZE;

/**
 * Created by paul on 4/18/17.
 */
public class FileExportProvider implements PersistenceProvider {
    private static final Logger log = LoggerFactory.getLogger(FileExportProvider.class);

    @Override
    public void init() throws Exception {

    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean save(Persistable model, Indexable index) throws Exception {
        if(model instanceof Transaction) {
            Transaction transaction = ((Transaction) model);
            try {
                PrintWriter writer;
                Path path = Paths.get(transaction.height != 0? "export-solid": "export", String.valueOf(getFileNumber()) + ".tx");
                writer = new PrintWriter(path.toString(), "UTF-8");
                writer.println(index.toString());
                writer.println(Converter.trytes(trits(transaction)));
                writer.println(transaction.sender);
                if(transaction.height != 0) {
                    writer.println("Height: " + String.valueOf(transaction.height));
                } else {
                    writer.println("Height: ");
                }
                writer.close();
                return true;
            } catch (UnsupportedEncodingException | FileNotFoundException e) {
                log.error("File export failed", e);
            } catch (Exception e) {
                log.error("Transaction load failed. ", e);
            } finally {

            }
        }
        return false;
    }

    @Override
    public void delete(Class<?> model, Indexable index) throws Exception {

    }

    @Override
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        return false;
    }

    @Override
    public boolean exists(Class<?> model, Indexable key) throws Exception {
        return false;
    }

    @Override
    public Persistable latest(Class<?> model) throws Exception {
        return null;
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> modelClass) throws Exception {
        return null;
    }

    @Override
    public Persistable get(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public boolean mayExist(Class<?> model, Indexable index) throws Exception {
        return false;
    }

    @Override
    public long count(Class<?> model) throws Exception {
        return 0;
    }

    @Override
    public Set<Indexable> keysStartingWith(Class<?> modelClass, byte[] value) {
        return null;
    }

    @Override
    public Persistable seek(Class<?> model, byte[] key) throws Exception {
        return null;
    }

    @Override
    public Persistable next(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public Persistable previous(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public Persistable first(Class<?> model) throws Exception {
        return null;
    }

    private static long lastFileNumber = 0L;
    private static Object lock = new Object();

    public static long getFileNumber() {
        long now = System.currentTimeMillis() * 1000;
        synchronized (lock) {
            if (now <= lastFileNumber) {
                return ++lastFileNumber;
            }
            lastFileNumber = now;
        }
        return now;
    }
    int[] trits(Transaction transaction) {
        int[] trits = new int[TRINARY_SIZE];
        if(transaction.bytes != null) {
            Converter.getTrits(transaction.bytes, trits);
        }
        return trits;
    }
}

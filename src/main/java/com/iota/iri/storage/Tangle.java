package com.iota.iri.storage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.iota.iri.model.Hash;
import com.iota.iri.model.Hashes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by paul on 3/3/17 for iri.
 */
public class Tangle {
    private static final Logger log = LoggerFactory.getLogger(Tangle.class);

    private static final Tangle instance = new Tangle();
    private final List<PersistenceProvider> persistenceProviders = new ArrayList<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private int QUERY_TIMEOUT;

    public void addPersistenceProvider(PersistenceProvider provider) {
        this.persistenceProviders.add(provider);
    }

    public void init(int timeout) throws Exception {
        QUERY_TIMEOUT = timeout;
        for(PersistenceProvider provider: this.persistenceProviders) {
            provider.init();
        }
    }


    public void shutdown() throws Exception {
        log.info("Shutting down Tangle Persistence Providers... ");
        executor.shutdown();
        executor.awaitTermination(50, TimeUnit.MILLISECONDS);
        this.persistenceProviders.forEach(PersistenceProvider::shutdown);
        this.persistenceProviders.clear();
    }

    public Persistable load(Class<?> model, Indexable index) throws Exception {
        return executor.submit(() -> {
            Persistable out = null;
            for(PersistenceProvider provider: this.persistenceProviders) {
                if((out = provider.get(model, index)) != null) {
                    break;
                }
            }
            return out;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Boolean save(Persistable model, Indexable index) throws Exception {
        return executor.submit(() -> {
            boolean exists = false;
            for(PersistenceProvider provider: persistenceProviders) {
                if(exists) {
                    provider.save(model, index);
                } else {
                   exists = provider.save(model, index);
                }
            }
            return exists;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public void delete(Class<?> model, Indexable index) throws Exception {
        executor.submit(() -> {
            for(PersistenceProvider provider: persistenceProviders) {
                provider.delete(model, index);
            }
            return null;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Persistable getLatest(Class<?> model) throws Exception {
        return executor.submit(() -> {
            Persistable latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if (latest == null) {
                    latest = provider.latest(model);
                }
            }
            return latest;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Boolean update(Persistable model, Indexable index, String item) throws Exception {
        return executor.submit(() -> {
            boolean success = false;
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(success) {
                    provider.update(model, index, item);
                } else {
                    success = provider.update(model, index, item);
                }
            }
            return success;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public static Tangle instance() {
        return instance;
    }

    public Set<Indexable> keysWithMissingReferences(Class<?> modelClass) throws Exception {
        return executor.submit(() -> {
            Set<Indexable> output = null;
            for(PersistenceProvider provider: this.persistenceProviders) {
                output = provider.keysWithMissingReferences(modelClass);
                if(output != null && output.size() > 0) {
                    break;
                }
            }
            return output;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Set<Indexable> keysStartingWith(Class<?> modelClass, byte[] value) throws Exception {
        return executor.submit(() -> {
            Set<Indexable> output = null;
            for(PersistenceProvider provider: this.persistenceProviders) {
                output = provider.keysStartingWith(modelClass, value);
                if(output.size() != 0) {
                    break;
                }
            }
            return output;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Boolean exists(Class<?> modelClass, Indexable hash) throws Exception {
        return executor.submit(() -> {
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(provider.exists(modelClass, hash)) return true;
            }
            return false;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Boolean maybeHas(Class<?> model, Indexable index) throws Exception {
        return executor.submit(() -> {
            for(PersistenceProvider provider: this.persistenceProviders) {
                if(provider.mayExist(model, index)) return true;
            }
            return false;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Long getCount(Class<?> modelClass) throws Exception {
        return executor.submit(() -> {
            long value = 0;
            for(PersistenceProvider provider: this.persistenceProviders) {
                if((value = provider.count(modelClass)) != 0) {
                    break;
                }
            }
            return value;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Persistable find(Class<?> model, byte[] key) throws Exception {
        return executor.submit(() -> {
            Persistable out = null;
            for (PersistenceProvider provider : this.persistenceProviders) {
                if ((out = provider.seek(model, key)) != null) {
                    break;
                }
            }
            return out;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Persistable next(Class<?> model, Indexable index) throws Exception {
        return executor.submit(() -> {
            Persistable latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.next(model, index);
                }
            }
            return latest;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Persistable previous(Class<?> model, Indexable index) throws Exception {
        return executor.submit(() -> {
            Persistable latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.previous(model, index);
                }
            }
            return latest;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public Persistable getFirst(Class<?> model) throws Exception {
        return executor.submit(() -> {
            Persistable latest = null;
            for(PersistenceProvider provider: persistenceProviders) {
                if(latest == null) {
                    latest = provider.first(model);
                }
            }
            return latest;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }

    public boolean merge(Persistable model, Indexable index) throws Exception {
        return executor.submit(() -> {
            boolean exists = false;
            for(PersistenceProvider provider: persistenceProviders) {
                if(exists) {
                    provider.save(model, index);
                } else {
                    exists = provider.merge(model, index);
                }
            }
            return exists;
        }).get(QUERY_TIMEOUT, TimeUnit.MICROSECONDS);
    }
}

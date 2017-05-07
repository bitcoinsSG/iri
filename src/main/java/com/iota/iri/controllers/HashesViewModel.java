package com.iota.iri.controllers;

import com.iota.iri.model.Hashes;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by paul on 5/6/17.
 */
public class HashesViewModel {
    private Hashes self;
    private Hash hash;

    private HashesViewModel(Hashes hashes, Hash hash) {
        self = hashes == null || hashes.set == null ? new Hashes(): hashes;
        this.hash = hash;
    }

    public static HashesViewModel load(Hash hash) throws ExecutionException, InterruptedException {
        return new HashesViewModel((Hashes) Tangle.instance().load(Hashes.class, hash).get(), hash);
    }

    public boolean store() throws ExecutionException, InterruptedException {
        return Tangle.instance().save(self, hash).get();
    }

    public boolean addHash(Hash theHash) {
        return getHashes().add(theHash);
    }

    public Hash getHash() {
        return hash;
    }

    public Set<Hash> getHashes() {
        if(self.set == null) {
            self.set = new HashSet<>();
        }
        return self.set;
    }
}
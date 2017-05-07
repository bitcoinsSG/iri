package com.iota.iri.model;

import com.iota.iri.storage.Persistable;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by paul on 3/8/17 for iri.
 */
public class Hashes implements Persistable {
    public Set<Hash> set;

    public byte[] bytes() {
        return set.parallelStream()
                .map(Hash::bytes)
                .reduce(ArrayUtils::addAll)
                .orElse(new byte[0]);
    }

    public void read(byte[] bytes) {
        if(bytes != null) {
            set = new HashSet<>();
            for (int i = 0; i < bytes.length; i += Hash.SIZE_IN_BYTES) {
                set.add(new Hash(bytes, i, Hash.SIZE_IN_BYTES));
            }
        }
    }
}

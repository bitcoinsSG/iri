package com.iota.iri.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by paul on 4/11/17.
 */
public class Milestone implements Serializable{
    public Integer index;
    public Hash hash;
    public Map<Hash, Long> snapshot;
}

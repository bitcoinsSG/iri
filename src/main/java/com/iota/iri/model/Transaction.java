package com.iota.iri.model;

import java.io.Serializable;

/**
 * Created by paul on 3/2/17 for iri.
 */
public class Transaction implements Serializable{

    public Hash hash;

    public byte[] bytes;
    public int validity = 0;
    public int type = 1;
    public long arrivalTime = 0;

    public final Tag tag = new Tag();
    public final Address address = new Address();
    public Bundle bundle = new Bundle();
    public Approvee trunk = new Approvee();
    public Approvee branch = new Approvee();

    public byte[] solid = new byte[]{0};
    public boolean snapshot = false;
    public long height = 0;
    public String sender = "";

    public Transaction() {}
    public Transaction(Hash hash) { this.hash = hash;}
}

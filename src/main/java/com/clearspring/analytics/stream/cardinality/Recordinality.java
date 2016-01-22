package com.clearspring.analytics.stream.cardinality;


/**
 * Java implementation of Recordinality (R) algorithm from this paper:
 * <p/>
 * http://www.dmtcs.org/pdfpapers/dmAQ0124.pdf
 * <p/>
 * Recordinality counts the number of records (more
 * generally, k-records) in the sequence
 * <p/>
 * It depends in the underlying permutation of the first
 * occurrences of distinct values, very different from the other
 * estimators
 * <p/>
 * The Recordinality estimator =>
 * Z = k * (1 + 1/k)^(rk - k + 1) -1
 * <p/>
 * E_n[Z] = n (It's an unbiased estimator of n)
 * <p/>
 * The accuracy of Recordinality in terms of SE, asymptotically, satisfacted:
 * <p/>
 * SE_n[Z] = sqrt( (n/ke)^(1/k) - 1 )
 * <p/>
 * You can find more information in these slides:
 * <p/>
 * https://www.cs.upc.edu/~conrado/research/talks/aofa2012.pdf
 * <p/>
 * <p>
 * Users have different motivations to use different types of hashing functions.
 * Rather than try to keep up with all available hash functions and to remove
 * the concern of causing future binary incompatibilities this class allows clients
 * to offer the value in hashed int or long form.  This way clients are free
 * to change their hash function on their own time line.  We recommend using Google's
 * Guava Murmur3_128 implementation as it provides good performance and speed when
 * high precision is required.  In our tests the 32bit MurmurHash function included
 * in this project is faster and produces better results than the 32 bit murmur3
 * implementation google provides.
 * </p>
 */


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.PriorityQueue;
import java.util.HashSet;
import java.util.Iterator;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.util.Bits;
import com.clearspring.analytics.util.IBuilder;
import com.clearspring.analytics.util.Varint;


public class Recordinality implements ICardinality, Serializable {
    private final int sampleSize;
    private final PriorityQueue<Long> sampleSet = new PriorityQueue<>();
    private final HashSet<Long> elements = new HashSet<>();
    private long rk;


    /**
     *  Initializes a new Recordinality instance with a configurable 'k'-size.
    */
    public Recordinality(int sampleSize) {
        this.sampleSize = sampleSize;
        this.rk = 0;
    }

    /**
     *  Process the offered hash.
     *  You can find a pseudocode in the description links
     */
    public boolean offerHashed(long hashedLong) {
        // if the element is not in the hashmap...
        if (!elements.contains(hashedLong)) {
            //if we don't have k-values this is a k-max
            if (sampleSize > sampleSet.size()) {
                elements.add(hashedLong);
                sampleSet.add(hashedLong);
                rk+=1;
                return true;
            // if we have k values but this is a k-max insert it and remove the minimum
            } else if (sampleSet.peek() < hashedLong) {
                elements.remove(sampleSet.peek());
                elements.add(hashedLong);
                sampleSet.poll();
                sampleSet.add(hashedLong);
                rk+=1;
                return true;
            }
        }
        return false;
    }


    @Override
    //I don't give support to ints yet...
    public boolean offerHashed(int hashedInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        long x = MurmurHash.hash64(o);
        return offerHashed(x);
    }

    /**
     *  Return a estimation of distinct values
     *  You can find a pseudocode in the description links
     */
    @Override
    public long cardinality() {
        if (sampleSet.size() < sampleSize) return sampleSet.size();
        else {
            long pow = rk - sampleSize + 1;
            double estimate = (sampleSize * (Math.pow(1 + (1.0 / sampleSize), pow))) - 1;
            return (long) estimate;
        }
    }


    /**
     *  Return a estimated Standar Error from the estimated cardinality
     */
    public double estimatedStandarError(){
        if (sampleSet.size() < sampleSize) return 0;
        else {
            long estimateCardinality = cardinality();
            double pow = 1.0/sampleSize;
            return Math.sqrt( Math.pow(
                        estimateCardinality/(sampleSize*Math.E), pow)
                        - 1);
        }
    }

    //Below I am not sure about how to process...

    //TODO check the sizeof, getBytes and writeBytes
    @Override
    public int sizeof() {
        return sampleSet.size() * 8;
    }

    @Override
    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        writeBytes(dos);

        return baos.toByteArray();
    }

    private void writeBytes(DataOutput serializedByteStream) throws IOException {
        serializedByteStream.writeInt(sampleSize);
        serializedByteStream.writeInt(elements.size() * 8);

        for (Long e : elements) {
            serializedByteStream.writeLong(e);
        }
    }

    //TODO check merge function
    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        throw new RecordinalityMergeException("Cannot merge Recordinality");
    }

    @SuppressWarnings("serial")
    protected static class RecordinalityMergeException extends CardinalityMergeException {
        public RecordinalityMergeException(String message) {
            super(message);
        }
    }



}

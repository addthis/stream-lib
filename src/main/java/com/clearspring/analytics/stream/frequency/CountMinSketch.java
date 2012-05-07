package com.clearspring.analytics.stream.frequency;

import java.io.*;
import java.util.Random;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch implements IFrequency {
    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private int depth;
    private int width;
    private long[][] table;
    private long[] hashA;
    private long size;
    private double eps;
    private double confidence;

    private CountMinSketch() {}

    public CountMinSketch(int depth, int width, int seed) {
	this.depth = depth;
	this.width = width;
	this.eps = 2.0/width;
	this.confidence = 1-1/Math.pow(2, depth);
	initTablesWith(depth, width, seed);
    }

    public CountMinSketch(double epsOfTotalCount, double confidence, int seed) {
	// 2/w = eps ; w = 2/eps
	// 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
	this.eps = epsOfTotalCount;
	this.confidence = confidence;
	this.width = (int)Math.ceil(2 / epsOfTotalCount);
	this.depth = (int)Math.ceil(-Math.log(1-confidence)/Math.log(2));
	initTablesWith(depth, width, seed);
    }

    private void initTablesWith(int depth, int width, int seed) {
	this.table = new long[depth][width];
	this.hashA = new long[depth];
	Random r = new Random(seed);
	// We're using a linear hash functions
	// of the form (a*x+b) mod p.
	// a,b are chosen independently for each hash function.
	// However they MUST be coprime with p, and it seems that we can set b=0.
	// See http://www.reddit.com/r/compsci/comments/t9iel/what_happens_when_you_use_a_linear_random_number/
	for(int i = 0; i < depth; ++i) {
	    hashA[i] = generateRandomCoprime(r, width);
	}
    }

    private static int gcd(int a, int b) {
	if(a<0) a=-a;
	if(b<0) b=-b;
	while(a != 0 && b != 0) {
	    if(a>b) a%=b;
	    else b%=a;
	}
	return a+b;
    }

    private static int generateRandomCoprime(Random r, int other) {
	while(true) {
	    int x = r.nextInt(Integer.MAX_VALUE);
	    if(gcd(x,other) == 1)
		return x;
	}
    }

    public double getRelativeError() {
	return eps;
    }

    public double getConfidence() {
	return confidence;
    }

    private int hash(long item, int i) {
	long res = ((hashA[i] * item) % PRIME_MODULUS) % width;
	return (int) res;
    }

    @Override
    public void add(long item, long count) {
	if(count < 0) {
	    // Actually for negative increments we'll need to use the median
	    // instead of minimum, and accuracy will suffer somewhat.
	    // Probably makes sense to add an "allow negative increments"
	    // parameter to constructor.
	    throw new IllegalArgumentException("Negative increments not implemented");
	}
	for(int i = 0; i < depth; ++i) {
	    table[i][hash(item, i)] += count;
	}
	size += count;
    }

    @Override
    public long size() {
	return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    @Override
    public long estimateCount(long item) {
	long res = Long.MAX_VALUE;
	for(int i = 0; i < depth; ++i) {
	    res = Math.min(res, table[i][hash(item, i)]);
	}
	return res;
    }

    public static byte[] serialize(CountMinSketch sketch) {
	ByteArrayOutputStream bos = new ByteArrayOutputStream();
	DataOutputStream s = new DataOutputStream(bos);
	try {
	    s.writeLong(sketch.size);
	    s.writeInt(sketch.depth);
	    s.writeInt(sketch.width);
	    for(int i = 0; i < sketch.depth; ++i) {
		s.writeLong(sketch.hashA[i]);
		for(int j = 0; j < sketch.width; ++j) {
		    s.writeLong(sketch.table[i][j]);
		}
	    }
	    return bos.toByteArray();
	} catch(IOException e) {
	    // Shouldn't happen
	    throw new RuntimeException(e);
	}
    }

    public static CountMinSketch deserialize(byte[] data) {
	ByteArrayInputStream bis = new ByteArrayInputStream(data);
	DataInputStream s = new DataInputStream(bis);
	try {
	    CountMinSketch sketch = new CountMinSketch();
	    sketch.size = s.readLong();
	    sketch.depth = s.readInt();
	    sketch.width = s.readInt();
	    sketch.eps = 2.0/sketch.width;
	    sketch.confidence = 1-1/Math.pow(2, sketch.depth);
	    sketch.hashA = new long[sketch.depth];
	    sketch.table = new long[sketch.depth][sketch.width];
	    for(int i = 0; i < sketch.depth; ++i) {
		sketch.hashA[i] = s.readLong();
		for(int j = 0; j < sketch.width; ++j) {
		    sketch.table[i][j] = s.readLong();
		}
	    }
	    return sketch;
	} catch(IOException e) {
	    // Shouldn't happen
	    throw new RuntimeException(e);
	}
    }
}

package com.clearspring.analytics.stream.frequency;

import java.util.Random;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch implements IFrequency {
    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private final int depth;
    private final int width;
    private long[][] table;
    private long[] hashA;
    private long size;
    private double eps;
    private double confidence;

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
	this.width = (int)(2 / epsOfTotalCount);
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
	// It seems that we can set b=0.
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
}

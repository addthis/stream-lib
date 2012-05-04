package com.clearspring.analytics.stream.quantile;

import java.util.*;

public class QDigest implements IQuantileEstimator {
    private long size;
    private int logCapacity;
    private double compressionFactor;
    private Map<Long,Long> node2count = new HashMap<Long,Long>();

    public QDigest(double compressionFactor) {
	this.compressionFactor = compressionFactor;
    }

    private long    value2leaf(long  x) { return (1L << logCapacity) + x; }
    private long    leaf2value(long id) { return id - (1L << logCapacity); }
    private boolean isRoot    (long id) { return id==1; }
    private boolean isLeaf    (long id) { return id >= 1L << logCapacity; }
    private long    sibling   (long id) { return (id%2 == 0) ? (id+1) : (id-1); }
    private long    parent    (long id) { return id/2; }
    private long    leftChild (long id) { return 2*id; }
    private long    rightChild(long id) { return 2*id + 1; }
    private long    rangeLeft (long id) { while (!isLeaf(id)) id = leftChild(id); return leaf2value(id); }
    private long    rangeRight(long id) { while (!isLeaf(id)) id = rightChild(id); return leaf2value(id); }

    @Override
    public void offer(long value) {
	if(value >= 1L << logCapacity) {
	    int newLogCapacity = logCapacity;
	    while(value >= 1L << newLogCapacity)
		newLogCapacity++;

	    rebuildToLogCapacity(newLogCapacity);
	}

	long leaf = value2leaf(value);
	node2count.put(leaf, get(leaf)+1);
	size++;
	compressAt(leaf);
	if(node2count.size() > 3 * compressionFactor) {
	    compressFully();
	}
    }

    public static QDigest unionOf(QDigest a, QDigest b) {
	if(a.compressionFactor != b.compressionFactor) {
	    throw new IllegalArgumentException(
		    "Compression factors must be the same: " +
		    "left is " + a.compressionFactor + ", " +
		    "right is " + b.compressionFactor);
	}
	if(a.logCapacity > b.logCapacity) return unionOf(b,a);
	QDigest res = new QDigest(a.compressionFactor);
	res.logCapacity = a.logCapacity;
	for(long k : a.node2count.keySet())
	    res.node2count.put(k, a.node2count.get(k));
	if(b.logCapacity > res.logCapacity)
	    res.rebuildToLogCapacity(b.logCapacity);
	for(long k : b.node2count.keySet())
	    res.node2count.put(k, b.get(k) + res.get(k));
	res.compressFully();

	return res;
    }

    private void rebuildToLogCapacity(int newLogCapacity) {
	Map<Long,Long> newNode2count = new HashMap<Long,Long>();
	// "rehash" to newLogCapacity.
	// E.g. when rehashing a tree with logCapacity = 2 to logCapacity = 5:
	// node 1 => 8 (+= 7 = 2^0*(2^3-1))
	// nodes 2..3 => 16..17 (+= 14 = 2^1*(2^3-1))
	// nodes 4..7 => 32..35 (+= 28 = 2^2*(2^3-1))
	// Process the keys by "layers" in the original tree.
	long scaleR = (1L << (newLogCapacity - logCapacity)) - 1;
	Long[] keys = node2count.keySet().toArray(new Long[node2count.size()]);
	Arrays.sort(keys);
	long scaleL = 1;
	for(long k : keys) {
	    while(scaleL <= k/2) scaleL <<= 1;
	    newNode2count.put(k + scaleL*scaleR, node2count.get(k));
	}
	node2count = newNode2count;
	logCapacity = newLogCapacity;
	compressFully();
    }

    private void compressFully() {
	List<Long> badNodes = new ArrayList<Long>();
	for(long node : node2count.keySet()) {
	    long atNode = node2count.get(node);
	    long atSibling = get(sibling(node));
	    long atParent = get(parent(node));
	    if (atNode + atSibling + atParent <= Math.floor(size/compressionFactor))
		badNodes.add(node);
	}
	for(long node : badNodes)
	    compressAt(node);
    }

    private void compressAt(long node) {
	double threshold = Math.floor(size / compressionFactor);
	while(!isRoot(node)) {
	    long atNode = get(node);
	    if(atNode > threshold)
		break;

	    long atSibling = get(sibling(node));
	    if(atNode + atSibling > threshold)
		break;

	    long atParent = get(parent(node));
	    if (atNode + atSibling + atParent > threshold)
		break;

	    node2count.put(parent(node), atParent + atNode + atSibling);
	    node2count.remove(node);
	    if(atSibling > 0)
		node2count.remove(sibling(node));

	    node = parent(node);
	}
    }

    private long get(long node) {
	Long res = node2count.get(node);
	return (res == null) ? 0 : res;
    }

    @Override
    public long getQuantile(double q) {
	List<long[]> ranges = toAscRanges();
	long s = 0;
	for(long[] r : ranges) {
	    if(s > q * size)
		return r[1];
	    s += r[2];
	}
	return ranges.get(ranges.size()-1)[1];
    }

    public List<long[]> toAscRanges() {
	List<long[]> ranges = new ArrayList<long[]>();
	for(long key : node2count.keySet())
	    ranges.add(new long[]{rangeLeft(key), rangeRight(key), node2count.get(key)});

	Collections.sort(ranges, new Comparator<long[]>() {
	    @Override
	    public int compare(long[] ra, long[] rb) {
		long rightA = ra[1], rightB = rb[1], sizeA = ra[1] - ra[0], sizeB = rb[1] - rb[0];
		if (rightA < rightB) return -1;
		if (rightA > rightB) return 1;
		if (sizeA < sizeB) return -1;
		if (sizeA > sizeB) return 1;
		return 0;
	    }
	});
	return ranges;
    }

}

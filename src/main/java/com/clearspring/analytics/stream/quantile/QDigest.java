package com.clearspring.analytics.stream.quantile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;

/**
 * Q-Digest datastructure.
 * <p/>
 * Answers approximate quantile queries: actual rank of the result of query(q)
 * is in q-eps .. q+eps, where eps = log(sigma)/compressionFactor
 * and log(sigma) is ceiling of binary log of the largest value inserted,
 * i.e. height of the tree.
 * <p/>
 * Two Q-Digests can be joined (see {@link #unionOf(QDigest, QDigest)}).
 * <p/>
 * Source:
 * N.Shrivastava, C.Buragohain, D.Agrawal
 * Medians and Beyond: New Aggregation Techniques for Sensor Networks
 * http://www.cs.virginia.edu/~son/cs851/papers/ucsb.sensys04.pdf
 * <p/>
 * This is a slightly modified version.
 * There is a small problem with the compression algorithm in the paper,
 * see https://plus.google.com/u/0/109909935680879695595/posts/768ZZ9Euqz6
 * <p/>
 * So we use a different algorithm here:
 * <ul>
 * <li>When an item is inserted, we compress along the path to root from the item's leaf
 * <li>When the structure becomes too large (above the theoretical bound), or
 * at "too destructive" operations (e.g. union or rebuild) we compress fully
 * </ul>
 * <p/>
 * Note that the accuracy of the structure does NOT suffer if "property 2"
 * from the paper is violated (in fact, restoring property 2 at any node
 * decreases accuracy).
 * <p/>
 * So we can say that we preserve the paper's accuracy and memory consumption claims.
 */
public class QDigest implements IQuantileEstimator {

    private static final Comparator<long[]> RANGES_COMPARATOR = new Comparator<long[]>() {
        @Override
        public int compare(long[] ra, long[] rb) {
            long rightA = ra[1], rightB = rb[1], sizeA = ra[1] - ra[0], sizeB = rb[1] - rb[0];
            if (rightA < rightB) {
                return -1;
            }
            if (rightA > rightB) {
                return 1;
            }
            if (sizeA < sizeB) {
                return -1;
            }
            if (sizeA > sizeB) {
                return 1;
            }
            return 0;
        }
    };

    private static final int MAP_INITIAL_SIZE = Hash.DEFAULT_INITIAL_SIZE;
    private static final float MAP_LOAD_FACTOR = Hash.VERY_FAST_LOAD_FACTOR;

    private long size;
    private long capacity = 1;
    private double compressionFactor;
    private Long2LongOpenHashMap node2count = new Long2LongOpenHashMap(MAP_INITIAL_SIZE, MAP_LOAD_FACTOR);

    public QDigest(double compressionFactor) {
        this.compressionFactor = compressionFactor;
    }

    private long value2leaf(long x) {
        return capacity + x;
    }

    private long leaf2value(long id) {
        return id - capacity;
    }

    private boolean isRoot(long id) {
        return id == 1;
    }

    private boolean isLeaf(long id) {
        return id >= capacity;
    }

    private long sibling(long id) {
        return (id % 2 == 0) ? (id + 1) : (id - 1);
    }

    private long parent(long id) {
        return id / 2;
    }

    private long leftChild(long id) {
        return 2 * id;
    }

    private long rightChild(long id) {
        return 2 * id + 1;
    }

    private long rangeLeft(long id) {
        while (!isLeaf(id)) {
            id = leftChild(id);
        }
        return leaf2value(id);
    }

    private long rangeRight(long id) {
        while (!isLeaf(id)) {
            id = rightChild(id);
        }
        return leaf2value(id);
    }

    @Override
    public void offer(long value) {
        if (value < 0 || value > Long.MAX_VALUE / 2) {
            throw new IllegalArgumentException("Can only accept values in the range 0.." + Long.MAX_VALUE / 2 + ", got " + value);
        }
        // Rebuild if the value is too large for the current tree height
        if (value >= capacity) {
            rebuildToCapacity(Long.highestOneBit(value) << 1);
        }

        long leaf = value2leaf(value);
        node2count.addTo(leaf, 1);
        size++;
        // Always compress at the inserted node, and recompress fully
        // if the tree becomes too large.
        // This is one sensible strategy which both is fast and keeps
        // the tree reasonably small (within the theoretical bound of 3k nodes)
        compressUpward(leaf);
        if (node2count.size() > 3 * compressionFactor) {
            compressFully();
        }
    }

    public static QDigest unionOf(QDigest a, QDigest b) {
        if (a.compressionFactor != b.compressionFactor) {
            throw new IllegalArgumentException(
                    "Compression factors must be the same: " +
                    "left is " + a.compressionFactor + ", " +
                    "right is " + b.compressionFactor);
        }
        if (a.capacity > b.capacity) {
            return unionOf(b, a);
        }

        QDigest res = new QDigest(a.compressionFactor);
        res.capacity = a.capacity;
        res.size = a.size + b.size;
        for (long k : a.node2count.keySet()) {
            res.node2count.put(k, a.node2count.get(k));
        }

        if (b.capacity > res.capacity) {
            res.rebuildToCapacity(b.capacity);
        }

        for (long k : b.node2count.keySet()) {
            res.node2count.put(k, b.get(k) + res.get(k));
        }

        res.compressFully();

        return res;
    }

    private void rebuildToCapacity(long newCapacity) {
        Long2LongOpenHashMap newNode2count = new Long2LongOpenHashMap(MAP_INITIAL_SIZE, MAP_LOAD_FACTOR);
        // rebuild to newLogCapacity.
        // This means that our current tree becomes a leftmost subtree
        // of the new tree.
        // E.g. when rebuilding a tree with logCapacity = 2
        // (i.e. storing values in 0..3) to logCapacity = 5 (i.e. 0..31):
        // node 1 => 8 (+= 7 = 2^0*(2^3-1))
        // nodes 2..3 => 16..17 (+= 14 = 2^1*(2^3-1))
        // nodes 4..7 => 32..35 (+= 28 = 2^2*(2^3-1))
        // This is easy to see if you draw it on paper.
        // Process the keys by "layers" in the original tree.
        long scaleR = newCapacity / capacity - 1;
        Long[] keys = node2count.keySet().toArray(new Long[node2count.size()]);
        Arrays.sort(keys);
        long scaleL = 1;
        for (long k : keys) {
            while (scaleL <= k / 2) {
                scaleL <<= 1;
            }
            newNode2count.put(k + scaleL * scaleR, node2count.get(k));
        }
        node2count = newNode2count;
        capacity = newCapacity;
        compressFully();
    }

    private void compressFully() {
        // Restore property 2 at each node.
        Long[] allNodes = node2count.keySet().toArray(new Long[node2count.size()]);
        for (long node : allNodes) {
            // The root node is not compressible: it has no parent and no sibling
            if (!isRoot(node)) {
                compressDownward(node);
            }
        }
    }

    /**
     * Restore P2 at node and upward the spine. Note that P2 can vanish
     * at some nodes sideways as a result of this. We'll fix that later
     * in compressFully when needed.
     */
    private void compressUpward(long node) {
        double threshold = Math.floor(size / compressionFactor);
        long atNode = get(node);
        while (!isRoot(node)) {
            if (atNode > threshold) {
                break;
            }
            long atSibling = get(sibling(node));
            if (atNode + atSibling > threshold) {
                break;
            }
            long atParent = get(parent(node));
            if (atNode + atSibling + atParent > threshold) {
                break;
            }

            node2count.addTo(parent(node), atNode + atSibling);
            node2count.remove(node);
            if (atSibling > 0) {
                node2count.remove(sibling(node));
            }
            node = parent(node);
            atNode = atParent + atNode + atSibling;
        }
    }

    /**
     * Restore P2 at seedNode and guarantee that no new violations of P2 appeared.
     */
    private void compressDownward(long seedNode) {
        double threshold = Math.floor(size / compressionFactor);
        // P2 check same as above but shorter and slower (and invoked rarely)
        LongArrayFIFOQueue q = new LongArrayFIFOQueue();
        q.enqueue(seedNode);
        while (!q.isEmpty()) {
            long node = q.dequeueLong();
            long atNode = get(node);
            long atSibling = get(sibling(node));
            if (atNode == 0 && atSibling == 0) {
                continue;
            }
            long atParent = get(parent(node));
            if (atParent + atNode + atSibling > threshold) {
                continue;
            }
            node2count.addTo(parent(node), atNode + atSibling);
            node2count.remove(node);
            node2count.remove(sibling(node));
            // Now P2 could have vanished at the node's and sibling's subtrees since they decreased.
            if (!isLeaf(node)) {
                q.enqueue(leftChild(node));
                q.enqueue(leftChild(sibling(node)));
            }
        }
    }

    private long get(long node) {
        return node2count.get(node);
    }

    @Override
    public long getQuantile(double q) {
        List<long[]> ranges = toAscRanges();
        long s = 0;
        for (long[] r : ranges) {
            s += r[2];
            if (s > q * size) {
                return r[1];
            }
        }
        return ranges.get(ranges.size() - 1)[1];
    }

    public List<long[]> toAscRanges() {
        List<long[]> ranges = new ArrayList<long[]>();
        for (long key : node2count.keySet()) {
            ranges.add(new long[]{rangeLeft(key), rangeRight(key), node2count.get(key)});
        }

        Collections.sort(ranges, RANGES_COMPARATOR);
        return ranges;
    }

    public String toString() {
        List<long[]> ranges = toAscRanges();
        StringBuilder res = new StringBuilder();
        for (long[] range : ranges) {
            if (res.length() > 0) {
                res.append(", ");
            }
            res.append(range[0]).append(" .. ").append(range[1]).append(": ").append(range[2]);
        }
        return res.toString();
    }

    public static byte[] serialize(QDigest d) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(d.size);
            s.writeDouble(d.compressionFactor);
            s.writeLong(d.capacity);
            s.writeInt(d.node2count.size());
            for (long k : d.node2count.keySet()) {
                s.writeLong(k);
                s.writeLong(d.node2count.get(k));
            }
            s.close();
            return bos.toByteArray();
        } catch (IOException e) {
            // Should never happen
            throw new RuntimeException(e);
        }
    }

    public static QDigest deserialize(byte[] b) {
        ByteArrayInputStream bis = new ByteArrayInputStream(b);
        DataInputStream s = new DataInputStream(bis);
        try {
            long size = s.readLong();
            double compressionFactor = s.readDouble();
            long capacity = s.readLong();
            int count = s.readInt();
            QDigest d = new QDigest(compressionFactor);
            d.size = size;
            d.capacity = capacity;
            for (int i = 0; i < count; ++i) {
                long k = s.readLong();
                long n = s.readLong();
                d.node2count.put(k, n);
            }
            return d;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // For debugging purposes.
    public long computeActualSize() {
        long res = 0;
        for (long x : node2count.values()) res += x;
        return res;
    }
}

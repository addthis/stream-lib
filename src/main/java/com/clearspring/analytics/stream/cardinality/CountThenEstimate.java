/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.stream.cardinality;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.clearspring.analytics.util.ExternalizableUtil;
import com.clearspring.analytics.util.IBuilder;

/**
 * Exact -> Estimator cardinality counting
 * <p/>
 * <p>
 * Avoids allocating a large block of memory for cardinality estimation until
 * a specified "tipping point" cardinality is reached.
 * </p>
 */
public class CountThenEstimate implements ICardinality, Externalizable {

    protected final static byte LC = 1;
    protected final static byte AC = 2;
    protected final static byte HLC = 3;
    protected final static byte LLC = 4;
    protected final static byte HLPC = 5;

    /**
     * Cardinality after which exact counting gives way to estimation
     */
    protected int tippingPoint;

    /**
     * True after switching to estimation
     */
    protected boolean tipped = false;

    /**
     * Factory for instantiating estimator after the tipping point is reached
     */
    protected IBuilder<ICardinality> builder;

    /**
     * Cardinality estimator
     * Null until tipping point is reached
     */
    protected ICardinality estimator;

    /**
     * Cardinality counter
     * Null after tipping point is reached
     */
    protected Set<Object> counter;

    /**
     * Default constructor
     * Exact counts up to 1000, estimation done with default Builder
     */
    public CountThenEstimate() {
        this(1000, AdaptiveCounting.Builder.obyCount(1000000000));
    }

    /**
     * @param tippingPoint Cardinality at which exact counting gives way to estimation
     * @param builder      Factory for instantiating estimator after the tipping point is reached
     */
    public CountThenEstimate(int tippingPoint, IBuilder<ICardinality> builder) {
        this.tippingPoint = tippingPoint;
        this.builder = builder;
        this.counter = new HashSet<Object>();
    }

    /**
     * Deserialization constructor
     *
     * @param bytes
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public CountThenEstimate(byte[] bytes) throws IOException, ClassNotFoundException {
        readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));

        if (!tipped && builder.sizeof() <= bytes.length) {
            tip();
        }
    }

    @Override
    public long cardinality() {
        if (tipped) {
            return estimator.cardinality();
        }
        return counter.size();
    }

    @Override
    public boolean offerHashed(long hashedLong) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(int hashedInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        boolean modified = false;

        if (tipped) {
            modified = estimator.offer(o);
        } else {
            if (counter.add(o)) {
                modified = true;
                if (counter.size() > tippingPoint) {
                    tip();
                }
            }
        }

        return modified;
    }

    @Override
    public int sizeof() {
        if (tipped) {
            return estimator.sizeof();
        }
        return -1;
    }

    /**
     * Switch from exact counting to estimation
     */
    private void tip() {
        estimator = builder.build();

        for (Object o : counter) {
            estimator.offer(o);
        }

        counter = null;
        builder = null;
        tipped = true;
    }

    public boolean tipped() {
        return tipped;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return ExternalizableUtil.toBytes(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tipped = in.readBoolean();
        if (tipped) {
            byte type = in.readByte();
            byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);

            switch (type) {
                case LC:
                    estimator = new LinearCounting(bytes);
                    break;
                case AC:
                    estimator = new AdaptiveCounting(bytes);
                    break;
                case HLC:
                    estimator = HyperLogLog.Builder.build(bytes);
                    break;
                case HLPC:
                    estimator = HyperLogLogPlus.Builder.build(bytes);
                    break;
                case LLC:
                    estimator = new LogLog(bytes);
                    break;
                default:
                    throw new IOException("Unrecognized estimator type: " + type);
            }
        } else {
            tippingPoint = in.readInt();
            builder = (IBuilder) in.readObject();
            int count = in.readInt();

            assert (count <= tippingPoint) : String.format("Invalid serialization: count (%d) > tippingPoint (%d)", count, tippingPoint);

            counter = new HashSet<Object>(count);
            for (int i = 0; i < count; i++) {
                counter.add(in.readObject());
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(tipped);
        if (tipped) {
            if (estimator instanceof LinearCounting) {
                out.writeByte(LC);
            } else if (estimator instanceof AdaptiveCounting) {
                out.writeByte(AC);
            } else if (estimator instanceof HyperLogLog) {
                out.writeByte(HLC);
            } else if (estimator instanceof HyperLogLogPlus) {
                out.writeByte(HLPC);
            } else if (estimator instanceof LogLog) {
                out.writeByte(LLC);
            } else {
                throw new IOException("Estimator unsupported for serialization: " + estimator.getClass().getName());
            }

            byte[] bytes = estimator.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } else {
            out.writeInt(tippingPoint);
            out.writeObject(builder);
            out.writeInt(counter.size());
            for (Object o : counter) {
                out.writeObject(o);
            }
        }
    }

    @Override
    public ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        if (estimators == null) {
            return mergeEstimators(this);
        }

        CountThenEstimate[] all = Arrays.copyOf(estimators, estimators.length + 1, CountThenEstimate[].class);
        all[all.length - 1] = this;
        return mergeEstimators(all);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CountThenEstimateMergeException if estimators are not mergeable (all must be CountThenEstimate made with the same builder)
     */
    public static CountThenEstimate mergeEstimators(CountThenEstimate... estimators) throws CardinalityMergeException {
        CountThenEstimate merged = null;
        int numEstimators = (estimators == null) ? 0 : estimators.length;
        if (numEstimators > 0) {
            List<ICardinality> tipped = new ArrayList<ICardinality>(numEstimators);
            List<CountThenEstimate> untipped = new ArrayList<CountThenEstimate>(numEstimators);

            for (CountThenEstimate estimator : estimators) {
                if (estimator.tipped) {
                    tipped.add(estimator.estimator);
                } else {
                    untipped.add(estimator);
                }
            }

            if (untipped.size() > 0) {
                merged = new CountThenEstimate(untipped.get(0).tippingPoint, untipped.get(0).builder);

                for (CountThenEstimate cte : untipped) {
                    for (Object o : cte.counter) {
                        merged.offer(o);
                    }
                }
            } else {
                merged = new CountThenEstimate(0, new LinearCounting.Builder(1));
                merged.tip();
                merged.estimator = tipped.remove(0);
            }

            if (!tipped.isEmpty()) {
                if (!merged.tipped) {
                    merged.tip();
                }
                merged.estimator = merged.estimator.merge(tipped.toArray(new ICardinality[tipped.size()]));
            }

        }
        return merged;
    }

    @SuppressWarnings("serial")
    protected static class CountThenEstimateMergeException extends CardinalityMergeException {

        public CountThenEstimateMergeException(String message) {
            super(message);
        }
    }
}

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

public class HyperBitBit implements ICardinality {

    public HyperBitBit() {}

    @Override
    boolean offer(Object o) {
        return false;
    }

    @Override
    boolean offerHashed(long hashedLong) {
        return false;
    }

    @Override
    boolean offerHashed(int hashedInt) {
        return false;
    }

    @Override
    long cardinality() {
        return 0;
    }

    @Override
    int sizeof() {
        return 0;
    }

    @Override
    byte[] getBytes() throws IOException {
        return new byte[0];
    }

    @Override
    ICardinality merge(ICardinality... estimators) throws CardinalityMergeException {
        HyperBitBit merged = new HyperBitBit();
        return merged;
    }

}

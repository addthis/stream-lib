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


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHyperBitBit {

    @Test
    public void testComputeCount() {
        HyperBitBit hyperBitBit = new HyperBitBit();
        hyperBitBit.offer(0);
        hyperBitBit.offer(1);
        hyperBitBit.offer(2);
        hyperBitBit.offer(3);
        hyperBitBit.offer(16);
        hyperBitBit.offer(17);
        hyperBitBit.offer(18);
        hyperBitBit.offer(19);
        hyperBitBit.offer(19);
        assertEquals(8, hyperBitBit.cardinality());
    }
}

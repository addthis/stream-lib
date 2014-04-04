/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearspring.analytics.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Toy version of the guava class.  Only implemented here to avoid adding
 * a dependency.  It would be better to just depend on guava.
 */
public class Lists {

    public static <T> List<T> newArrayList(Iterable<T> source) {
        List<T> r = new ArrayList<T>();
        for (T x : source) {
            r.add(x);
        }
        return r;
    }

    public static <T> List<T> newArrayList() {
        return new ArrayList<T>();
    }
}

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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Rough and ready clone of the Guava AbstractIterator.  I just did this
 * to avoid needing to add the guava dependency.  It would be better to
 * just use quava.
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    private enum State {
        NOT_STARTED, DONE, HAS_DATA, EMPTY
    }

    private T next;

    private State currentState = State.NOT_STARTED;

    @Override
    public boolean hasNext() {
        switch (currentState) {
            case DONE:
                return false;
            case NOT_STARTED:
                currentState = State.HAS_DATA;
                next = computeNext();
                break;
            case HAS_DATA:
                return true;
            case EMPTY:
                currentState = State.HAS_DATA;
                next = computeNext();
                break;
        }
        return currentState != State.DONE;
    }

    @Override
    public T next() {
        if (hasNext()) {
            T r = next;
            currentState = State.EMPTY;
            return r;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Can't remove from an abstract iterator");
    }

    protected abstract T computeNext();

    public T endOfData() {
        currentState = State.DONE;
        return null;
    }
}

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

package com.clearspring.analytics.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.List;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;


/**
 * Simple TopK command line utility
 * <p/>
 * Usage:
 * > topk [capacity] [update-rate]
 * <p/>
 * capacity   : size of top / k (defaults to 1000)
 * update-rate: output results after every update-rate elements/lines
 * <p/>
 * Example:
 * > cat elements.txt | topk 10
 */
public class TopK {

    public static void usage() {
        System.err.println
                (
                        "topk [capacity] [update-rate]\n" +
                        "\n" +
                        "capacity   : size of top / k (defaults to 1000)" +
                        "update-rate: output results after every update-rate elements/lines" +
                        "\n" +
                        "Example:" +
                        "> cat elements.txt | topk 10" +
                        "\n"
                );

        System.exit(-1);
    }

    public static void main(String[] args) throws IOException {
        long updateRate = -1;
        long count = 0;
        int capacity = 1000;

        if (args.length > 0) {
            try {
                capacity = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.print("Bad capacity: '" + args[0] + "'  Capacity must be an integer.");
                usage();
            }
        }

        if (args.length > 1) {
            try {
                updateRate = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                System.err.print("Bade update rate: '" + args[1] + "'  Update rate must be an integer.");
                usage();
            }
        }

        StreamSummary<String> topk = new StreamSummary<String>(capacity);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        String line = null;
        while ((line = in.readLine()) != null) {
            topk.offer(line);
            count++;

            if (updateRate > 0 && count % updateRate == 0) {
                System.out.println(formatSummary(topk));
                System.out.println("Item count: " + count);
                System.out.println();
            }
        }

        System.out.println(formatSummary(topk));
        System.out.println("Item count: " + count);
    }

    public static String formatSummary(StreamSummary<String> topk) {
        StringBuilder sb = new StringBuilder();

        List<Counter<String>> counters = topk.topK(topk.getCapacity());
        String itemHeader = "item";
        String countHeader = "count";
        String errorHeader = "error";

        int maxItemLen = itemHeader.length();
        int maxCountLen = countHeader.length();
        int maxErrorLen = errorHeader.length();

        for (Counter<String> counter : counters) {
            maxItemLen = Math.max(counter.getItem().length(), maxItemLen);
            maxCountLen = Math.max(Long.toString(counter.getCount()).length(), maxCountLen);
            maxErrorLen = Math.max(Long.toString(counter.getError()).length(), maxErrorLen);
        }

        sb.append(String.format("%" + maxItemLen + "s %" + maxCountLen + "s %" + maxErrorLen + "s", itemHeader, countHeader, errorHeader));
        sb.append('\n');
        sb.append(String.format("%" + maxItemLen + "s %" + maxCountLen + "s %" + maxErrorLen + "s", string('-', maxItemLen), string('-', maxCountLen), string('-', maxErrorLen)));
        sb.append('\n');

        for (Counter<String> counter : counters) {
            sb.append(String.format("%" + maxItemLen + "s %" + maxCountLen + "d %" + maxErrorLen + "d", counter.getItem(), counter.getCount(), counter.getError()));
            sb.append('\n');
        }

        return sb.toString();
    }

    public static String string(char c, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}

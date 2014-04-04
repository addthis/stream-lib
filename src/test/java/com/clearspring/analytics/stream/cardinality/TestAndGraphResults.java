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

import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.clearspring.analytics.util.IBuilder;
import com.clearspring.analytics.util.Pair;

import com.googlecode.charts4j.AxisLabelsFactory;
import com.googlecode.charts4j.Color;
import com.googlecode.charts4j.Data;
import com.googlecode.charts4j.DataUtil;
import com.googlecode.charts4j.GCharts;
import com.googlecode.charts4j.LineStyle;
import com.googlecode.charts4j.Plots;
import com.googlecode.charts4j.Shape;
import com.googlecode.charts4j.XYLine;
import com.googlecode.charts4j.XYLineChart;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@Ignore
@RunWith(Parameterized.class)
public class TestAndGraphResults {

    private static int CARDINALITY = 50000000;
    private static int NUM_RESULTS = 10000;
    private static int NUM_TRIALS = 1000;

    private final int k;

    @Parameters
    public static Collection<Object[]> kValues() {
        return Arrays.asList(new Object[][]{{15}, {16}, {17}, {10}});
    }

    public TestAndGraphResults(int k) {
        this.k = k;
    }

    @Test
    public void testLogLog() {
        testLogLog(new LogLog.Builder(), "k%2d %8d max:%f avg:%f min:%f");
    }

    private List<Pair<Integer, double[]>> testLogLog(IBuilder<ICardinality> builder, String fmt) {
        final int outputIncrement = CARDINALITY / NUM_RESULTS;
        List<Pair<Integer, double[]>> maxMeanMin = new ArrayList<Pair<Integer, double[]>>(NUM_RESULTS);
        ICardinality[] estimators = new ICardinality[NUM_TRIALS];

        for (int t = 0; t < NUM_TRIALS; t++) {
            estimators[t] = builder.build();
            //estimators[t] = new AdaptiveCounting(k);
            //estimators[t] = new LinearCounting(1 << k);
        }

        for (int n = 0; n < CARDINALITY; n++) {
            double[] mmm = {Double.MIN_VALUE, 0, Double.MAX_VALUE};
            int card = n + 1;
            boolean output = card % outputIncrement == 0;
            for (int t = 0; t < NUM_TRIALS; t++) {
                estimators[t].offer(TestICardinality.streamElement(0));

                if (output) {
                    double err = Math.abs(estimators[t].cardinality() - card) / (double) card;
                    if (err > mmm[0]) {
                        mmm[0] = err;
                    }
                    mmm[1] += err;
                    if (err < mmm[2]) {
                        mmm[2] = err;
                    }
                }
            }

            if (output) {
                mmm[1] /= NUM_TRIALS;
                maxMeanMin.add(new Pair<Integer, double[]>(card, mmm));
                System.out.println(String.format(fmt, k, card, mmm[0], mmm[1], mmm[2]));
            }
        }
        return maxMeanMin;
    }


    public static int parseInt(String val, int def, int radix) {
        try {
            return Integer.parseInt(val, radix);
        } catch (Exception ex) {
            return def;
        }
    }


    /**
     * @param args {k}
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public static void main(String[] args) throws SecurityException, NoSuchMethodException {
        int k = 16;
        boolean graph = true;
        if (args.length > 0) {
            k = parseInt(args[0], k, 10);
        }
        if (args.length > 1) {
            CARDINALITY = parseInt(args[1], CARDINALITY, 10);
        }
        if (args.length > 2) {
            NUM_TRIALS = parseInt(args[2], NUM_TRIALS, 10);
        }
        if (args.length > 3) {
            NUM_RESULTS = parseInt(args[3], NUM_RESULTS, 10);
        }
        if (args.length > 4) {
            graph = Boolean.parseBoolean(args[4]);
        }

        IBuilder<ICardinality> builder = new AdaptiveCounting.Builder(k);
        //IBuilder<ICardinality> builder = new LogLog.Builder(k);
        //IBuilder<ICardinality> builder = new LinearCounting.Builder(1 << k);
        //IBuilder<ICardinality> builder = LinearCounting.Builder.onePercentError(100000000);
        String estimator = builder.getClass().getDeclaredMethod("build").getReturnType().getSimpleName();

        System.out.println("estimator : " + estimator);
        System.out.println("k         : " + k);
        System.out.println("n-Max     : " + CARDINALITY);
        System.out.println("Trials    : " + NUM_TRIALS);
        System.out.println("Results   : " + NUM_RESULTS);
        System.out.println();

        System.out.println(String.format("%8s %-8s %-8s %-8s", "n", "max", "avg", "min"));
        List<Pair<Integer, double[]>> results = new TestAndGraphResults(k).testLogLog(builder, "%2$8d %3$8f %4$8f %5$8f");

        if (graph) {
            graphResults(results, k, estimator);
        }
    }

    private static void graphResults(List<Pair<Integer, double[]>> results, int k, String estimator) {
        double[] xData = new double[results.size()];
        double[] maxData = new double[results.size()];
        double[] meanData = new double[results.size()];
        double[] minData = new double[results.size()];

        double maxMax = Double.MIN_VALUE;
        double minMin = Double.MAX_VALUE;

        for (int r = 0; r < results.size(); r++) {
            Pair<Integer, double[]> result = results.get(r);
            xData[r] = result.left;
            maxData[r] = result.right[0];
            meanData[r] = result.right[1];
            minData[r] = result.right[2];

            maxMax = Math.max(maxData[r], maxMax);
            minMin = Math.min(minData[r], minMin);
        }

        Data x = DataUtil.scale(xData);
        Data max = DataUtil.scaleWithinRange(minMin, maxMax, maxData);
        Data mean = DataUtil.scaleWithinRange(minMin, maxMax, meanData);
        Data min = DataUtil.scaleWithinRange(minMin, maxMax, minData);

        XYLine maxLine = Plots.newXYLine(x, max);
        maxLine.addShapeMarkers(Shape.SQUARE, Color.GRAY, 2);
        maxLine.setColor(Color.RED);
        maxLine.setLineStyle(LineStyle.THIN_LINE);
        maxLine.setLegend("Max");

        XYLine meanLine = Plots.newXYLine(x, mean);
        meanLine.addShapeMarkers(Shape.SQUARE, Color.GRAY, 2);
        meanLine.setColor(Color.YELLOW);
        meanLine.setLineStyle(LineStyle.THIN_LINE);
        meanLine.setLegend("Mean");

        XYLine minLine = Plots.newXYLine(x, min);
        minLine.addShapeMarkers(Shape.SQUARE, Color.GRAY, 2);
        minLine.setColor(Color.GREEN);
        minLine.setLineStyle(LineStyle.THIN_LINE);
        minLine.setLegend("Min");


        String title = String.format("%s (k=%d, %d trials per data point)", estimator, k, NUM_TRIALS);
        XYLineChart chart = GCharts.newXYLineChart(maxLine, meanLine, minLine);

        /* 
        List<String> xAxisLabels = new ArrayList<String>(xData.length);
        for(int i=0; i<xData.length; i++)
        {
            if(i % (xData.length / 10) == 0)
            {
                double d = xData[i];
                xAxisLabels.add(shortenNumericLabel(d));
            }
        }
            
        chart.addXAxisLabels(AxisLabelsFactory.newAxisLabels(xAxisLabels));
        */
        chart.addXAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(xData[0], xData[xData.length - 1]));
        chart.addXAxisLabels(AxisLabelsFactory.newAxisLabels("Cardinality", 50));
        chart.addYAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(minMin * 100, maxMax * 100));
        chart.addYAxisLabels(AxisLabelsFactory.newAxisLabels("% Error", 50));
        chart.setTitle(title);
        chart.setSize(533, 400);
        chart.setMargins(25, 25, 25, 25);

        double good = DataUtil.scaleWithinRange(minMin, maxMax, new double[]{0.01}).getData()[0];
        double bad = DataUtil.scaleWithinRange(minMin, maxMax, new double[]{0.03}).getData()[0];
        double wrong = DataUtil.scaleWithinRange(minMin, maxMax, new double[]{0.05}).getData()[0];

        if (0 <= good && good < 100) {
            chart.addHorizontalRangeMarker(good, Math.min(bad, 100), Color.LIGHTGREEN);
        }
        if (0 <= bad && bad < 100) {
            chart.addHorizontalRangeMarker(bad, Math.min(wrong, 100), Color.LIGHTGOLDENRODYELLOW);
        }
        if (wrong < 100) {
            chart.addHorizontalRangeMarker(Math.max(wrong, 0), 100, Color.LIGHTCORAL);
        }


        String url = chart.toURLForHTML();

        // Somewhat less than clean
        String pageStart = "<html><head><title>" + title + "</title></head><body>";
        String img = String.format("<img src=\"%s\"/>", url);
        String pageEnd = "</body></html>";

        String page = pageStart + img + pageEnd;

        FileWriter fout = null;
        try {
            fout = new FileWriter(String.format("%s_k%02d_n%010d_t%08d_r%06d.html", acronym(estimator), k, CARDINALITY, NUM_TRIALS, NUM_RESULTS));
            fout.append(page);
            fout.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String acronym(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    @SuppressWarnings("unused")
    private static String shortenNumericLabel(double d) {
        String suffix = " ";
        if (d >= 1000000) {
            d /= 1000000;
            suffix = "M";
        } else if (d >= 1000) {
            d /= 1000;
            suffix = "K";
        }
        return String.format("%.0f%s", d, suffix);

    }

}

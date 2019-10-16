/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.mapping.QuadraticallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.CollapsingHighestDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

/**
 * {@link #memoryOptimal} constructs a sketch with a logarithmic index mapping, hence low
 * memory footprint, whereas {@link #fast} and {@link #balanced} offer faster ingestion speeds at the cost of
 * larger memory footprints. The size of the sketch can be upper-bounded by using collapsing stores. For instance,
 * {@link #memoryOptimalCollapsingLowest} is the version of {@code DDSketch} described in the paper, and also
 * implemented in <a href="https://github.com/DataDog/sketches-go/">Go</a>
 * and <a href="https://github.com/DataDog/sketches-py/">Python</a>.
 *
 * <p>
 * The memory size of the sketch depends on the range that is covered by the input values: the larger that range, the
 * more bins are needed to keep track of the input values. As a rough estimate, if working on durations using
 * {@link #memoryOptimal} with a relative accuracy of 2%, about 2kB (275 bins) are needed to cover values between 1
 * millisecond and 1 minute, and about 6kB (802 bins) to cover values between 1 nanosecond and 1 day. The number of
 * bins that are maintained can be upper-bounded using collapsing stores (see for example
 * {@link #memoryOptimalCollapsingLowest} and {@link #memoryOptimalCollapsingHighest}).
 */
public class DDSketchFactory {

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a balanced instance of {@code DDSketch}
     */
    public static DDSketch balanced(double relativeAccuracy) {
        return new DDSketch(
                new QuadraticallyInterpolatedMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint, using
     * a limited number of bins. When the maximum number of bins is reached, bins with lowest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch balancedCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new QuadraticallyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a balanced instance of {@code DDSketch}, with high ingestion speed and low memory footprint,, using
     * a limited number of bins. When the maximum number of bins is reached, bins with highest indices are collapsed,
     * which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a balanced instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch balancedCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new QuadraticallyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a fast instance of {@code DDSketch}
     */
    public static DDSketch fast(double relativeAccuracy) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage, using a limited number of bins. When the maximum number of bins is reached, bins with lowest indices
     * are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch fastCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a fast instance of {@code DDSketch}, with optimized ingestion speed, at the cost of higher memory
     * usage, using a limited number of bins. When the maximum number of bins is reached, bins with highest indices
     * are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a fast instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch fastCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new BitwiseLinearlyInterpolatedMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch
     * @return a memory-optimal instance of {@code DDSketch}
     */
    public static DDSketch memoryOptimal(double relativeAccuracy) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                UnboundedSizeDenseStore::new
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with lowest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on lowest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch memoryOptimalCollapsingLowest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingLowestDenseStore(maxNumBins)
        );
    }

    /**
     * Constructs a memory-optimal instance of {@code DDSketch}, with optimized memory usage, at the cost of lower
     * ingestion speed, using a limited number of bins. When the maximum number of bins is reached, bins with highest
     * indices are collapsed, which causes the relative accuracy guarantee to be lost on highest quantiles.
     *
     * @param relativeAccuracy the relative accuracy guaranteed by the sketch (for non-collapsed bins)
     * @param maxNumBins the maximum number of bins to be maintained
     * @return a memory-optimal instance of {@code DDSketch} using a limited number of bins
     */
    public static DDSketch memoryOptimalCollapsingHighest(double relativeAccuracy, int maxNumBins) {
        return new DDSketch(
                new LogarithmicMapping(relativeAccuracy),
                () -> new CollapsingHighestDenseStore(maxNumBins)
        );
    }
}

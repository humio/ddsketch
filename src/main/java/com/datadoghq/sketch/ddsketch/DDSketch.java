/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import com.datadoghq.sketch.QuantileSketch;
import com.datadoghq.sketch.ddsketch.mapping.BitwiseLinearlyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.mapping.QuadraticallyInterpolatedMapping;
import com.datadoghq.sketch.ddsketch.store.Bin;
import com.datadoghq.sketch.ddsketch.store.CollapsingHighestDenseStore;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A {@link QuantileSketch} with relative-error guarantees. This sketch computes quantile values with an
 * approximation error that is relative to the actual quantile value. It works on non-negative input values.
 * <p>
 * For instance, using {@code DDSketch} with a relative accuracy guarantee set to 1%, if the expected quantile
 * value is 100, the computed quantile value is guaranteed to be between 99 and 101. If the expected quantile value
 * is 1000, the computed quantile value is guaranteed to be between 990 and 1010.
 * <p>
 * It collapses lowest bins when the maximum number of buckets is reached. For using a specific
 * {@link IndexMapping} or a specific implementation of {@link Store}, the constructor can be used
 * ({@link #DDSketch(IndexMapping, Supplier)}).
 * <p>
 * {@code DDSketch} works by mapping floating-point input values to bins and counting the number of values for each
 * bin. The mapping to bins is handled by {@link IndexMapping}, while the underlying structure that keeps track of
 * bin counts is {@link Store}.
 * <p>
 * Note that this implementation is not thread-safe.
 */
public class DDSketch implements QuantileSketch<DDSketch>, Serializable {

    private final IndexMapping indexMapping;
    private final double minIndexedValue;
    private final double maxIndexedValue;

    private final Store store;
    private long zeroCount;

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     * @see DDSketchFactory#balanced
     * @see DDSketchFactory#balanced
     * @see DDSketchFactory#balancedCollapsingLowest
     * @see DDSketchFactory#balancedCollapsingHighest
     * @see DDSketchFactory#fast
     * @see DDSketchFactory#fastCollapsingLowest
     * @see DDSketchFactory#fastCollapsingHighest
     * @see DDSketchFactory#memoryOptimal
     * @see DDSketchFactory#memoryOptimalCollapsingLowest
     * @see DDSketchFactory#memoryOptimalCollapsingHighest
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier) {
        this(indexMapping, storeSupplier, 0);
    }

    /**
     * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and {@link Store}
     * supplier.
     *
     * @param indexMapping the mapping between floating-point values and integer indices to be used by the sketch
     * @param storeSupplier the store constructor for keeping track of added values
     * @param minIndexedValue the least value that should be distinguished from zero
     * @see DDSketchFactory#balanced
     * @see DDSketchFactory#balancedCollapsingLowest
     * @see DDSketchFactory#balancedCollapsingHighest
     * @see DDSketchFactory#fast
     * @see DDSketchFactory#fastCollapsingLowest
     * @see DDSketchFactory#fastCollapsingHighest
     * @see DDSketchFactory#memoryOptimal
     * @see DDSketchFactory#memoryOptimalCollapsingLowest
     * @see DDSketchFactory#memoryOptimalCollapsingHighest
     */
    public DDSketch(IndexMapping indexMapping, Supplier<Store> storeSupplier, double minIndexedValue) {
        this.indexMapping = indexMapping;
        this.minIndexedValue = Math.max(minIndexedValue, indexMapping.minIndexableValue());
        this.maxIndexedValue = indexMapping.maxIndexableValue();
        this.store = storeSupplier.get();
        this.zeroCount = 0;
    }

    private DDSketch(DDSketch sketch) {
        this.indexMapping = sketch.indexMapping;
        this.minIndexedValue = sketch.minIndexedValue;
        this.maxIndexedValue = sketch.maxIndexedValue;
        this.store = sketch.store.copy();
        this.zeroCount = sketch.zeroCount;
    }

    public IndexMapping getIndexMapping() {
        return indexMapping;
    }

    public Store getStore() {
        return store;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the value is outside the range that is tracked by the sketch
     */
    @Override
    public void accept(double value) {

        checkValueTrackable(value);

        if (value < minIndexedValue) {
            zeroCount++;
        } else {
            store.add(indexMapping.index(value));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the value is outside the range that is tracked by the sketch
     */
    @Override
    public void accept(double value, long count) {

        checkValueTrackable(value);

        if (value < minIndexedValue) {
            if (count < 0) {
                throw new IllegalArgumentException("The count cannot be negative.");
            }
            zeroCount += count;
        } else {
            store.add(indexMapping.index(value), count);
        }
    }

    private void checkValueTrackable(double value) {

        if (value < 0 || value > maxIndexedValue) {
            throw new IllegalArgumentException("The input value is outside the range that is tracked by the sketch.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the other sketch does not use the same index mapping
     */
    @Override
    public void mergeWith(DDSketch other) {

        if (!indexMapping.equals(other.indexMapping)) {
            throw new IllegalArgumentException(
                    "The sketches are not mergeable because they do not use the same index mappings."
            );
        }

        store.mergeWith(other.store);
        zeroCount += other.zeroCount;
    }

    @Override
    public DDSketch copy() {
        return new DDSketch(this);
    }

    @Override
    public boolean isEmpty() {
        return zeroCount == 0 && store.isEmpty();
    }

    @Override
    public long getCount() {
        return zeroCount + store.getTotalCount();
    }

    @Override
    public double getMinValue() {
        if (zeroCount > 0) {
            return 0;
        } else {
            return indexMapping.value(store.getMinIndex());
        }
    }

    @Override
    public double getMaxValue() {
        if (zeroCount > 0 && store.isEmpty()) {
            return 0;
        } else {
            return indexMapping.value(store.getMaxIndex());
        }
    }

    @Override
    public double getValueAtQuantile(double quantile) {
        return getValueAtQuantile(quantile, getCount());
    }

    @Override
    public double[] getValuesAtQuantiles(double[] quantiles) {
        final long count = getCount();
        return Arrays.stream(quantiles)
                .map(quantile -> getValueAtQuantile(quantile, count))
                .toArray();
    }

    private double getValueAtQuantile(double quantile, long count) {

        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("The quantile must be between 0 and 1.");
        }

        if (count == 0) {
            throw new NoSuchElementException();
        }

        final long rank = (long) (quantile * (count - 1));
        if (rank < zeroCount) {
            return 0;
        }

        Bin bin;
        if (quantile <= 0.5) {

            final Iterator<Bin> binIterator = store.getAscendingIterator();
            long n = zeroCount;
            do {
                bin = binIterator.next();
                n += bin.getCount();
            } while (n <= rank && binIterator.hasNext());

        } else {

            final Iterator<Bin> binIterator = store.getDescendingIterator();
            long n = count;
            do {
                bin = binIterator.next();
                n -= bin.getCount();
            } while (n > rank && binIterator.hasNext());
        }

        return indexMapping.value(bin.getIndex());
    }
/*
    // TODO(gburd): implement
    public final void writeObject(Object obj)
            throws IOException {
        indexMapping.writeObject()
        store.writeObject()
    }

    // TODO(gburd): implement
    public final Object readObject()
            throws IOException,
            ClassNotFoundException {
        return null;
    }
*/
}

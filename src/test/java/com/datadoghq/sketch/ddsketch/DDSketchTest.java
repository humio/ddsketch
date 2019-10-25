/* Unless explicitly stated otherwise all files in this repository are licensed under the Apache License 2.0.
 * This product includes software developed at Datadog (https://www.datadoghq.com/).
 * Copyright 2019 Datadog, Inc.
 */

package com.datadoghq.sketch.ddsketch;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.datadoghq.sketch.QuantileSketchTest;
import com.datadoghq.sketch.ddsketch.mapping.IndexMapping;
import com.datadoghq.sketch.ddsketch.mapping.LogarithmicMapping;
import com.datadoghq.sketch.ddsketch.store.Store;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

abstract class DDSketchTest extends QuantileSketchTest<DDSketch> {

    abstract double relativeAccuracy();

    IndexMapping mapping() {
        return new LogarithmicMapping(relativeAccuracy());
    }

    Supplier<Store> storeSupplier() {
        return UnboundedSizeDenseStore::new;
    }


    @Override
    public DDSketch newSketch() {
        return new DDSketch(mapping(), storeSupplier());
    }

    @Override
    protected void assertAccurate(boolean merged, double[] sortedValues, double quantile, double actualQuantileValue) {

        if (sortedValues[0] < 0) {
            throw new IllegalArgumentException();
        }
        if (actualQuantileValue < 0) {
            fail();
        }

        final double lowerQuantileValue = sortedValues[(int) Math.floor(quantile * (sortedValues.length - 1))];
        final double upperQuantileValue = sortedValues[(int) Math.ceil(quantile * (sortedValues.length - 1))];

        final double minExpected = lowerQuantileValue * (1 - relativeAccuracy());
        final double maxExpected = upperQuantileValue * (1 + relativeAccuracy());

        if (actualQuantileValue < minExpected || actualQuantileValue > maxExpected) {
            fail();
        }
    }

    @Test
    @Override
    protected void throwsExceptionWhenExpected() {

        super.throwsExceptionWhenExpected();

        final DDSketch sketch = newSketch();

        assertThrows(
            IllegalArgumentException.class,
            () -> sketch.accept(-1)
        );
    }

    @Test
    void serializesSketch() {
        Random rng = new Random();
        Double percentiles[] = new Double[25];
        DDSketch sketch = DDSketch.balanced(0.1);

        // The percentiles we'll test before/after serialization.
        for (int i = 0; i < 25; i++) {
            final double rangeMin = .01;
            final double rangeMax = .99;
            percentiles[i] = rangeMin + (rangeMax - rangeMin) * rng.nextDouble();
        }
        Arrays.sort(percentiles);

        // Some random data to initialize the sketch.
        for (int i = 0; i < 10000; i++) {
            sketch.accept(rng.nextDouble());
        }
        sketch.accept(0.0);

        // Serialization
        byte[] data = new byte[0];
        try {
            //Saving of object in a file
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);

            // Method for serialization of object
            oos.writeObject(sketch);

            oos.close();
            data = baos.toByteArray();
            baos.close();

            System.out.println("Object has been serialized");
        } catch(IOException ex) {
            System.out.println("IOException is caught");
        }


        DDSketch sketch2 = null;

        // Deserialization
        try {
            // Reading the object from a file
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bais);

            // Method for deserialization of object
            sketch2 = (DDSketch)ois.readObject();

            ois.close();
            bais.close();

            System.out.println("Object has been deserialized ");
        } catch(IOException ex) {
            System.out.println("IOException is caught");
        } catch(ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }

        for (Double percentile : percentiles) {
            assert(sketch.getValueAtQuantile(percentile) == sketch2.getValueAtQuantile(percentile));
        }
    }

    static class DDSketchTest1 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-1;
        }
    }

    static class DDSketchTest2 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-2;
        }
    }

    static class DDSketchTest3 extends DDSketchTest {

        @Override
        double relativeAccuracy() {
            return 1e-3;
        }
    }
}

package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoublePairWritable implements Writable {

    private DoubleWritable first;
    private DoubleWritable second;

    public DoublePairWritable() {
        this.first = new DoubleWritable();
        this.second = new DoubleWritable();
    }

    public DoublePairWritable(double first, double second) {
        this.first = new DoubleWritable(first);
        this.second = new DoubleWritable(second);
    }

    public double getFirst() {
        return first.get();
    }

    public double getSecond() {
        return second.get();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}

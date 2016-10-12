package com.cjy.test.write_file.akka;

public class FinishWriteMessage {
    private int index;
    private long totalLength;

    public FinishWriteMessage(int index, long totalLength) {
        this.index = index;
        this.totalLength = totalLength;
    }

    public long getTotalLength() {
        return totalLength;
    }

    public int getIndex() {
        return index;
    }
}

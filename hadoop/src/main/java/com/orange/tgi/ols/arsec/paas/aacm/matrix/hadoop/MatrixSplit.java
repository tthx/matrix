package com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class MatrixSplit extends InputSplit implements Writable {
  private final static String[] EMPTY = {};
  private int iTask, jTask;
  private long length;

  public int getiTask() {
    return iTask;
  }

  public void setiTask(int iTask) {
    this.iTask = iTask;
  }

  public int getjTask() {
    return jTask;
  }

  public void setjTask(int jTask) {
    this.jTask = jTask;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public MatrixSplit() {
  }

  public MatrixSplit(final int iTask, final int jTask, final long length) {
    this.iTask = iTask;
    this.jTask = jTask;
    this.length = length;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return EMPTY;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    iTask = in.readInt();
    jTask = in.readInt();
    length = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(iTask);
    out.writeInt(jTask);
    out.writeLong(length);
    ;
  }

}

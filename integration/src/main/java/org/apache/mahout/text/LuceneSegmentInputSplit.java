package org.apache.mahout.text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * {@link InputSplit} implementation that contains a single Lucene segment.
 */
public class LuceneSegmentInputSplit extends InputSplit implements Writable {

  private String segmentInfoName;
  private long length;

  public LuceneSegmentInputSplit() {
    // For deserialization
  }

  public LuceneSegmentInputSplit(String segmentInfoName, long length) {
    this.segmentInfoName = segmentInfoName + "*";
    this.length = length;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[]{};
  }

  public String getSegmentInfoName() {
    return segmentInfoName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(segmentInfoName);
    out.writeLong(length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.segmentInfoName = in.readUTF();
    this.length = in.readLong();
  }
}
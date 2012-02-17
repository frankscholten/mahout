package org.apache.mahout.text;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * {@link InputSplit} implementation that represents a Lucene segment.
 */
public class LuceneSegmentInputSplit extends InputSplit implements Writable {

  private String segmentInfoName;
  private long length;

  public LuceneSegmentInputSplit() {
    // For deserialization
  }

  public LuceneSegmentInputSplit(String segmentInfoName, long length) {
    this.segmentInfoName = segmentInfoName;
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

  /**
   * Get the {@link SegmentInfo} of this {@link InputSplit} in the given lucene {@link Directory}
   * 
   * @param directory that contains the segment
   *
   * @return the segment info or throws exception if not found
   *
   * @throws IOException if an error occurs when accessing the directory
   */
  public SegmentInfo getSegment(Directory directory) throws IOException {
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(directory);

    List<SegmentInfo> segmentInfoList = segmentInfos.asList();

    for (SegmentInfo segmentInfo : segmentInfoList) {
      if (segmentInfo.name.equals(segmentInfoName)) {
        return segmentInfo;
      }
    }

    throw new IllegalArgumentException("No such segment: '" + segmentInfoName + "' in directory " + directory.toString());
  }
}
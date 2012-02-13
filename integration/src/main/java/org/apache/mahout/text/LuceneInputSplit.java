package org.apache.mahout.text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
*
*/
public class LuceneInputSplit extends InputSplit implements Writable {

  int pos;
  int nrOfDocs;

  public LuceneInputSplit() {
  }

  LuceneInputSplit(int pos, int nrOfDocs) {
    this.pos = pos;
    this.nrOfDocs = nrOfDocs;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return nrOfDocs;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[]{};
  }

  public int getPos() {
    return pos;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(pos);
    out.writeInt(nrOfDocs);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.pos = in.readInt();
    this.nrOfDocs = in.readInt();
  }
}

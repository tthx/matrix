package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.BlockKey;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.Matrix;

/** {@inheritDoc} */
public class MatrixFileRecordWriter<T extends Number>
    extends RecordWriter<BlockKey, Matrix<T>> {
  private final Configuration conf;

  public MatrixFileRecordWriter(TaskAttemptContext taskAttemptContext) {
    conf = taskAttemptContext.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
  }

  /** {@inheritDoc} */
  @Override
  public void write(BlockKey key, Matrix<T> x)
      throws IOException, InterruptedException {
    FSDataOutputStream out = FileSystem.get(conf)
        .create(Matrix.getPartitionPath(
            new Path(conf.get(MatrixParameter.outputDir, null),
                conf.get(MatrixParameter.rPrefixFilename, null)),
            key.geti(), key.getj()));
    x.write(out);
    out.close();
  }

}

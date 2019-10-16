package com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.BlockKey;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.Matrix;

public class MatrixOutputFormat<T extends Number>
    extends OutputFormat<BlockKey, Matrix<T>> {

  @Override
  public void checkOutputSpecs(JobContext jobContext)
      throws IOException, InterruptedException {

  }

  @Override
  public OutputCommitter
      getOutputCommitter(TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    return new FileOutputCommitter(new Path(taskAttemptContext
        .getConfiguration().get(MatrixParameter.rPrefixFilename, null)),
        taskAttemptContext);
  }

  @Override
  public RecordWriter<BlockKey, Matrix<T>>
      getRecordWriter(TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    return new MatrixRecordWriter<T>(taskAttemptContext);
  }

}

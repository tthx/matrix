package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.BlockKey;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.Matrix;

/** {@inheritDoc} */
public class MatrixFileOutputFormat<T extends Number>
    extends FileOutputFormat<BlockKey, Matrix<T>> {
  private OutputCommitter committer = null;

  /** {@inheritDoc} */
  @Override
  public void checkOutputSpecs(JobContext job)
      throws InvalidJobConfException, IOException {
    // Verifier qu'un chemin pour des sorties est renseigne
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    // Creation du chemin pour des sorties
    FileSystem.get(job.getConfiguration()).mkdirs(outDir);
  }

  /** {@inheritDoc} */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new FileOutputCommitter(output, context);
    }
    return committer;
  }

  /** {@inheritDoc} */
  @Override
  public RecordWriter<BlockKey, Matrix<T>>
      getRecordWriter(TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    return new MatrixFileRecordWriter<T>(taskAttemptContext);
  }
}

package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.BlockKey;

/** {@inheritDoc} */
public class MatrixRecordReader extends RecordReader<BlockKey, NullWritable> {
  private MatrixSplit matrixSplit;
  private BlockKey blockKey;
  private int i, iKey, j, jKey;
  private int nb1, nb2, mb1, mb2;

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
  }

  /** {@inheritDoc} */
  @Override
  public BlockKey getCurrentKey() throws IOException, InterruptedException {
    return blockKey;
  }

  /** {@inheritDoc} */
  @Override
  public NullWritable getCurrentValue()
      throws IOException, InterruptedException {
    return NullWritable.get();
  }

  /** {@inheritDoc} */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (i * j) / (matrixSplit.getLength());
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    int ntasks = conf.getInt(MatrixParameter.nTasks, 0);
    int mtasks = conf.getInt(MatrixParameter.mTasks, 0);
    int nb = conf.getInt(MatrixParameter.nBlocks, 0);
    int mb;
    matrixSplit = (MatrixSplit) inputSplit;
    blockKey = new BlockKey();
    nb1 = Math.floorDiv(nb, ntasks);
    mb = (taskAttemptContext.getNumReduceTasks() == 0) ? nb
        : conf.getInt(MatrixParameter.mBlocks, 0);
    mb1 = Math.floorDiv(mb, mtasks);
    nb2 = (matrixSplit.getiTask() == ntasks - 1)
        ? (nb - (matrixSplit.getiTask() * nb1)) : nb1;
    mb2 = (matrixSplit.getjTask() == mtasks - 1)
        ? (mb - (matrixSplit.getjTask() * mb1)) : mb1;
    i = 0;
    iKey = matrixSplit.getiTask() * nb1;
    j = 0;
    jKey = matrixSplit.getjTask() * mb1;
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (i < nb2) {
      if (j >= mb2) {
        i++;
        iKey++;
        j = 0;
        jKey = matrixSplit.getjTask() * mb1;
      }
      blockKey.set(iKey, jKey);
      j++;
      jKey++;
    }
    return (i < nb2) ? true : false;
  }

}

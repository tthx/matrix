package com.orange.tgi.ols.arsec.paas.aacm.matrix.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.BlockKey;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.Matrix;

/** {@inheritDoc} */
public class MatrixPartitioner<T extends Number>
    extends Partitioner<BlockKey, Matrix<T>> implements Configurable {
  private Configuration conf;

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public int getPartition(BlockKey key, Matrix<T> value, int numPartitions) {
    int nb =
        Math.floorDiv(conf.getInt(MatrixParameter.nBlocks, 0), numPartitions);
    return Math.floorMod(key.geti() * nb, numPartitions);
  }

}

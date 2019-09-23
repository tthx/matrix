package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.orange.imt.ist.isad.stag.matrix.lambda.BlockKey;

/** {@inheritDoc} */
public class MatrixInputFormat extends InputFormat<BlockKey, NullWritable> {

	/** {@inheritDoc} */
	@Override
	public RecordReader<BlockKey, NullWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		return new MatrixRecordReader();
	}

	/** {@inheritDoc} */
	@Override
	public List<InputSplit> getSplits(JobContext jobContext)
			throws IOException, InterruptedException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Configuration conf = jobContext.getConfiguration();
		int ntasks = conf.getInt(MatrixParameter.nTasks, 0);
		int mtasks = conf.getInt(MatrixParameter.mTasks, 0);
		int nb = conf.getInt(MatrixParameter.nBlocks, 0);
		int mb;
		int nb1 = Math.floorDiv(nb, ntasks), nb2;
		mb = (jobContext.getNumReduceTasks() == 0) ? nb : conf.getInt(
				MatrixParameter.mBlocks, 0);
		int mb1 = Math.floorDiv(mb, mtasks), mb2;
		for (int i = 0; i < ntasks; i++) {
			nb2 = (i == ntasks - 1) ? (nb - (i * nb1)) : nb1;
			for (int j = 0; j < mtasks; j++) {
				mb2 = (j == mtasks - 1) ? (mb - (j * mb1)) : mb1;
				splits.add(new MatrixSplit(i, j, nb2 * mb2));
			}
		}
		return splits;
	}

}

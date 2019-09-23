package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.orange.imt.ist.isad.stag.matrix.lambda.BlockKey;
import com.orange.imt.ist.isad.stag.matrix.lambda.Matrix;

public class MatrixRecordWriter<T extends Number> extends
		RecordWriter<BlockKey, Matrix<T>> {
	private final Configuration conf;

	public MatrixRecordWriter(TaskAttemptContext taskAttemptContext) {
		conf = taskAttemptContext.getConfiguration();
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
	}

	@Override
	public void write(BlockKey key, Matrix<T> x) throws IOException,
			InterruptedException {
		FSDataOutputStream out = FileSystem.get(conf).create(
				new Path(conf.get(MatrixParameter.rPrefixFilename, ""))
						.suffix("." + key.geti() + "." + key.getj() + ".data"));
		x.write(out);
		out.close();
	}

}

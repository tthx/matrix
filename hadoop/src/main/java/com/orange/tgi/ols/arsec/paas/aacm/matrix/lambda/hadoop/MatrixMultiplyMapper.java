package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.orange.imt.ist.isad.stag.matrix.lambda.BlockKey;
import com.orange.imt.ist.isad.stag.matrix.lambda.DataOperator;
import com.orange.imt.ist.isad.stag.matrix.exception.MatrixBoundMultiplicationException;
import com.orange.imt.ist.isad.stag.matrix.exception.MatrixBoundReadException;
import com.orange.imt.ist.isad.stag.matrix.lambda.Matrix;

/** {@inheritDoc} */
public class MatrixMultiplyMapper<T extends Number, U extends Number, R extends Number>
		extends Mapper<BlockKey, NullWritable, BlockKey, Matrix<R>> {
	private Matrix<T> x, a;
	private Matrix<U> y, b;
	private FSDataInputStream xIn, yIn;
	private BlockKey yk;
	private Map<BlockKey, Matrix<T>> xBlocks;
	private Map<BlockKey, Matrix<U>> yBlocks;
	private int nbsize, mbsize, kbsize, nb, mb;
	private int nthreads, mthreads;
	private BiFunction<R, R, R> sumDataFunction;
	private BiFunction<T, U, R> multiplyDataFunction;

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		xIn = FileSystem.get(conf).open(
				new Path(conf.get(MatrixParameter.xPath)));
		yIn = FileSystem.get(conf).open(
				new Path(conf.get(MatrixParameter.yPath)));
		x = (Matrix<T>) Matrix.readHeader(xIn);
		y = (Matrix<U>) Matrix.readHeader(yIn);
		yk = new BlockKey();
		nb = conf.getInt(MatrixParameter.nBlocks, 0);
		mb = conf.getInt(MatrixParameter.mBlocks, 0);
		nthreads = conf.getInt(MatrixParameter.nThreads, 0);
		mthreads = conf.getInt(MatrixParameter.mThreads, 0);
		xBlocks = Matrix.toBlocks(x, nb, mb);
		yBlocks = Matrix.toBlocks(y, mb, nb);
		nbsize = Math.floorDiv(x.getHeight(), nb);
		mbsize = Math.floorDiv(y.getWidth(), nb);
		kbsize = Math.floorDiv(x.getWidth(), mb);
		DataOperator<T, U, R> dataOperator = (DataOperator<T, U, R>) DataOperator
				.builder(x.getDataType(), y.getDataType());
		sumDataFunction = dataOperator.getSumR();
		multiplyDataFunction = dataOperator.getMultiply();
	}

	/** {@inheritDoc} */
	@Override
	protected void map(BlockKey key, NullWritable value, Context context)
			throws IOException, InterruptedException {
		try {
			a = xBlocks.get(key);
			xIn.seek(Matrix.HEADER_SIZE);
			a = Matrix.readData(xIn, x, key.geti() * nbsize, key.getj()
					* kbsize, a.getHeight(), a.getWidth());
			for (int k = 0; k < nb; k++) {
				b = yBlocks.get(yk.set(key.geti(), k));
				yIn.seek(Matrix.HEADER_SIZE);
				context.write(yk, Matrix.multiply(a, Matrix.readData(yIn, y,
						key.geti() * kbsize, k * mbsize, b.getHeight(),
						b.getWidth()), nthreads, mthreads, sumDataFunction,
						multiplyDataFunction));
			}
		} catch (MatrixBoundMultiplicationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MatrixBoundReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/** {@inheritDoc} */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		xIn.close();
		yIn.close();
	}
}

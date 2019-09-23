package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiFunction;

import org.apache.hadoop.mapreduce.Reducer;

import com.orange.imt.ist.isad.stag.matrix.lambda.BlockKey;
import com.orange.imt.ist.isad.stag.matrix.lambda.DataOperator;
import com.orange.imt.ist.isad.stag.matrix.lambda.Matrix;
import com.orange.imt.ist.isad.stag.matrix.exception.MatrixBoundAdditionException;

public class MatrixSumReducer<T extends Number> extends
		Reducer<BlockKey, Matrix<T>, BlockKey, Matrix<T>> {

	@Override
	protected void reduce(BlockKey key, Iterable<Matrix<T>> values,
			Context context) throws IOException, InterruptedException {
		try {
			Iterator<Matrix<T>> i = values.iterator();
			Matrix<T> r = i.next(), x;
			@SuppressWarnings("unchecked")
			DataOperator<T, T, T> dataOperator = (DataOperator<T, T, T>) DataOperator
					.builder(r.getDataType(), r.getDataType());
			BiFunction<T, T, T> sumDataFunction = dataOperator.getSumR();
			while (i.hasNext()) {
				x = i.next();
				if ((r.getHeight() < x.getHeight())
						|| (r.getWidth() < x.getWidth())) {
					Matrix.add(x, r, sumDataFunction, true);
					r = x;
				} else
					Matrix.add(r, x, sumDataFunction, true);
			}
			context.write(key, r);
		} catch (MatrixBoundAdditionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

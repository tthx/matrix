package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.util.Tool;

import com.orange.imt.ist.isad.stag.matrix.lambda.Matrix;
import com.orange.imt.ist.isad.stag.matrix.lambda.DataOperator;

public class MatrixMultiply<T extends Number, U extends Number, R extends Number>
		extends Configured implements Tool {

	/** {@inheritDoc} */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String xType = conf.get(MatrixParameter.xType, null), yType = conf.get(
				MatrixParameter.yType, null);
		if ((xType == null) || (yType == null))
			throw new InvalidJobConfException("Le parametre "
					+ MatrixParameter.xType + " ou " + MatrixParameter.yType
					+ " n'a pas ete renseigne.");
		@SuppressWarnings("unchecked")
		DataOperator<T, U, R> dataOperator = (DataOperator<T, U, R>) DataOperator
				.builder(Matrix.DataTypeDescriptionID.get(xType),
						Matrix.DataTypeDescriptionID.get(yType));
		return Matrix.multiply(conf, dataOperator.getSumR(),
				dataOperator.getMultiply());
	}
}

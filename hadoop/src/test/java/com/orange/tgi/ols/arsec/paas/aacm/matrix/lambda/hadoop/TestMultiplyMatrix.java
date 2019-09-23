package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.DataOperator;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.Matrix;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixMultiply;

public class TestMultiplyMatrix {
	private static final Log LOG = LogFactory.getLog(TestMultiplyMatrix.class);

	@Autowired
	private Job matrixMultiplyJobConfiguration;

	@Before
	public void setUp() throws IOException {
		Configuration conf = matrixMultiplyJobConfiguration.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path inputDir = fs.makeQualified(new Path(conf.get(
				MatrixParameter.inputDir, null)));
		fs.mkdirs(inputDir);
		Path xPath = fs.makeQualified(new Path(inputDir, conf.get(
				MatrixParameter.xFilename, null)));
		Path yPath = fs.makeQualified(new Path(inputDir, conf.get(
				MatrixParameter.yFilename, null)));
		Matrix.builder(conf.getInt(MatrixParameter.xWidth, -1), conf.getInt(
				MatrixParameter.xHeight, -1), Matrix.DataTypeDescriptionID
				.get(conf.get(MatrixParameter.xType, null)), true, null, true,
				conf, xPath);
		Matrix.builder(conf.getInt(MatrixParameter.yWidth, -1), conf.getInt(
				MatrixParameter.yHeight, -1), Matrix.DataTypeDescriptionID
				.get(conf.get(MatrixParameter.yType, null)), true, null, true,
				conf, yPath);
	}

	@Test
	public void matrixMultiply() throws Exception {
		Configuration conf = matrixMultiplyJobConfiguration.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		ToolRunner.run(conf, new MatrixMultiply(), null);
		Path xPath = fs.makeQualified(new Path(conf.get(
				MatrixParameter.inputDir, null), conf.get(
				MatrixParameter.xFilename, null)));
		Path yPath = fs.makeQualified(new Path(conf.get(
				MatrixParameter.inputDir, null), conf.get(
				MatrixParameter.yFilename, null)));
		Path rPath = fs.makeQualified(new Path(conf.get(
				MatrixParameter.outputDir, null), conf.get(
				MatrixParameter.rPrefixFilename, null)));
		Matrix<BigDecimal> x = new Matrix<BigDecimal>(), y = new Matrix<BigDecimal>(), z, r = new Matrix<BigDecimal>();
		FSDataInputStream in;
		in = fs.open(xPath);
		x.readFields(in);
		in.close();
		in = fs.open(yPath);
		y.readFields(in);
		in.close();
		DataOperator<BigDecimal, BigDecimal, BigDecimal> dataOperator = (DataOperator<BigDecimal, BigDecimal, BigDecimal>) DataOperator
				.builder(x.getDataType(), y.getDataType());
		z = Matrix.multiply(x, y, dataOperator.getSumR(),
				dataOperator.getMultiply());
		in = fs.open(yPath);
		in = fs.open(rPath.suffix(".data"));
		r.readFields(in);
		in.close();
		assertTrue(z.equals(r));
	}
}

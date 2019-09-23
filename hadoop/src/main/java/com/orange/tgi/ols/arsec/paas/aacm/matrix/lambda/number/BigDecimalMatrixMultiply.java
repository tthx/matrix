package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.number;

import java.math.BigDecimal;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixMultiply;

public class BigDecimalMatrixMultiply extends
		MatrixMultiply<BigDecimal, BigDecimal, BigDecimal> {

	public static void main(String[] args) throws Exception {
		int res = 1;
		String classPathXmlApplicationContext = null, bean = null;
		for (int i = 0; i < args.length; ++i)
			if ("-ctx".equals(args[i]))
				classPathXmlApplicationContext = args[++i];
			else if ("-j".equals(args[i]))
				bean = args[++i];
		if ((classPathXmlApplicationContext == null) || (bean == null)) {
			System.out
					.println("MatrixMultiply [-ctx classPathXmlApplicationContext] "
							+ "[-j jobConfiguration]");
			System.out.println("Exemple: MatrixMultiply "
					+ "-ctx classpath:META-INF/spring/applicationContext.xml "
					+ "-j matrixMultiplyJobConfiguration");
		} else {
			@SuppressWarnings("resource")
			ApplicationContext ctx = new ClassPathXmlApplicationContext(
					classPathXmlApplicationContext);
			Job job = (Job) ctx.getBean(bean);
			res = ToolRunner.run(job.getConfiguration(),
					new BigDecimalMatrixMultiply(), args);
		}
		System.exit(res);
	}
}

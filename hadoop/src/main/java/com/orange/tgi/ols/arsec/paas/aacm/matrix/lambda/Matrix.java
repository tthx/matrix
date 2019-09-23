/*
 * 
 */
package com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.BlockKey;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixFileOutputFormat;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixInputFormat;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixMultiply;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixMultiplyAndSumMapper;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixMultiplyMapper;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixParameter;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.hadoop.MatrixSumReducer;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util.BiConsumerWithException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.lambda.util.FunctionWithException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundAdditionException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundCopyException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundMultiplicationException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundReadException;
import com.orange.tgi.ols.arsec.paas.aacm.matrix.exception.MatrixBoundWriteException;

/**
 * La classe {@code Matrix} implemente l'interface
 * {@link org.apache.hadoop.io.WritableComparable} pour respecter les
 * contraintes imposees par Hadoop sur les clefs. La classe {@code Matrix}
 * aurait pu se restreindre a implementer {@link org.apache.hadoop.io.Writable}
 * pour respecter les contraintes de Hadoop sur les valeurs car {@code Matrix}
 * ne sera pas utilisee comme une clef. Mais nous avions voulu faire un plus.
 * <p>
 * Les elements d'une matrice derivent de la classe {@link java.lang.Number}
 * pour limiter le domaine de definition: les matrices, ici, se limitent aux
 * operations arithmetiques.
 * 
 * @param <T> Type des elements d'une matrice
 */
public class Matrix<T extends Number> implements WritableComparable<Matrix<T>> {

	/**
	 * Identifiants des types supportes.
	 */
	public static enum DataType {
		/** Le type {@link java.lang.Byte}. */
		Byte(0, "Byte"),
		/** Le type {@link java.lang.Short}. */
		Short(1, "Short"),
		/** Le type {@link java.lang.Integer}. */
		Integer(2, "Integer"),
		/** Le type {@link java.lang.Long}. */
		Long(3, "Long"),
		/** Le type {@link java.lang.Float}. */
		Float(4, "Float"),
		/** Le type {@link java.lang.Double}. */
		Double(5, "Double"),
		/** Le type {@link java.math.BigInteger}. */
		BigInteger(6, "BigInteger"),
		/** Le type {@link java.math.BigDecimal}. */
		BigDecimal(7, "BigDecimal");

		/**
		 * Identifiant en un nombre entier d'un type. Les identifiants sont ordonnes:
		 * soient {@code id1} et {@code id2} deux identifiants, si {@code id1.typeID >=
		 * id2.typeID} alors {@code id1} occupe en memoire/disque un espace plus
		 * important que {@code id2}.
		 */
		private final int typeID;

		/**
		 * Identifiant en une chaine de caractere d'un type respectant l'ordre de
		 * l'identifiant en un nombre entier.
		 */
		private final String descriptionID;

		/**
		 * Donne l'identifiant en un nombre entier d'un {@code DataType}.
		 *
		 * @return l'identifiant en un nombre entier
		 */
		public int getTypeID() {
			return typeID;
		}

		/**
		 * Donne l'identifiant en une chaine de caractere d'un {@code DataType}.
		 *
		 * @return l'identifiant en une chaine de caractere
		 */
		public String getDescriptionID() {
			return descriptionID;
		}

		/**
		 * Constructeur d'un {@code DataType}.
		 *
		 * @param typeID        la partie en un nombre entier d'un type
		 * @param descriptionID la partie en une chaine de caractere d'un type
		 */
		DataType(final int typeID, final String descriptionID) {
			this.typeID = typeID;
			this.descriptionID = descriptionID;
		}
	};

	/**
	 * Permet, d'un entier, de retrouver le {@code DataType} correspondant, si il
	 * existe.
	 */
	@SuppressWarnings("serial")
	public static final Map<Integer, DataType> DataTypeID = new HashMap<Integer, DataType>() {
		{
			put(DataType.Byte.typeID, DataType.Byte);
			put(DataType.Short.typeID, DataType.Short);
			put(DataType.Integer.typeID, DataType.Integer);
			put(DataType.Long.typeID, DataType.Long);
			put(DataType.Float.typeID, DataType.Float);
			put(DataType.Double.typeID, DataType.Double);
			put(DataType.BigInteger.typeID, DataType.BigInteger);
			put(DataType.BigDecimal.typeID, DataType.BigDecimal);
		}
	};

	/**
	 * Permet, d'une chaine de caractere, de retrouver le {@code DataType}
	 * correspondant, si il existe.
	 */
	@SuppressWarnings("serial")
	public static final Map<String, DataType> DataTypeDescriptionID = new HashMap<String, DataType>() {
		{
			put(DataType.Byte.descriptionID, DataType.Byte);
			put(DataType.Short.descriptionID, DataType.Short);
			put(DataType.Integer.descriptionID, DataType.Integer);
			put(DataType.Long.descriptionID, DataType.Long);
			put(DataType.Float.descriptionID, DataType.Float);
			put(DataType.Double.descriptionID, DataType.Double);
			put(DataType.BigInteger.descriptionID, DataType.BigInteger);
			put(DataType.BigDecimal.descriptionID, DataType.BigDecimal);
		}
	};

	/**
	 * Taille, en nombre de byte, de l'entete d'une matrice. Elle est utilisee,
	 * notamment, lors des operations entrees/sorties.
	 */
	public static final long HEADER_SIZE = 3 * Integer.BYTES;

	/**
	 * Les tailles des deux dimensions d'une matrice. {@code n} est le nombre de
	 * ligne et {@code m} est le nombre de colonne.
	 */
	private int n, m;

	/** La type des elements d'une matrice. */
	private DataType dataType = null;

	/**
	 * Le nombre zero du type des elements d'une matrice. Par exemple, {@code ZERO}
	 * a la valeur {@code BigDecimal.ZERO} si le membre {@code dataType} a la valeur
	 * {@code DataType.BigDecimal}.
	 */
	private T ZERO = null;

	/** Les elements d'une matrice. */
	private T[][] data = null;

	/**
	 * Fonction permettant d'allouer les elements d'une matrice.
	 * 
	 * @param n le nombre de ligne
	 * @param m le nombre de colonne
	 * @return un tableau a deux dimensions
	 */
	private BiFunction<Integer, Integer, T[][]> newDataFunction = null;

	/**
	 * Fonction permettant de lire un element dans un flux.
	 * 
	 * @param in le flux ou lire
	 * @return l'element lu
	 * @throw {@link java.io.IOException}
	 */
	private FunctionWithException<DataInput, T, IOException> readDataFunction = null;

	/**
	 * Fonction permettant d'ecrire un element dans un flux.
	 * 
	 * @param out le flux ou ecrire
	 * @param x   l'element a ecrire
	 * @throw {@link java.io.IOException}
	 */
	private BiConsumerWithException<DataOutput, T, IOException> writeDataFunction = null;

	/**
	 * Fonction permettant sauter un nombre de byte dans un flux.
	 * 
	 * @param in le flux concerne
	 * @param n  le nombre de byte a sauter
	 * @throw {@link java.io.IOException}
	 */
	private BiConsumerWithException<DataInput, Integer, IOException> skipDataFunction = null;

	/**
	 * Retourne le nombre de ligne d'une matrice.
	 *
	 * @return le nombre de ligne d'une matrice
	 */
	public int getHeight() {
		return n;
	}

	/**
	 * Retourne le nombre de colonne d'une matrice.
	 *
	 * @return le nombre de colonne d'une matrice
	 */
	public int getWidth() {
		return m;
	}

	/**
	 * Retourne le tableau a deux dimensions constitue des elements d'une matrice.
	 *
	 * @return le tableau a deux dimensions constitue des elements d'une matrice
	 */
	public T[][] getData() {
		return data;
	}

	/**
	 * Retourne un element d'une matrice.
	 *
	 * @param i indice d'une ligne
	 * @param j indice d'une colonne
	 * @return un element d'une matrice
	 * @throws MatrixBoundReadException si {@code i} ou {@code j} sont hors des
	 *                                  bornes d'une matrice
	 */
	public T getData(final int i, final int j) throws MatrixBoundReadException {
		if ((i >= 0) && (i < n) && (j >= 0) && (j < m))
			return data[i][j];
		else
			throw new MatrixBoundReadException();
	}

	/**
	 * Positionne un element d'une matrice.
	 *
	 * @param i indice d'une ligne
	 * @param j indice d'une colonne
	 * @param x l'element a positionner
	 * @throws MatrixBoundWriteException si {@code i} ou {@code j} sont hors des
	 *                                   bornes d'une matrice
	 */
	public void setData(final int i, final int j, final T x) throws MatrixBoundWriteException {
		if ((i >= 0) && (i < n) && (j >= 0) && (j < m))
			data[i][j] = x;
		else
			throw new MatrixBoundWriteException();
	}

	/**
	 * Retourne le type des elements d'une matrice.
	 *
	 * @return le type des elements d'une matrice
	 */
	public DataType getDataType() {
		return dataType;
	}

	/**
	 * Positionne le type des elements d'une matrice.
	 *
	 * @param dataType le type des elements d'une matrice
	 */
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	/**
	 * Retourne la fonction d'allocation des elements d'une matrice.
	 *
	 * @return la fonction d'allocation des elements d'une matrice
	 */
	public BiFunction<Integer, Integer, T[][]> getNewDataFunction() {
		return newDataFunction;
	}

	/**
	 * Positionne la fonction d'allocation des elements d'une matrice.
	 *
	 * @param newDataFunction la fonction d'allocation des elements d'une matrice
	 */
	public void setNewDataFunction(final BiFunction<Integer, Integer, T[][]> newDataFunction) {
		this.newDataFunction = newDataFunction;
	}

	/**
	 * Retourne la fonction de lecture d'un element d'une matrice dans un flux.
	 *
	 * @return la fonction de lecture d'un element d'une matrice dans un flux
	 */
	public FunctionWithException<DataInput, T, IOException> getReadDataFunction() {
		return readDataFunction;
	}

	/**
	 * Positionne la fonction de lecture d'un element d'une matrice dans un flux.
	 *
	 * @param readDataFunction la fonction de lecture d'un element d'une matrice
	 *                         dans un flux
	 */
	public void setReadDataFunction(final FunctionWithException<DataInput, T, IOException> readDataFunction) {
		this.readDataFunction = readDataFunction;
	}

	/**
	 * Retourne la fonction d'ecriture d'un element d'une matrice dans un flux.
	 *
	 * @return la fonction d'ecriture d'un element d'une matrice dans un flux
	 */
	public BiConsumerWithException<DataOutput, T, IOException> getWriteDataFunction() {
		return writeDataFunction;
	}

	/**
	 * Positionne la fonction d'ecriture d'un element d'une matrice dans un flux.
	 *
	 * @param writeDataFunction la fonction d'ecriture d'un element d'une matrice
	 *                          dans un flux
	 */
	public void setWriteDataFunction(final BiConsumerWithException<DataOutput, T, IOException> writeDataFunction) {
		this.writeDataFunction = writeDataFunction;
	}

	/**
	 * Retourne la fonction de saut d'un nombre de byte d'un flux.
	 *
	 * @return la fonction de saut d'un nombre de byte d'un flux
	 */
	public BiConsumerWithException<DataInput, Integer, IOException> getSkipDataFunction() {
		return skipDataFunction;
	}

	/**
	 * Positionne la fonction de saut d'un nombre de byte d'un flux.
	 *
	 * @param skipDataFunction la fonction de saut d'un nombre de byte d'un flux
	 */
	public void setSkipDataFunction(BiConsumerWithException<DataInput, Integer, IOException> skipDataFunction) {
		this.skipDataFunction = skipDataFunction;
	}

	/**
	 * Initialise les membres d'une matrice.
	 *
	 * @param n                      le nombre de ligne
	 * @param m                      le nombre de colonne
	 * @param dataType               le type des elements
	 * @param ZERO                   l'element zero
	 * @param alloc                  si {@code true} alors l'espace reserve aux
	 *                               elements sera alloue, si {@code false} alors
	 *                               l'espace reserve aux elements ne sera pas
	 *                               alloue
	 * @param value                  une valeur qui sera affectee a tous les
	 *                               elements d'une matrice, si {@code alloc} est
	 *                               {@code true}
	 * @param random                 des valeurs aleatoires seront affectees a tous
	 *                               les elements d'une matrice, si {@code alloc}
	 *                               est {@code true} et si {@code value} est
	 *                               {@code null}
	 * @param conf                   une configuration de Apache Hadoop
	 * @param f                      un nom de fichier ou ecrire tous les elements
	 *                               d'une matrice
	 * @param newDataFunction        la fonction d'allocation des elements d'une
	 *                               matrice
	 * @param readDataFunction       la fonction de lecture d'un element d'une
	 *                               matrice dans un flux
	 * @param writeDataFunction      la fonction d'ecriture d'un element d'une
	 *                               matrice dans un flux
	 * @param skipDataFunction       la fonction de saut d'un nombre de byte dans un
	 *                               flux
	 * @param randomDataFunction     la fonction generant un nombre alleatoire
	 *                               correspondant au type des element d'une matrice
	 * @param convertionDataFunction la fonction de conversion d'une chaine de
	 *                               caractere au type des elements d'une matrice
	 * @throws IOException une exception I/O a ete relevee
	 */
	private void init(final int n, final int m, final DataType dataType, final T ZERO, final boolean alloc,
			final String value, final boolean random, final Configuration conf, final Path f,
			final BiFunction<Integer, Integer, T[][]> newDataFunction,
			final FunctionWithException<DataInput, T, IOException> readDataFunction,
			final BiConsumerWithException<DataOutput, T, IOException> writeDataFunction,
			final BiConsumerWithException<DataInput, Integer, IOException> skipDataFunction,
			final Function<Random, T> randomDataFunction, final Function<String, T> convertionDataFunction)
			throws IOException {
		this.n = n;
		this.m = m;
		this.dataType = dataType;
		this.ZERO = ZERO;
		this.newDataFunction = newDataFunction;
		this.readDataFunction = readDataFunction;
		this.writeDataFunction = writeDataFunction;
		this.skipDataFunction = skipDataFunction;
		if (alloc)
			this.data = newDataFunction.apply(n, m);
		if ((value != null) || (random) || (f != null)) {
			Random r = new Random();
			FileSystem fs;
			FSDataOutputStream out = null;
			if (f != null) {
				fs = FileSystem.get(conf);
				out = fs.create(f);
				writeHeader(out, this);
			}
			for (int i = 0; i < n; i++)
				for (int j = 0; j < m; j++) {
					T x = (value != null) ? convertionDataFunction.apply(value) : randomDataFunction.apply(r);
					if (f != null)
						writeDataFunction.apply(out, x);
					if (alloc)
						data[i][j] = x;
				}
			if (out != null)
				out.close();
		}
	}

	/**
	 * Initialise les membres d'une matrice à partir des membres d'une autre matrice
	 * mais avec d'autres valeurs aux dimensions et aux ressources allouees aux
	 * elements.
	 *
	 * @param n     le nombre de ligne
	 * @param m     le nombre de colonne
	 * @param x     la matrice ou sera recupere des valeurs
	 * @param alloc si {@code true} alors l'espace reserve aux elements sera alloue,
	 *              si {@code false} alors l'espace reserve aux elements ne sera pas
	 *              alloue
	 * @throws IOException une exception I/O a ete relevee
	 */
	private void init(final int n, final int m, final Matrix<T> x, final boolean alloc) {
		try {
			init(n, m, x.dataType, x.ZERO, alloc, null, false, null, null, x.newDataFunction, x.readDataFunction,
					x.writeDataFunction, x.skipDataFunction, null, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Initialise les membres d'une matrice avec les valeurs des membres d'une autre
	 * matrice. Attention: La matrice initialisee ne sera pas un clone.
	 *
	 * @param x la matrice ou sera recupere des valeurs
	 */
	void init(final Matrix<T> x) {
		n = x.n;
		m = x.m;
		dataType = x.dataType;
		ZERO = x.ZERO;
		data = x.data;
		newDataFunction = x.newDataFunction;
		readDataFunction = x.readDataFunction;
		writeDataFunction = x.writeDataFunction;
		skipDataFunction = x.skipDataFunction;
	}

	/**
	 * Instancie une nouvelle matrice en initialisant ses membres.
	 *
	 * @param n                      le nombre de ligne
	 * @param m                      le nombre de colonne
	 * @param dataType               le type des elements
	 * @param ZERO                   l'element zero
	 * @param alloc                  si {@code true} alors l'espace reserve aux
	 *                               elements sera alloue, si {@code false} alors
	 *                               l'espace reserve aux elements ne sera pas
	 *                               alloue
	 * @param value                  une valeur qui sera affectee a tous les
	 *                               elements d'une matrice, si {@code alloc} est
	 *                               {@code true}
	 * @param random                 des valeurs aleatoires seront affectees a tous
	 *                               les elements d'une matrice, si {@code alloc}
	 *                               est {@code true} et si {@code value} est
	 *                               {@code null}
	 * @param conf                   une configuration de Apache Hadoop
	 * @param f                      un nom de fichier ou ecrire tous les elements
	 *                               d'une matrice
	 * @param newDataFunction        la fonction d'allocation des elements d'une
	 *                               matrice
	 * @param readDataFunction       la fonction de lecture d'un element d'une
	 *                               matrice dans un flux
	 * @param writeDataFunction      la fonction d'ecriture d'un element d'une
	 *                               matrice dans un flux
	 * @param skipDataFunction       la fonction de saut d'un nombre de byte dans un
	 *                               flux
	 * @param randomDataFunction     la fonction generant un nombre alleatoire
	 *                               correspondant au type des element d'une matrice
	 * @param convertionDataFunction la fonction de conversion d'une chaine de
	 *                               caractere au type des elements d'une matrice
	 * @throws IOException une exception I/O a ete relevee
	 */
	public Matrix(final int n, final int m, final DataType dataType, final T ZERO, final boolean alloc,
			final String value, final boolean random, final Configuration conf, final Path f,
			final BiFunction<Integer, Integer, T[][]> newDataFunction,
			final FunctionWithException<DataInput, T, IOException> readDataFunction,
			final BiConsumerWithException<DataOutput, T, IOException> writeDataFunction,
			final BiConsumerWithException<DataInput, Integer, IOException> skipDataFunction,
			final Function<Random, T> randomDataFunction, final Function<String, T> convertionDataFunction)
			throws IOException {
		init(n, m, dataType, ZERO, alloc, value, random, conf, f, newDataFunction, readDataFunction, writeDataFunction,
				skipDataFunction, randomDataFunction, convertionDataFunction);
	}

	/**
	 * Instancie une nouvelle matrice vide.
	 */
	public Matrix() {
	}

	/**
	 * Instancie une nouvelle matrice en recuperant les valeurs des membres d'une
	 * autre matrice. Attention: la nouvelle matrice n'est pas un clone.
	 *
	 * @param x la matrice ou sera recupere des valeurs
	 */
	public Matrix(final Matrix<T> x) {
		init(x);
	}

	/**
	 * Instancie une nouvelle matrice en recuperant les valeurs des membres d'une
	 * autre matrice {@code x} en precisant si un nouvel espace, des memes
	 * dimensions que {@code x}, sera reserve aux elements de la nouvelle matrice.
	 *
	 * @param x     la matrice ou sera recupere des valeurs
	 * @param alloc si {@code true} alors l'espace reserve aux elements sera alloue,
	 *              si {@code false} alors l'espace reserve aux elements ne sera pas
	 *              alloue et la nouvelle matrice aura le meme espace des elements
	 *              que le parametre {@code x}
	 */
	public Matrix(final Matrix<T> x, final boolean alloc) {
		init(x.n, x.m, x, alloc);
		if (!alloc)
			data = x.data;
	}

	/**
	 * Instancie une nouvelle matrice en recuperant les valeurs des membres d'une
	 * autre matrice {@code x} en precisant, par {@code alloc}, si un nouvel espace,
	 * de dimensions differentes, par {@code n} et {@code m}, que {@code x}, sera
	 * reserve aux elements de la nouvelle matrice.
	 *
	 * @param n     le nombre de ligne
	 * @param m     le nombre de colonne
	 * @param x     la matrice ou sera recupere des valeurs
	 * @param alloc si {@code true} alors l'espace reserve aux elements sera alloue,
	 *              si {@code false} alors l'espace reserve aux elements ne sera pas
	 *              alloue
	 */
	public Matrix(final int n, final int m, final Matrix<T> x, final boolean alloc) {
		init(n, m, x, alloc);
	}

	/**
	 * Construit une nouvelle instance d'une matrice aux dimensions {@code n} et
	 * {@code m} et les autres membres seront determines a partir des matrices
	 * {@code x} et {@code y}.
	 * <p>
	 * La construction suit les regles accumulees suivantes:
	 * <ul>
	 * <li>Si {@code x.dataType.typeID > y.dataType.typeID} alors le type des
	 * elements de la nouvelle matrice est {@code x.dataType}, sinon
	 * {@code y.dataType},
	 * <li>Si {@code x.dataType=DataType.Byte} ou {@code x.dataType=DataType.Short}
	 * et si {@code y.dataType=DataType.Byte} ou {@code y.dataType=DataType.Short}
	 * alors le type des elements de la nouvelle matrice est
	 * {@code DataType.Integer},
	 * <li>Si {@code x.dataType=DataType.BigInteger} ou
	 * {@code y.dataType=DataType.BigInteger} et si
	 * {@code x.dataType=DataType.Float} ou {@code x.dataType=DataType.Double} ou
	 * {@code y.dataType=DataType.Float} ou {@code y.dataType=DataType.Double} alors
	 * le type des elements de la nouvelle matrice est {@code DataType.BigDecimal}.
	 * </ul>
	 * 
	 * @param n     le nombre de ligne
	 * @param m     le nombre de colonne
	 * @param x     une matrice
	 * @param y     une matrice
	 * @param alloc si {@code true} alors l'espace reserve aux elements sera alloue,
	 *              si {@code false} alors l'espace reserve aux elements ne sera pas
	 *              alloue
	 * @return la nouvelle matrice
	 */
	public static Matrix<? extends Number> builder(final int n, final int m, final Matrix<? extends Number> x,
			final Matrix<? extends Number> y, final boolean alloc) {
		Matrix<? extends Number> r = null;
		DataType dataType = (x.dataType.typeID > y.dataType.typeID) ? x.dataType : y.dataType;
		if (x.dataType == DataType.Byte || x.dataType == DataType.Short)
			if (y.dataType == DataType.Byte || y.dataType == DataType.Short)
				dataType = DataType.Integer;
		if (x.dataType == DataType.BigInteger || y.dataType == DataType.BigInteger)
			if (x.dataType == DataType.Float || y.dataType == DataType.Float || x.dataType == DataType.Double
					|| y.dataType == DataType.Double)
				dataType = DataType.BigDecimal;
		try {
			r = builder(n, m, dataType, alloc, null, false, null, null);
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}

	/**
	 * Construit une nouvelle instance d'une matrice aux dimensions {@code width} et
	 * {@code height} et les autre membres seront determines a partir
	 * {@code dataType}.
	 *
	 * @param width    le nombre de ligne
	 * @param height   le nombre de colonne
	 * @param dataType le type des elements de la nouvelle matrice
	 * @param alloc    si {@code true} alors l'espace reserve aux elements sera
	 *                 alloue, si {@code false} alors l'espace reserve aux elements
	 *                 ne sera pas alloue
	 * @param value    une valeur qui sera affectee a tous les elements de la
	 *                 nouvelle matrice, si alloc est {@code true}
	 * @param random   des valeurs aleatoires seront affectes a tous les elements de
	 *                 la nouvelle matrice, si alloc est {@code true} et si
	 *                 {@code value} est {@code null}
	 * @param conf     une configuration de Apache Hadoop
	 * @param f        un nom de fichier ou ecrire tous les elements d'une matrice
	 * @return la nouvelle matrice
	 * @throws NumberFormatException Cette exception est levee si {@code value} en
	 *                               peut etre converti dans le type
	 *                               {@code dataType}
	 * @throws IOException           une exception I/O a ete relevee
	 */
	public static Matrix<? extends Number> builder(final int width, final int height, final DataType dataType,
			final boolean alloc, final String value, final boolean random, final Configuration conf, final Path f)
			throws NumberFormatException, IOException {
		Matrix<? extends Number> x = null;
		switch (dataType) {
		case Byte:
			x = new Matrix<Byte>(width, height, DataType.Byte, (byte) 0, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Byte[n][m], (DataInput in) -> in.readByte(),
					(DataOutput out, final Byte a) -> out.writeByte(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Byte.BYTES),
					(Random r) -> new Integer(r.nextInt()).byteValue(), (final String s) -> new Byte(s));
			break;
		case Short:
			x = new Matrix<Short>(width, height, DataType.Short, (short) 0, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Short[n][m], (DataInput in) -> in.readShort(),
					(DataOutput out, final Short a) -> out.writeShort(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Short.BYTES),
					(Random r) -> new Integer(r.nextInt()).shortValue(), (final String s) -> new Short(s));
			break;
		case Integer:
			x = new Matrix<Integer>(width, height, DataType.Integer, 0, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Integer[n][m], (DataInput in) -> in.readInt(),
					(DataOutput out, final Integer a) -> out.writeInt(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Integer.BYTES),
					(Random r) -> r.nextInt(), (final String s) -> new Integer(s));
			break;
		case Long:
			x = new Matrix<Long>(width, height, DataType.Long, 0l, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Long[n][m], (DataInput in) -> in.readLong(),
					(DataOutput out, final Long a) -> out.writeLong(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Long.BYTES),
					(Random r) -> r.nextLong(), (final String s) -> new Long(s));
			break;
		case Float:
			x = new Matrix<Float>(width, height, DataType.Float, 0f, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Float[n][m], (DataInput in) -> in.readFloat(),
					(DataOutput out, final Float a) -> out.writeFloat(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Float.BYTES),
					(Random r) -> r.nextFloat(), (final String s) -> new Float(s));
			break;
		case Double:
			x = new Matrix<Double>(width, height, DataType.Double, 0d, alloc, value, random, conf, f,
					(final Integer n, final Integer m) -> new Double[n][m], (DataInput in) -> in.readDouble(),
					(DataOutput out, final Double a) -> out.writeDouble(a),
					(DataInput in, final Integer toSkip) -> in.skipBytes(toSkip * Double.BYTES),
					(Random r) -> r.nextDouble(), (final String s) -> new Double(s));
			break;
		case BigInteger:
			x = new Matrix<BigInteger>(width, height, DataType.BigInteger, BigInteger.ZERO, alloc, value, random, conf,
					f, (final Integer n, final Integer m) -> new BigInteger[n][m], (DataInput in) -> {
						byte[] a = new byte[in.readInt()];
						in.readFully(a);
						return new BigInteger(a);
					}, (DataOutput out, final BigInteger a) -> {
						byte[] buffer = a.toByteArray();
						out.writeInt(buffer.length);
						out.write(buffer);
					}, (DataInput in, final Integer toSkip) -> {
						for (int i = 0; i < toSkip; i++)
							in.skipBytes(in.readInt());
					}, (Random r) -> new BigInteger(64, r), (final String s) -> new BigInteger(s));
			break;
		case BigDecimal:
			x = new Matrix<BigDecimal>(width, height, DataType.BigDecimal, BigDecimal.ZERO, alloc, value, random, conf,
					f, (final Integer n, final Integer m) -> new BigDecimal[n][m], (DataInput in) -> {
						byte[] a = new byte[in.readInt()];
						in.readFully(a);
						return new BigDecimal(new BigInteger(a), in.readInt());
					}, (DataOutput out, final BigDecimal a) -> {
						byte[] buffer = a.unscaledValue().toByteArray();
						out.writeInt(buffer.length);
						out.write(buffer);
						out.writeInt(a.scale());
					}, (DataInput in, final Integer toSkip) -> {
						for (int i = 0; i < toSkip; i++)
							in.skipBytes(in.readInt() + Integer.BYTES);
					}, (Random r) -> new BigDecimal(r.nextDouble()), (final String s) -> new BigDecimal(s));
			break;
		}
		return x;
	}

	/**
	 * Partitionne une matrice en {@code nb*mb} partitions ou {@code nb} est le
	 * nombre de partition sur les lignes et {@code mb} le nombre de partition sur
	 * les colonnes.
	 * <p>
	 * Chaque partition resultante correspond a une clef constituee d'un couple
	 * d'entier {@code (i,j)}, represente par la classe {@link BlockKey}, qui
	 * indique la position de la partition dans la matrice partitionnee:
	 * <ul>
	 * <li>Les partitions resultantes avec {@code 0<=i<nb-1} et {@code 0<=j<mb-1}
	 * sont des matrices de dimensions, entieres, de {@code n/nb} lignes et
	 * {@code m/mb} colonnes.
	 * <li>Les partitions d'indices {@code i=nb-1} et {@code 0<=j<mb-1} sont de
	 * dimensions {@code n-(nb-1)*n/nb} lignes et {@code m/mb} colonnes.
	 * <li>Les partitions d'indice {@code 0<=i<nb-1} et {@code j=mb-1} sont de
	 * dimensions de {@code n/nb} lignes et {@code m-(mb-1)*m/mb} colonnes.
	 * <li>La partition {@code i=nb-1} et {@code j=mb-1} est de dimension
	 * {@code n-(nb-1)*n/nb} lignes et {@code m-(mb-1)*m/mb} colonnes.
	 * </ul>
	 * Une partition est representee par la classe {@link Matrice} ou tous les
	 * membres sont renseignes sauf l'espace de stockage des elements: une partition
	 * est un squelette de matrice.
	 * 
	 * @param <T> le type des elements d'une matrice
	 * @param x   une matrice
	 * @param nb  le nombre de partition sur les lignes
	 * @param mb  le nombre de partition sur les colonnes
	 * @return une map contenant les partitions desirees d'une matrice. Un element
	 *         de la map est constitue d'une d'un couple d'entier {@code (i,j)}, une
	 *         clef, representee par la classe {@link BlockKey}, et d'un squelette
	 *         de matrice, la valeur correspondante, une partition, representee par
	 *         la classe {@link Matrice}
	 */
	public static <T extends Number> Map<BlockKey, Matrix<T>> toBlocks(final Matrix<T> x, final int nb, final int mb) {
		Map<BlockKey, Matrix<T>> r = new ConcurrentHashMap<BlockKey, Matrix<T>>();
		int n1 = Math.floorDiv(x.n, nb), n2, m1 = Math.floorDiv(x.m, mb), m2;
		for (int i = 0; i < nb; i++) {
			n2 = (i == nb - 1) ? (x.n - (i * n1)) : n1;
			for (int j = 0; j < mb; j++) {
				m2 = (j == mb - 1) ? (x.m - (j * m1)) : m1;
				r.put(new BlockKey(i, j), new Matrix<T>(n2, m2, x, false));
			}
		}
		return r;
	}

	/**
	 * Fournit une copie d'une partie d'une matrice.
	 *
	 * @param <T> le type des elements d'une matrice
	 * @param x   la matrice ou copier
	 * @param i   la ligne ou commencer la copie
	 * @param j   la colonne ou commencer la copie
	 * @param n   le nombre de ligne a copier
	 * @param m   le nombre de colonne a copier
	 * @return une copie d'une partie d'une matrice
	 * @throws MatrixBoundCopyException cette exception est levee si
	 *                                  {@code (i + n > x.getHeight())} ou
	 *                                  {@code (j + m > x.getWidth())}
	 */
	public static <T extends Number> Matrix<T> copy(final Matrix<T> x, final int i, final int j, final int n,
			final int m) throws MatrixBoundCopyException {
		if ((i + n > x.n) || (j + m > x.m))
			throw new MatrixBoundCopyException();
		Matrix<T> r = new Matrix<T>(n, m, x, true);
		for (int k = i; k < i + n; k++)
			r.data[k - i] = Arrays.copyOfRange(x.data[k], j, j + m);
		return r;
	}

	/**
	 * Copie une partie d'une matrice vers une autre.
	 *
	 * @param <T>              le type des elements de la matrice source
	 * @param <U>              le type des elements de la matrice destination
	 * @param dest             la matrice destination
	 * @param src              la matrice source
	 * @param srci             la ligne de la matrice source ou commencer la copie
	 * @param srcj             la colonne de la matrice source ou commencer la copie
	 * @param desti            la ligne de la matrice destination ou commencer la
	 *                         copie
	 * @param destj            la colonne de la matrice destination ou commencer la
	 *                         copie
	 * @param n                le nombre de ligne a copier
	 * @param m                le nombre de colonne a copier
	 * @param copyDataFunction la fonction de copie permettant de transformer un
	 *                         element de type T en un element de type U
	 * @throws MatrixBoundCopyException Cette exception est levee si
	 *                                  {@code (srci + n > src.getHeight())} ou
	 *                                  {@code (srcj + m > src.getWidth())} ou
	 *                                  {@code (desti + n > dest.getHeight())} ou
	 *                                  {@code (destj + m > dest.getWidth())}
	 */
	public static <T extends Number, U extends Number> void copy(Matrix<U> dest, final Matrix<T> src, final int srci,
			final int srcj, final int desti, final int destj, final int n, final int m,
			final Function<T, U> copyDataFunction) throws MatrixBoundCopyException {
		if ((srci + n > src.n) || (srcj + m > src.m) || (desti + n > dest.n) || (destj + m > dest.m))
			throw new MatrixBoundCopyException();
		for (int i = 0; i < n; i++)
			for (int j = 0; j < m; j++)
				dest.data[desti + i][destj + j] = copyDataFunction.apply(src.data[srci + i][srcj + j]);
	}

	/** {@inheritDoc} */
	@Override
	public int compareTo(Matrix<T> x) {
		int d = (n * m) - (x.n * x.m);
		if (d > 0)
			return 1;
		if (d < 0)
			return -1;
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public boolean equals(final Object o) {
		if (o instanceof Matrix<?>) {
			Matrix<?> x = (Matrix<?>) o;
			if ((n == x.n) && (m == x.m) && (dataType == x.dataType)) {
				for (int i = 0; i < n; i++)
					if (!Arrays.equals(data[i], x.data[i]))
						return false;
				return true;
			}
		}
		return false;
	}

	/** {@inheritDoc} */
	@Override
	public String toString() {
		String r = String.format("n=[%d],m=[%d],dataType=[%s],data=[\n", n, m, dataType.getDescriptionID());
		for (T[] i : data)
			r += Arrays.toString(i) + "\n";
		return r.replaceFirst("\n$", "]");
	}

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		init((Matrix<T>) read(in, true));
	}

	/**
	 * Lit une matrice a partir d'un flux. Tous les membres d'une matrice seront
	 * initialises, sauf les elements qui sont optionnels.
	 *
	 * @param in    le flux ou lire
	 * @param alloc si {@code true} alors l'espace reserve aux elements sera alloue
	 *              et les elements de la matrice seront lus, si {@code false} alors
	 *              l'espace reserve aux elements ne sera pas alloue et les elments
	 *              de la matrice ne seront pas lus
	 * @return la matrice lue
	 * @throws IOException une exception I/O a ete relevee
	 */
	public static Matrix<? extends Number> read(DataInput in, final boolean alloc) throws IOException {
		int n = in.readInt(), m = in.readInt();
		DataType dataType = DataTypeID.get(in.readInt());
		Matrix<? extends Number> x = builder(n, m, dataType, alloc, null, false, null, null);
		if (alloc)
			readData(in, x);
		return x;
	}

	/**
	 * Lit l'entete d'une matrice a partir d'un flux. Tous les membres d'ume matrice
	 * seront initialises, sauf les elements.
	 *
	 * @param in le flux ou lire
	 * @return une matrice ne contenant pas d'element
	 * @throws IOException une exception I/O a ete relevee
	 */
	public static Matrix<? extends Number> readHeader(DataInput in) throws IOException {
		return read(in, false);
	}

	/**
	 * lit tous les elements d'une matrice.
	 *
	 * @param <T> le type des elements de la matrice à lire
	 * @param in  le flux ou lire
	 * @param x   la matrice ou les elements lu seront stockes. Il est suppose que
	 *            les membres, a part les elements, sont renseignes.
	 * @throws IOException une exception I/O a ete relevee
	 */
	public static <T extends Number> void readData(DataInput in, Matrix<T> x) throws IOException {
		for (int i = 0; i < x.n; i++)
			for (int j = 0; j < x.m; j++)
				x.data[i][j] = x.readDataFunction.apply(in);
	}

	/**
	 * Lit, a partir d'un flux, une partie d'element d'une matrice.
	 *
	 * @param <T> le type des elements de la matrice a lire
	 * @param in  le flux ou lire
	 * @param x   une matrice correspondant aux donnees stockees dans le flux. La
	 *            matrice peut etre vide d'element.
	 * @param i0  la ligne ou commencer la lecture
	 * @param j0  la colonne ou commencer la lecture
	 * @param n   le nombre de ligne a lire
	 * @param m   le nombre de colonne a lire
	 * @return la matrice contenant les elements lus
	 * @throws IOException              une exception I/O a ete relevee
	 * @throws MatrixBoundReadException cette exception est levee si
	 *                                  {@code (i0 + n > x.getHeight())} ou
	 *                                  {@code (j0 + m > x.getWidth())}
	 */
	public static <T extends Number> Matrix<T> readData(DataInput in, final Matrix<T> x, final int i0, final int j0,
			final int n, final int m) throws IOException, MatrixBoundReadException {
		if ((i0 + n > x.n) || (j0 + m > x.m))
			throw new MatrixBoundReadException();
		Matrix<T> r = new Matrix<T>(n, m, x, true);
		for (int i = 0, toSkip = j0 + (i0 * x.m); i < n; i++, toSkip = x.m - m) {
			x.skipDataFunction.apply(in, toSkip);
			for (int j = 0; j < m; j++)
				r.data[i][j] = x.readDataFunction.apply(in);
		}
		return r;
	}

	/**
	 * Ecrit l'entete d'une matrice dans un flux.
	 *
	 * @param <T> le type des elements de la matrice a ecrire
	 * @param out le flux ou ecrire
	 * @param x   la matrice contenant l'entete a ecrire
	 * @throws IOException une exception I/O a ete relevee
	 */
	public static <T extends Number> void writeHeader(DataOutput out, final Matrix<T> x) throws IOException {
		out.writeInt(x.n);
		out.writeInt(x.m);
		out.writeInt(x.dataType.typeID);
	}

	/**
	 * Ecrit tous les elements d'une matrice dans un flux.
	 *
	 * @param <T> le type des elements de la matrice a ecrire
	 * @param out le flux ou ecrire
	 * @param x   la matrice contenant les elements a ecrire
	 * @throws IOException une exception I/O a ete relevee
	 */
	public static <T extends Number> void writeData(DataOutput out, final Matrix<T> x) throws IOException {
		for (int i = 0; i < x.n; i++)
			for (int j = 0; j < x.m; j++)
				x.writeDataFunction.apply(out, x.data[i][j]);
	}

	/**
	 * Ecrit, dans un flux, une partie des elements d'une matrice.
	 *
	 * @param <T>    le type des elements de la matrice a ecrire
	 * @param out    le flux ou ecrire
	 * @param x      la matrice contenant les elements a ecrire
	 * @param i0     la ligne ou commencer a ecrire
	 * @param j0     la colonne ou commencer a ecrire
	 * @param n      le nombre de ligne a ecrire
	 * @param m      le nombre de colonne a ecrire
	 * @param header si {@code true} alors l'entete de la matrice sera ecrit, si
	 *               {@code false} alors l'entete ne sera pas ecrit
	 * @throws MatrixBoundWriteException cette exception est levee si
	 *                                   {@code (i0 + n > x.getHeight())} ou
	 *                                   {@code (j0 + m > x.getWidth())}
	 * @throws IOException               une exception I/O a ete relevee
	 */
	public static <T extends Number> void writeData(DataOutput out, final Matrix<T> x, final int i0, final int j0,
			final int n, final int m, final boolean header) throws IOException, MatrixBoundWriteException {
		if ((i0 + n > x.n) || (j0 + m > x.m))
			throw new MatrixBoundWriteException();
		if (header) {
			out.writeInt(n);
			out.writeInt(m);
			out.writeInt(x.dataType.typeID);
		}
		for (int i = i0; i < i0 + n; i++)
			for (int j = j0; j < j0 + m; j++)
				x.writeDataFunction.apply(out, x.data[i][j]);
	}

	/** {@inheritDoc} */
	@Override
	public void write(DataOutput out) throws IOException {
		writeHeader(out, this);
		writeData(out, this);
	}

	/**
	 * Donne la somme de deux matrices. Les termes de la somme peuvent ne pas etre
	 * de memes dimensions, la somme suppose alors que les elements absents de la
	 * matrice la plus petite est zero.
	 *
	 * @param <T>             le type des elements d'un des termes
	 * @param <U>             le type des elements d'un des termes
	 * @param <R>             le type des elements de la matrice resultante
	 * @param x               un des termes de la somme
	 * @param y               un des termes de la somme
	 * @param sumDataFunction la fonction permettant de sommer un element de type
	 *                        {@code T} avec un element de type {@code U} et de
	 *                        retourner une element de type {@code R}
	 * @param virtual         si {@code true} alors il est possible de sommer des
	 *                        matrices de dimensions differentes, si {@code false}
	 *                        alors si les dimensions des matrices different une
	 *                        exception de type MatrixBoundAdditionException est
	 *                        levee
	 * @return une matrice resultant de la somme de deux matrices
	 * @throws MatrixBoundAdditionException si le parametre {@code virtual} est
	 *                                      {@code false} alors si les dimensions
	 *                                      des matrices different une exception de
	 *                                      type MatrixBoundAdditionException est
	 *                                      levee
	 */
	public static <T extends Number, U extends Number, R extends Number> Matrix<R> sum(final Matrix<T> x,
			final Matrix<U> y, final BiFunction<T, U, R> sumDataFunction, final boolean virtual)
			throws MatrixBoundAdditionException {
		int n = x.n, m = x.m;
		if (virtual == true) {
			n = Math.max(n, y.n);
			m = Math.max(m, y.m);
		} else if ((n != y.n) || (m != y.m))
			throw new MatrixBoundAdditionException();
		@SuppressWarnings("unchecked")
		Matrix<R> r = (Matrix<R>) builder(n, m, x, y, true);
		for (int i = 0; i < n; i++)
			for (int j = 0; j < m; j++)
				r.data[i][j] = sumDataFunction.apply((i < x.n && j < x.m) ? x.data[i][j] : x.ZERO,
						(i < y.n && j < y.m) ? y.data[i][j] : y.ZERO);
		return r;
	}

	/**
	 * Additionne une matrice y dans une matrice x. Les elements des deux matrices
	 * sont supposes etre de meme type. Les termes de l'addition peuvent ne pas etre
	 * de memes dimensions, l'addition suppose alors que les elements absents de la
	 * matrice la plus petite est zero.
	 *
	 * @param <T>             le type des elements des matrices
	 * @param x               Une matrice ou sera ecrit les resultats de l'addition
	 * @param y               Une matrice
	 * @param sumDataFunction la fonction permettant d'additionner un element de
	 *                        type {@code T} avec un element de type {@code T} et de
	 *                        retourner une element de type {@code T}
	 * @param virtual         si {@code true} alors il est possible d'additionner
	 *                        des matrices de dimensions differentes, si
	 *                        {@code false} alors si les dimensions des matrices
	 *                        different une exception de type
	 *                        MatrixBoundAdditionException est levee
	 * @throws MatrixBoundAdditionException si le parametre {@code virtual} est
	 *                                      {@code false} alors si les dimensions
	 *                                      des matrices different une exception de
	 *                                      type MatrixBoundAdditionException est
	 *                                      levee
	 */
	public static <T extends Number> void add(Matrix<T> x, final Matrix<T> y, BiFunction<T, T, T> sumDataFunction,
			boolean virtual) throws MatrixBoundAdditionException {
		int n = x.n, m = x.m;
		if (virtual == true) {
			n = Math.min(n, y.n);
			m = Math.min(m, y.m);
		} else if ((n != y.n) || (m != y.m))
			throw new MatrixBoundAdditionException();
		for (int i = 0; i < n; i++)
			for (int j = 0; j < m; j++)
				x.data[i][j] = sumDataFunction.apply(x.data[i][j], y.data[i][j]);
	}

	/**
	 * Multiplication, sequentielle, de deux matrices. Les termes de la
	 * multiplication et son resultat sont integralement charges en memoire.
	 *
	 * @param <T>                  le type des elements d'un des termes
	 * @param <U>                  le type des elements d'un des termes
	 * @param <R>                  le type des elements de la matrice resultante
	 * @param x                    un des termes de la multiplication
	 * @param y                    un des termes de la multiplication
	 * @param sumDataFunction      la fonction permettant d'additionner un element
	 *                             de type {@code R} avec un element de type
	 *                             {@code R} et de retourner une element de type
	 *                             {@code R}
	 * @param multiplyDataFunction la fonction permettant de multiplier un element
	 *                             de type {@code T} avec un element de type
	 *                             {@code U} et de retourner une element de type
	 *                             {@code R}
	 * @return une matrice resultante d'une multiplication
	 * @throws MatrixBoundMultiplicationException cette exception est levee si
	 *                                            {@code x.getWidth()!=y.getHeight()}
	 */
	public static <T extends Number, U extends Number, R extends Number> Matrix<R> multiply(final Matrix<T> x,
			final Matrix<U> y, final BiFunction<R, R, R> sumDataFunction,
			final BiFunction<T, U, R> multiplyDataFunction) throws MatrixBoundMultiplicationException {
		if (x.m != y.n)
			throw new MatrixBoundMultiplicationException();
		// Creation de la matrice resultante
		@SuppressWarnings("unchecked")
		Matrix<R> r = (Matrix<R>) builder(x.n, y.m, x, y, true);
		// Boucle sur les lignes de la matrice resultante
		for (int i = 0; i < x.n; i++)
			// Boucle sur les colonnes de la matrice resultante
			for (int j = 0; j < y.m; j++) {
				R a = multiplyDataFunction.apply(x.data[i][0], y.data[0][j]);
				// Boucle sur les elements des matrices des
				// termes de la multiplication
				for (int k = 1; k < x.m; k++)
					a = sumDataFunction.apply(a, multiplyDataFunction.apply(x.data[i][k], y.data[k][j]));
				r.data[i][j] = a;
			}
		return r;
	}

	/**
	 * Classe soutraitant la multiplication parallele, par multithreading, de deux
	 * matrices. Une instance de la classe est associee a un thread executant une
	 * partie d'une multiplication de deux matrices. Les termes de la multiplication
	 * et son resultat sont integralement charges en memoire.
	 *
	 * @param <T> le type des elements d'un des termes
	 * @param <U> le type des elements d'un des termes
	 * @param <R> le type des elements de la matrice resultante
	 */
	private static class ConcurrentMultiplication<T extends Number, U extends Number, R extends Number>
			implements Runnable {

		/** Un des termes de la multiplication. */
		private final Matrix<T> x;

		/** Un des termes de la multiplication. */
		private final Matrix<U> y;

		/** Une matrice resultante d'une multiplication. */
		private Matrix<R> r;

		/**
		 * {@code ithread} et {@code jthread} sont les indices du thread charge des
		 * traitements.
		 */
		private final int ithread, jthread;

		/**
		 * {@code nthreads} et {@code mthreads} sont les nombres de partition,
		 * respectivement sur les lignes et les colonnes de la matrice resultante.
		 */
		private final int nthreads, mthreads;

		/**
		 * La fonction permettant d'additionner un element de type {@code R} avec un
		 * element de type {@code R} et de retourner une element de type {@code R}.
		 */
		private final BiFunction<R, R, R> sumDataFunction;

		/**
		 * La fonction permettant de multiplier un element de type {@code T} avec un
		 * element de type {@code U} et de retourner une element de type {@code R}.
		 */
		private final BiFunction<T, U, R> multiplyDataFunction;

		/**
		 * Constructeur permettant d'initialiser les membres de la classe.
		 */
		public ConcurrentMultiplication(Matrix<R> r, final Matrix<T> x, final Matrix<U> y, final int nthreads,
				final int mthreads, final int ithread, final int jthread, final BiFunction<R, R, R> sumDataFunction,
				final BiFunction<T, U, R> multiplyDataFunction) {
			this.x = x;
			this.y = y;
			this.r = r;
			this.nthreads = nthreads;
			this.mthreads = mthreads;
			this.ithread = ithread;
			this.jthread = jthread;
			this.sumDataFunction = sumDataFunction;
			this.multiplyDataFunction = multiplyDataFunction;
		}

		/**
		 * Chaque thread, identifie par le couple {@code (ithread,jthread)}, est charge
		 * de calculer la multiplication la partition de la partition de la matrice
		 * resultant qui lui est dediee.
		 */
		@Override
		public void run() {
			int n, n1, m, m1;
			// Calcul du nombre de ligne de la partition des threads dont
			// 0<=i<nthreads-1
			n = Math.floorDiv(x.n, nthreads);
			// Calcul du nombre de ligne de la partition des threads dont
			// i=nthreads-1
			n1 = (ithread == nthreads - 1) ? (x.n - (ithread * n)) : n;
			// Calcul du nombre de colonne de la partition des threads dont
			// 0<=j<mthreads-1
			m = Math.floorDiv(y.m, mthreads);
			// Calcul du nombre de colonne de la partition des threads dont
			// j=mthreads-1
			m1 = (jthread == mthreads - 1) ? (y.m - (jthread * m)) : m;
			// Boucle sur les lignes de la partition resultante
			for (int i1 = 0; i1 < n1; i1++)
				// Boucle sur les colonnes de la partition resultante
				for (int j1 = 0; j1 < m1; j1++) {
					R a = multiplyDataFunction.apply(x.data[(ithread * n) + i1][0], y.data[0][(jthread * m) + j1]);
					// Boucle sur les elements des partitions des matrices des
					// termes de la multiplication
					for (int k1 = 1; k1 < x.m; k1++)
						a = sumDataFunction.apply(a, multiplyDataFunction.apply(x.data[(ithread * n) + i1][k1],
								y.data[k1][(jthread * m) + j1]));
					// Enregistrement d'un element de la partition resultante
					r.data[(ithread * n) + i1][(jthread * m) + j1] = a;
				}
		}
	}

	/**
	 * Multiplication, parallele par multithreading, de deux matrices. Les termes de
	 * la multiplication et son resultat sont integralement charges en memoire.
	 *
	 * @param <T>                  le type des elements d'un des termes
	 * @param <U>                  le type des elements d'un des termes
	 * @param <R>                  le type des elements de la matrice resultante
	 * @param x                    un des termes de la multiplication
	 * @param y                    un des termes de la multiplication
	 * @param nthreads             le nombre de thread qui est le nombre de
	 *                             partition sur les lignes de la matrice resultante
	 * @param mthreads             le nombre de thread qui est le nombre de
	 *                             partition sur les colonnes de la matrice
	 *                             resultante
	 * @param sumDataFunction      la fonction permettant d'additionner un element
	 *                             de type {@code R} avec un element de type
	 *                             {@code R} et de retourner une element de type
	 *                             {@code R}
	 * @param multiplyDataFunction la fonction permettant de multiplier un element
	 *                             de type {@code T} avec un element de type
	 *                             {@code U} et de retourner une element de type
	 *                             {@code R}
	 * @return une matrice resultante d'une multiplication
	 * @throws MatrixBoundMultiplicationException cette exception est levee si
	 *                                            {@code x.getWidth()!=y.getHeight()}
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Number, U extends Number, R extends Number> Matrix<R> multiply(final Matrix<T> x,
			final Matrix<U> y, final int nthreads, final int mthreads, final BiFunction<R, R, R> sumDataFunction,
			final BiFunction<T, U, R> multiplyDataFunction) throws MatrixBoundMultiplicationException {
		if (x.m != y.n)
			throw new MatrixBoundMultiplicationException();
		Matrix<R> r;
		int np = nthreads, mp = mthreads;
		if (np > x.n)
			np = 1;
		if (mp > y.m)
			mp = 1;
		// Si le nombre de partition est 1, alors une multiplication
		// sequentielle est executée
		if (np * mp == 1)
			r = multiply(x, y, sumDataFunction, multiplyDataFunction);
		else {
			// Creation de la matrice resultante
			r = (Matrix<R>) builder(x.n, y.m, x, y, true);
			// Creation d'un pool de thread
			ExecutorService executor = Executors.newFixedThreadPool(np * mp);
			// Demarrer les threads
			for (int i = 0; i < np; i++)
				for (int j = 0; j < mp; j++)
					executor.execute(new ConcurrentMultiplication<T, U, R>(r, x, y, np, mp, i, j, sumDataFunction,
							multiplyDataFunction));
			// Demande au thread lance de se terminer quand leur traitement est
			// termine
			executor.shutdown();
			// Boucle permettant d'attendre que tous les threads lances se
			// terminent.
			while (!executor.isTerminated()) {
			}
		}
		return r;
	}

	/**
	 * Classe soutraitant la multiplication parallele, par multithreading, de deux
	 * matrices. Une instance de la classe est associee a un thread executant une
	 * partie d'une multiplication de deux matrices. Les termes de la multiplication
	 * et son resultat sont stockes sur disque. Des partitions des termes de la
	 * multiplication sont chargees en memoire en fonction de la partition a
	 * calculer, et chargee en memoire, de la matrice resultante.
	 *
	 * @param <T> le type des elements d'un des termes
	 * @param <U> le type des elements d'un des termes
	 * @param <R> le type des elements de la matrice resultante
	 */
	private static class ConcurrentBlocksMultiplication<T extends Number, U extends Number, R extends Number>
			implements Runnable {

		/** Le chemin ou stocker les partitions de la matrice resultante. */
		private final Path rPath;

		/** Un squelette d'un des termes de la multiplication. */
		private Matrix<T> x;

		/** Un squelette d'un des termes de la multiplication. */
		private Matrix<U> y;

		/**
		 * Les flux permettant de lire les matrices des termes de la multiplication.
		 */
		private FSDataInputStream xIn, yIn;

		/** Les partitions d'un des termes de la multiplication. */
		private final Map<BlockKey, Matrix<T>> xBlocks;

		/** Les partitions d'un des termes de la multiplication. */
		private final Map<BlockKey, Matrix<U>> yBlocks;

		/**
		 * {@code ithread} et {@code jthread} sont les indices du thread charge des
		 * traitements.
		 */
		private final int ithread, jthread;

		/**
		 * {@code nthreads} et {@code mthreads} sont les nombres de partition,
		 * respectivement sur les partitions sur les lignes et sur les partitions sur
		 * les colonnes.
		 */
		private final int nthreads, mthreads;

		/**
		 * {@code nbsize} et {@code mbsize} sont respectivement le nombre de ligne et le
		 * nombre de colonne d'une partition de la matrice resultante
		 */
		private final int nbsize, mbsize;

		/**
		 * {@code kbsize} est le nombre de ligne et colonne d'une dimension d'une
		 * partition des matrices des termes de la multiplication.
		 */
		private final int kbsize;

		/**
		 * {@code nb} et {@code mb} sont les nombres de partition, respectivement sur
		 * les lignes et sur les colonnes de la matrice resultante.
		 */
		private final int nb, mb;

		/**
		 * La configuration de l'environnement d'execution de Apache Hadoop. Cette
		 * configuration permet de recuperer des parametres pour effectuer des
		 * operations E/S.
		 */
		private final Configuration conf;

		/**
		 * La fonction permettant d'additionner un element de type {@code R} avec un
		 * element de type {@code R} et de retourner une element de type {@code R}.
		 */
		private final BiFunction<R, R, R> sumDataFunction;

		/**
		 * La fonction permettant de multiplier un element de type {@code T} avec un
		 * element de type {@code U} et de retourner une element de type {@code R}.
		 */
		private final BiFunction<T, U, R> multiplyDataFunction;

		/**
		 * Constructeur permettant d'initialiser les membres de la classe.
		 * 
		 * @throws IOException une exception I/O a ete relevee
		 */
		@SuppressWarnings("unchecked")
		public ConcurrentBlocksMultiplication(final Path rPath, final Path xPath, final Path yPath,
				final Map<BlockKey, Matrix<T>> xBlocks, final Map<BlockKey, Matrix<U>> yBlocks, final int ithread,
				final int jthread, final int nbsize, final int mbsize, final int kbsize, final int nb, final int mb,
				final int nthreads, final int mthreads, final Configuration conf,
				final BiFunction<R, R, R> sumDataFunction, final BiFunction<T, U, R> multiplyDataFunction)
				throws IOException {
			FileSystem fs = FileSystem.get(conf);
			this.rPath = rPath;
			xIn = fs.open(xPath);
			yIn = fs.open(yPath);
			x = (Matrix<T>) readHeader(xIn);
			y = (Matrix<U>) readHeader(yIn);
			this.xBlocks = xBlocks;
			this.yBlocks = yBlocks;
			this.ithread = ithread;
			this.jthread = jthread;
			this.nbsize = nbsize;
			this.mbsize = mbsize;
			this.kbsize = kbsize;
			this.nb = nb;
			this.mb = mb;
			this.nthreads = nthreads;
			this.mthreads = mthreads;
			this.conf = conf;
			this.sumDataFunction = sumDataFunction;
			this.multiplyDataFunction = multiplyDataFunction;
		}

		/** {@inheritDoc} */
		@Override
		public void run() {
			try {
				Matrix<T> a;
				Matrix<U> b;
				Matrix<R> r, s;
				BlockKey xk = new BlockKey(), yk = new BlockKey();
				int nb1, nb2, mb1, mb2;
				// Calcul du nombre de partition, sur les lignes, de la
				// partition des threads dont
				// 0<=i<nthreads-1
				nb1 = Math.floorDiv(nb, nthreads);
				// Calcul du nombre de partition, sur les lignes, de la
				// partition des threads dont
				// i=nthreads-1
				nb2 = (ithread == nthreads - 1) ? (nb - (ithread * nb1)) : nb1;
				// Calcul du nombre de partition, sur les colonnes, de la
				// partition des threads dont
				// 0<=j<mthreads-1
				mb1 = Math.floorDiv(nb, mthreads);
				// Calcul du nombre de partition, sur les colonnes, de la
				// partition des threads dont
				// j=mthreads-1
				mb2 = (jthread == mthreads - 1) ? (nb - (jthread * mb1)) : mb1;
				// Boucle sur les partitions, sur les lignes, de la partition
				// resultante
				for (int i = 0, xi = ithread * nb1; i < nb2; i++, xi++)
					// Boucle sur les partitions, sur les colonnes, de la
					// partition resultante
					for (int j = 0, xj = jthread * mb1; j < mb2; j++, xj++) {
						// Recuperation du squelette de la partition (xi,0) d'un
						// terme de la multiplication
						a = xBlocks.get(xk.set(xi, 0));
						// Recuperation du squelette de la partition (0,xj) de
						// l'autre terme de la multiplication
						b = yBlocks.get(yk.set(0, xj));
						// Positionne le curseur de lecture du flux d'un terme
						// de la multiplication apres son entete
						xIn.seek(HEADER_SIZE);
						// Positionne le curseur de lecture du flux de l'autre
						// terme de la multiplication apres son entete
						yIn.seek(HEADER_SIZE);
						// Multiplication, sequentielle, des deux partitions,
						// identifiees ci-dessus, des deux termes de la
						// multiplication, apres que les elements soient lus
						// dans leur flux respectif.
						r = multiply(readData(xIn, x, xi * nbsize, 0, a.n, a.m),
								readData(yIn, y, 0, xj * mbsize, b.n, b.m), sumDataFunction, multiplyDataFunction);
						// Boucle sur les partitions des matrices des
						// termes de la multiplication
						for (int k = 1; k < mb; k++) {
							a = xBlocks.get(xk.set(xi, k));
							b = yBlocks.get(yk.set(k, xj));
							xIn.seek(HEADER_SIZE);
							yIn.seek(HEADER_SIZE);
							s = multiply(readData(xIn, x, xi * nbsize, k * kbsize, a.n, a.m),
									readData(yIn, y, k * kbsize, xj * mbsize, b.n, b.m), sumDataFunction,
									multiplyDataFunction);
							// Addition des resultats des precedentes
							// multiplications. Les dimensions de la matrice
							// resultante de l'addition correspond a la celles
							// de la matrice resultante des multiplications
							// precedentes aux plus grandes dimensions
							if ((r.n < s.n) || (r.m < s.m)) {
								add(s, r, sumDataFunction, true);
								r = s;
							} else
								add(r, s, sumDataFunction, true);
						}
						// Sauvegarde d'une partition de la matrice resultante
						FSDataOutputStream out = FileSystem.get(conf).create(getPartitionPath(rPath, xi, xj));
						r.write(out);
						out.close();
					}
				xIn.close();
				yIn.close();
			} catch (MatrixBoundMultiplicationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MatrixBoundAdditionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MatrixBoundReadException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Joindre les partitions, stockees sur disque, resultantes d'une multiplication
	 * de deux matrices et sauvegarde la matrice jointe sur disque.
	 *
	 * @param <T>    le type des elements d'un des termes
	 * @param <U>    le type des elements d'un des termes
	 * @param fs     Le systeme de fichier ou les fichiers seront lu et ecrits
	 * @param f      le chemin correspondant au prefix des fichiers des partions à
	 *               joindre et du fichier ou la matrice resultante sera stocker
	 * @param x      un des termes de la multiplication
	 * @param y      un des termes de la multiplication
	 * @param delete si {@code true} alors les fichiers des partitions seront
	 *               effaces apres la jointure
	 * @param nb     le nombre de partition a joindre
	 * @throws IOException              une exception I/O a ete relevee
	 * @throws MatrixBoundReadException une exception sur les bornes des partitions
	 *                                  a lire a ete levee
	 */
	public static <T extends Number, U extends Number> void joinBlocks(FileSystem fs, final Path f, final Matrix<T> x,
			final Matrix<U> y, final int nb, final boolean delete) throws IOException, MatrixBoundReadException {
		FSDataInputStream in;
		FSDataOutputStream out = fs.create(f.suffix(".data"));
		int nbsize = Math.floorDiv(x.n, nb);
		Matrix<?> r = builder(x.n, y.m, x, y, false);
		writeHeader(out, r);
		for (int i = 0; i < nb; i++)
			for (int k = 0, nb1 = (i == nb - 1) ? (x.n - (i * nbsize)) : nbsize; k < nb1; k++)
				for (int j = 0; j < nb; j++) {
					in = fs.open(getPartitionPath(f, i, j));
					r = readHeader(in);
					r = Matrix.readData(in, r, k, 0, 1, r.m);
					writeData(out, r);
					in.close();
				}
		out.close();
		if (delete)
			for (int i = 0; i < nb; i++)
				for (int j = 0; j < nb; j++)
					fs.delete(getPartitionPath(f, i, j), true);
	}

	/**
	 * Determine le nom du fichier d'une partition {@code (i,j}.
	 * 
	 * @param f le chemin correspondant au prefix des fichiers des partions
	 * @param i indice d'une partition sur les lignes
	 * @param j indice d'une partition sur les colonnes
	 * @return le chemin correspondant a la partition desiree
	 */
	public static Path getPartitionPath(final Path f, final int i, final int j) {
		return f.suffix("." + i + "." + j + ".data");
	}

	/**
	 * Multiplication, parallele par multithreading, de deux matrices. Les termes de
	 * la multiplication et son resultat sont integralement stockes sur disque.
	 *
	 * @param <T>                  le type des elements d'un des termes
	 * @param <U>                  le type des elements d'un des termes
	 * @param <R>                  le type des elements de la matrice resultante
	 * @param rPath                le chemin correspondant au prefix des fichiers
	 *                             des partions à joindre et du fichier ou la
	 *                             matrice resultante sera stocker
	 * @param xPath                le chemin correspondant au fichier d'une matrice
	 *                             des termes de la multiplication
	 * @param yPath                le chemin correspondant au fichier d'une matrice
	 *                             des termes de la multiplication
	 * @param nb                   nombre de partition sur les lignes de la matrice
	 *                             resultante
	 * @param mb                   nombre de partition sur les colonnes de la
	 *                             matrice resultante
	 * @param nthreads             le nombre de thread qui est le nombre de
	 *                             partition sur les partitions sur les lignes de la
	 *                             matrice resultante
	 * @param mthreads             le nombre de thread qui est le nombre de
	 *                             partition sur les partitions sur les colonnes de
	 *                             la matrice resultante
	 * @param conf                 La configuration de l'environnement d'execution
	 *                             de Apache Hadoop. Cette configuration permet de
	 *                             recuperer des parametres pour effectuer des
	 *                             operations E/S.
	 * @param sumDataFunction      la fonction permettant d'additionner un element
	 *                             de type {@code R} avec un element de type
	 *                             {@code R} et de retourner une element de type
	 *                             {@code R}
	 * @param multiplyDataFunction la fonction permettant de multiplier un element
	 *                             de type {@code T} avec un element de type
	 *                             {@code U} et de retourner une element de type
	 *                             {@code R}
	 * @throws IOException                        une exception I/O a ete relevee
	 * @throws MatrixBoundMultiplicationException cette exception est levee si
	 *                                            {@code x.getWidth()!=y.getHeight()}
	 * @throws MatrixBoundReadException           une exception sur les bornes des
	 *                                            partitions a lire a ete levee
	 * @throws MatrixBoundWriteException          une exception sur les bornes des
	 *                                            partitions a ecrire a ete levee
	 */
	public static <T extends Number, U extends Number, R extends Number> void multiply(final Path rPath,
			final Path xPath, final Path yPath, final int nb, final int mb, final int nthreads, final int mthreads,
			final Configuration conf, final BiFunction<R, R, R> sumDataFunction,
			final BiFunction<T, U, R> multiplyDataFunction) throws IOException, MatrixBoundMultiplicationException,
			MatrixBoundReadException, MatrixBoundWriteException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream xIn = fs.open(xPath), yIn = fs.open(yPath);
		FSDataOutputStream out;
		// Lecture du squelette d'un des termes de la multiplication afin de
		// determiner, entre autre, ses dimensions et le type des ses elements
		@SuppressWarnings("unchecked")
		Matrix<T> x = (Matrix<T>) readHeader(xIn);
		// Lecture du squelette de l'autre terme de la multiplication afin de
		// determiner, entre autre, ses dimensions et le type des ses elements
		@SuppressWarnings("unchecked")
		Matrix<U> y = (Matrix<U>) readHeader(yIn);
		Matrix<R> r;
		int np = nb, mp = mb;
		if (x.m != y.n)
			throw new MatrixBoundMultiplicationException();
		if (np > Math.min(x.n, y.m))
			np = 1;
		if (mp > x.m)
			mp = 1;
		// Si le nombre de partition est 1, alors les termes sont integralement
		// charges en memoire pour une multiplication parallele par
		// multithreading
		if (np * mp == 1) {
			x.data = x.newDataFunction.apply(x.n, x.m);
			readData(xIn, x);
			y.data = y.newDataFunction.apply(y.n, y.m);
			readData(yIn, y);
			r = multiply(x, y, nthreads, mthreads, sumDataFunction, multiplyDataFunction);
			// Sauvegarde de la matrice resultante
			out = fs.create(rPath.suffix(".data"));
			r.write(out);
			out.close();
		}
		xIn.close();
		yIn.close();
		if (np * mp > 1) {
			// Creation d'un pool de thread
			ExecutorService executor = Executors.newFixedThreadPool(nthreads * mthreads);
			// Creation des partitions d'un des termes de la multiplication
			Map<BlockKey, Matrix<T>> xBlocks = toBlocks(x, nb, mb);
			// Creation des partitions de l'autre terme de la multiplication
			Map<BlockKey, Matrix<U>> yBlocks = toBlocks(y, mb, nb);
			// Calcule du nombre de ligne d'une partition de la matrice
			// resultante
			int nbsize = Math.floorDiv(x.n, nb);
			// Calcule du nombre de colonne d'une partition de la matrice
			// resultante
			int mbsize = Math.floorDiv(y.m, nb);
			// Calcule du nombre de colonne et de ligne d'une dimension d'une
			// partition des matrices des termes de la multiplication
			int kbsize = Math.floorDiv(x.m, mb);
			// Demarrer les threads
			for (int i = 0; i < nthreads; i++)
				for (int j = 0; j < mthreads; j++)
					executor.execute(new ConcurrentBlocksMultiplication<T, U, R>(rPath, xPath, yPath, xBlocks, yBlocks,
							i, j, nbsize, mbsize, kbsize, nb, mb, nthreads, mthreads, conf, sumDataFunction,
							multiplyDataFunction));
			// Demande au thread lance de se terminer quand leur traitement est
			// termine
			executor.shutdown();
			// Boucle permettant d'attendre que tous les threads lances se
			// terminent.
			while (!executor.isTerminated()) {
			}
			// Joindre les partitions de la matrice resultante
			joinBlocks(fs, rPath, x, y, nb, true);
		}
	}

	public static Path checkConfigurationName(FileSystem fs, Configuration conf, final String property,
			final Path prefix, final boolean input) throws InvalidJobConfException, IOException {
		Path f = null;
		String name = conf.get(property, null), err = null;
		if (name == null)
			err = new String("Le parametre " + property + " n'a pas ete renseigne.");
		else {
			f = fs.makeQualified((prefix != null) ? new Path(prefix, name) : new Path(name));
			if (input && !fs.exists(f))
				err = new String(f.toString() + " n'existe pas.");
		}
		if (err != null)
			throw new InvalidJobConfException(err);
		return f;
	}

	/**
	 * Multiply.
	 *
	 * @param <T>                  the generic type
	 * @param <U>                  the generic type
	 * @param <R>                  the generic type
	 * @param conf                 the conf
	 * @param sumDataFunction      the sum data function
	 * @param multiplyDataFunction the multiply data function
	 * @throws IOException                        Signals that an I/O exception has
	 *                                            occurred.
	 * @throws MatrixBoundMultiplicationException the matrix bound multiplication
	 *                                            exception
	 * @throws MatrixBoundReadException           the matrix bound read exception
	 * @throws ClassNotFoundException             the class not found exception
	 * @throws InterruptedException               the interrupted exception
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Number, U extends Number, R extends Number> int multiply(Configuration conf,
			final BiFunction<R, R, R> sumDataFunction, final BiFunction<T, U, R> multiplyDataFunction)
			throws IOException, MatrixBoundMultiplicationException, MatrixBoundReadException, ClassNotFoundException,
			InterruptedException {
		FileSystem fs = FileSystem.get(conf);
		Path inputDir, xPath, yPath, rPath, outputDir;
		FSDataInputStream xIn, yIn;
		FSDataOutputStream out;
		Matrix<T> x = new Matrix<T>();
		Matrix<U> y = new Matrix<U>();
		Matrix<R> r;
		int nb = conf.getInt(MatrixParameter.nBlocks, 0), mb = conf.getInt(MatrixParameter.mBlocks, 0);
		int nthreads = conf.getInt(MatrixParameter.nThreads, 0), mthreads = conf.getInt(MatrixParameter.mThreads, 0);
		int result = 1;
		inputDir = checkConfigurationName(fs, conf, MatrixParameter.inputDir, null, true);
		xPath = checkConfigurationName(fs, conf, MatrixParameter.xFilename, inputDir, true);
		yPath = checkConfigurationName(fs, conf, MatrixParameter.yFilename, inputDir, true);
		outputDir = checkConfigurationName(fs, conf, MatrixParameter.outputDir, null, false);
		rPath = checkConfigurationName(fs, conf, MatrixParameter.rPrefixFilename, outputDir, false);
		xIn = fs.open(xPath);
		yIn = fs.open(yPath);
		x = (Matrix<T>) readHeader(xIn);
		y = (Matrix<U>) readHeader(yIn);
		if (x.m != y.n)
			throw new MatrixBoundMultiplicationException();
		if (nb > Math.min(x.n, y.m))
			nb = 1;
		if (mb > x.m)
			mb = 1;
		if (nb * mb == 1) {
			x.data = x.newDataFunction.apply(x.n, x.m);
			readData(xIn, x);
			y.data = y.newDataFunction.apply(y.n, y.m);
			readData(yIn, y);
			r = multiply(x, y, nthreads, mthreads, sumDataFunction, multiplyDataFunction);
			out = fs.create(rPath.suffix(".data"));
			r.write(out);
			out.close();
			result = 0;
		}
		xIn.close();
		yIn.close();
		if (nb * mb > 1) {
			conf.set(MatrixParameter.xPath, xPath.toString());
			conf.set(MatrixParameter.yPath, yPath.toString());
			conf.set(MatrixParameter.rPrefixPath, rPath.toString());
			fs.delete(outputDir, true);

			Job job = Job.getInstance(conf);
			job.setJobName(conf.get(MatrixParameter.jobName, null));
			job.setJarByClass(MatrixMultiply.class);
			job.setNumReduceTasks(conf.getInt(MatrixParameter.nReducers, 0));
			if (job.getNumReduceTasks() == 0) {
				job.setMapperClass(new MatrixMultiplyAndSumMapper<T, U, R>() {
				}.getClass());
			} else {
				job.setMapperClass(new MatrixMultiplyMapper<T, U, R>() {
				}.getClass());
				job.setCombinerClass(new MatrixSumReducer<R>() {
				}.getClass());
				job.setPartitionerClass(new HashPartitioner<BlockKey, Matrix<R>>() {
				}.getClass());
				job.setReducerClass(new MatrixSumReducer<R>() {
				}.getClass());
			}
			job.setInputFormatClass(MatrixInputFormat.class);
			job.setOutputFormatClass(new MatrixFileOutputFormat<R>() {
			}.getClass());
			job.setOutputKeyClass(BlockKey.class);
			job.setOutputValueClass(new Matrix<R>() {
			}.getClass());
			FileInputFormat.setInputPaths(job, xPath, yPath);
			FileOutputFormat.setOutputPath(job, outputDir);
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
			if (job.waitForCompletion(true)) {
				joinBlocks(fs, rPath, x, y, nb, true);
				result = 0;
			}
		}
		return result;
	}
}

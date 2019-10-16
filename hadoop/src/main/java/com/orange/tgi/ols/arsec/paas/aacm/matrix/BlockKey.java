package com.orange.tgi.ols.arsec.paas.aacm.matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Classe permettant d'associer une clef a une partition d'une matrice. Une clef
 * est constituee d'un couple d'entier {@code (i,j)} ou {@code i} et {@code j}
 * designent respectivement une ligne et une colonne d'un element, qui peut etre
 * une partition, d'une matrice. Les clefs respectent l'ordre suivant:
 * <ul>
 * <li>{@code si i<i' alors (i,j)<(i',j')},
 * <li>{@code si i>i' alors (i,j)>(i',j')},
 * <li>{@code si i=i' alors}
 * <ul>
 * <li>{@code si j<j' alors (i,j)<(i',j')},
 * <li>{@code si j>j' alors (i,j)>(i',j')},
 * <li>{@code si j=j' alors (i,j)=(i',j')},
 * </ul>
 * </ul>
 * La classe {@code BlockKey} implemente l'interface
 * {@link org.apache.hadoop.io.WritableComparable} pour respecter les
 * contraintes imposees par Hadoop sur les clefs.
 */
public class BlockKey implements WritableComparable<BlockKey> {
  /**
   * {@code i} et {@code j} designent respectivement une ligne et une colonne
   * d'un element, qui peut etre une partition, d'une matrice.
   */
  private int i, j;

  /**
   * Recupere le numero de la ligne d'une clef.
   * 
   * @return le numero de la ligne d'une clef
   */
  public int geti() {
    return i;
  }

  /**
   * Recupere le numero de la colonne d'une clef.
   * 
   * @return le numero de la colonne d'une clef
   */
  public int getj() {
    return j;
  }

  /**
   * Initialise les numeros de ligne et de colonne d'une clef
   * 
   * @param i un numero de ligne
   * @param j un numero de colonne
   */
  private void init(final int i, final int j) {
    this.i = i;
    this.j = j;
  }

  /**
   * Initialise les numeros de ligne et de colonne d'une clef
   * 
   * @param  i un numero de ligne
   * @param  j un numero de colonne
   * @return   la clef avec les nouvelles valeurs de ligne et colonne
   */
  public BlockKey set(final int i, final int j) {
    init(i, j);
    return this;
  }

  /**
   * Instancie une clef vide
   */
  public BlockKey() {
  }

  /**
   * Instancie une clef avec les numeros de ligne et de colonne
   * 
   * @param i un numero de ligne
   * @param j un numero de colonne
   */
  public BlockKey(final int i, final int j) {
    init(i, j);
  }

  /**
   * Instancie une clef avec une autre clef. Attention: l'instance resultante
   * est un clone car les membres de {@code BlockKey} sont de type primitive.
   * 
   * @param x une clef
   */
  public BlockKey(final BlockKey x) {
    init(x.i, x.j);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    init(in.readInt(), in.readInt());
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(i);
    out.writeInt(j);
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(final BlockKey x) {
    int d1 = i - x.i, d2 = j - x.j;
    if (d1 > 0)
      return 1;
    if (d1 < 0)
      return -1;
    if (d2 > 0)
      return 1;
    if (d2 < 0)
      return -1;
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return String.format("%d%d", i, j).hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof BlockKey))
      return false;
    BlockKey x = (BlockKey) o;
    return ((i == x.i) && (j == x.j));
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format("i:[%d],j:[%d]", i, j);
  }
}

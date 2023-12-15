/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.sail.readonly.datastructures.roaringbitmap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.LoggerFactory;

public class Roaring64BitmapAdder {
	private static final boolean SIGNED_LONGS = true;
	private static final int TEMP_LONG_ARRAY_SIZE = 1024 * 16;
	private final LongBitmapDataProvider bitmap;
	private final boolean navigable;

	public Roaring64BitmapAdder(boolean navigable) {
		super();
		this.navigable = navigable;
		if (navigable) {
			bitmap = new Roaring64NavigableMap(SIGNED_LONGS);
		} else {
			bitmap = new Roaring64Bitmap();
		}
	}

	private final long[] listToAdd = new long[TEMP_LONG_ARRAY_SIZE];
	private int at = 0;
	private int added;

	/**
	 * Adds a long to be added to the final bitset.
	 * 
	 * @param toAdd which will be added (set to true)
	 */
	public void add(long toAdd) {
		if (at > 0 && listToAdd[at] == toAdd) {
			return;
		}
		listToAdd[at++] = toAdd;
		if (at == TEMP_LONG_ARRAY_SIZE) {
			addToBitmap();
			at = 0;
		}
	}

	/**
	 * Make the final Roaring64Bitmap or Roaring64Navigablemap
	 * @return the final run optimized LongBitmapDataProvider
	 */
	public LongBitmapDataProvider build() {
		addToBitmap();
		runOptimize();
		return bitmap;
	}

	/**
	 * Try to be as efficient possible with adding values.
	 */
	private void addToBitmap() {
		if (at > 0) {
			Arrays.sort(listToAdd, 0, at);
			int loop = 0;
			int from = 0;
			while (from < at) {
				int monotonicallyIncreasesTill = monotonicallyIncreasesTill(listToAdd, from, at);
				if (monotonicallyIncreasesTill > from) {
					addRange(from, monotonicallyIncreasesTill);
					from = monotonicallyIncreasesTill;
					added++;
				} else {
					assert from == monotonicallyIncreasesTill;
					bitmap.addLong(listToAdd[from++]);
					added++;
				}
				if (loop > listToAdd.length) {
					LoggerFactory.getLogger(getClass()).error("Loop should have terminated already at:" + at + " , from:" + from);
				}
				loop++;
			}
			if (added > 1_000_000) {
				runOptimize();
				added = 0;
			}
		}
	}

	private void runOptimize() {
		if (navigable) {
			((Roaring64NavigableMap) bitmap).runOptimize();
		} else {
			((Roaring64Bitmap) bitmap).runOptimize();
		}
	}

	private void addRange(int from, int monotonicallyIncreasesTill) {
		if (navigable) {
			((Roaring64NavigableMap) bitmap).addRange(listToAdd[from], listToAdd[monotonicallyIncreasesTill] + 1);
		} else {
			((Roaring64Bitmap) bitmap).addRange(listToAdd[from], listToAdd[monotonicallyIncreasesTill] + 1);
		}
	}

	/**
	 * Read either the Roaring64Bitmap or Roaring64Navigablemap
	 * 
	 * @param ois the stream to read from.
	 * @return a LongBitmapProdider (Roaring64Bitmap or Roaring64Navigablemap)
	 * @throws IOException in case of io issues.
	 */
	public static LongBitmapDataProvider readLongBitmapDataProvider(ObjectInputStream ois) throws IOException {
		LongBitmapDataProvider rb;
		int r = ois.readInt();
		if (r == 1) {
			rb = new Roaring64Bitmap();
			((Roaring64Bitmap) rb).readExternal(ois);
		} else if (r == 2) {
			rb = new Roaring64NavigableMap(SIGNED_LONGS, MutableRoaringBitmap::new);

			try {
				((Roaring64NavigableMap) rb).readExternal(ois);
			} catch (ClassNotFoundException e) {
				throw new IOException("Serialization error expected a type of known Roaring64 ", e);
			}
		} else {
			throw new IOException("Serialization error expected a type of known Roaring64 ");
		}
		return rb;
	}

	/**
	 * Write out the LongBitmapDataProvider either a Roaring64Bitmap or a Roaring64NavigableMap.
	 * which does
	 * @param oos We use java serialization to write this out.
	 * @param values To serialize.
	 * @throws IOException if there is an io issue.
	 */
	public static void writeLongBitmapDataProvider(ObjectOutputStream oos, LongBitmapDataProvider values)
			throws IOException {
		if (values instanceof Roaring64Bitmap rbm) {
			oos.writeInt(1);
			rbm.writeExternal(oos);
		} else if (values instanceof Roaring64NavigableMap rnm) {
			oos.writeInt(2);
			rnm.writeExternal(oos);
		}
	}

	/**
	 * Any monotonically increasing range should be in here
	 *
	 * @param temps the sorted list of temorary values (sorted till the at value)
	 * @param from  the point to start checking if we have a monotincally increasing
	 *              values in the temp array
	 * @param at    the last filled value in the temp array
	 * @return the value of from if there is not mononotic increase
	 */
	private static int monotonicallyIncreasesTill(long[] temps, int from, int at) {
		// Find a monotonically increasing (or not increasing)
		for (int i = from + 1; i < at; i++) {
			long cur = temps[i];
			long prev = temps[i - 1];
			// test if one higher did not change.
			if (cur - 1 != prev && cur != prev) {
				return i - 1;
			}
		}
		return at - 1;
	}
}

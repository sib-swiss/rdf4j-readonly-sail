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

public class Roaring64BitmapAdder {
	private static final int TEMP_LONG_ARRAY_SIZE = 1024 * 16;

	public Roaring64BitmapAdder() {
		super();
	}

	private final Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
	private final long[] listToAdd = new long[TEMP_LONG_ARRAY_SIZE];
	private int at = 0;
	private int added;

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

	public LongBitmapDataProvider build() {
		addToBitmap();
		bitmap.runOptimize();
		return bitmap;
	}

	private void addToBitmap() {
		if (at > 0) {
			Arrays.sort(listToAdd, 0, at);
			int from = 0;
			while (from < at) {
				int monotonicallyIncreasesTill = monotonicallyIncreasesTill(listToAdd, from, at);
				if (monotonicallyIncreasesTill > from) {
					bitmap.add(listToAdd[from], listToAdd[monotonicallyIncreasesTill] + 1);
					from = monotonicallyIncreasesTill;
					added++;
				} else {
					assert from == monotonicallyIncreasesTill;
					bitmap.addLong(listToAdd[from++]);
					added++;
				}
			}
			if (added > 1_000_000) {
				bitmap.runOptimize();
				added = 0;
			}
		}
	}

	public static LongBitmapDataProvider readLongBitmapDataProvider(ObjectInputStream bis) throws IOException {
		LongBitmapDataProvider rb;
		int r = bis.readInt();
		if (r == 1) {
			rb = new Roaring64Bitmap();
			((Roaring64Bitmap) rb).readExternal(bis);
		} else if (r == 2) {
			rb = new Roaring64NavigableMap(MutableRoaringBitmap::new);

			try {
				((Roaring64NavigableMap) rb).readExternal(bis);
			} catch (ClassNotFoundException e) {
				throw new IOException("Serialization error expected a type of known Roaring64 ", e);
			}
		} else {
			throw new IOException("Serialization error expected a type of known Roaring64 ");
		}
		return rb;
	}

	public static void writeLongBitmapDataProvider(ObjectOutputStream dos, LongBitmapDataProvider values)
			throws IOException {
		if (values instanceof Roaring64Bitmap) {
			dos.writeInt(1);
			((Roaring64Bitmap) values).writeExternal(dos);
		} else if (values instanceof Roaring64NavigableMap) {
			dos.writeInt(2);
			((Roaring64NavigableMap) values).writeExternal(dos);
		}
	}

	/**
	 * Any monotonically increasing range should be in here
	 *
	 * @param temps the sorted list of temorary values (sorted till the at value)
	 * @param from  the point to start checking if we have a monotincally increasing values in the temp array
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

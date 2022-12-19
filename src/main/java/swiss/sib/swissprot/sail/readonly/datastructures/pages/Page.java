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
package swiss.sib.swissprot.sail.readonly.datastructures.pages;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 *
 * @author Jerven Bolleman <jerven.bolleman@sib.swiss>
 */
public interface Page<T> {

	public long size();

	public boolean isFull();

	public void sort();

	public default Iterator<T> iterator() {
		return iterator(0);
	}

	public Iterator<T> iterator(int start);

	public PrimitiveIterator.OfLong keyIterator();

	public PrimitiveIterator.OfLong valueIterator();

	T first();

	T last();

	long firstKey();

	long lastKey();

	public default boolean hasKey(long key) {
		if (key < firstKey()) {
			return false;
		}
		if (key > lastKey()) {
			return false;
		}
		java.util.PrimitiveIterator.OfLong keys = keyIterator();
		while (keys.hasNext()) {
			if (keys.nextLong() == key) {
				return true;
			}
		}
		return false;
	}

	public default Iterator<T> fromKey(long key, ToLong<T> getKey) {
		if (key < firstKey()) {
			return Collections.emptyIterator();
		} else if (key > lastKey()) {
			return Collections.emptyIterator();
		}
		return iterator();
	}

	public void toStream(DataOutputStream stream) throws IOException;

	public Type getType();

	enum Type {
		BASIC(1),
		BASIC_BUFFERED(2),
		COMPRESSED(3),
		COMPRESSED_BUFFERED(4),
		SORT(-1);

		public static Type fromCode(byte type) {
			for (Type t : values()) {
				if (t.getCode() == type) {
					return t;
				}
			}
			return null;
		}

		private final byte code;

		private Type(int code) {
			this.code = (byte) code;
		}

		public byte getCode() {
			return code;
		}

	}
}

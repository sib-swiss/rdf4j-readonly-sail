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
package swiss.sib.swissprot.sail.readonly.datastructures;

import java.util.Comparator;
import java.util.Iterator;

import swiss.sib.swissprot.sail.readonly.datastructures.iterators.Iterators;

public interface SortedLongLongMap {

	public Iterator<KeyValue> iteratorForKey(long key);

	public Iterator<KeyValue> iteratorForValue(long value);

	public default Iterator<KeyValue> iteratorForKeyValue(long key, long value) {
		return Iterators.filter(iteratorForKey(key), lp -> lp.value() == value);
	}

	public Iterator<KeyValue> iterator();

	public Iterator<KeyValue> subjectOrderedIterator();

	public static class KeyValue {
		private final long key;
		private final long value;
		private final long position;

		public KeyValue(long string, long value, long position) {
			super();
			this.key = string;
			this.value = value;
			this.position = position;
		}

		public long key() {
			return key;
		}

		public long value() {
			return value;
		}

		public long position() {
			return position;
		}
	}

	public static Comparator<KeyValue> compareByKeyValuePosition() {
		return Comparator.comparingLong(KeyValue::key)
				.thenComparingLong(KeyValue::value)
				.thenComparingLong(KeyValue::position);
	}

	static String[] split(String line) {
		int afterGraph = line.indexOf('\t');
		String graph = line.substring(0, afterGraph);
		int afterSubject = line.indexOf('\t', afterGraph + 1);
		String subject = line.substring(afterGraph + 1, afterSubject);
		String object = line.substring(afterSubject + 1);
		return new String[] { graph, subject, object };
	}

	public long size();

}

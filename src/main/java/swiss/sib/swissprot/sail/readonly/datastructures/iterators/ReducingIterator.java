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
package swiss.sib.swissprot.sail.readonly.datastructures.iterators;

import java.util.Comparator;
import java.util.Iterator;

public final class ReducingIterator<T> implements Iterator<T> {
	private final Iterator<T> mergeSort;
	private final Comparator<T> comparator;
	T next;
	T last = null;

	public ReducingIterator(Iterator<T> mergeSort, Comparator<T> comparator) {
		this.mergeSort = mergeSort;
		this.comparator = comparator;
		if (mergeSort.hasNext()) {
			this.next = mergeSort.next();
			this.last = next;
		} else
			this.next = null;
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
//		} else {
//			advance(mergeSort);
		}
		return next != null;
	}

	private void advance(Iterator<T> mergeSort) {
		while (mergeSort.hasNext()) {
			next = mergeSort.next();
			if (last == null || comparator.compare(next, last) != 0) {
				last = next;
				return;
			}
		}
		next = null;
	}

	@Override
	public T next() {
		try {
			return next;
		} finally {
			advance(mergeSort);
//					last = next;
		}
	}
}

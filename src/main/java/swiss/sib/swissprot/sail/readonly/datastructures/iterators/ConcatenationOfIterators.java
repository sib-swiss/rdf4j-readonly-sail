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

import java.util.Iterator;

/**
 *
 @author <a href="mailto:jerven.bolleman@sib.swiss">Jerven Bolleman</a>
 */
class ConcatenationOfIterators<T> implements Iterator<T> {

	private final Iterator<Iterator<T>> iter;

	ConcatenationOfIterators(Iterator<Iterator<T>> iter) {
		this.iter = iter;
	}

	Iterator<T> current;

	@Override
	public T next() {
		return current.next();
	}

	@Override
	public boolean hasNext() {
		if (current != null && current.hasNext()) {
			return true;
		}
		while (iter.hasNext()) {
			current = iter.next();
			if (current.hasNext()) {
				return true;
			}
		}
		return false;
	}
}

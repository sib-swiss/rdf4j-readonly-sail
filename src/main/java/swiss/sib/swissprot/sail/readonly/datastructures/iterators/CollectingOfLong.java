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
import java.util.PrimitiveIterator;

/**
 *
 * @author Jerven Bolleman <jerven.bolleman@sib.swiss>
 */
public class CollectingOfLong implements PrimitiveIterator.OfLong {

	private final Iterator<PrimitiveIterator.OfLong> iter;

	public CollectingOfLong(Iterator<PrimitiveIterator.OfLong> iter) {
		this.iter = iter;
	}

	PrimitiveIterator.OfLong current;

	@Override
	public long nextLong() {
		return current.nextLong();
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

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

import java.util.Collections;
import java.util.Iterator;

import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMap.KeyValue;

public class FilteredKeyValueOrderIterator implements Iterator<KeyValue> {
	private final Iterator<KeyValue> base;
	private final Iterator<Long> filterRepeater;
	private KeyValue nextPotentialKV;
	private KeyValue nextKV;
	private long nextPotentialPos;
	private boolean done = false;

	public static Iterator<KeyValue> supply(Iterator<KeyValue> base, Iterator<Long> filterRepeater) {
		if (!base.hasNext()) {
			return Collections.emptyIterator();
		} else if (!filterRepeater.hasNext()) {
			return Collections.emptyIterator();
		} else {
			return new FilteredKeyValueOrderIterator(base, filterRepeater);
		}
	}

	private FilteredKeyValueOrderIterator(Iterator<KeyValue> base, Iterator<Long> filterRepeater) {
		super();
		this.base = base;
		this.filterRepeater = filterRepeater;
		this.nextPotentialKV = this.base.next();
		this.nextPotentialPos = this.filterRepeater.next();
		advance();
	}

	private void advance() {
		while (this.nextKV == null && !done) {
			if (this.nextPotentialKV.position() == this.nextPotentialPos) {
				nextKV = this.nextPotentialKV;
				if (this.filterRepeater.hasNext())
					this.nextPotentialPos = this.filterRepeater.next();
				else
					done = true;
				return;
			} else if (this.nextPotentialKV.position() < this.nextPotentialPos) {
				if (this.base.hasNext())
					this.nextPotentialKV = this.base.next();
				else
					done = true;
			} else {
				if (this.filterRepeater.hasNext())
					this.nextPotentialPos = this.filterRepeater.next();
				else
					done = true;
			}
		}
	}

	@Override
	public boolean hasNext() {
		return !done || nextKV != null;
	}

	@Override
	public KeyValue next() {
		KeyValue temp = nextKV;
		nextKV = null;
		advance();
		return temp;
	}

}

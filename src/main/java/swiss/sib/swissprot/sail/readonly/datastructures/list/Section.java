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
package swiss.sib.swissprot.sail.readonly.datastructures.list;

import java.util.ListIterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;

public interface Section<T> extends Iterable<TPosition<T>> {

	public byte[] first();

	public TPosition<T> get(int i);

	public long sectionId();

	public TPosition<T> findByBinarySearch(T element);

	public default long findPositionByBinarySearch(T element) {
		TPosition<T> findByBinarySearch = findByBinarySearch(element);
		if (findByBinarySearch == null)
			return WriteOnce.NOT_FOUND;
		else
			return findByBinarySearch.position();
	}

	public ListIterator<TPosition<T>> listIterator();

	public long sizeOnDisk();

	public interface Writer<T> extends Consumer<Section<T>> {

	};

	public interface Reader<T> extends Supplier<Section<T>> {

	};
}

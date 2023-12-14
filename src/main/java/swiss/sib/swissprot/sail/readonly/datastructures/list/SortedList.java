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

import java.io.IOException;
import java.util.function.Function;

import swiss.sib.swissprot.sail.readonly.datastructures.TPosition;

public interface SortedList<T> {

	long positionOf(T element) throws IOException;

	IterateInSortedOrder<T> iterator() throws IOException;

	T get(long id);

	Function<T, TPosition<T>> searchInOrder() throws IOException;

	long size();

}

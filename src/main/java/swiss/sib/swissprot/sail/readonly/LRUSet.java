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
package swiss.sib.swissprot.sail.readonly;

import java.util.LinkedHashSet;

//Almost standard java LRU Set
public final class LRUSet<V> extends LinkedHashSet<V> {
	private final int maxSize;

	public LRUSet(int maxSize) {
		super(maxSize);
		this.maxSize = maxSize;
	}

	private static final long serialVersionUID = 1L;

	protected boolean removeEldestEntry(V eldest) {
		return size() > maxSize;
	}
}

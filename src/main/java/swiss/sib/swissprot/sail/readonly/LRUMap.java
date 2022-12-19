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

import java.util.LinkedHashMap;
import java.util.Map;

//Almost standard java LRU map
public final class LRUMap<K, V> extends LinkedHashMap<K, V> {
	private final int maxSize;

	public LRUMap(int maxSize) {
		super();
		this.maxSize = maxSize;
	}

	private static final long serialVersionUID = 1L;

	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > maxSize;
	}
}

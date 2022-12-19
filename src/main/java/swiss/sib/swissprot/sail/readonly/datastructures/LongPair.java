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

public final class LongPair {
	private final long key;
	private final long value;

	public LongPair(long key, long value) {
		super();
		this.key = key;
		this.value = value;
	}

	public Long key() {
		return key;
	}

	public Long value() {
		return value;
	}

	public static final int keyCompare(LongPair a, LongPair b) {
		return Long.compare(a.key, b.key);
	}
}

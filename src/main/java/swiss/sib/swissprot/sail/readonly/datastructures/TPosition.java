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

public final class TPosition<T> {
	private final T t;
	private final long position;

	public TPosition(T string, long position) {
		super();
		this.t = string;
		this.position = position;
	}

	public T t() {
		return t;
	}

	public long position() {
		return position;
	}
}

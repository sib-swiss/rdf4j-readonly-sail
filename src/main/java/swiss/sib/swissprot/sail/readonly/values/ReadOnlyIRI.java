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
package swiss.sib.swissprot.sail.readonly.values;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.AbstractIRI;
import org.eclipse.rdf4j.model.util.URIUtil;

import swiss.sib.swissprot.sail.readonly.WriteOnce;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;

public class ReadOnlyIRI extends AbstractIRI implements ReadOnlyValue {

	private static final long serialVersionUID = 1L;
	private final long id;
	private final SortedList<Value> backingstore;

	public ReadOnlyIRI(long id, SortedList<Value> backingstore) {
		super();
		assert id != WriteOnce.NOT_FOUND;
		this.id = id;
		this.backingstore = backingstore;
	}

	@Override
	public String stringValue() {
		return backingstore.get(id).stringValue();
	}

	@Override
	public String getNamespace() {
		String sv = stringValue();
		int localNameIdx = URIUtil.getLocalNameIndex(sv);
		return sv.substring(0, localNameIdx);
	}

	@Override
	public String getLocalName() {
		String sv = stringValue();
		int localNameIdx = URIUtil.getLocalNameIndex(sv);
		return sv.substring(localNameIdx);
	}

	@Override
	public long id() {
		return id;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ReadOnlyIRI or && or.backingstore == this.backingstore) {
			return or.id == this.id;
		}
		return super.equals(o);
	}
}

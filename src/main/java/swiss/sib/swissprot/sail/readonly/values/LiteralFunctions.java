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

import org.eclipse.rdf4j.model.Literal;

public class LiteralFunctions {
	private LiteralFunctions() {

	}

	static boolean standardLiteralEquals(Literal l, Object o) {
		if (o instanceof Literal) {
			Literal other = (Literal) o;

			// Compare datatypes
			if (!l.getDatatype().equals(other.getDatatype())) {
				return false;
			}

			// Compare labels
			if (!l.getLabel().equals(other.getLabel())) {
				return false;
			}

			if (l.getLanguage().isPresent() && other.getLanguage().isPresent()) {
				return l.getLanguage().get().equalsIgnoreCase(other.getLanguage().get());
			}
			// If only one has a language, then return false
			else {
				return !l.getLanguage().isPresent() && !other.getLanguage().isPresent();
			}
		}

		return false;
	}
}

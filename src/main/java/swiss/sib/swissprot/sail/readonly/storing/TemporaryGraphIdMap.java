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
package swiss.sib.swissprot.sail.readonly.storing;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.sail.readonly.WriteOnce;

public class TemporaryGraphIdMap {
	private static final Logger logger = LoggerFactory.getLogger(TemporaryGraphIdMap.class);
	private final Set<IRI> graphIriInOrder = Collections.synchronizedSet(new LinkedHashSet<>());
	private final Map<IRI, Integer> graphIriOrder = new ConcurrentHashMap<>();
	private final Map<Integer, IRI> iriOrderGraph = new ConcurrentHashMap<>();
	private final Lock lock = new ReentrantLock();

	public int tempGraphId(Resource graphIri) {
		if (graphIri instanceof IRI) {
			Integer got = graphIriOrder.get(graphIri);
			if (got == null) {
				try {
					lock.lock();
					if (!graphIriInOrder.contains(graphIri)) {
						graphIriInOrder.add((IRI) graphIri);
						int tempId = graphIriInOrder.size();
						graphIriOrder.put((IRI) graphIri, tempId);
						iriOrderGraph.put(tempId, (IRI) graphIri);
						return tempId;
					}
					got = graphIriOrder.get(graphIri);
				} finally {
					lock.unlock();
				}
			}
			return got;
		}
		logger.error("recieved graph that is not an IRI" + graphIri);
		WriteOnce.Failures.GRAPH_ID_NOT_IRI.exit();
		return (int) WriteOnce.NOT_FOUND;
	}

	public Collection<IRI> graphs() {
		return graphIriInOrder;
	}

	public IRI iriFromTempGraphId(int id) {
		return iriOrderGraph.get(Integer.valueOf(id));
	}

	public Integer parseInt(String s) {
		return Integer.parseUnsignedInt(s, 16);
	}

	public void toDisk(File rootDir) throws IOException {
		Files.writeString(extracted(rootDir).toPath(),
				graphIriInOrder.stream().map(IRI::stringValue).collect(Collectors.joining("\n")),
				StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
	}

	public static TemporaryGraphIdMap fromDisk(File rootDir) throws IOException {
		TemporaryGraphIdMap r = new TemporaryGraphIdMap();

		Path path = extracted(rootDir).toPath();
		if (Files.exists(path)) {
			try (Stream<String> lines = Files.lines(path)) {
				lines.map(s -> SimpleValueFactory.getInstance().createIRI(s)).forEach(r::tempGraphId);
			}
		}
		return r;

	}

	private static File extracted(File rootDir) {
		return new File(rootDir, "graphs");
	}
}

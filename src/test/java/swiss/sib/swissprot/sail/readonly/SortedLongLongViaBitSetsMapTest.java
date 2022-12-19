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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.function.ObjIntConsumer;
import java.util.function.ToLongFunction;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMap.KeyValue;
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongViaBitSetsMap;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaBitSetsIO;

public class SortedLongLongViaBitSetsMapTest {

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void testDirectRead() throws IOException {
		int repeat = 1024;
		int subjects = 52 * 1024;

		Roaring64Bitmap[] gbms = new Roaring64Bitmap[] { new Roaring64Bitmap(), new Roaring64Bitmap(),
				new Roaring64Bitmap(), new Roaring64Bitmap() };
		TempSortedFile in = writeInput(repeat, subjects, gbms.length);
		ObjIntConsumer<Long> forGraph = (l, g) -> {
			gbms[g].add(l);
		};

		File mapbs = temp.newFile();
		ToLongFunction<Value> undo = s -> Long.parseLong(s.stringValue().substring(19));
		boolean rewrite = SortedLongLongMapViaBitSetsIO.rewrite(in, mapbs, undo, undo, forGraph);
		assertTrue(rewrite);
		SortedLongLongViaBitSetsMap readin = SortedLongLongMapViaBitSetsIO.readin(mapbs);
		Iterator<KeyValue> iteratorForKey = testIterators(repeat, subjects, readin);
		assertFalse(iteratorForKey.hasNext());
		for (Roaring64Bitmap rb : gbms) {
			assertEquals(subjects / gbms.length, rb.getIntCardinality());
		}
	}

	private TempSortedFile writeInput(int repeat, int subjects, int graphs) throws IOException {
		File in = temp.newFile();
		TempSortedFile tsf = new TempSortedFile(in, Kind.IRI, Kind.IRI, null, null, WriteOnce.COMPRESSION);
		try (OutputStream fw = WriteOnce.COMPRESSION.compress(in); DataOutputStream ods = new DataOutputStream(fw)) {

			for (int s = 0; s < subjects; s++) {
				Resource subject = SimpleValueFactory.getInstance().createIRI("http://example.org/" + s);
				Resource object = SimpleValueFactory.getInstance().createIRI("http://example.org/" + s % repeat);
				int graphId = s % graphs;
				tsf.write(ods, subject, object, graphId);

			}
		}
		File t = temp.newFile();
		TempSortedFile tempSortedFile = new TempSortedFile(t, Kind.IRI, Kind.IRI, null, null, WriteOnce.COMPRESSION);
		tempSortedFile.from(in);
		return tempSortedFile;

	}

	private Iterator<KeyValue> testIterators(int repeat, int subjects, SortedLongLongViaBitSetsMap readin) {
		for (int s = 0; s < subjects; s++) {
			Iterator<KeyValue> iteratorForKey = readin.iteratorForKey(s);
			assertTrue(iteratorForKey.hasNext());
			KeyValue next = iteratorForKey.next();
			assertNotNull(next);
			assertEquals((long) s, next.key());
			assertEquals((long) s % repeat, next.value());
			assertFalse(iteratorForKey.hasNext());
		}

		Iterator<KeyValue> iteratorForKey = readin.subjectOrderedIterator();
		for (int s = 0; s < subjects; s++) {
			assertTrue(iteratorForKey.hasNext());
			KeyValue next = iteratorForKey.next();
			assertNotNull(next);
			assertEquals((long) s, next.key());
			assertEquals((long) s % repeat, next.value());
		}
		return iteratorForKey;
	}
}

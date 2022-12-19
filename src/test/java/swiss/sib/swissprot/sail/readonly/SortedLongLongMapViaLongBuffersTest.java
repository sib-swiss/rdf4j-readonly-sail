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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import swiss.sib.swissprot.sail.readonly.datastructures.SortedLongLongMapViaLongBuffers;
import swiss.sib.swissprot.sail.readonly.datastructures.io.SortedLongLongMapViaLongBuffersIO;
import swiss.sib.swissprot.sail.readonly.sorting.Comparators;

public class SortedLongLongMapViaLongBuffersTest {

	private static final String EX = "http://example.org/";
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

		List<String> subv = new ArrayList<>();
		for (int s = 0; s < subjects; s++) {
			subv.add(EX + Long.toString(s));
		}
		sortList(subv);
		int i = 0;
		Map<String, Integer> iriMap = new HashMap<>();
		for (String s : subv) {
			iriMap.put(s, i++);
		}
		File mapbs = temp.newFile();
		ToLongFunction<Value> undo = s -> iriMap.get(s.stringValue());
		SortedLongLongMapViaLongBuffersIO.rewrite(in, mapbs, undo, undo, forGraph);

		SortedLongLongMapViaLongBuffers readin = SortedLongLongMapViaLongBuffersIO.readin(mapbs);
		Iterator<KeyValue> iteratorForKey = testIterators(repeat, subjects, readin);
		assertFalse(iteratorForKey.hasNext());
		for (Roaring64Bitmap rb : gbms) {
			assertEquals(subjects / gbms.length, rb.getIntCardinality());
		}
	}

	private void sortList(List<String> subv) {
		Comparator<byte[]> forIRIBytes = Comparators.forIRIBytes();
		subv.sort(
				(a, b) -> forIRIBytes.compare(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8)));
	}

	private TempSortedFile writeInput(int repeat, int subjects, int graphs) throws IOException {
		File in = temp.newFile("unsorted");
		TempSortedFile tsf = new TempSortedFile(in, Kind.IRI, Kind.IRI, null, null, Compression.NONE);
		try (OutputStream fw = Compression.NONE.compress(in); DataOutputStream ods = new DataOutputStream(fw)) {

			for (int s = 0; s < subjects; s++) {
				Resource subject = SimpleValueFactory.getInstance().createIRI(EX + s);
				Resource object = SimpleValueFactory.getInstance().createIRI(EX + s % repeat);
				int graphId = s % graphs;
				tsf.write(ods, subject, object, graphId);

			}
		}
		File t = temp.newFile("sorted");
		TempSortedFile tempSortedFile = new TempSortedFile(t, Kind.IRI, Kind.IRI, null, null, Compression.NONE);
		tempSortedFile.from(in);
		return tempSortedFile;

	}

	private Iterator<KeyValue> testIterators(int repeat, int subjects, SortedLongLongMapViaLongBuffers readin) {

		List<String> subv = new ArrayList<>();
		for (int s = 0; s < subjects; s++) {
			subv.add(EX + Long.toString(s));
		}
		sortList(subv);
		Iterator<KeyValue> iterator = readin.subjectOrderedIterator();
		for (int r = 0; r < subjects; r++) {
			long s = Long.parseLong(subv.get(r).substring(EX.length()));
			assertTrue(iterator.hasNext());
			KeyValue next = iterator.next();
			assertNotNull(next);
			long keyV = Long.parseLong(subv.get((int) next.key()).substring(EX.length()));
			long valV = Long.parseLong(subv.get((int) next.value()).substring(EX.length()));
			assertEquals(s, keyV);
			assertEquals("" + s, (long) s % repeat, valV);
		}

		for (int r = 0; r < subjects; r++) {
			long s = Long.parseLong(subv.get(r).substring(EX.length()));

			Iterator<KeyValue> iteratorForKey = readin.iteratorForKey(r);
			assertTrue("" + s, iteratorForKey.hasNext());
			KeyValue next = iteratorForKey.next();
			assertNotNull(next);
			long keyV = Long.parseLong(subv.get((int) next.key()).substring(EX.length()));
			long valV = Long.parseLong(subv.get((int) next.value()).substring(EX.length()));
			assertEquals(s, keyV);
			assertEquals("" + s, (long) s % repeat, valV);
			assertFalse(iteratorForKey.hasNext());
		}
		return iterator;
	}
}

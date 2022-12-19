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
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.sail.readonly.TempSortedFile.SubjectObjectGraph;
import swiss.sib.swissprot.sail.readonly.WriteOnce.Kind;

public class TempSortedFileTest {
	private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		File f = temp.newFile();
		File sf = temp.newFile();
		TempSortedFile t = new TempSortedFile(f, Kind.IRI, Kind.IRI, null, null, Compression.NONE);
		try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(f))) {
			t.write(dos, RDF.ALT, RDF.BAG, 0);
			t.write(dos, RDF.ALT, RDF.NIL, 0);
			t.write(dos, RDF.ALT, RDF.BAG, 0);
		}

		assertTrue(f.length() > 30);
		try (DataInputStream dis = t.openSubjectObjectGraph()) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			int i = 0;
			while (subjectIterator.hasNext()) {
				assertEquals(RDF.ALT, subjectIterator.next());
				i++;
			}
			assertEquals(3, i);
		}
		TempSortedFile s = new TempSortedFile(sf, Kind.IRI, Kind.IRI, null, null, Compression.NONE);
		s.from(f);
		try (DataInputStream dis = new DataInputStream(new FileInputStream(sf))) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
		}
		try (DataInputStream dis = s.openObjects()) {
			Iterator<Value> objectIterator = s.objectIterator(dis);

			assertTrue(objectIterator.hasNext());
			assertEquals(RDF.BAG, objectIterator.next());
			assertTrue(objectIterator.hasNext());
			assertEquals(RDF.NIL, objectIterator.next());
			assertFalse(objectIterator.hasNext());
		}
	}

	@Test
	public void testNumbers() throws IOException {

		File f = temp.newFile();
		File sf = temp.newFile();

		TempSortedFile t = new TempSortedFile(f, Kind.IRI, Kind.LITERAL, XSD.INT, null, Compression.NONE);

		int noOfTriples = 20;
		try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(f))) {
			for (int i = 0; i < noOfTriples; i++) {
				t.write(dos, RDF.ALT, VF.createLiteral(i), 0);
			}
		}
		assertTrue(f.length() > 30);
		try (DataInputStream dis = t.openSubjectObjectGraph()) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			int i = 0;
			while (subjectIterator.hasNext()) {
				assertEquals(RDF.ALT, subjectIterator.next());
				i++;
			}
			assertEquals(noOfTriples, i);
		}
		TempSortedFile s = new TempSortedFile(sf, Kind.IRI, Kind.LITERAL, XSD.INT, null, Compression.NONE);
		s.from(f);
		try (DataInputStream dis = s.openSubjectObjectGraph()) {
			Iterator<SubjectObjectGraph> subjectIterator = t.iterator(dis);
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, s.subjectValue(subjectIterator.next().subject()));
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, s.subjectValue(subjectIterator.next().subject()));
		}

		try (DataInputStream dis = s.openSubjectObjectGraph()) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
		}
		try (DataInputStream dis = s.openObjects()) {
			Iterator<Value> objectIterator = s.objectIterator(dis);
			int i = 0;
			while (objectIterator.hasNext()) {
				assertEquals(VF.createLiteral(i), objectIterator.next());
				i++;
			}
			assertEquals(noOfTriples, i);
		}
	}

	@Test
	public void testLanguage() throws IOException {

		File f = temp.newFile();
		File sf = temp.newFile();
		TempSortedFile t = new TempSortedFile(f, Kind.IRI, Kind.LITERAL, null, "en_UK", Compression.NONE);
		Literal a = VF.createLiteral(String.valueOf("a"), "en_UK");
		Literal b = VF.createLiteral(String.valueOf("b"), "en_UK");
		try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(f))) {
			t.write(dos, RDF.ALT, a, 0);
			t.write(dos, RDF.ALT, b, 0);
		}

		assertTrue(f.length() > 30);
		try (DataInputStream dis = t.openSubjectObjectGraph()) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			int i = 0;
			while (subjectIterator.hasNext()) {
				assertEquals(RDF.ALT, subjectIterator.next());
				i++;
			}
			assertEquals(2, i);
		}
		TempSortedFile s = new TempSortedFile(sf, Kind.IRI, Kind.LITERAL, null, "en_UK", Compression.NONE);
		s.from(f);
		try (DataInputStream dis = new DataInputStream(new FileInputStream(sf))) {
			Iterator<Resource> subjectIterator = t.subjectIterator(dis);
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
			assertTrue(subjectIterator.hasNext());
			assertEquals(RDF.ALT, subjectIterator.next());
		}
		try (DataInputStream dis = s.openObjects()) {
			Iterator<Value> objectIterator = s.objectIterator(dis);

			assertTrue(objectIterator.hasNext());
			assertEquals(a, objectIterator.next());
			assertTrue(objectIterator.hasNext());
			assertEquals(b, objectIterator.next());
			assertFalse(objectIterator.hasNext());
		}
	}
}

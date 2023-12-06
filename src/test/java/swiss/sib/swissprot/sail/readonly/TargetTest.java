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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.sail.readonly.TempSortedFile.SubjectObjectGraph;
import swiss.sib.swissprot.sail.readonly.storing.TemporaryGraphIdMap;

public class TargetTest {
	private static final SimpleValueFactory VF = SimpleValueFactory.getInstance();
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		Statement template = SimpleValueFactory.getInstance().createStatement(RDF.ALT, RDF.TYPE, RDF.BAG, RDF.FIRST);
		Statement template2 = SimpleValueFactory.getInstance().createStatement(RDF.ALT, RDF.TYPE, RDF.BAG, RDF.NIL);
		ExecutorService exec = Executors.newSingleThreadExecutor();
		Target target = new Target(template, temp.newFolder(), new TemporaryGraphIdMap(), exec, 2, new Semaphore(2), Compression.LZ4);
		target.write(template);
		target.write(template2);
		target.close();
		assertTrue(target.getTripleFile().file().length() > 30);
	}

	@Test
	public void testNumbers() throws IOException {

		Statement template = VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral(0), RDF.FIRST);
		ExecutorService exec = Executors.newSingleThreadExecutor();
		TemporaryGraphIdMap tgid = new TemporaryGraphIdMap();
		Target target = new Target(template, temp.newFolder(), tgid, exec, 1, new Semaphore(1), Compression.LZ4);
		int openFilesBefore = OpenFileCount.openFileCount();
		int noOfTriples = 2;
		for (int i = 0; i < noOfTriples; i++) {
			template = VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral(i), RDF.FIRST);
			target.write(template);
		}
		target.close();
		int openFilesAfter = OpenFileCount.openFileCount();
		assertTrue(openFilesBefore >= openFilesAfter);
		List<Statement> readLines = new ArrayList<>();
		TempSortedFile ts = target.getTripleFile();
		int triplesReadIn = 0;
		try (DataInputStream dis = ts.openSubjectObjectGraph()) {
			Iterator<SubjectObjectGraph> iterator = ts.iterator(dis);
			while (iterator.hasNext()) {
				SubjectObjectGraph next = iterator.next();
				Resource subject = ts.subjectValue(next.subject());
				Value object = ts.objectValue(next.object());
				readLines.add(VF.createStatement(subject, RDF.TYPE, object, RDF.FIRST));
				triplesReadIn++;
				if (triplesReadIn > noOfTriples)
					fail("To many things to read");
			}
		}
		for (int i = 0; i < noOfTriples; i++) {
			assertEquals("at " + i, readLines.get(i),
					VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral(i), RDF.FIRST));
		}
		assertEquals(noOfTriples, readLines.size());
		assertTrue(ts.file().length() > 70);
	}

	@Test
	public void testLanguage() throws IOException {

		Statement template = VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral("0", "en_UK"), RDF.FIRST);
		ExecutorService exec = Executors.newSingleThreadExecutor();
		TemporaryGraphIdMap tgid = new TemporaryGraphIdMap();
		Target target = new Target(template, temp.newFolder(), tgid, exec, 1, new Semaphore(1), Compression.LZ4);
		int openFilesBefore = OpenFileCount.openFileCount();
//		operatingSystemMXBean.
		int noOfTriples = 2;
		for (int i = 0; i < noOfTriples; i++) {
			template = VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral(String.valueOf(i), "en_UK"), RDF.FIRST);
			target.write(template);
		}
		target.close();
		int openFilesAfter = OpenFileCount.openFileCount();
		assertTrue(openFilesBefore >= openFilesAfter);
		List<Statement> readLines = new ArrayList<>();
		TempSortedFile ts = target.getTripleFile();
		try (DataInputStream dis = ts.openSubjectObjectGraph()) {
			Iterator<SubjectObjectGraph> iterator = ts.iterator(dis);
//			String line = r.readLine();
			while (iterator.hasNext()) {
				SubjectObjectGraph next = iterator.next();
				Resource subject = ts.subjectValue(next.subject());
				Value object = ts.objectValue(next.object());
				readLines.add(VF.createStatement(subject, RDF.TYPE, object, RDF.FIRST));
			}
		}
//		List<String> readLines = Files.readLines(target.getTripleFile(), StandardCharsets.UTF_8);
		for (int i = 0; i < noOfTriples; i++) {
			assertEquals("at " + i, readLines.get(i),
					VF.createStatement(RDF.ALT, RDF.TYPE, VF.createLiteral(String.valueOf(i), "en_UK"), RDF.FIRST));
		}
		assertEquals(noOfTriples, readLines.size());
		assertTrue(ts.file().length() > 70);
	}
}

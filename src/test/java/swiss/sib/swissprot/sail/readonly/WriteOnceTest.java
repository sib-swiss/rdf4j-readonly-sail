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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import swiss.sib.swissprot.sail.readonly.datastructures.Triples;

public class WriteOnceTest {
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void simpleTest() throws IOException {
		File newFolder = temp.newFolder("db");
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		List<Statement> statements = List.of(vf.createStatement(RDF.BAG, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.ALT, RDF.TYPE, RDF.BAG), vf.createStatement(RDF.ALT, RDF.TYPE, RDF.ALT),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral(true)),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createLiteral("杭州市")),
				vf.createStatement(RDF.ALT, RDFS.LABEL, vf.createBNode("1")));
		Optional<RDFWriterFactory> optional = RDFWriterRegistry.getInstance().get(RDFFormat.RDFXML);
		File input = temp.newFile("input.rdf");
		if (optional.isEmpty())
			fail("Test config error");
		else {
			try (FileOutputStream out = new FileOutputStream(input)) {
				RDFWriter writer = optional.get().getWriter(out);
				writer.startRDF();
				for (Statement st : statements)
					writer.handleStatement(st);
				writer.endRDF();
			}
		}

		try (WriteOnce wo = new WriteOnce(newFolder)) {
			wo.parse(List.of(input.getAbsolutePath() + "\thttp://example.org/graph"));
			assertTrue(Files.isDirectory(newFolder.toPath()));
		}
		ReadOnlyStore readOnlyStore = new ReadOnlyStore(newFolder);
		try (CloseableIteration<? extends Statement, SailException> statements2 = readOnlyStore.getConnection()
				.getStatements(null, RDF.TYPE, null, false)) {
			// TODO account for literals
			assertEquals(3, statements2.stream().count());
		}
		SailRepository repo = new SailRepository(readOnlyStore);
		try (SailRepositoryConnection connection = repo.getConnection()) {
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, null)) {
				Iterator<Statement> iterator = statements.iterator();
				assertTrue(iterator.hasNext());
				Statement first = iterator.next();
				assertNotNull(first);
				assertTrue(iterator.hasNext());
				Statement second = iterator.next();
				assertNotNull(second);
				assertTrue(iterator.hasNext());
				Statement third = iterator.next();
				assertNotNull(third);

				assertTrue(iterator.hasNext());
				Statement fourth = iterator.next();
				assertNotNull(fourth);

				assertTrue(iterator.hasNext());
				Statement fifth = iterator.next();
				assertNotNull(fifth);

				assertTrue(iterator.hasNext());
				Statement sixth = iterator.next();
				assertNotNull(sixth);

				assertFalse(iterator.hasNext());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, null)) {
				// TODO account for literals
				assertEquals(3, statements2.stream().count());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, RDF.ALT)) {
				assertEquals(2, statements2.stream().count());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, RDF.BAG)) {
				assertEquals(1, statements2.stream().count());
			}

			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDFS.LABEL, null)) {
				assertEquals(3, statements2.stream().count());
			}
		}
	}

	@Test
	public void booleanTest() throws IOException {
		File newFolder = temp.newFolder("db");
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		List<Statement> statements = new ArrayList<>();
		for (int i = 0; i < 1_000; i++) {
			IRI subject = makeSubject(vf, i);
			statements.add(vf.createStatement(subject, RDFS.LABEL, vf.createLiteral(true)));
		}
		File input = writeTestInput(statements);
		try (WriteOnce wo = new WriteOnce(newFolder)) {
			wo.parse(List.of(input.getAbsolutePath() + "\thttp://example.org/graph"));
		}
		ReadOnlyStore readOnlyStore = new ReadOnlyStore(newFolder);

		List<Triples> triples = readOnlyStore.getTriples(RDFS.LABEL);
		assertEquals(1, triples.size());
		assertEquals(1_000, triples.get(0).size());
		Iterator<Statement> iterateStatements = triples.get(0).iterateStatements(null, null, null);
		for (int i = 0; i < 1_000; i++) {
			assertTrue(iterateStatements.hasNext());
			assertNotNull(iterateStatements.next());
		}
		for (int i = 0; i < 1_000; i++) {
			IRI subject = makeSubject(vf, i);
			iterateStatements = triples.get(0).iterateStatements(subject, null, null);
			assertTrue(iterateStatements.hasNext());
			Statement next = iterateStatements.next();
			assertNotNull(next);
			assertEquals(next.getSubject(), subject);
			assertFalse(iterateStatements.hasNext());
		}
	}

	@Test
	public void biggerTest() throws IOException {
		File newFolder = temp.newFolder("db");
		SimpleValueFactory vf = SimpleValueFactory.getInstance();

		List<Statement> statements = new ArrayList<>();
		for (int i = 0; i < 1_000; i++) {
			IRI subject = makeSubject(vf, i);
			statements.add(vf.createStatement(subject, RDF.TYPE, RDF.BAG));
			statements.add(vf.createStatement(subject, RDF.TYPE, RDF.ALT));
			statements.add(vf.createStatement(subject, RDFS.LABEL, vf.createLiteral(true)));
		}
		File input = writeTestInput(statements);

		try (WriteOnce wo = new WriteOnce(newFolder)) {
			wo.parse(List.of(input.getAbsolutePath() + "\thttp://example.org/graph"));
			assertTrue(Files.isDirectory(newFolder.toPath()));

			File iris = new File(newFolder, "iris");
			assertTrue(iris.exists());
			File pred0 = new File(newFolder, "pred_0");

			assertTrue(new File(newFolder, "graphs").exists());
			File pred0irisDir = new File(pred0, "iri");
			File[] pred0iris = new File[] { new File(pred0irisDir, "iri"), new File(pred0irisDir, "iri-compr"),
					new File(pred0irisDir, "iri-bitsets") };
			assertTrue(pred0iris[0].exists() || pred0iris[1].exists() | pred0iris[2].exists());
			for (File f : pred0iris) {
				if (f.exists()) {
					assertTrue(3 * Long.BYTES * 1_000 * 2 > f.length());
					assertTrue("File length is:" + f.length(), f.length() > 50);
				}
			}

			File booleans = new File(newFolder, "datatype_xsd_boolean");
			assertTrue(booleans.exists());
		}
		ReadOnlyStore readOnlyStore = new ReadOnlyStore(newFolder);
		List<Triples> triples = readOnlyStore.getTriples(RDF.TYPE);
		assertEquals(triples.size(), 1);
		Triples types = triples.get(0);

		IRI knownSubject = makeSubject(vf, 2);
		Iterator<Statement> iterateStatements = types.iterateStatements(knownSubject, null, null);
		assertTrue(iterateStatements.hasNext());
		assertNotNull(iterateStatements.next());
		assertTrue(iterateStatements.hasNext());
		assertNotNull(iterateStatements.next());
		assertFalse(iterateStatements.hasNext());

		List<String> sorted = IntStream.range(0, 1_000)
				.mapToObj(Integer::toString)
				.sorted()
				.collect(Collectors.toList());

		{
			Iterator<Statement> iterator = types.iterateStatements(null, RDF.ALT, null);
			for (int i = 0; i < 1_000; i++) {
				assertTrue(iterator.hasNext());
				Statement next = iterator.next();
				assertNotNull(next);
				assertEquals(RDF.ALT, next.getObject());
				assertEquals(makeSubject(vf, sorted.get(i)), next.getSubject());
			}
			assertFalse(iterator.hasNext());
		}

		SailRepository repo = new SailRepository(readOnlyStore);
		try (SailRepositoryConnection connection = repo.getConnection()) {
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, null, null)) {
				assertEquals(3_000, statements2.stream().count());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, null)) {
				assertEquals(2_000, statements2.stream().count());
			}

			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, RDF.ALT)) {
				Iterator<Statement> iterator = statements2.iterator();

				for (int i = 0; i < 1_000; i++) {
					assertTrue(iterator.hasNext());
					Statement next = iterator.next();
					assertNotNull(next);
					assertEquals(RDF.ALT, next.getObject());
					assertEquals(makeSubject(vf, sorted.get(i)), next.getSubject());
				}
				assertFalse(iterator.hasNext());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, RDF.TYPE, RDF.BAG)) {
				Iterator<Statement> iterator = statements2.iterator();
				for (int i = 0; i < 1_000; i++) {
					assertTrue(iterator.hasNext());
					Statement next = iterator.next();
					assertNotNull(next);
					assertEquals(RDF.BAG, next.getObject());
					assertEquals(makeSubject(vf, sorted.get(i)), next.getSubject());
				}
				assertFalse(iterator.hasNext());
			}
			try (RepositoryResult<Statement> statements2 = connection.getStatements(null, null, RDF.ALT)) {
				Iterator<Statement> iterator = statements2.iterator();
				for (int i = 0; i < 1_000; i++) {
					assertTrue(iterator.hasNext());
					Statement next = iterator.next();
					assertNotNull("err at:" + i, next);
					assertEquals(RDF.ALT, next.getObject());
					assertEquals(makeSubject(vf, sorted.get(i)), next.getSubject());
				}
				assertFalse(iterator.hasNext());
			}

			try (RepositoryResult<Statement> statements2 = connection.getStatements(knownSubject, null, null)) {
				Iterator<Statement> iterator = statements2.iterator();
				assertTrue(iterator.hasNext());
				Statement next = iterator.next();
				assertNotNull(next);
				assertEquals(RDFS.LABEL, next.getPredicate());
				assertTrue(next.getObject().isLiteral());
				assertEquals(knownSubject, next.getSubject());
				assertNotNull(next);
				assertTrue(iterator.hasNext());
				next = iterator.next();
				assertEquals(RDF.TYPE, next.getPredicate());
				assertEquals(RDF.ALT, next.getObject());
				assertEquals(knownSubject, next.getSubject());
				assertTrue(iterator.hasNext());
				next = iterator.next();
				assertNotNull(next);
				assertEquals(RDF.BAG, next.getObject());
				assertEquals(RDF.TYPE, next.getPredicate());
				assertEquals(knownSubject, next.getSubject());
				assertFalse(iterator.hasNext());
			}
			TupleQuery ptq = connection
					.prepareTupleQuery("SELECT (COUNT(?s) AS ?c) WHERE {?s a <" + RDF.NAMESPACE + "Alt>}");
			try (TupleQueryResult evaluate = ptq.evaluate()) {
				assertTrue(evaluate.hasNext());
				BindingSet next = evaluate.next();
				assertNotNull(next);
				Value value = next.getBinding("c").getValue();
				assertTrue(value.isLiteral());
				Literal literal = (Literal) value;
				assertEquals(1_000, literal.intValue());
				assertFalse(evaluate.hasNext());
			}

		}
	}

	private File writeTestInput(List<Statement> statements) throws IOException, FileNotFoundException {
		Optional<RDFWriterFactory> optional = RDFWriterRegistry.getInstance().get(RDFFormat.RDFXML);
		File input = temp.newFile("input.rdf");
		if (optional.isEmpty())
			fail("Test config error");
		else {
			RDFWriter writer = optional.get().getWriter(new FileOutputStream(input));
			writer.startRDF();
			for (Statement st : statements)
				writer.handleStatement(st);
			writer.endRDF();
		}
		return input;
	}

	IRI makeSubject(SimpleValueFactory vf, String i) {
		return vf.createIRI("http://example.org/iri/", i);
	}

	IRI makeSubject(SimpleValueFactory vf, int i) {
		return vf.createIRI("http://example.org/iri/", String.valueOf(i));
	}
}

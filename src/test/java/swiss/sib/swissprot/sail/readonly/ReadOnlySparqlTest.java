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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReadOnlySparqlTest {
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

		try (WriteOnce wo = new WriteOnce(newFolder, 0, Compression.LZ4)) {
			wo.parse(List.of(input.getAbsolutePath() + "\thttp://example.org/graph"));
			assertTrue(Files.isDirectory(newFolder.toPath()));
		}
		ReadOnlyStore readOnlyStore = new ReadOnlyStore(newFolder);

		SailRepository repo = new SailRepository(readOnlyStore);
		try (SailRepositoryConnection connection = repo.getConnection()) {
			TupleQuery ptq = connection
					.prepareTupleQuery("SELECT ?subject WHERE {?subject <" + RDFS.LABEL + "> true , \"杭州市\"}");
			try (TupleQueryResult evaluate = ptq.evaluate()) {
				expectAlt(evaluate);
				assertFalse(evaluate.hasNext());
			}

			ptq = connection.prepareTupleQuery(
					"SELECT ?subject WHERE {?subject <" + RDFS.LABEL + "> ?o . FILTER(isBlank(?o))}");
			try (TupleQueryResult evaluate = ptq.evaluate()) {
				expectAlt(evaluate);
				assertFalse(evaluate.hasNext());
			}
			ptq = connection.prepareTupleQuery(
					"SELECT ?subject WHERE {?subject <" + RDFS.LABEL + "> ?o . FILTER(isLiteral(?o))}");
			try (TupleQueryResult evaluate = ptq.evaluate()) {
				BindingSet next;
				Value value;
				expectAlt(evaluate);
				assertTrue(evaluate.hasNext());
				next = evaluate.next();
				assertNotNull(next);
				value = next.getBinding("subject").getValue();
				assertTrue(value.isIRI());
				assertEquals(RDF.ALT, value);
				assertFalse(evaluate.hasNext());
			}

			ptq = connection.prepareTupleQuery("SELECT ?subject WHERE {?subject <" + RDFS.LABEL + "> ?o , ?o2 .}");
			try (TupleQueryResult evaluate = ptq.evaluate()) {
				// Expect 9 results
				for (int i = 0; i < 9; i++) {
//					assertTrue(evaluate.hasNext());
					expectAlt(evaluate);
				}
//				expectAlt(evaluate);
//				assertTrue(evaluate.hasNext());
//				expectAlt(evaluate);
				assertFalse(evaluate.hasNext());
			}
		}
	}

	private void expectAlt(TupleQueryResult evaluate) {
		assertTrue(evaluate.hasNext());
		BindingSet next = evaluate.next();
		assertNotNull(next);
		Value value = next.getBinding("subject").getValue();
		assertTrue(value.isIRI());
		assertEquals(RDF.ALT, value);
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
			statements.add(vf.createStatement(subject, RDFS.LABEL, vf.createLiteral("hello", "en_uk")));
		}
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

		try (WriteOnce wo = new WriteOnce(newFolder, 0, Compression.LZ4)) {
			wo.parse(List.of(input.getAbsolutePath() + "\thttp://example.org/graph"));
			assertTrue(Files.isDirectory(newFolder.toPath()));
			File booleans = new File(newFolder, "datatype_xsd_boolean");
			assertTrue(booleans.exists());
		}
		ReadOnlyStore readOnlyStore = new ReadOnlyStore(newFolder);

		SailRepository repo = new SailRepository(readOnlyStore);
		try (SailRepositoryConnection connection = repo.getConnection()) {

			TupleQuery ptq = connection.prepareTupleQuery("SELECT (COUNT(?s) AS ?c) WHERE {?s a <" + RDF.ALT + ">}");
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
		try (SailRepositoryConnection connection = repo.getConnection()) {

			TupleQuery ptq = connection.prepareTupleQuery("SELECT (COUNT(?s) AS ?c) WHERE {?s a <" + RDF.BAG + ">}");
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
		try (SailRepositoryConnection connection = repo.getConnection()) {

			TupleQuery ptq = connection
					.prepareTupleQuery("SELECT (COUNT(?s) AS ?c) WHERE {?s a <" + RDF.ALT + ">, <" + RDF.BAG + ">}");
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

	IRI makeSubject(SimpleValueFactory vf, String i) {
		return vf.createIRI("http://example.org/iri/", i);
	}

	IRI makeSubject(SimpleValueFactory vf, int i) {
		return vf.createIRI("http://example.org/iri/", String.valueOf(i));
	}
}

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XSD;

import swiss.sib.swissprot.sail.readonly.datastructures.list.FitsInLongSortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedList;
import swiss.sib.swissprot.sail.readonly.datastructures.list.SortedListInSections;
import swiss.sib.swissprot.sail.readonly.datastructures.list.FitsInLongSortedList.FitingDatatypes;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyCoreLiteral;
import swiss.sib.swissprot.sail.readonly.values.ReadOnlyLiteral;

public class ReadOnlyLiteralStore {
	public static final String LANG = "langString_";
	public static final String DATATYPE_FN_XSD_PART = "datatype_xsd_";
	public static final String DATATYPE_FN_PART = "datatype_";

	private final Map<String, SortedList<Value>> langStrings = new HashMap<>();
	private final Map<IRI, SortedList<Value>> datatypeStrings = new HashMap<>();

	public ReadOnlyLiteralStore(File rootDir) throws FileNotFoundException, IOException {
		this(allFiles(rootDir), allLangFiles(rootDir));
	}

	private static Map<IRI, SortedList<Value>> allFiles(File rootDir) throws FileNotFoundException, IOException {
		File[] findDatatTypeFiles = ReadOnlyLiteralStore.findDataTypeFiles(rootDir);
		Map<IRI, SortedList<Value>> dts = new HashMap<>();
		for (File dt : findDatatTypeFiles) {
			Optional<IRI> dataTypeInFile = ReadOnlyLiteralStore.dataTypeInFile(dt);
			if (dataTypeInFile.isPresent()) {

				IRI datatype = dataTypeInFile.get();
				FitingDatatypes forDatatype = FitsInLongSortedList.FitingDatatypes.forDatatype(datatype);
				SortedList<Value> readinStrings;
				if (forDatatype == null) {
					readinStrings = SortedListInSections.readinValues(dt, datatype);
				} else {
					readinStrings = FitsInLongSortedList.readInValues(dt, forDatatype);
				}
				assert readinStrings != null;
				dts.put(datatype, readinStrings);
			}
		}

		return dts;
	}

	private static Map<String, SortedList<Value>> allLangFiles(File rootDir)
			throws FileNotFoundException, IOException {
		File[] findLangFiles = ReadOnlyLiteralStore.findLangFiles(rootDir);
		Map<String, SortedList<Value>> lts = new HashMap<>();
		for (File dt : findLangFiles) {
			Optional<String> langInFile = ReadOnlyLiteralStore.langInFile(dt);
			if (langInFile.isPresent()) {
				SortedList<Value> readinStrings = SortedListInSections.readinValues(dt, langInFile.get());
				assert readinStrings != null;
				lts.put(langInFile.get(), readinStrings);
			}
		}
		return lts;
	}

	public ReadOnlyLiteralStore(Map<IRI, SortedList<Value>> dts, Map<String, SortedList<Value>> lts) {
		this.datatypeStrings.putAll(dts);
		this.langStrings.putAll(lts);
	}

	public static File[] findDataTypeFiles(File directoryToWriteToo) {
		return directoryToWriteToo.listFiles(f -> isLiteralFile(f.getName()));
	}

	public static File[] findLangFiles(File directoryToWriteToo) {
		return directoryToWriteToo.listFiles(f -> isLangFile(f.getName()));
	}

	public static Optional<IRI> dataTypeInFile(File dataFile) {
		String fileName = dataFile.getName();
		if (!fileName.startsWith(DATATYPE_FN_PART)) {
			return Optional.empty();
		} else if (fileName.startsWith(DATATYPE_FN_XSD_PART)) {
			String ds = fileName.substring(DATATYPE_FN_XSD_PART.length());
			ds = removeSpecialCoding(ds);
			Optional<IRI> dt = Optional.of(SimpleValueFactory.getInstance().createIRI(XSD.NAMESPACE, ds));
			return dt;
		} else {
			return fileNameToDatatypeIri(fileName);
		}
	}

	public static Optional<IRI> fileNameToDatatypeIri(String fileName) {
		String substring = fileName.substring(fileName.indexOf('_') + 1);
		byte[] decode = Base64.getDecoder().decode(substring);
		String decoded = new String(decode, StandardCharsets.UTF_8);
		decoded = removeSpecialCoding(decoded);
		return Optional.of(SimpleValueFactory.getInstance().createIRI(decoded));
	}

	public static Optional<String> langInFile(File dataFile) {
		String fileName = dataFile.getName();
		if (!fileName.startsWith(LANG)) {
			return Optional.empty();
		} else {
			String substring = fileName.substring(fileName.indexOf('_') + 1);
			return Optional.of(substring);
		}
	}

	private static String removeSpecialCoding(String ds) {
		int indexOf = ds.lastIndexOf('-');
		if (indexOf > 0) {
			ds = ds.substring(0, indexOf);
		}
		return ds;
	}

	public static String fileNameForLiteral(IRI datatype, String lang) {
		if (lang == null) {
			return fileNameForDataType(datatype);
		} else {
			return LANG + lang;
		}
	}

	private static String fileNameForDataType(IRI datatype) {
		if (datatype.getNamespace().equals(XSD.NAMESPACE))
			return DATATYPE_FN_XSD_PART + datatype.getLocalName();
		else
			return DATATYPE_FN_PART
					+ Base64.getEncoder().encodeToString(datatype.stringValue().getBytes(StandardCharsets.UTF_8));
	}

	public SortedList<Value> getSortedListFor(IRI datatype) {
		return datatypeStrings.get(datatype);
	}

	public SortedList<Value> getSortedListFor(String lang) {
		return langStrings.get(lang);
	}

	public SortedList<Value> getSortedListForStrings() {
		return datatypeStrings.get(XSD.STRING);
	}

	public static boolean isLangFile(String name) {
		return name.startsWith(LANG);
	}

	public static boolean isLiteralFile(String name) {
		return name.startsWith(DATATYPE_FN_PART);
	}

	public LongFunction<Value> getLongToValue(IRI dt) {
		SortedList<Value> sortedList = datatypeStrings.get(dt);
		return l -> new ReadOnlyLiteral(l, sortedList, dt);
	}
	
	public LongFunction<Value> getLongToValue(CoreDatatype dt) {
		SortedList<Value> sortedList = datatypeStrings.get(dt.getIri());
		return l -> new ReadOnlyCoreLiteral(l, sortedList, dt);
	}
}

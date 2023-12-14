package swiss.sib.swissprot.sail.readonly.values;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.eclipse.rdf4j.model.IRI;

import swiss.sib.swissprot.sail.readonly.datastructures.list.IRISection;

public record ByteArrayBackedIRI(byte[] backing) implements IRI {

	@Override
	public String stringValue() {
		return new String(backing, StandardCharsets.UTF_8);
	}

	@Override
	public String getNamespace() {
		return new String(backing, 0, IRISection.getLocalNameIndex(backing), StandardCharsets.UTF_8);
	}

	@Override
	public String getLocalName() {
		return new String(backing, IRISection.getLocalNameIndex(backing), backing.length, StandardCharsets.UTF_8);
	}

	@Override
	public String toString() {
		return stringValue();
	}

	@Override
	public int hashCode() {
		return stringValue().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj instanceof ByteArrayBackedIRI other)
			return Arrays.equals(backing, other.backing);
		else if (obj instanceof IRI other)
			return stringValue().equals(other.stringValue());
		else {
			return false;
		}
	}
}
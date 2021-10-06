/*
========================================================================
SchemaCrawler
http://www.schemacrawler.com
Copyright (c) 2000-2016, Sualeh Fatehi <sualeh@hotmail.com>.
All rights reserved.
------------------------------------------------------------------------

SchemaCrawler is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

SchemaCrawler and the accompanying materials are made available under
the terms of the Eclipse Public License v1.0, GNU General Public License
v3 or GNU Lesser General Public License v3.

You may elect to redistribute this code under any of these licenses.

The Eclipse Public License is available at:
http://www.eclipse.org/legal/epl-v10.html

The GNU General Public License v3 and the GNU Lesser General Public
License v3 are available at:
http://www.gnu.org/licenses/

========================================================================
*/
package schemacrawler.tools.text.utility;

import java.io.PrintWriter;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.org.apache.xml.internal.utils.XMLChar;

import schemacrawler.tools.options.TextOutputFormat;

public class MetadataXMLFormattingHelper extends PlainTextFormattingHelper {

	XMLOutputFactory factory;
	XMLStreamWriter writer;
	@SuppressWarnings("unused")
	private int count;

	public MetadataXMLFormattingHelper(final PrintWriter out, final TextOutputFormat outputFormat) {
		super(out, outputFormat);
		count = 0;
		factory = XMLOutputFactory.newInstance();
		try {
			writer = factory.createXMLStreamWriter(out);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeDocumentStart() {
		try {
			writer.writeStartDocument("UTF-8", "1.0");
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeDocumentEnd() {
		try {
			writer.writeEndDocument();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeRootElementStart(String tablename) {
		try {
			writer.writeCharacters("\n");
			writer.writeStartElement(getTextFormatted(tablename));
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeRootElementEnd() {
		try {
			writer.writeCharacters("\n");
			writer.writeEndElement();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeRecordStart(String tablename, String suffix) {
		try {
			writer.writeCharacters("\n\t");
			writer.writeStartElement(getTextFormatted(tablename + suffix));
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeRecordEnd() {
		try {
			writer.writeCharacters("\n\t");
			writer.writeEndElement();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeElementStart(String columnName) {
		try {
			writer.writeCharacters("\n\t\t");
			writer.writeStartElement(getTextFormatted(columnName));
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeElementEnd() {
		try {
			writer.writeEndElement();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeValue(String value) {
		try {
			writer.writeCharacters(value.trim());
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void newlineWriter() {
		try {
			writer.writeCharacters("\n");
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void flushOutput() {
		try {
			writer.flush();
			writer.close();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public void writeAttribute(String name, String value) {
		try {
			writer.writeAttribute(getTextFormatted(name), value);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	private String stripNonValidXMLCharacters(String in) {
		if (in == null || ("".equals(in)))
			return null;
		StringBuffer out = new StringBuffer();
		for (int i = 0; i < in.length(); i++) {
			char c = in.charAt(i);
			if (XMLChar.isValid(c))
				out.append(c);
			else
				out.append("");
		}
		return (out.toString().trim());
	}

	public String getXMLFromDocument(Document document) throws TransformerException {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StreamResult streamResult = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(document);
		transformer.transform(source, streamResult);
		return streamResult.getWriter().toString();
	}

	public void writeCData(String value) {
		try {
			if (value == null)
				writeValue("");
			else if (value.equals(""))
				writer.writeCData("");
			else {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				Document document = builder.newDocument();
				Element rootElement = (Element) document.createElement("_");
				document.appendChild(rootElement);
				rootElement.appendChild(document.createCDATASection(stripNonValidXMLCharacters(value)));
				String xmlString = getXMLFromDocument(document);
				writer.writeCData(xmlString.substring(0, xmlString.length() - 7).substring(12));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

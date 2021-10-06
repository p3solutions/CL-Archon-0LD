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
import java.util.TreeMap;
import java.util.TreeSet;

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

public class XMLFormattingHelper extends PlainTextFormattingHelper {

	XMLOutputFactory factory;
	XMLStreamWriter writer;
	PrintWriter writerConsolidate;
	@SuppressWarnings("unused")
	private int count;
	private static TreeMap<Integer, TreeMap<String, TreeSet<Integer>>> arMap;
	private static TreeMap<String, String> charReplace = null;
	private String writefilename = null;
	TextOutputFormat outputFormatRef;

	public static TreeMap<Integer, TreeMap<String, TreeSet<Integer>>> getArMap() {
		return arMap;
	}

	public static void setArMap(TreeMap<Integer, TreeMap<String, TreeSet<Integer>>> arMap) {
		XMLFormattingHelper.arMap = arMap;
	}

	public XMLFormattingHelper(final PrintWriter out, final TextOutputFormat outputFormat) {
		super(out, outputFormat);
		outputFormatRef = outputFormat;
		count = 0;
		factory = XMLOutputFactory.newInstance();
		try {
			writer = factory.createXMLStreamWriter(out);
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	public XMLFormattingHelper(final PrintWriter out, final TextOutputFormat outputFormat, String filename) {
		super(out, outputFormat);
		outputFormatRef = outputFormat;
		setWritefilename(filename);
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
			writer.writeStartElement(getTextFormatted(tablename.toUpperCase()));
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
			writer.writeStartElement(getTextFormatted((tablename + suffix).toUpperCase()));
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
			writer.writeStartElement(getTextFormatted(columnName.toUpperCase()));
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
			writer.writeCharacters(stripNonValidXMLCharacters(value));
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

	public String getXMLFromDocument(Document document) throws TransformerException {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StreamResult streamResult = new StreamResult(new StringWriter());
		DOMSource source = new DOMSource(document);
		transformer.transform(source, streamResult);
		return streamResult.getWriter().toString();
	}

	// public static void main(String[] args) {
	// XMLFormattingHelper cDataTest = new XMLFormattingHelper(null, null);
	// cDataTest.writeCData("<T1><![CDATA[AA]]></T1>");
	// }

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
				// System.out.println(xmlString.substring(0, xmlString.length()
				// -
				// 7).substring(12));
				writer.writeCData(xmlString.substring(0, xmlString.length() - 7).substring(12));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private int rid;
	private String rtable;

	public void setInfo(int id, String table) {
		rid = id;
		rtable = table;
	}

	private String stripNonValidXMLCharacters(String in) {
		if (in == null || ("".equals(in)))
			return "";
		StringBuffer out = new StringBuffer();
		for (int i = 0; i < in.length(); i++) {
			char c = in.charAt(i);
			if (XMLChar.isValid(c))
				out.append(c);
			else
				out.append(checkReplace(in.codePointAt(i), outputFormatRef != TextOutputFormat.arxml));
		}
		return (out.toString().trim());
	}

	private String checkReplace(int key, boolean extract) {
		if (getCharReplace() != null && getCharReplace().containsKey(key + ""))
			return getCharReplace().get(key + "");
		if (extract)
			return "";
		else
			return (":::::" + getSymbolCode(key) + ":::::");
	}

	private int getSymbolCode(int codePoint) {
		if (arMap == null)
			arMap = new TreeMap<Integer, TreeMap<String, TreeSet<Integer>>>();
		TreeMap<String, TreeSet<Integer>> files = arMap.get(codePoint);
		if (files == null)
			files = new TreeMap<String, TreeSet<Integer>>();

		String identifier = rtable; // writefilename
		TreeSet<Integer> ids = files.get(identifier);
		if (ids == null)
			ids = new TreeSet<Integer>();
		ids.add(rid);
		files.put(identifier, ids);
		arMap.put(codePoint, files);
		return codePoint;
	}

	public static TreeMap<String, String> getCharReplace() {
		return charReplace;
	}

	public static void setCharReplace(TreeMap<String, String> charReplace) {
		XMLFormattingHelper.charReplace = charReplace;
	}

	public String getWritefilename() {
		return writefilename;
	}

	public void setWritefilename(String writefilename) {
		this.writefilename = writefilename;
	}
}

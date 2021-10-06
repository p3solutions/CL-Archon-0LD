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

//import static sf.util.Utility.isBlank;

import java.io.PrintWriter;

import schemacrawler.tools.options.TextOutputFormat;
import sf.util.Color;

/**
 * Methods to format entire rows of output as text.
 *
 * @author Sualeh Fatehi
 */
public class PlainTextFormattingHelper extends BaseTextFormattingHelper {

	public PlainTextFormattingHelper(final PrintWriter out, final TextOutputFormat outputFormat) {
		super(out, outputFormat);
	}

	@Override
	public String createLeftArrow() {
		return "<--";
	}

	@Override
	public String createRightArrow() {
		return "-->";
	}

	@Override
	public String createWeakLeftArrow() {
		return "<~~";
	}

	@Override
	public String createWeakRightArrow() {
		return "~~>";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeDocumentEnd() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeDocumentStart() {
	}

	@Override
	public void writeHeader(final DocumentHeaderType type, final String header) {
		// if (!isBlank(header)) {
		// final String defaultSeparator = separator("=");
		//
		// final String prefix;
		// final String separator;
		// if (type == null) {
		// prefix = System.lineSeparator();
		// separator = defaultSeparator;
		// } else {
		// switch (type) {
		// case title:
		// prefix = System.lineSeparator();
		// separator = separator("_");
		// break;
		// case subTitle:
		// prefix = System.lineSeparator();
		// separator = defaultSeparator;
		// break;
		// case section:
		// prefix = "";
		// separator = separator("-=-");
		// break;
		// default:
		// prefix = System.lineSeparator();
		// separator = defaultSeparator;
		// break;
		// }
		// }
		// out.println(System.lineSeparator() + prefix + header + System.lineSeparator()
		// + separator + prefix);
		// }
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeObjectEnd() {
		out.println();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeObjectNameRow(final String id, final String name, final String description,
			final Color backgroundColor) {
		// writeNameRow(name, description);
		// out.println(DASHED_SEPARATOR);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeObjectStart() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeValue(String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeElementEnd() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeElementStart(String elm) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRecordStart(String recordElm, String suffix) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRecordEnd() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRootElementEnd() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRootElementStart(String rootElm) {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

	@Override
	public void newlineWriter() {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeAttribute(String name, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeCData(String object) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setInfo(int id, String tablename) {
		// TODO Auto-generated method stub

	}

}

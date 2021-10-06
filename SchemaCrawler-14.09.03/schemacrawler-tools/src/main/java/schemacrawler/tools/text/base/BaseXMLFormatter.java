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

package schemacrawler.tools.text.base;

import java.util.logging.Logger;

import schemacrawler.schema.CrawlInfo;
import schemacrawler.schema.DatabaseInfo;
import schemacrawler.schema.JdbcDriverInfo;
import schemacrawler.schema.SchemaCrawlerInfo;
import schemacrawler.schemacrawler.SchemaCrawlerException;
import schemacrawler.tools.options.OutputOptions;

/**
 * Text formatting of schema.
 *
 * @author Sualeh Fatehi
 */
public abstract class BaseXMLFormatter<O extends BaseTextOptions> extends BaseFormatter<O> {

	protected static final Logger LOGGER = Logger.getLogger(BaseXMLFormatter.class.getName());

	protected BaseXMLFormatter(final O options, final boolean printVerboseDatabaseInfo,
			final OutputOptions outputOptions) throws SchemaCrawlerException {
		super(options, printVerboseDatabaseInfo, outputOptions);
	}

	@Override
	public void begin() throws SchemaCrawlerException {
	}

	@Override
	public void end() throws SchemaCrawlerException {
		formattingHelper.flush();
		super.end();
	}

	@Override
	public void handle(final CrawlInfo crawlInfo) throws SchemaCrawlerException {
	}

	@Override
	public void handle(final DatabaseInfo dbInfo) {
	}

	@Override
	public void handle(final JdbcDriverInfo driverInfo) {
	}

	@Override
	public void handle(final SchemaCrawlerInfo schemaCrawlerInfo) {
	}

	@Override
	public void handleHeaderEnd() throws SchemaCrawlerException {
	}

	@Override
	public void handleHeaderStart() throws SchemaCrawlerException {
	}

	@Override
	public void handleInfoEnd() throws SchemaCrawlerException {
	}

	@Override
	public void handleInfoStart() throws SchemaCrawlerException {
	}

	public static String getTextFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string.length() > 0 ? ((string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" : "") + string
				: string;
	}
}

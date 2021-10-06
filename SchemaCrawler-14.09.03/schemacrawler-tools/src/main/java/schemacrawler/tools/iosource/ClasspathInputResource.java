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
package schemacrawler.tools.iosource;


import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClasspathInputResource
  implements InputResource
{

  private static final Logger LOGGER = Logger
    .getLogger(ClasspathInputResource.class.getName());

  private final String classpathResource;

  public ClasspathInputResource(final String classpathResource)
    throws IOException
  {
    this.classpathResource = requireNonNull(classpathResource,
                                            "No classpath resource provided");
    if (ClasspathInputResource.class
      .getResource(this.classpathResource) == null)
    {
      throw new IOException("Cannot read classpath resource, "
                            + this.classpathResource);
    }
  }

  @Override
  public Reader openNewInputReader(final Charset charset)
    throws IOException
  {
    requireNonNull(charset, "No input charset provided");
    final InputStream inputStream = ClasspathInputResource.class
      .getResourceAsStream(classpathResource);
    final Reader reader = new BufferedReader(new InputStreamReader(inputStream,
                                                                   charset));
    LOGGER
      .log(Level.INFO,
           "Opened input reader to classpath resource, " + classpathResource);

    return new InputReader(getDescription(), reader, true);
  }

  @Override
  public String toString()
  {
    return getDescription();
  }

  private String getDescription()
  {
    return InputReader.class.getResource(classpathResource).toExternalForm();
  }

}

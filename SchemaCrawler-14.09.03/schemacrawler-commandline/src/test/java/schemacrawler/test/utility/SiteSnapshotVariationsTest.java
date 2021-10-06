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
package schemacrawler.test.utility;


import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newBufferedWriter;
import static schemacrawler.test.utility.TestUtility.flattenCommandlineArgs;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import schemacrawler.Main;
import schemacrawler.schemacrawler.Config;
import schemacrawler.tools.integration.graph.GraphOutputFormat;
import schemacrawler.tools.options.OutputFormat;
import schemacrawler.tools.options.TextOutputFormat;

public class SiteSnapshotVariationsTest
  extends BaseDatabaseTest
{

  private static Path directory;

  @BeforeClass
  public static void setupDirectory()
    throws IOException, URISyntaxException
  {
    final Path codePath = Paths.get(SiteSnapshotVariationsTest.class
      .getProtectionDomain().getCodeSource().getLocation().toURI()).normalize()
      .toAbsolutePath();
    directory = codePath
      .resolve("../../../schemacrawler-site/src/site/resources/snapshot-examples")
      .normalize().toAbsolutePath();
  }

  @Rule
  public TestRule rule = new CompleteBuildRule();

  @Test
  public void snapshots()
    throws Exception
  {
    for (final OutputFormat outputFormat: new OutputFormat[] {
                                                               TextOutputFormat.csv,
                                                               TextOutputFormat.html,
                                                               TextOutputFormat.json,
                                                               TextOutputFormat.text,
                                                               GraphOutputFormat.htmlx })
    {
      final String outputFormatValue = outputFormat.getFormat();
      final String extension;
      if ("htmlx".equals(outputFormatValue))
      {
        extension = "svg.html";
      }
      else
      {
        extension = outputFormatValue;
      }
      final Map<String, String> args = new HashMap<String, String>();
      args.put("infolevel", "maximum");
      args.put("outputformat", outputFormatValue);

      final Map<String, String> config = new HashMap<>();

      run(args, config, directory.resolve("snapshot." + extension));
    }
  }

  private Path createConfig(final Map<String, String> config)
    throws IOException
  {
    final String prefix = SiteSnapshotVariationsTest.class.getName();
    final Path configFile = createTempFile(prefix, "properties");
    final Properties configProperties = new Properties();
    configProperties.putAll(config);
    configProperties
      .store(newBufferedWriter(configFile, StandardCharsets.UTF_8), prefix);
    return configFile;
  }

  private void run(final Map<String, String> argsMap,
                   final Map<String, String> config,
                   final Path outputFile)
    throws Exception
  {
    deleteIfExists(outputFile);

    argsMap.put("url", "jdbc:hsqldb:hsql://localhost/schemacrawler");
    argsMap.put("user", "sa");
    argsMap.put("password", "");
    argsMap.put("title", "Details of Example Database");
    argsMap.put("command", "details,count,dump");
    argsMap.put("outputfile", outputFile.toString());

    final Config runConfig = new Config();
    final Config informationSchema = Config
      .loadResource("/hsqldb.INFORMATION_SCHEMA.config.properties");
    runConfig.putAll(informationSchema);
    if (config != null)
    {
      runConfig.putAll(config);
    }

    final Path configFile = createConfig(runConfig);
    argsMap.put("g", configFile.toString());

    Main.main(flattenCommandlineArgs(argsMap));
    System.out.println(outputFile.toString());
  }

}

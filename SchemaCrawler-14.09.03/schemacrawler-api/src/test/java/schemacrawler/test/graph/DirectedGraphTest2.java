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
package schemacrawler.test.graph;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;

import schemacrawler.test.utility.TestName;
import sf.util.graph.DirectedGraph;

public class DirectedGraphTest2
  extends GraphTestBase
{

  @Rule
  public TestName testName = new TestName();

  @Test
  public void dbcycles1()
    throws Exception
  {
    final DirectedGraph<String> graph = new DirectedGraph<String>(testName
      .currentMethodFullName())
    {
      {
        addEdge("ACTIVITIES", "PRODUCTION_VARIANTS");
        addEdge("PRODUCTION_VARIANTS", "ACTIVITIES");
        addEdge("PRODUCTION_VARIANTS", "TARGETGROUPS");
        addEdge("TARGETGROUPS", "ORDERS");
        addEdge("TARGETGROUPS", "ORDER_LAYOUTS");
        addEdge("ORDERS", "ACTIVITIES");
        addEdge("ORDERS", "AD_CARRIERS");
        addEdge("ORDERS", "AD_SPACES");
        addEdge("ORDERS", "PRODUCTION_VARIANTS");
        addEdge("AD_CARRIERS", "ACTIVITIES");
        addEdge("AD_SPACES", "AD_CARRIERS");
        addEdge("ORDER_LAYOUTS", "ORDERS");
      }
    };

    assertTrue(containsCycleSimple(graph));
    assertTrue(containsCycleTarjan(graph));

  }

  @Test
  public void dbcycles2()
    throws Exception
  {
    final DirectedGraph<String> graph = new DirectedGraph<String>(testName
      .currentMethodFullName())
    {
      {
        addEdge("ORDERS", "ACTIVITIES");
        addEdge("ORDERS", "AD_CARRIERS");
        addEdge("ORDERS", "AD_SPACES");
        addEdge("ORDERS", "PRODUCTION_VARIANTS");
        addEdge("AD_CARRIERS", "ACTIVITIES");
        addEdge("AD_SPACES", "AD_CARRIERS");
        addEdge("ORDER_LAYOUTS", "ORDERS");
      }
    };

    assertFalse(containsCycleSimple(graph));
    assertFalse(containsCycleTarjan(graph));

  }

  @Test
  public void dbcycles3()
    throws Exception
  {
    final DirectedGraph<String> graph = new DirectedGraph<String>(testName
      .currentMethodFullName())
    {
      {
        addEdge("TARGETGROUPS", "ORDERS");
        addEdge("TARGETGROUPS", "ORDER_LAYOUTS");
        addEdge("ORDERS", "ACTIVITIES");
        addEdge("ORDERS", "AD_CARRIERS");
        addEdge("ORDERS", "AD_SPACES");
        addEdge("ORDERS", "PRODUCTION_VARIANTS");
        addEdge("AD_CARRIERS", "ACTIVITIES");
        addEdge("AD_SPACES", "AD_CARRIERS");
        addEdge("ORDER_LAYOUTS", "ORDERS");
      }
    };

    assertFalse(containsCycleSimple(graph));
    assertFalse(containsCycleTarjan(graph));

  }

}

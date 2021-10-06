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
package sf.util.graph;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DirectedGraph<T extends Comparable<? super T>>
{

  private final String name;
  private final Map<T, Vertex<T>> verticesMap;
  private final Set<DirectedEdge<T>> edges;

  public DirectedGraph(final String name)
  {
    this.name = name;
    verticesMap = new HashMap<>();
    edges = new HashSet<>();
  }

  /**
   * Adds vertices, and a directed edge between them. Simple directed
   * graphs do not allow self-loops.
   * https://en.wikipedia.org/wiki/Loop_(graph_theory)
   *
   * @param from
   *        Vertex value at the start of the edge
   * @param to
   *        Vertex value at the end of the edge
   */
  public void addEdge(final T from, final T to)
  {
    if (!from.equals(to))
    {
      edges.add(new DirectedEdge<T>(addVertex(from), addVertex(to)));
    }
  }

  /**
   * Adds a vertex.
   *
   * @param value
   *        Vertex value
   * @return The newly added vertex
   */
  public Vertex<T> addVertex(final T value)
  {
    final Vertex<T> vertex;
    if (verticesMap.containsKey(value))
    {
      vertex = verticesMap.get(value);
    }
    else
    {
      vertex = new Vertex<T>(value);
      verticesMap.put(value, vertex);
    }
    return vertex;
  }

  public Set<DirectedEdge<T>> edgeSet()
  {
    return new HashSet<>(edges);
  }

  public Set<DirectedEdge<T>> getIncomingEdges(final Vertex<T> vertexTo)
  {
    Objects.requireNonNull(vertexTo);

    final Set<DirectedEdge<T>> incomingEdges = new HashSet<>();
    for (final DirectedEdge<T> edge: edges)
    {
      if (edge.getTo().equals(vertexTo))
      {
        incomingEdges.add(edge);
      }
    }
    return incomingEdges;
  }

  /**
   * @return the name
   */
  public String getName()
  {
    return name;
  }

  public Set<DirectedEdge<T>> getOutgoingEdges(final Vertex<T> vertexFrom)
  {
    Objects.requireNonNull(vertexFrom);

    final Set<DirectedEdge<T>> outgoingEdges = new HashSet<>();
    for (final DirectedEdge<T> edge: edges)
    {
      if (edge.getFrom().equals(vertexFrom))
      {
        outgoingEdges.add(edge);
      }
    }
    return outgoingEdges;
  }

  @Override
  public String toString()
  {
    final StringBuilder writer = new StringBuilder(4096);
    writer.append("digraph {\n");
    if (name != null && !name.isEmpty())
    {
      writer.append(String.format("  [label=\"%s\"]\n", name));
    }
    // writer.append(" graph [rankdir=\"LR\"];\n");
    for (final Vertex<T> vertex: verticesMap.values())
    {
      writer.append("  ").append(vertex);
      if (vertex.hasAttribute("fillcolor"))
      {
        writer.append(String.format(" [fillcolor=%s, style=filled]",
                                    vertex.getAttribute("fillcolor")));
      }
      writer.append(";\n");
    }
    for (final DirectedEdge<T> edge: edges)
    {
      writer.append("  ").append(edge).append(";\n");
    }
    writer.append("}\n");
    return writer.toString();
  }

  public Set<Vertex<T>> vertexSet()
  {
    return new HashSet<>(verticesMap.values());
  }

}

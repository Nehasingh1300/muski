/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyser needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, true)

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(catalog: Catalog,
               registry: FunctionRegistry,
               caseSensitive: Boolean,
               maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with HiveTypeCoercion {

  val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution

  val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ImplicitGenerate ::
      ResolveFunctions ::
      GlobalAggregates ::
      UnresolvedHavingClauseAttributes ::
      TrimGroupingAliases ::
      typeCoercionRules ++
      extendedResolutionRules : _*)
  )







  /**
   * Removes no-op Alias expressions from the plan.
   */
  object TrimGroupingAliases extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Aggregate(groups, aggs, child) =>
        Aggregate(groups.map(_.transform { case Alias(c, _) => c }), aggs, child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /**
     * Extract attribute set according to the grouping id
     * @param bitmask bitmask to represent the selected of the attribute sequence
     * @param exprs the attributes in sequence
     * @return the attributes of non selected specified via bitmask (with the bit set to 1)
     */
    private def buildNonSelectExprSet(bitmask: Int, exprs: Seq[Expression])
    : OpenHashSet[Expression] = {
      val set = new OpenHashSet[Expression](2)

      var bit = exprs.length - 1
      while (bit >= 0) {
        if (((bitmask >> bit) & 1) == 0) set.add(exprs(bit))
        bit -= 1
      }

      set
    }

    /*
     *  GROUP BY a, b, c WITH ROLLUP
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( ) ).
     *  Group Count: N + 1 (N is the number of group expressions)
     *
     *  We need to get all of its subsets for the rule described above, the subset is
     *  represented as the bit masks.
     */
    def bitmasks(r: Rollup): Seq[Int] = {
      Seq.tabulate(r.groupByExprs.length + 1)(idx => {(1 << idx) - 1})
    }

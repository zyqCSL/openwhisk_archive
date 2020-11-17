/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.entity

// import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import spray.json._
// import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.ConfigKeys
import pureconfig._

case class CpuLimitConfig(min: Double, max: Double, std: Double)

/**
 * CpuLimit encapsulates allowed Cpu for an action. The limit must be within a
 * permissible range (by default [1, 4]).
 *
 * It is a value type (hence == is .equals, immutable and cannot be assigned null).
 * The constructor is private so that argument requirements are checked and normalized
 * before creating a new instance.
 *
 * @param cores the cpu limit in core number for the action
 */
protected[entity] class CpuLimit private (val cores: Double) extends AnyVal

protected[core] object CpuLimit extends ArgNormalizer[CpuLimit] {
  val config = loadConfigOrThrow[CpuLimitConfig](ConfigKeys.cpu)

  /** These values are set once at the beginning. Dynamic configuration updates are not supported at the moment. */
  protected[core] val MIN_CPU: Double = config.min
  protected[core] val MAX_CPU: Double = config.max
  protected[core] val STD_CPU: Double = config.std

  /** A singleton CpuLimit with default value */
  protected[core] val standardCpuLimit = CpuLimit(STD_CPU)

  /** Gets CpuLimit with default value */
  protected[core] def apply(): CpuLimit = standardCpuLimit

  /**
   * Creates CpuLimit for limit, iff limit is within permissible range.
   *
   * @param cores the limit in cores, must be within permissible range
   * @return CpuLimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(cores: Double): CpuLimit = {
    require(cores >= MIN_CPU, s"CPU $cores below allowed threshold of $MIN_CPU")
    require(cores <= MAX_CPU, s"CPU $cores exceeds allowed threshold of $MAX_CPU")
    new CpuLimit(cores)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[CpuLimit] {
    def write(c: CpuLimit) = JsNumber(c.cores)

    def read(value: JsValue) =
      Try {
        val JsNumber(c) = value
        require(c.isWhole(), "cpu limit must be whole number")
        CpuLimit(c.doubleValue)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("cpu limit malformed", e)
      }
  }
}

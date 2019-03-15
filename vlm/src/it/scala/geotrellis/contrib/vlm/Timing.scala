/*
 * Copyright 2019 Astraea, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.contrib.vlm

case class Timing(label: String, start: Long, end: Long) {
  def durationMillis: Double = end - start
  def durationSecs: Double = durationMillis * 1e-3
  override def toString: String = f"Elapsed time of $label: $durationSecs%.4fs"
}
object Timing {
  def time[R](label: String)(block: ⇒ R): (R, Timing) = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    (result, Timing(label, start, end))
  }
}

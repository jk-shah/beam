/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.postgres.cdc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.VarLongCoder;

/** Coder for {@link LsnRange} restrictions. */
public class LsnRangeCoder extends AtomicCoder<LsnRange> {

  private static final LsnRangeCoder INSTANCE = new LsnRangeCoder();
  private static final VarLongCoder VAR_LONG_CODER = VarLongCoder.of();

  public static LsnRangeCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(LsnRange value, OutputStream outStream) throws IOException {
    VAR_LONG_CODER.encode(value.getFromLsn(), outStream);
    VAR_LONG_CODER.encode(value.getToLsn(), outStream);
  }

  @Override
  public LsnRange decode(InputStream inStream) throws IOException {
    long fromLsn = VAR_LONG_CODER.decode(inStream);
    long toLsn = VAR_LONG_CODER.decode(inStream);
    return LsnRange.of(fromLsn, toLsn);
  }
}

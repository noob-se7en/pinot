/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.utils.retry;

import java.util.concurrent.Callable;


/**
 * The <code>AttemptFailureException</code> indicates that the {@link RetryPolicy#attempt(Callable)} failed because of
 * either operation throwing an exception or running out of attempts.
 */
public class AttemptFailureException extends Exception {
  private final int _attempts;

  public AttemptFailureException(String message) {
    super(message);
    _attempts = 0;
  }

  public AttemptFailureException(String message, int attempts) {
    super(message);
    _attempts = attempts;
  }

  public AttemptFailureException(Throwable cause) {
    super(cause);
    _attempts = 0;
  }

  public AttemptFailureException(Throwable cause, int attempts) {
    super(cause);
    _attempts = attempts;
  }

  public int getAttempts() {
    return _attempts;
  }
}

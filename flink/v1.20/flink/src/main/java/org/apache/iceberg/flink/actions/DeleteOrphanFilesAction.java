/*
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
package org.apache.iceberg.flink.actions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.io.FileIO;

/** A Flink action to remove orphan files from a table. */
public class DeleteOrphanFilesAction implements DeleteOrphanFiles {

  private Table table;
  private final StreamExecutionEnvironment env;
  private int maxParallelism;
  private String location;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;

  public DeleteOrphanFilesAction(StreamExecutionEnvironment env, Table table) {
    this.env = env;
    this.maxParallelism = env.getParallelism();
    this.table = table;
    this.location = table.location();
  }

  /**
   * Executes this action.
   *
   * @return the result of this action
   */
  @Override
  public Result execute() {
    return null;
  }

  /**
   * Passes a location which should be scanned for orphan files.
   *
   * <p>If not set, the root table location will be scanned potentially removing both orphan data
   * and metadata files.
   *
   * @param location the location where to look for orphan files
   * @return this for method chaining
   */
  @Override
  public DeleteOrphanFiles location(String location) {
    this.location = location;
    return this;
  }

  /**
   * Removes orphan files only if they are older than the given timestamp.
   *
   * <p>This is a safety measure to avoid removing files that are being added to the table. For
   * example, there may be a concurrent operation adding new files while this action searches for
   * orphan files. New files may not be referenced by the metadata yet but they are not orphan.
   *
   * <p>If not set, defaults to a timestamp 3 days ago.
   *
   * @param olderThanTimestamp a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  @Override
  public DeleteOrphanFiles olderThan(long olderThanTimestamp) {
    this.olderThanTimestamp = olderThanTimestamp;
    return this;
  }

  /**
   * Passes an alternative delete implementation that will be used for orphan files.
   *
   * <p>This method allows users to customize the delete function. For example, one may set a custom
   * delete func and collect all orphan files into a set instead of physically removing them.
   *
   * <p>If not set, defaults to using the table's {@link FileIO io} implementation.
   *
   * @param deleteFunc a function that will be called to delete files
   * @return this for method chaining
   */
  @Override
  public DeleteOrphanFiles deleteWith(Consumer<String> deleteFunc) {
    this.deleteFunc = deleteFunc;
    return this;
  }

  /**
   * Passes an alternative executor service that will be used for removing orphaned files. This
   * service will only be used if a custom delete function is provided by {@link
   * #deleteWith(Consumer)} or if the FileIO does not {@link SupportsBulkOperations support bulk
   * deletes}. Otherwise, parallelism should be controlled by the IO specific {@link
   * SupportsBulkOperations#deleteFiles(Iterable) deleteFiles} method.
   *
   * <p>If this method is not called and bulk deletes are not supported, orphaned manifests and data
   * files will still be deleted in the current thread.
   *
   * @param executorService the service to use
   * @return this for method chaining
   */
  @Override
  public DeleteOrphanFiles executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }
}

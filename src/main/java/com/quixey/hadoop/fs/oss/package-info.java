/* Copyright 2014, Quixey Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An implementation of the Hadoop FS API using OSS.
 *
 * <p>
 *   Built and maintained by Data Engineering @ Quixey. Much of the code here is adapted from the classes written for
 *   {@code org.apache.hadoop.fs.s3native.NativeS3FileSystem}.
 * </p>
 *
 * @author Jim Lim - jim@quixey.com
 */
@ParametersAreNonnullByDefault package com.quixey.hadoop.fs.oss;

/* We encourage using the following annotations
 * @Nullable, @VisibleForTesting, @Beta
 */

import javax.annotation.ParametersAreNonnullByDefault;
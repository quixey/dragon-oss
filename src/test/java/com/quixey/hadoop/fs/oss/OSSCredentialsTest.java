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
package com.quixey.hadoop.fs.oss;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link com.quixey.hadoop.fs.oss.OSSCredentials}.
 *
 * @author Jim Lim - jim@quixey.com
 */
public class OSSCredentialsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testExtractFromURI() {
    OSSCredentials oss = new OSSCredentials();
    Configuration config = new Configuration();
    config.clear();

    oss.initialize(URI.create("oss://xyz:abc@bucket"), config);

    assertEquals(oss.getAccessKeyId(), "xyz");
    assertEquals(oss.getSecretAccessKey(), "abc");
  }

  @Test
  public void testExtractFromConfiguration() {
    Configuration config = new Configuration();
    config.clear();
    config.set("fs.oss.accessKeyId", "xyz");
    config.set("fs.oss.secretAccessKey", "abc");

    OSSCredentials oss = new OSSCredentials();
    oss.initialize(URI.create("oss://bucket"), config);

    assertEquals(oss.getAccessKeyId(), "xyz");
    assertEquals(oss.getSecretAccessKey(), "abc");
  }

  @Test
  public void testThrowIAEOnMissingKeyID() {
    OSSCredentials oss = new OSSCredentials();
    Configuration config = new Configuration();
    config.clear();

    thrown.expect(IllegalArgumentException.class);
    oss.initialize(URI.create("oss://abc@bucket"), config);
  }

  @Test
  public void testThrowIAEOnMissingSecretKey() {
    Configuration config = new Configuration();
    config.clear();
    config.set("fs.oss.secretAccessKey", "abc");

    OSSCredentials oss = new OSSCredentials();

    thrown.expect(IllegalArgumentException.class);
    oss.initialize(URI.create("oss://bucket"), config);
  }
}

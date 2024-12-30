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

#include <gtest/gtest.h>

#include "utils/ConfigExtractor.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"

namespace gluten {

TEST(S3ConfigTest, defaultConfig) {
  std::unordered_map<std::string, std::string> emptyConfig;
  auto base = std::make_shared<facebook::velox::config::ConfigBase>(std::move(emptyConfig));
  auto s3Config = facebook::velox::filesystems::S3Config("", getHiveConfig(base));
  ASSERT_EQ(s3Config.accessKey(), std::nullopt);
  ASSERT_EQ(s3Config.secretKey(), std::nullopt);
  ASSERT_EQ(s3Config.endpoint(), "");
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useVirtualAddressing(), true);
  ASSERT_EQ(s3Config.maxAttempts(), std::nullopt);
  ASSERT_EQ(s3Config.retryMode(), "legacy");
  ASSERT_EQ(s3Config.maxConnections(), 15);
  ASSERT_EQ(s3Config.connectTimeout(), "200s");
  ASSERT_EQ(s3Config.useInstanceCredentials(), false);
  ASSERT_EQ(s3Config.iamRole(), std::nullopt);
  ASSERT_EQ(s3Config.iamRoleSessionName(), "gluten-session");
}

TEST(S3ConfigTest, defaultConfigOverride) {
  std::unordered_map<std::string, std::string> emptyConfig = {
	  {"spark.hadoop.fs.s3a.access.key", "access"},
	  {"spark.hadoop.fs.s3a.secret.key", "secret"},
	  {"spark.hadoop.fs.s3a.endpoint", "endpoint"},
	  {"spark.hadoop.fs.s3a.connection.ssl.enabled", "true"},
	  {"spark.hadoop.fs.s3a.path.style.access", "true"},
	  {"spark.hadoop.fs.s3a.retry.limit", "10"},
	  {"spark.hadoop.fs.s3a.retry.mode", "adaptive"},
	  {"spark.hadoop.fs.s3a.connection.maximum", "8"},
	  {"spark.hadoop.fs.s3a.connection.timeout", "100s"},
	  {"spark.hadoop.fs.s3a.instance.credentials", "true"},
	  {"spark.hadoop.fs.s3a.iam.role", "gluten"},
	  {"spark.hadoop.fs.s3a.iam.role.session.name", "gluten-new-session"}
  };
  auto base = std::make_shared<facebook::velox::config::ConfigBase>(std::move(emptyConfig));
  auto s3Config = facebook::velox::filesystems::S3Config("", getHiveConfig(base));
  ASSERT_EQ(s3Config.accessKey(), "access");
  ASSERT_EQ(s3Config.secretKey(), "secret");
  ASSERT_EQ(s3Config.endpoint(), "endpoint");
  ASSERT_EQ(s3Config.useSSL(), true);
  ASSERT_EQ(s3Config.useVirtualAddressing(), false);
  ASSERT_EQ(s3Config.maxAttempts(), 10);
  ASSERT_EQ(s3Config.retryMode(), "adaptive");
  ASSERT_EQ(s3Config.maxConnections(), 8);
  ASSERT_EQ(s3Config.connectTimeout(), "100s");
  ASSERT_EQ(s3Config.useInstanceCredentials(), true);
  ASSERT_EQ(s3Config.iamRole(), "gluten");
  ASSERT_EQ(s3Config.iamRoleSessionName(), "gluten-new-session");

  // Set Env
  setenv("AWS_ENDPOINT", "env-endpoint", 1);
  setenv("AWS_MAX_ATTEMPTS", "4", 1);
  setenv("AWS_RETRY_MODE", "adaptive", 1);
  setenv("AWS_ACCESS_KEY_ID", "env-access", 1);
  setenv("AWS_SECRET_ACCESS_KEY", "env-secret", 1);
  s3Config = facebook::velox::filesystems::S3Config("", getHiveConfig(base));
  ASSERT_EQ(s3Config.accessKey(), "env-access");
  ASSERT_EQ(s3Config.secretKey(), "env-secret");
  ASSERT_EQ(s3Config.endpoint(), "env-endpoint");
  ASSERT_EQ(s3Config.maxAttempts(), 4);
  ASSERT_EQ(s3Config.retryMode(), "adaptive");
  unsetenv("AWS_ENDPOINT");
  unsetenv("AWS_MAX_ATTEMPTS");
  unsetenv("AWS_RETRY_MODE");
  unsetenv("AWS_ACCESS_KEY_ID");
  unsetenv("AWS_SECRET_ACCESS_KEY");
}

TEST(S3ConfigTest, bucketConfigOverride) {
  std::unordered_map<std::string, std::string> emptyConfig = {
	  {"spark.hadoop.fs.s3a.access.key", "access"},
	  {"spark.hadoop.fs.s3a.bucket.foo.access.key", "foo-access"},
	  {"spark.hadoop.fs.s3a.secret.key", "secret"},
	  {"spark.hadoop.fs.s3a.bucket.foo.secret.key", "foo-secret"},
	  {"spark.hadoop.fs.s3a.endpoint", "endpoint"},
	  {"spark.hadoop.fs.s3a.bucket.foo.endpoint", "foo-endpoint"},
	  {"spark.hadoop.fs.s3a.connection.ssl.enabled", "true"},
	  {"spark.hadoop.fs.s3a.bucket.foo.connection.ssl.enabled", "false"},
	  {"spark.hadoop.fs.s3a.path.style.access", "true"},
	  {"spark.hadoop.fs.s3a.bucket.foo.path.style.access", "false"},
	  {"spark.hadoop.fs.s3a.retry.limit", "10"},
	  {"spark.hadoop.fs.s3a.bucket.foo.retry.limit", "1"},
	  {"spark.hadoop.fs.s3a.retry.mode", "adaptive"},
	  {"spark.hadoop.fs.s3a.bucket.foo.retry.mode", "standard"},
	  {"spark.hadoop.fs.s3a.connection.maximum", "8"},
	  {"spark.hadoop.fs.s3a.bucket.foo.connection.maximum", "4"},
	  {"spark.hadoop.fs.s3a.connection.timeout", "100s"},
	  {"spark.hadoop.fs.s3a.bucket.foo.connection.timeout", "10s"},
	  {"spark.hadoop.fs.s3a.instance.credentials", "true"},
	  {"spark.hadoop.fs.s3a.bucket.foo.instance.credentials", "false"},
	  {"spark.hadoop.fs.s3a.iam.role", "gluten"},
	  {"spark.hadoop.fs.s3a.bucket.foo.iam.role", "foo-gluten"},
	  {"spark.hadoop.fs.s3a.iam.role.session.name", "gluten-new-session"},
	  {"spark.hadoop.fs.s3a.bucket.foo.iam.role.session.name", "foo-gluten-new-session"}
  };
  auto base = std::make_shared<facebook::velox::config::ConfigBase>(std::move(emptyConfig));
  auto s3Config = facebook::velox::filesystems::S3Config("foo", getHiveConfig(base));
  ASSERT_EQ(s3Config.accessKey(), "foo-access");
  ASSERT_EQ(s3Config.secretKey(), "foo-secret");
  ASSERT_EQ(s3Config.endpoint(), "foo-endpoint");
  ASSERT_EQ(s3Config.useSSL(), false);
  ASSERT_EQ(s3Config.useVirtualAddressing(), true);
  ASSERT_EQ(s3Config.maxAttempts(), 1);
  ASSERT_EQ(s3Config.retryMode(), "standard");
  ASSERT_EQ(s3Config.maxConnections(), 4);
  ASSERT_EQ(s3Config.connectTimeout(), "10s");
  ASSERT_EQ(s3Config.useInstanceCredentials(), false);
  ASSERT_EQ(s3Config.iamRole(), "foo-gluten");
  ASSERT_EQ(s3Config.iamRoleSessionName(), "foo-gluten-new-session");
}

} // namespace gluten

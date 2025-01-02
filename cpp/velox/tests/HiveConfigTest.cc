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

#include "velox/common/config/Config.h"

namespace gluten {

TEST(AbfsConfigTest, overrideConfig) {
 std::unordered_map<std::string, std::string> config = 
    {{"fs.azure.account.auth.type.efg.dfs.core.windows.net", "SAS"},
     {"fs.azure.sas.fixed.token.bar.dfs.core.windows.net", "sas=test"}};
 auto base = std::make_shared<facebook::velox::config::ConfigBase>(std::move(config));
 auto abfsConfig = getHiveConfig(base);
 ASSERT_EQ(abfsConfig->get<std::string>("fs.azure.account.auth.type.efg.dfs.core.windows.net"), "SAS");
 ASSERT_EQ(abfsConfig->get<std::string>("fs.azure.sas.fixed.token.bar.dfs.core.windows.net"), "sas=test");

 std::unordered_map<std::string, std::string> sparkConfig =
    {{"spark.hadoop.fs.azure.account.auth.type.efg.dfs.core.windows.net", "SAS"},
     {"spark.hadoop.fs.azure.sas.fixed.token.bar.dfs.core.windows.net", "sas=test"}};
 base = std::make_shared<facebook::velox::config::ConfigBase>(std::move(sparkConfig));
 abfsConfig = getHiveConfig(base);
 ASSERT_EQ(abfsConfig->get<std::string>("fs.azure.account.auth.type.efg.dfs.core.windows.net"), "SAS");
 ASSERT_EQ(abfsConfig->get<std::string>("fs.azure.sas.fixed.token.bar.dfs.core.windows.net"), "sas=test");
}

}

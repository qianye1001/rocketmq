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

package org.apache.rocketmq.remoting;

import java.util.Properties;
import org.apache.rocketmq.common.annotation.Sensitive;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ConfigurationTest {

    @Test
    public void testGetAllConfigsSnapshotRefreshesAndCopiesProperties() {
        TestConfig testConfig = new TestConfig();
        Configuration configuration = new Configuration(mock(Logger.class), testConfig);
        testConfig.customPath = "C:\\rocketmq\\store";

        Properties snapshot = configuration.getAllConfigsSnapshot();

        assertEquals("C:\\rocketmq\\store", snapshot.getProperty("customPath"));
        assertNotSame(configuration.getAllConfigs(), snapshot);
        snapshot.remove("customPath");
        assertEquals("C:\\rocketmq\\store", configuration.getAllConfigs().getProperty("customPath"));
    }

    private static class TestConfig {
        private String customPath = "initial";
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static class AnnotatedConfig {
        @Sensitive
        private String opaqueValue = "old-secret";
        private String databasePassword = "old-password";
        private String unchangedValue = "same-value";

        public void setOpaqueValue(String opaqueValue) {
            this.opaqueValue = opaqueValue;
        }

        public void setDatabasePassword(String databasePassword) {
            this.databasePassword = databasePassword;
        }

        public void setUnchangedValue(String unchangedValue) {
            this.unchangedValue = unchangedValue;
        }
    }

    @Test
    public void testUpdateLogsOnlyChangedPropertiesAndMasksExplicitAnnotations() throws Exception {
        Logger logger = mock(Logger.class);
        AnnotatedConfig config = new AnnotatedConfig();
        Configuration configuration = new Configuration(logger,
            temporaryFolder.newFile().getAbsolutePath(), config);
        Properties properties = new Properties();
        properties.setProperty("opaqueValue", "new-secret");
        properties.setProperty("databasePassword", "new-password");
        properties.setProperty("unchangedValue", "same-value");

        configuration.update(properties);

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "opaqueValue", "******", "******");
        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "databasePassword", "old-password", "new-password");
        verify(logger, never()).info("Replace, key: {}, value: {} -> {}",
            "unchangedValue", "same-value", "same-value");
        assertThat(config.opaqueValue).isEqualTo("new-secret");
        assertThat(config.databasePassword).isEqualTo("new-password");
        assertThat(configuration.getAllConfigs().getProperty("opaqueValue")).isEqualTo("new-secret");
    }

    @Test
    public void testLogOnlyRegistrationDoesNotRegisterOrUpdateValues() throws Exception {
        Logger logger = mock(Logger.class);
        Configuration configuration = new Configuration(logger, temporaryFolder.newFile().getAbsolutePath());
        AnnotatedConfig config = new AnnotatedConfig();
        configuration.registerConfigForLog(config);
        assertThat(configuration.getAllConfigs()).isEmpty();
        Properties initial = new Properties();
        initial.setProperty("opaqueValue", "old-secret");
        configuration.registerConfig(initial);
        Properties update = new Properties();
        update.setProperty("opaqueValue", "new-secret");

        configuration.update(update);

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "opaqueValue", "******", "******");
        assertThat(config.opaqueValue).isEqualTo("old-secret");
        assertThat(configuration.getAllConfigsSnapshot().getProperty("opaqueValue")).isEqualTo("new-secret");
    }

    @Test
    public void testRegistrationMasksReplacementUsingIncomingObjectMetadata() {
        Logger logger = mock(Logger.class);
        Configuration configuration = new Configuration(logger);
        Properties initial = new Properties();
        initial.setProperty("opaqueValue", "initial-secret");
        configuration.registerConfig(initial);

        configuration.registerConfig(new AnnotatedConfig());

        verify(logger).info("Replace, key: {}, value: {} -> {}",
            "opaqueValue", "******", "******");
    }
}

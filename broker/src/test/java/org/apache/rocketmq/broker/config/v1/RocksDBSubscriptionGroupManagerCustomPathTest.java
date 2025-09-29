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

package org.apache.rocketmq.broker.config.v1;

import java.io.File;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RocksDBSubscriptionGroupManagerCustomPathTest {

    private BrokerController brokerController;
    private String storePath;
    private String customStorePath;

    @Before
    public void init() {

        brokerController = Mockito.mock(BrokerController.class);
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        storePath = System.getProperty("java.io.tmpdir") + File.separator + "rocketmq-test-" + System.currentTimeMillis();
        messageStoreConfig.setStorePathRootDir(storePath);
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        
        customStorePath = storePath + File.separator + "custom";
        UtilAll.ensureDirOK(customStorePath);
    }

    @After
    public void destroy() {

        // Clean up test directories
        UtilAll.deleteFile(new File(storePath));
    }

    @Test
    public void testConstructorWithCustomPath() {

        // Test constructor with custom path
        RocksDBSubscriptionGroupManager manager = new RocksDBSubscriptionGroupManager(brokerController, false, customStorePath);
        Assert.assertNotNull("Manager should be created successfully", manager);
        
        // Test loading with custom path
        boolean loaded = manager.load();
        Assert.assertTrue("Manager should load successfully", loaded);
        
        manager.stop();
    }

    @Test
    public void testConstructorWithNullCustomPath() {
        
        // Test constructor with null custom path (should use default path)
        RocksDBSubscriptionGroupManager manager = new RocksDBSubscriptionGroupManager(brokerController, false, null);
        Assert.assertNotNull("Manager should be created successfully", manager);
        
        // Test loading with default path
        boolean loaded = manager.load();
        Assert.assertTrue("Manager should load successfully", loaded);
        
        manager.stop();
    }

    @Test
    public void testDefaultConstructor() {
        
        // Test default constructor (should work as before)
        RocksDBSubscriptionGroupManager manager = new RocksDBSubscriptionGroupManager(brokerController);
        Assert.assertNotNull("Manager should be created successfully", manager);
        
        // Test loading
        boolean loaded = manager.load();
        Assert.assertTrue("Manager should load successfully", loaded);
        
        manager.stop();
    }

}
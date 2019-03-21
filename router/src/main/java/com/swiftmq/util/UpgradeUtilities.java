/*
 * Copyright 2019 IIT Software GmbH
 *
 * IIT Software GmbH licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.swiftmq.util;

import org.dom4j.Attribute;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;

public class UpgradeUtilities {
  public static final String UPGRADE_ATTRIBUTE = "_upgrade";
  public static boolean askReturnDefault = false;
  public static boolean upgrade_900_add_amqp_listeners = true;

  private static String ask(BufferedReader reader, String message, String defaultValue) throws Exception {
    if (askReturnDefault)
      return defaultValue;
    System.out.println();
    System.out.print("+++ " + message + " [" + defaultValue + "]: ");
    String s = reader.readLine();
    System.out.println();
    return s == null || s.trim().length() == 0 ? defaultValue : s;
  }

  private static Element getElement(Element root, String name) throws Exception {
    Element ele = null;
    for (Iterator iter = root.elementIterator(); iter.hasNext(); ) {
      Element e = (Element) iter.next();
      if (e.attribute("name") != null) {
        if (e.attributeValue("name").equals(name)) {
          ele = e;
          break;
        }
      }
    }
    return ele;
  }

  private static void removeElement(Element parent, String name) throws Exception {
    for (Iterator iter = parent.elementIterator(); iter.hasNext(); ) {
      Element e = (Element) iter.next();
      if (e.attribute("name") != null) {
        if (e.attributeValue("name").equals(name)) {
          parent.remove(e);
          break;
        }
      }
    }
  }

  private static Element addElement(Element parent, String name) {
    Element element = parent.element(name);
    if (element == null) {
      element = DocumentHelper.createElement(name);
      element.addAttribute(UPGRADE_ATTRIBUTE, "true");
      parent.add(element);
    }
    return element;
  }

  public static void convert310to320(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 3.2.0 ...");
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    Element pools = threadpool.element("pools");
    UpgradeUtilities.removeElement(pools, "jms.consumer");
    UpgradeUtilities.removeElement(pools, "jms.producer");
    for (Iterator iter = pools.elementIterator(); iter.hasNext(); ) {
      Element e = (Element) iter.next();
      if (e.attributeValue("name").equals("jms.connection")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("1");
        a = e.attribute("max-threads");
        a.setValue("1");
      } else if (e.attributeValue("name").equals("jms.session")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("1");
        a = e.attribute("max-threads");
        a.setValue("1");
      } else if (e.attributeValue("name").equals("queue.timeout")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("1");
        a = e.attribute("max-threads");
        a.setValue("1");
      } else if (e.attributeValue("name").equals("routing.service")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("3");
        a = e.attribute("max-threads");
        a.setValue("3");
      } else if (e.attributeValue("name").equals("routing.msgfwd")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("3");
        a = e.attribute("max-threads");
        a.setValue("3");
      } else if (e.attributeValue("name").equals("timer.tasks")) {
        Attribute a = e.attribute("min-threads");
        a.setValue("3");
        a = e.attribute("max-threads");
        a.setValue("3");
      }
    }
  }

  public static void convert320to400(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 4.0.0 ...");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    // Routername
    String routerName = root.attributeValue("name");

    // start order
    Attribute startOrder = root.attribute("startorder");
    startOrder.setValue("sys$log sys$authentication sys$threadpool sys$timer sys$net sys$store sys$queuemanager sys$topicmanager sys$mgmt sys$xa sys$routing sys$jndi sys$jms sys$deploy sys$jac");

    // Deploy
    Element deploy = UpgradeUtilities.getElement(root, "sys$deploy");
    Attribute deployPath = deploy.attribute("deploy-path");
    Attribute checkInterval = deploy.attribute("check-interval");
    Element deploySpaces = DocumentHelper.createElement("deploy-spaces");
    deploy.add(deploySpaces);
    Element space = DocumentHelper.createElement("deploy-space");
    space.addAttribute("name", "extension-swiftlets");
    space.addAttribute("path", deployPath.getValue());
    deploy.remove(deployPath);
    if (checkInterval != null) {
      space.addAttribute("check-interval", checkInterval.getValue());
      deploy.remove(checkInterval);
    }
    deploySpaces.add(space);
    String def = "../../jmsapp/" + routerName;
    String s = UpgradeUtilities.ask(reader, "Path to hot deploy JMS Apps", def);
    space = DocumentHelper.createElement("deploy-space");
    space.addAttribute("name", "jms-app");
    space.addAttribute("path", s);
    deploySpaces.add(space);
    File f = new File(s);
    if (!f.exists()) {
      if (!f.mkdirs())
        System.out.println("+++ Unable to create directory '" + s + "' - please create it manually!");
    }

    // JMS
    Element jms = UpgradeUtilities.getElement(root, "sys$jms");
    Element ivms = DocumentHelper.createElement("intravm-connection-factories");
    jms.add(ivms);
    Element ivm1 = DocumentHelper.createElement("intravm-connection-factory");
    ivm1.addAttribute("name", "IVMConnectionFactory");
    ivms.add(ivm1);
    Element ivm2 = DocumentHelper.createElement("intravm-connection-factory");
    ivm2.addAttribute("name", "IVMQueueConnectionFactory");
    ivms.add(ivm2);
    Element ivm3 = DocumentHelper.createElement("intravm-connection-factory");
    ivm3.addAttribute("name", "IVMTopicConnectionFactory");
    ivms.add(ivm3);

    // Threadpool
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "jac.runner");
    pool.addAttribute("kernel-pool", "true");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$jac.runner");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "jms.ivm.client.connection");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "10");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$jms.client.connection.%");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "jms.ivm.client.session");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "10");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$jms.client.session.%");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "mgmt");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "2");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$mgmt.%");
    threads.add(thread);

    UpgradeUtilities.removeElement(pools, "routing.connection.mgr");
    UpgradeUtilities.removeElement(pools, "routing.msgfwd");
    UpgradeUtilities.removeElement(pools, "routing.service");

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "routing.connection.mgr");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$routing.connection.mgr");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "routing.exchanger");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$routing.route.exchanger");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "routing.scheduler");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("min-threads", "3");
    pool.addAttribute("max-threads", "3");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$routing.scheduler");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "routing.service");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("min-threads", "3");
    pool.addAttribute("max-threads", "3");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$routing.connection.service");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "routing.throttle");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("min-threads", "3");
    pool.addAttribute("max-threads", "3");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$routing.connection.throttlequeue");
    threads.add(thread);

    UpgradeUtilities.removeElement(pools, "system");

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "timer.dispatcher");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("min-threads", "1");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$timer.dispatcher");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "topic");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "2");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$topicmanager.topic.%");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "jndi");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("min-threads", "1");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$jndi.listener");
    threads.add(thread);

    // Trace
    def = "../../trace/" + routerName;
    s = UpgradeUtilities.ask(reader, "Path of trace files", def);
    Element trace = UpgradeUtilities.getElement(root, "sys$trace");
    Element tspaces = trace.element("spaces");
    Element tspace = UpgradeUtilities.getElement(tspaces, "kernel");
    Element preds = tspace.element("predicates");
    Element pred = DocumentHelper.createElement("predicate");
    pred.addAttribute("name", "14");
    pred.addAttribute("filename", s + "/jac.trace");
    pred.addAttribute("value", "sys$jac");
    preds.add(pred);
  }

  public static void convert400to450(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 4.5.0 ...");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    // Routername
    String routerName = root.attributeValue("name");

    // start order
    Attribute startOrder = root.attribute("startorder");
    startOrder.setValue("sys$log sys$authentication sys$threadpool sys$timer sys$net sys$store sys$queuemanager sys$topicmanager sys$mgmt sys$xa sys$routing sys$jndi sys$jms sys$deploy sys$jac sys$scheduler");

    // Queue Manager
    Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
    Attribute defCacheSize = queueMgr.attribute("cache-size");
    Attribute defFC = queueMgr.attribute("flowcontrol-start-queuesize");
    Attribute defCleanUp = queueMgr.attribute("cleanup-interval");
    if (defCacheSize != null)
      queueMgr.remove(defCacheSize);
    if (defFC != null)
      queueMgr.remove(defFC);
    if (defCleanUp != null)
      queueMgr.remove(defCleanUp);
    Element queues = queueMgr.element("queues");
    for (Iterator iter = queues.elementIterator(); iter.hasNext(); ) {
      Element queue = (Element) iter.next();
      if (defCacheSize != null) {
        Attribute cacheSize = queue.attribute("cache-size");
        if (cacheSize == null)
          queue.addAttribute("cache-size", defCacheSize.getValue());
      }
      if (defFC != null) {
        Attribute fc = queue.attribute("flowcontrol-start-queuesize");
        if (fc == null)
          queue.addAttribute("flowcontrol-start-queuesize", defFC.getValue());
      }
      if (defCleanUp != null) {
        Attribute cu = queue.attribute("cleanup-interval");
        if (cu == null)
          queue.addAttribute("cleanup-interval", defCleanUp.getValue());
      }
    }
    Element controllers = DocumentHelper.createElement("queue-controllers");
    queueMgr.add(controllers);
    Element controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "01");
    controller.addAttribute("predicate", "tmp$%");
    controller.addAttribute("persistence-mode", "non_persistent");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "02");
    controller.addAttribute("predicate", "sys$%");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "03");
    controller.addAttribute("predicate", "swiftmq%");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "04");
    controller.addAttribute("predicate", "rt$%");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "05");
    controller.addAttribute("predicate", "unroutable");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "06");
    controller.addAttribute("predicate", "%$%");
    controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute("name", "07");
    controller.addAttribute("predicate", "%");

    // JMS
    Element jms = UpgradeUtilities.getElement(root, "sys$jms");
    Attribute defSF = jms.attribute("socketfactory-class");
    if (defSF != null) {
      jms.remove(defSF);
      Element listeners = jms.element("listeners");
      for (Iterator iter = listeners.elementIterator(); iter.hasNext(); ) {
        Element l = (Element) iter.next();
        Attribute sf = l.attribute("socketfactory-class");
        if (sf == null)
          l.addAttribute("socketfactory-class", defSF.getValue());
      }
    }

    // Routing
    Element routing = UpgradeUtilities.getElement(root, "sys$routing");
    defSF = routing.attribute("socketfactory-class");
    if (defSF != null) {
      routing.remove(defSF);
      Element listeners = routing.element("listeners");
      for (Iterator iter = listeners.elementIterator(); iter.hasNext(); ) {
        Element l = (Element) iter.next();
        Attribute sf = l.attribute("socketfactory-class");
        if (sf == null)
          l.addAttribute("socketfactory-class", defSF.getValue());
      }
      Element connectors = routing.element("connectors");
      for (Iterator iter = connectors.elementIterator(); iter.hasNext(); ) {
        Element l = (Element) iter.next();
        Attribute sf = l.attribute("socketfactory-class");
        if (sf == null)
          l.addAttribute("socketfactory-class", defSF.getValue());
      }
    }

    // Threadpool
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "scheduler.job");
    pool.addAttribute("kernel-pool", "true");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$scheduler.runner");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute("name", "scheduler.system");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "2");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$scheduler.requestprocessor");
    threads.add(thread);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute("name", "sys$scheduler.scheduler");
    threads.add(thread);

    // Trace
    String def = "../../trace/" + routerName;
    String s = UpgradeUtilities.ask(reader, "Path of trace files", def);
    Element trace = UpgradeUtilities.getElement(root, "sys$trace");
    Element tspaces = trace.element("spaces");
    Element tspace = UpgradeUtilities.getElement(tspaces, "kernel");
    Element preds = tspace.element("predicates");
    Element pred = DocumentHelper.createElement("predicate");
    pred.addAttribute("name", "15");
    pred.addAttribute("filename", s + "/timer.trace");
    pred.addAttribute("value", "sys$timer");
    preds.add(pred);
    pred = DocumentHelper.createElement("predicate");
    pred.addAttribute("name", "16");
    pred.addAttribute("filename", s + "/scheduler.trace");
    pred.addAttribute("value", "sys$scheduler");
    preds.add(pred);
  }

  public static void convert520to521(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 5.2.1 ...");

    // Queue Manager
    Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
    Element controllers = queueMgr.element("queue-controllers");
    for (Iterator iter = controllers.elementIterator(); iter.hasNext(); ) {
      Element l = (Element) iter.next();
      Attribute a = l.attribute("name");
      if (a != null && a.getValue().equals("07"))
        a.setValue("08");
    }
    Element controller = DocumentHelper.createElement("queue-controller");
    controllers.add(controller);
    controller.addAttribute(UPGRADE_ATTRIBUTE, "true");
    controller.addAttribute("name", "07");
    controller.addAttribute("predicate", "routerdlq");
    controller.addAttribute("cleanup-interval", "-1");
    controller.addAttribute("flowcontrol-start-queuesize", "-1");
  }

  public static void convert521to600(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 6.0.0 ...");

    // start order
    Attribute startOrder = root.attribute("startorder");
    String s = startOrder.getText();
    if (s.indexOf("sys$monitor") == -1)
      startOrder.setValue(s + " sys$monitor");
  }

  public static void convert600to610(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 6.1.0 ...");

    // Mgmt, add JMX
    Element mgmt = UpgradeUtilities.getElement(root, "sys$mgmt");
    Element jmx = DocumentHelper.createElement("jmx");
    jmx.addAttribute(UPGRADE_ATTRIBUTE, "true");
    jmx.addAttribute("enabled", "false");
    jmx.addAttribute("groupable-objectnames", "true");
    Element mbean = DocumentHelper.createElement("mbean-server");
    mbean.addAttribute(UPGRADE_ATTRIBUTE, "true");
    mbean.addAttribute("usage-option", "use-platform-server");
    mbean.addAttribute("server-name", "");
    jmx.add(mbean);
    mgmt.add(jmx);
  }

  public static void convert610to700(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 7.0.0 ...");

    // Clustered Queues
    Element queuemgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
    Element cqueues = DocumentHelper.createElement("clustered-queues");
    cqueues.addAttribute(UPGRADE_ATTRIBUTE, "true");
    queuemgr.add(cqueues);

    // Threadpool
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "queue.cluster");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$queuemanager.cluster.subscriber");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "queue.redispatcher");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$queuemanager.cluster.redispatcher");
    threads.add(thread);

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {

      // HA Monitor
      Element hamon = UpgradeUtilities.getElement(root, "sys$monitor");
      Element hastatealerts = DocumentHelper.createElement("ha-state-alerts");
      hastatealerts.addAttribute(UPGRADE_ATTRIBUTE, "true");
      hastatealerts.addAttribute("alert-active-transition", "false");
      hastatealerts.addAttribute("alert-standalone-transition", "false");
      hamon.add(hastatealerts);

      // Threadpool Freeze (HA only)
      Element freezes = hactl.element("threadpool-freezes");
      hactl.remove(freezes);
      freezes = DocumentHelper.createElement("threadpool-freezes");
      hactl.add(freezes);
      Element freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "01");
      freeze.addAttribute("poolname", "mgmt");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "02");
      freeze.addAttribute("poolname", "jndi");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "03");
      freeze.addAttribute("poolname", "topic");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "04");
      freeze.addAttribute("poolname", "default");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "05");
      freeze.addAttribute("poolname", "queue.timeout");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "06");
      freeze.addAttribute("poolname", "queue.cluster");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "07");
      freeze.addAttribute("poolname", "queue.redispatcher");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "08");
      freeze.addAttribute("poolname", "jms.session");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "09");
      freeze.addAttribute("poolname", "timer.tasks");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "10");
      freeze.addAttribute("poolname", "routing.service");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "11");
      freeze.addAttribute("poolname", "routing.scheduler");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "12");
      freeze.addAttribute("poolname", "routing.exchanger");
      freezes.add(freeze);
      freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "13");
      freeze.addAttribute("poolname", "store.log");
      freezes.add(freeze);
    }

  }

  public static void convert700to720(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 7.2.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element queueManager = UpgradeUtilities.getElement(root, "sys$queuemanager");
      Element queueController = queueManager.element("queue-controllers");
      for (Iterator iter = queueController.elementIterator(); iter.hasNext(); ) {
        Element qc = (Element) iter.next();
        qc.addAttribute("cache-size-bytes-kb", "-1");
      }
      Element queues = queueManager.element("queues");
      for (Iterator iter = queues.elementIterator(); iter.hasNext(); ) {
        Element queue = (Element) iter.next();
        queue.addAttribute("cache-size-bytes-kb", "-1");
      }
    }

  }

  public static void convert720to740(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 7.4.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element queueManager = UpgradeUtilities.getElement(root, "sys$queuemanager");
      Element cQueues = queueManager.element("clustered-queues");
      for (Iterator iter = cQueues.elementIterator(); iter.hasNext(); ) {
        Element qc = (Element) iter.next();
        qc.addAttribute("message-group-enabled", "false");
        qc.addAttribute("message-group-property", "JMSXGroupID");
        qc.addAttribute("message-group-expiration", "-1");
        qc.addAttribute("message-group-expiration-cleanup-interval", "-1");
      }
      // Composite Queues
      Element compQueues = DocumentHelper.createElement("composite-queues");
      compQueues.addAttribute(UPGRADE_ATTRIBUTE, "true");
      queueManager.add(compQueues);

      // JavaMail Bridge upgrade
      Element javaMail = UpgradeUtilities.getElement(root, "xt$javamail");
      if (javaMail != null) {
        System.out.println("+++ Upgrade JavaMail Bridge configuration.");
        Attribute ci = javaMail.attribute("collect-interval");
        if (ci == null)
          javaMail.addAttribute("collect-interval", "10000");
        Element usage = UpgradeUtilities.getElement(javaMail, "usage");
        if (usage == null) {
          usage = DocumentHelper.createElement("usage");
          usage.addAttribute(UPGRADE_ATTRIBUTE, "true");
          javaMail.add(usage);
        }
        Element aib = UpgradeUtilities.getElement(usage, "active-inbound-bridges");
        if (aib == null) {
          aib = DocumentHelper.createElement("active-inbound-bridges");
          aib.addAttribute(UPGRADE_ATTRIBUTE, "true");
          aib.addAttribute("last-transfer-time", "");
          aib.addAttribute("number-messages-transfered", "0");
          usage.add(aib);
        }
        Element aob = UpgradeUtilities.getElement(usage, "active-outbound-bridges");
        if (aob == null) {
          aob = DocumentHelper.createElement("active-outbound-bridges");
          aob.addAttribute(UPGRADE_ATTRIBUTE, "true");
          aob.addAttribute("last-transfer-time", "");
          aob.addAttribute("number-messages-transfered", "0");
          usage.add(aob);
        }
      }

      // JMS Bridge upgrade
      Element jmsBridge = UpgradeUtilities.getElement(root, "xt$bridge");
      if (jmsBridge != null) {
        System.out.println("+++ Upgrade JMS Bridge configuration.");
        Attribute ci = jmsBridge.attribute("collect-interval");
        if (ci == null)
          jmsBridge.addAttribute("collect-interval", "10000");
        Element usage = UpgradeUtilities.getElement(jmsBridge, "usage");
        if (usage == null) {
          usage = DocumentHelper.createElement("usage");
          usage.addAttribute(UPGRADE_ATTRIBUTE, "true");
          jmsBridge.add(usage);
        }
        Element ab = UpgradeUtilities.getElement(usage, "active-bridgings");
        if (ab == null) {
          ab = DocumentHelper.createElement("active-bridgings");
          ab.addAttribute(UPGRADE_ATTRIBUTE, "true");
          ab.addAttribute("last-transfer-time", "");
          ab.addAttribute("number-messages-transfered", "0");
          usage.add(ab);
        }
      }
    }
  }

  public static void convert740to750(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 7.5.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element jmsSwiftlet = UpgradeUtilities.getElement(root, "sys$jms");
      Element ivmCF = jmsSwiftlet.element("intravm-connection-factories");
      for (Iterator iter = ivmCF.elementIterator(); iter.hasNext(); ) {
        Element ele = (Element) iter.next();
        ele.addAttribute("smqp-consumer-cache-size-kb", "-1");
      }
      Element listeners = jmsSwiftlet.element("listeners");
      for (Iterator iter = listeners.elementIterator(); iter.hasNext(); ) {
        Element listener = (Element) iter.next();
        Element cF = listener.element("connection-factories");
        for (Iterator iter2 = cF.elementIterator(); iter2.hasNext(); ) {
          Element ele = (Element) iter2.next();
          ele.addAttribute("smqp-consumer-cache-size-kb", "-1");
        }
      }
    }

  }

  public static void convert750to752(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 7.5.2 ...");
  }

  public static void convert752to800(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 8.0.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl == null) {
      // start order non-HA Router
      Attribute startOrder = root.attribute("startorder");
      String s = startOrder.getText();
      if (s.indexOf("sys$accounting") == -1)
        startOrder.setText(SwiftUtilities.replace(s, "sys$topicmanager", "sys$topicmanager sys$accounting"));
    } else {
      Element harouterEle = null;
      for (Iterator iter = root.elementIterator(); iter.hasNext(); ) {
        Element e = (Element) iter.next();
        if (e.getName().equals("ha-router")) {
          harouterEle = e;
          break;
        }
      }
      if (harouterEle != null) {
        for (Iterator iter = harouterEle.elementIterator(); iter.hasNext(); ) {
          Element e = (Element) iter.next();
          Attribute order = e.attribute("order");
          String s = order.getText();
          if (s.indexOf("sys$accounting") == -1)
            order.setText(SwiftUtilities.replace(s, "sys$topicmanager", "sys$topicmanager sys$accounting"));
        }
      } else
        System.err.println("Unable to locate element 'ha-router' in routerconfig.xml!");

      // Accounting Swiftlet
      Element accounting = UpgradeUtilities.getElement(root, "sys$accounting");
      if (accounting == null) {
        accounting = DocumentHelper.createElement("swiftlet");
        accounting.addAttribute(UPGRADE_ATTRIBUTE, "true");
        accounting.addAttribute("name", "sys$accounting");
        root.add(accounting);
      }
      Element connections = accounting.element("connections");
      if (connections == null) {
        connections = DocumentHelper.createElement("connections");
        connections.addAttribute(UPGRADE_ATTRIBUTE, "true");
        accounting.add(connections);
      }
      Element usage = accounting.element("usage");
      if (usage == null) {
        usage = DocumentHelper.createElement("usage");
        usage.addAttribute(UPGRADE_ATTRIBUTE, "true");
        accounting.add(usage);
      }
      Element ac = accounting.element("active-connections");
      if (ac == null) {
        ac = DocumentHelper.createElement("active-connections");
        ac.addAttribute(UPGRADE_ATTRIBUTE, "true");
        usage.add(ac);
      }
      Element groups = accounting.element("groups");
      if (groups == null) {
        groups = DocumentHelper.createElement("groups");
        groups.addAttribute(UPGRADE_ATTRIBUTE, "true");
        usage.add(groups);
      }

      // Mgmt Swiftlet
      Element mgmt = UpgradeUtilities.getElement(root, "sys$mgmt");
//      mgmt.addAttribute(UPGRADE_ATTRIBUTE, "true");
      mgmt.addAttribute("admintool-connect-logging-enabled", "true");
      Element mi = mgmt.element("message-interface");
      if (mi == null) {
        mi = DocumentHelper.createElement("message-interface");
        mi.addAttribute("enabled", "false");
        mi.addAttribute("request-queue-name", "swiftmqmgmt-message-interface");
        mi.addAttribute(UPGRADE_ATTRIBUTE, "true");
        mgmt.add(mi);
      }

      Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
      Element queues = queueMgr.element("usage");
      if (queues != null) {
        for (Iterator iter = queues.elementIterator(); iter.hasNext(); ) {
          Element queue = (Element) iter.next();
          queue.addAttribute(UPGRADE_ATTRIBUTE, "true");
          queue.addAttribute("msg-consume-rate", "0");
          queue.addAttribute("msg-produce-rate", "0");
          queue.addAttribute("total-consumed", "0");
          queue.addAttribute("total-produced", "0");
        }
      }

      // JMS Swiftlet
      Element jmsSwiftlet = UpgradeUtilities.getElement(root, "sys$jms");
      Element listeners = jmsSwiftlet.element("listeners");
      for (Iterator iter = listeners.elementIterator(); iter.hasNext(); ) {
        Element listener = (Element) iter.next();
        listener.addAttribute(UPGRADE_ATTRIBUTE, "true");
        listener.addAttribute("max-connections", "-1");
      }
    }

    // Threadpools
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    threadpool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "accounting.events");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "1");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$accounting.eventprocessor");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "accounting.connections");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "-1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$accounting.sourcerunner");
    threads.add(thread);
  }

  public static void convert800to810(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 8.1.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      // Store Swiftlet
      Element storeSwiftlet = UpgradeUtilities.getElement(root, "sys$store");
      Element txLog = storeSwiftlet.element("transaction-log");
      if (txLog != null) {
        // It is the Replicated File Store
        Attribute groupCommit = txLog.attribute("group-commit-delay");
        if (groupCommit != null) {
          txLog.remove(groupCommit);
        }
        txLog.addAttribute(UPGRADE_ATTRIBUTE, "true");
      }

      // Threadpool Swiftlet
      Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
      Element pools = threadpool.element("pools");
      Element session = UpgradeUtilities.getElement(pools, "jms.session");
      if (session != null) {
        Attribute maxThreads = session.attribute("max-threads");
        if (maxThreads != null) {
          int val = Integer.parseInt(maxThreads.getValue());
          if (val < 100) {
            maxThreads.setValue("100");
          }
        } else {
          session.addAttribute("max-threads", "100");
        }
      }

    }
  }

  public static void convert810to900(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.0.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl == null) {
      // start order non-HA Router
      Attribute startOrder = root.attribute("startorder");
      String s = startOrder.getText();
      if (s.indexOf("sys$amqp") == -1)
        startOrder.setText(SwiftUtilities.replace(s, "sys$jms", "sys$jms sys$amqp"));
    } else {
      Element harouterEle = null;
      for (Iterator iter = root.elementIterator(); iter.hasNext(); ) {
        Element e = (Element) iter.next();
        if (e.getName().equals("ha-router")) {
          harouterEle = e;
          break;
        }
      }
      if (harouterEle != null) {
        for (Iterator iter = harouterEle.elementIterator(); iter.hasNext(); ) {
          Element e = (Element) iter.next();
          Attribute order = e.attribute("order");
          String s = order.getText();
          if (s.indexOf("sys$amqp") == -1)
            order.setText(SwiftUtilities.replace(s, "sys$jms", "sys$jms sys$amqp"));
        }
      } else
        System.err.println("Unable to locate element 'ha-router' in routerconfig.xml!");
    }

    // AMQP Swiftlet
    Element amqp = UpgradeUtilities.getElement(root, "sys$amqp");
    if (amqp == null) {
      amqp = DocumentHelper.createElement("swiftlet");
      amqp.addAttribute(UPGRADE_ATTRIBUTE, "true");
      amqp.addAttribute("name", "sys$amqp");
      root.add(amqp);
    }
    Element declarations = addElement(amqp, "declarations");
    Element transformer = addElement(declarations, "transformer");
    Element defaultInboundTransformer = addElement(transformer, "default-inbound-transformers");
    Element defaultOutboundTransformer = addElement(transformer, "default-outbound-transformers");
    Element destinationTransformer = addElement(transformer, "destination-transformers");
    Element connectionTemplates = addElement(declarations, "connection-templates");
    Element sslTemplate = getElement(connectionTemplates, "ssl");
    if (sslTemplate == null) {
      sslTemplate = DocumentHelper.createElement("connection-template");
      sslTemplate.addAttribute(UPGRADE_ATTRIBUTE, "true");
      sslTemplate.addAttribute("name", "ssl");
      sslTemplate.addAttribute("socketfactory-class", "com.swiftmq.net.JSSESocketFactory");
      connectionTemplates.add(sslTemplate);
    }
    Element listeners = addElement(amqp, "listeners");
    if (upgrade_900_add_amqp_listeners) {
      Element amqpListener = getElement(listeners, "amqp");
      if (amqpListener == null) {
        amqpListener = DocumentHelper.createElement("listener");
        amqpListener.addAttribute(UPGRADE_ATTRIBUTE, "true");
        amqpListener.addAttribute("name", "amqp");
        Element haList = addElement(amqpListener, "host-access-list");
        listeners.add(amqpListener);
        System.out.println("+++ Added AMQP listener (plain) on port 5672");
      }
      Element amqpsListener = getElement(listeners, "amqps");
      if (amqpsListener == null) {
        amqpsListener = DocumentHelper.createElement("listener");
        amqpsListener.addAttribute(UPGRADE_ATTRIBUTE, "true");
        amqpsListener.addAttribute("name", "amqps");
        amqpsListener.addAttribute("connection-template", "ssl");
        amqpsListener.addAttribute("port", "5671");
        Element haList = addElement(amqpsListener, "host-access-list");
        listeners.add(amqpsListener);
        System.out.println("+++ Added AMQP listener (SSL) on port 5671");
      }
    } else
      System.out.println("+++ Don't add AMQP listeners");
    Element usage = addElement(amqp, "usage");

    // Threadpools
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    threadpool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "amqp.connection");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "5");
    pool.addAttribute("min-threads", "1");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$amqp.connection.service");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "amqp.session");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "50");
    pool.addAttribute("min-threads", "1");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$amqp.sasl.service");
    threads.add(thread);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$amqp.session.service");
    threads.add(thread);

    // Log Swiftlet
    Element log = UpgradeUtilities.getElement(root, "sys$log");
    Attribute logSinkDir = log.attribute("logsink-directory");
    if (logSinkDir == null) {
      log.addAttribute("logsink-directory", "./");
      log.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }

  }

  public static void convert900to922(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.2.2 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element storeSwiftlet = UpgradeUtilities.getElement(root, "sys$store");
      Element transactionLog = storeSwiftlet.element("transaction-log");
      if (transactionLog != null)
        transactionLog.addAttribute("force-sync-in-standalone-mode", "true");

      Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
      queueMgr.addAttribute("multi-queue-transaction-global-lock", "false");
    }

  }

  public static void convert922to930(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.3.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      hactl.addAttribute("split-brain-instance-action", "stop");

      Element confController = hactl.element("configuration-controller");
      Element repExcludes = confController.element("replication-excludes");
      repExcludes.addAttribute(UPGRADE_ATTRIBUTE, "true");
      Element repExclude = DocumentHelper.createElement("replication-exclude");
      repExclude.addAttribute(UPGRADE_ATTRIBUTE, "true");
      repExclude.addAttribute("name", "sys$hacontroller/split-brain-instance-action");
      repExcludes.add(repExclude);
    }

  }

  public static void convert931to940(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.4.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl == null) {
      // start order non-HA Router
      Attribute startOrder = root.attribute("startorder");
      String s = startOrder.getText();
      if (s.indexOf("sys$filecache") == -1)
        startOrder.setText(SwiftUtilities.replace(s, "sys$monitor", "sys$monitor sys$filecache"));
    } else {
      Element harouterEle = null;
      for (Iterator iter = root.elementIterator(); iter.hasNext(); ) {
        Element e = (Element) iter.next();
        if (e.getName().equals("ha-router")) {
          harouterEle = e;
          break;
        }
      }
      if (harouterEle != null) {
        for (Iterator iter = harouterEle.elementIterator(); iter.hasNext(); ) {
          Element e = (Element) iter.next();
          Attribute order = e.attribute("order");
          String s = order.getText();
          if (s.indexOf("sys$filecache") == -1)
            order.setText(SwiftUtilities.replace(s, "sys$monitor", "sys$monitor sys$filecache"));
        }
      } else
        System.err.println("Unable to locate element 'ha-router' in routerconfig.xml!");

      Element confController = hactl.element("configuration-controller");
      Element propSubs = confController.element("property-substitutions");
      Element propSub = DocumentHelper.createElement("property-substitution");
      propSub.addAttribute(UPGRADE_ATTRIBUTE, "true");
      propSub.addAttribute("name", "sys$filecache/caches/%/directory");
      propSub.addAttribute("substitute-with", "directory2");
      propSubs.add(propSub);
      propSub = DocumentHelper.createElement("property-substitution");
      propSub.addAttribute(UPGRADE_ATTRIBUTE, "true");
      propSub.addAttribute("name", "sys$filecache/caches/%/directory2");
      propSub.addAttribute("substitute-with", "directory");
      propSubs.add(propSub);

      // FileCache Swiftlet
      Element filecache = UpgradeUtilities.getElement(root, "sys$filecache");
      if (filecache == null) {
        filecache = DocumentHelper.createElement("swiftlet");
        filecache.addAttribute(UPGRADE_ATTRIBUTE, "true");
        filecache.addAttribute("name", "sys$filecache");
        root.add(filecache);
      }
      addElement(filecache, "caches");
      addElement(filecache, "usage");

      Element monitorSwiftlet = UpgradeUtilities.getElement(root, "sys$monitor");
      Element mailSettings = monitorSwiftlet.element("settingsmail");
      if (mailSettings != null) {
        mailSettings.addAttribute(UPGRADE_ATTRIBUTE, "true");
        mailSettings.addAttribute("mailserver-authentication-enabled", "false");
        mailSettings.addAttribute("mailserver-username", "");
        mailSettings.addAttribute("mailserver-password", "");
      }
    }

    Element queueManager = UpgradeUtilities.getElement(root, "sys$queuemanager");
    Element compQueues = queueManager.element("composite-queues");
    if (compQueues != null) {
      for (Iterator iter = compQueues.elementIterator(); iter.hasNext(); ) {
        Element cq = (Element) iter.next();
        cq.addAttribute(UPGRADE_ATTRIBUTE, "true");
        cq.addAttribute("default-delivery", "false");
      }
    }

    // Threadpools
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    threadpool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "filecache.request");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "5");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$filecache.request");
    threads.add(thread);

    pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "filecache.session");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "5");
    pools.add(pool);
    threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$filecache.session");
    threads.add(thread);

  }

  public static void convert940to942(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.4.2 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element routingSwiftlet = UpgradeUtilities.getElement(root, "sys$routing");
      Element listeners = routingSwiftlet.element("listeners");
      for (Iterator iter = listeners.elementIterator(); iter.hasNext(); ) {
        Element element = (Element) iter.next();
        if (element.attribute("use-xa") == null) {
          element.addAttribute(UPGRADE_ATTRIBUTE, "true");
          element.addAttribute("use-xa", "true");
        }
      }
      Element connectors = routingSwiftlet.element("connectors");
      for (Iterator iter = connectors.elementIterator(); iter.hasNext(); ) {
        Element element = (Element) iter.next();
        if (element.attribute("use-xa") == null) {
          element.addAttribute(UPGRADE_ATTRIBUTE, "true");
          element.addAttribute("use-xa", "true");
        }
      }
    }

  }

  public static void convert942to960(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.6.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element jms = UpgradeUtilities.getElement(root, "sys$jms");
      if (jms.attribute("allow-same-clientid") == null) {
        jms.addAttribute(UPGRADE_ATTRIBUTE, "true");
        jms.addAttribute("allow-same-clientid", "false");
      }

      Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
      Element queues = queueMgr.element("queues");
      if (queues != null) {
        for (Iterator iter = queues.elementIterator(); iter.hasNext(); ) {
          Element queue = (Element) iter.next();
          Attribute consMode = queue.attribute("consumer-mode");
          if (consMode == null)
            queue.addAttribute("consumer-mode", "shared");
        }
      }
      Element queueControllers = queueMgr.element("queue-controllers");
      if (queueControllers != null) {
        for (Iterator iter = queueControllers.elementIterator(); iter.hasNext(); ) {
          Element queueController = (Element) iter.next();
          Attribute consMode = queueController.attribute("consumer-mode");
          if (consMode == null)
            queueController.addAttribute("consumer-mode", "shared");
        }
      }
    }

  }

  public static void convert960to970(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.7.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
      Element queues = queueMgr.element("queues");
      if (queues != null) {
        for (Iterator iter = queues.elementIterator(); iter.hasNext(); ) {
          Element queue = (Element) iter.next();
          Attribute monTh = queue.attribute("monitor-alert-threshold");
          if (monTh == null) {
            queue.addAttribute("monitor-alert-threshold", "-1");
            queue.addAttribute(UPGRADE_ATTRIBUTE, "true");
          }
        }
      }
      Element queueControllers = queueMgr.element("queue-controllers");
      if (queueControllers != null) {
        for (Iterator iter = queueControllers.elementIterator(); iter.hasNext(); ) {
          Element queueController = (Element) iter.next();
          Attribute monTh = queueController.attribute("monitor-alert-threshold");
          if (monTh == null) {
            queueController.addAttribute("monitor-alert-threshold", "-1");
            queueController.addAttribute(UPGRADE_ATTRIBUTE, "true");
          }
        }
      }

      Element mgmtSwiftlet = UpgradeUtilities.getElement(root, "sys$mgmt");
      mgmtSwiftlet.addAttribute(UPGRADE_ATTRIBUTE, "true");
      mgmtSwiftlet.addAttribute("admin-roles-enabled", "false");
      Element roles = mgmtSwiftlet.element("queue-controllers");
      if (roles == null) {
        roles = DocumentHelper.createElement("roles");
        roles.addAttribute(UPGRADE_ATTRIBUTE, "true");
        mgmtSwiftlet.add(roles);

      }

    }

  }


  public static void convert970to973(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 9.7.3 ...");

    // Log Swiftlet
    Element log = UpgradeUtilities.getElement(root, "sys$log");
    Attribute numGen = log.attribute("number-old-logfile-generations");
    if (numGen == null) {
      log.addAttribute("number-old-logfile-generations", "50");
      log.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }

    // Trace Swiftlet
    Element trace = UpgradeUtilities.getElement(root, "sys$trace");
    Attribute numGenTrace = trace.attribute("number-old-tracefile-generations");
    if (numGenTrace == null) {
      trace.addAttribute("number-old-tracefile-generations", "50");
      trace.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }

  }


  public static void convert973to1000(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 10.0.0 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl == null) {
      // start order non-HA Router
      Attribute startOrder = root.attribute("startorder");
      String s = startOrder.getText();
      if (s.indexOf("sys$streams") == -1)
        startOrder.setText(SwiftUtilities.replace(s, "sys$filecache", "sys$filecache sys$streams"));
    } else {
      Element harouterEle = null;
      for (Iterator iter = root.elementIterator(); iter.hasNext(); ) {
        Element e = (Element) iter.next();
        if (e.getName().equals("ha-router")) {
          harouterEle = e;
          break;
        }
      }
      if (harouterEle != null) {
        for (Iterator iter = harouterEle.elementIterator(); iter.hasNext(); ) {
          Element e = (Element) iter.next();
          Attribute order = e.attribute("order");
          String s = order.getText();
          if (s.indexOf("sys$streams") == -1)
            order.setText(SwiftUtilities.replace(s, "sys$filecache", "sys$filecache sys$streams"));
        }
      } else
        System.err.println("Unable to locate element 'ha-router' in routerconfig.xml!");

      Element freezes = hactl.element("threadpool-freezes");
      Element freeze = DocumentHelper.createElement("threadpool-freeze");
      freeze.addAttribute(UPGRADE_ATTRIBUTE, "true");
      freeze.addAttribute("name", "00");
      freeze.addAttribute("poolname", "streams.processor");
      freezes.add(freeze);

      // FileCache Swiftlet
      Element streamSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
      if (streamSwiftlet == null) {
        streamSwiftlet = DocumentHelper.createElement("swiftlet");
        streamSwiftlet.addAttribute(UPGRADE_ATTRIBUTE, "true");
        streamSwiftlet.addAttribute("name", "sys$streams");
        root.add(streamSwiftlet);
      }
      addElement(streamSwiftlet, "domains");
      addElement(streamSwiftlet, "usage");
    }

    // Threadpools
    Element threadpool = UpgradeUtilities.getElement(root, "sys$threadpool");
    threadpool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    Element pools = threadpool.element("pools");

    Element pool = DocumentHelper.createElement("pool");
    pool.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.addAttribute("name", "streams.processor");
    pool.addAttribute("kernel-pool", "true");
    pool.addAttribute("max-threads", "-1");
    pools.add(pool);
    Element threads = DocumentHelper.createElement("threads");
    threads.addAttribute(UPGRADE_ATTRIBUTE, "true");
    pool.add(threads);
    Element thread = DocumentHelper.createElement("thread");
    thread.addAttribute(UPGRADE_ATTRIBUTE, "true");
    thread.addAttribute("name", "sys$streams.stream.processor");
    threads.add(thread);

  }

  public static void convert1000to1011(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 10.1.1 ...");

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element streamSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
      addElement(streamSwiftlet, "domains");
    }

  }

  private static void addStream(Element root, String domain, String pkg, String streamName, boolean enabled, String scriptName, Map<String, String> parm, List<String> depList) throws Exception {
    Element streamsSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
    if (streamsSwiftlet == null) {
      streamsSwiftlet = DocumentHelper.createElement("swiftlet");
      streamsSwiftlet.addAttribute(UPGRADE_ATTRIBUTE, "true");
      streamsSwiftlet.addAttribute("name", "sys$streams");
      root.add(streamsSwiftlet);
    }
    Element domains = streamsSwiftlet.element("domains");
    if (domains == null) {
      domains = DocumentHelper.createElement("domains");
      domains.addAttribute(UPGRADE_ATTRIBUTE, "true");
      streamsSwiftlet.add(domains);
    }
    Element d = UpgradeUtilities.getElement(domains, domain);
    if (d == null) {
      d = DocumentHelper.createElement("domain");
      d.addAttribute("name", domain);
      d.addAttribute(UPGRADE_ATTRIBUTE, "true");
      domains.add(d);
    }
    Element packages = d.element("packages");
    if (packages == null) {
      packages = d.addElement("packages");
      packages.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }
    Element p = UpgradeUtilities.getElement(packages, pkg);
    if (p == null) {
      p = DocumentHelper.createElement("package");
      p.addAttribute("name", pkg);
      p.addAttribute(UPGRADE_ATTRIBUTE, "true");
      packages.add(p);
    }
    Element streams = p.element("streams");
    if (streams == null) {
      streams = p.addElement("streams");
      streams.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }
    Element s = UpgradeUtilities.getElement(streams, streamName);
    if (s == null) {
      s = DocumentHelper.createElement("stream");
      s.addAttribute("name", streamName);
      s.addAttribute("enabled", String.valueOf(enabled));
      s.addAttribute("script-file", scriptName);
      s.addAttribute(UPGRADE_ATTRIBUTE, "true");
      Element parameters = s.element("parameters");
      if (parameters == null) {
        parameters = DocumentHelper.createElement("parameters");
        parameters.addAttribute(UPGRADE_ATTRIBUTE, "true");
      }
      if (parm != null) {
        for (Iterator<Map.Entry<String, String>> iter = parm.entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<String, String> entry = iter.next();
          Element parameter = DocumentHelper.createElement("parameter");
          parameter.addAttribute("name", entry.getKey());
          parameter.addAttribute("value", entry.getValue());
          parameter.addAttribute(UPGRADE_ATTRIBUTE, "true");
          parameters.add(parameter);
        }
      }
      s.add(parameters);
      addStreamDependency(s, depList);
      streams.add(s);
    }
  }

  private static void addStreamDependency(Element streamElement, List<String> depList) {
    if (streamElement == null)
      return;
    Element dependencies = streamElement.element("dependencies");
    if (dependencies == null) {
      dependencies = DocumentHelper.createElement("dependencies");
      dependencies.addAttribute(UPGRADE_ATTRIBUTE, "true");
    }
    if (depList != null) {
      for (int i = 0; i < depList.size(); i++) {
        Element dependency = DocumentHelper.createElement("dependency");
        dependency.addAttribute("name", depList.get(i));
        dependency.addAttribute(UPGRADE_ATTRIBUTE, "true");
        dependencies.add(dependency);
      }
    }
    streamElement.add(dependencies);
  }

  private static Element getStreamElement(Element root, String domain, String pkg, String streamName) throws Exception {
    Element streamsSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
    if (streamsSwiftlet == null)
      return null;
    Element domains = streamsSwiftlet.element("domains");
    if (domains == null)
      return null;
    Element d = UpgradeUtilities.getElement(domains, domain);
    if (d == null)
      return null;
    Element packages = d.element("packages");
    if (packages == null)
      return null;
    Element p = UpgradeUtilities.getElement(packages, pkg);
    if (p == null)
      return null;
    Element streams = p.element("streams");
    if (streams == null)
      return null;
    return UpgradeUtilities.getElement(streams, streamName);

  }

  public static void convert1011to1020(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 10.2.0 ...");

    Element queueMgr = UpgradeUtilities.getElement(root, "sys$queuemanager");
    Element queues = queueMgr.element("queues");
    Element queue = DocumentHelper.createElement("queue");
    queue.addAttribute("name", "streams_mailout");
    queue.addAttribute(UPGRADE_ATTRIBUTE, "true");
    queues.add(queue);

    Map<String, String> parm = new HashMap<String, String>();
    parm.put("default-from", "default from address");
    parm.put("default-to", "default to address (your admin)");
    parm.put("password", "mailserver password");
    parm.put("servername", "mailserver hostname");
    parm.put("username", "mailserver username");
    addStream(root, "swiftmq", "mail", "mailout", false, "../../adminstreams/mail/mailout.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("clientid-predicate", "%");
    parm.put("mailout-queue", "streams_mailout");
    addStream(root, "swiftmq", "monitor", "clientinactivity", false, "../../adminstreams/monitor/clientinactivity.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("clientid-predicate", "%");
    parm.put("durablename-predicate", "%");
    parm.put("check-interval-minutes", "60");
    parm.put("inactivity-timeout-days", "30");
    parm.put("mailout-queue", "streams_mailout");
    addStream(root, "swiftmq", "monitor", "durableinactivity", false, "../../adminstreams/monitor/durableinactivity.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("mailout-queue", "streams_mailout");
    addStream(root, "swiftmq", "monitor", "routingobserver", false, "../../adminstreams/monitor/routingobserver.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("cli-context", ".env/router-memory-list");
    parm.put("name-predicate", "%");
    parm.put("property", "free-memory");
    parm.put("threshold", "100000");
    parm.put("threshold-direction", "below");
    parm.put("mailout-queue", "streams_mailout");
    addStream(root, "swiftmq", "monitor", "threshold_memory", false, "../../adminstreams/monitor/threshold.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("cli-context", "sys$queuemanager/usage");
    parm.put("name-predicate", "%");
    parm.put("property", "messagecount");
    parm.put("threshold", "10000");
    parm.put("mailout-queue", "streams_mailout");
    addStream(root, "swiftmq", "monitor", "threshold_messagecounts", false, "../../adminstreams/monitor/threshold.js", parm, null);

    parm = new HashMap<String, String>();
    parm.put("input-queue", "streams_scheduler_input");
    parm.put("store-queue", "streams_scheduler_store");
    addStream(root, "swiftmq", "scheduler", "messagescheduler", true, "../../adminstreams/schedule/messagescheduler.js", parm, null);
  }

  public static void convert1020to1100(Element root) throws Exception {
    System.out.println("+++ Converting configuration to 11.0.0 ...");
    Element streamsSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
    if (streamsSwiftlet != null) {
      Element domains = streamsSwiftlet.element("domains");
      if (domains != null) {
        Element domain = UpgradeUtilities.getElement(domains, "swiftmq");
        if (domain != null) {
          Element packages = domain.element("packages");
          if (packages != null) {
            Element pkg = UpgradeUtilities.getElement(packages, "monitor");
            if (pkg != null)
              packages.remove(pkg);
          }
        }
      }
    }

      addStream(root, "swiftmq", "system", "routeannouncer", true, "../../adminstreams/system/routeannouncer.js", null, null);

      List<String> depList = new ArrayList<String>();
      depList.add("swiftmq.system.streamregistry");
      addStream(root, "swiftmq", "system", "streammonitor", false, "../../adminstreams/system/streammonitor.js", null, depList);
      depList = new ArrayList<String>();
      depList.add("swiftmq.system.routeannouncer");
      addStream(root, "swiftmq", "system", "streamregistry", true, "../../adminstreams/system/streamregistry.js", null, depList);

    Element hactl = UpgradeUtilities.getElement(root, "sys$hacontroller");
    if (hactl != null) {
      Element streamSwiftlet = UpgradeUtilities.getElement(root, "sys$streams");
      streamSwiftlet.addAttribute("stream-grant-predicates", "stream\\_%");
      streamSwiftlet.addAttribute(UPGRADE_ATTRIBUTE, "true");

    }
  }

  /*
 *     DO NOT CONVERT EXTENSION SWIFTLET CONFIG HERE!!
 *     IT MUST BE DONE WITHIN THE RESP. SWIFTLET!!
  */

}
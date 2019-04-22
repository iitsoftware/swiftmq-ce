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

package com.swiftmq.mgmt;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;

import java.io.FileInputStream;
import java.util.Iterator;

public class PreConfigurator {
    private static String OP = "_op";
    Document routerconfig;
    Document changes;

    public PreConfigurator(Document routerconfig, Document changes) {
        this.routerconfig = routerconfig;
        this.changes = changes;
    }

    private String getOp(Element element) throws Exception {
        if (element.attribute(OP) == null)
            return null;
        return element.attribute(OP).getValue();
    }

    private boolean hasName(String name, Element element) {
        for (Iterator<Attribute> iter = element.attributeIterator(); iter.hasNext(); ) {
            Attribute attribute = iter.next();
            if (attribute.getName().equals("name") && attribute.getValue().equals(name))
                return true;
        }
        return false;
    }

    private Element findElement(Element searchFor, Element searchIn) {
        Attribute nameAttr = searchFor.attribute("name");
        for (Iterator<Element> iter = searchIn.elementIterator(); iter.hasNext(); ) {
            Element child = iter.next();
            if (nameAttr != null) {
                if (hasName(nameAttr.getValue(), child))
                    return child;
            } else
                if (child.getName().equals(searchFor.getName()))
                    return child;
        }
        return null;
    }

    private void clearElements(Element root) {
        for (Iterator<Element> iter = root.elementIterator(); iter.hasNext(); ) {
            Element child = iter.next();
            child.detach();
        }
    }

    private void processElement(Element changeEle, Element configEle, boolean isParent) throws Exception {
        if (!isParent)
            processAttributes(changeEle, configEle);
        boolean goDeeper = true;
        String op = getOp(changeEle);
        if (op != null) {
            Element copy = changeEle.createCopy();
            switch (op) {
                case "clear":
                    if (!isParent)
                        clearElements(configEle);
                    break;
                case "add":
                    copy.remove(copy.attribute(OP));
                    if (!isParent)
                        configEle.getParent().add(copy);
                    else
                        configEle.add(copy);
                    goDeeper = false;
                    break;
                case "remove":
                    if (!isParent)
                        configEle.detach();
                    goDeeper = false;
                    break;
                case "replace":
                    if (!isParent) {
                        Element parent = configEle.getParent();
                        configEle.detach();
                        copy.remove(copy.attribute(OP));
                        parent.add(copy);
                    }
                    goDeeper = false;
                    break;
            }
        }
        if (goDeeper) {
            for (Iterator<Element> iter = changeEle.elementIterator(); iter.hasNext(); ) {
                Element changeChild = iter.next();
                Element configChild = findElement(changeChild, configEle);
                if (configChild != null)
                    processElement(changeChild, configChild, false);
                else
                    processElement(changeChild, configEle, true);
            }
        }
    }

    private void processAttributes(Element changeEle, Element configEle) throws Exception {
        for (Iterator<Attribute> iter = changeEle.attributeIterator(); iter.hasNext(); ) {
            Attribute attribute = iter.next();
            if (!attribute.getName().equals(OP)) {
                Attribute configAttribute = configEle.attribute(attribute.getName());
                if (configAttribute == null)
                    configEle.addAttribute(attribute.getName(), attribute.getValue());
                else
                    configAttribute.setValue(attribute.getValue());
            }
        }
    }

    public Document applyChanges() throws Exception {
        processAttributes(changes.getRootElement(), routerconfig.getRootElement());
        for (Iterator<Element> iter = changes.getRootElement().elementIterator(); iter.hasNext(); ) {
            Element changeSwiftlet = iter.next();
            if (!changeSwiftlet.getName().equals("swiftlet"))
                throw new Exception("Next element after 'router' must be a 'swiftlet' element!");
            Attribute name = changeSwiftlet.attribute("name");
            if (name == null)
                throw new Exception("Missing 'name' attribute in 'swiftlet' element!");
            Element configSwiftlet = XMLUtilities.getSwiftletElement(name.getValue(), routerconfig.getRootElement());
            if (configSwiftlet == null)
                throw new Exception("Swiftlet with name '" + name.getValue() + "' not found!");
            processElement(changeSwiftlet, configSwiftlet, false);
        }
        return routerconfig;
    }

    public static void main(String[] args) {
        try {
            Document routerConfig = XMLUtilities.createDocument(new FileInputStream(args[0]));
            Document changes = XMLUtilities.createDocument(new FileInputStream(args[1]));
            String outputFile = args[2];

            Document result = new PreConfigurator(routerConfig, changes).applyChanges();

            XMLUtilities.writeDocument(result, outputFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

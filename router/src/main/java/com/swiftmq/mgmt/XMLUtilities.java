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

import com.swiftmq.tools.util.ObjectCloner;
import com.swiftmq.util.SwiftUtilities;
import com.swiftmq.util.UpgradeUtilities;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.util.*;

public class XMLUtilities
{
  private static HashMap possibleAttr = new HashMap();
  private static HashMap mandatoryAttr = new HashMap();
  private static HashMap possibleEle = new HashMap();
  private static HashMap mandatoryEle = new HashMap();
  private static MgmtFactory factory = new MgmtFactory();

  static
  {
    // Element: swiftlet
    HashSet set = new HashSet();
    set.add("name");
    set.add("displayname");
    set.add("class");
    set.add("icon");
    set.add("description");
    set.add("release");
    set.add("vendor");
    possibleAttr.put("swiftlet", set);
    set = new HashSet();
    set.add("name");
    set.add("displayname");
    set.add("class");
    set.add("description");
    set.add("release");
    set.add("vendor");
    mandatoryAttr.put("swiftlet", set);
    set = new HashSet();
    set.add("configuration");
    set.add("cli");
    possibleEle.put("swiftlet", set);
    set = new HashSet();
    set.add("configuration");
    mandatoryEle.put("swiftlet", set);

    // Element: configuration
    set = new HashSet();
    set.add("property");
    set.add("entity");
    set.add("entitylist");
    possibleEle.put("configuration", set);

    // Element: usage
    set = new HashSet();
    set.add("property");
    set.add("entity");
    set.add("entitylist");
    possibleEle.put("usage", set);

    // Element: property
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    set.add("type");
    set.add("min");
    set.add("max");
    set.add("default");
    set.add("choice");
    set.add("mandatory");
    set.add("reboot-required");
    set.add("read-only");
    possibleAttr.put("property", set);
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    set.add("type");
    mandatoryAttr.put("property", set);

    // Element: entity
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    set.add("icon");
    possibleAttr.put("entity", set);
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    mandatoryAttr.put("entity", set);
    set = new HashSet();
    set.add("property");
    set.add("entity");
    set.add("entitylist");
    possibleEle.put("entity", set);

    // Element: entitylist
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    set.add("icon");
    set.add("dynamic-properties");
    possibleAttr.put("entitylist", set);
    set = new HashSet();
    set.add("name");
    set.add("display");
    set.add("description");
    mandatoryAttr.put("entitylist", set);
    set = new HashSet();
    set.add("entitytemplate");
    possibleEle.put("entitylist", set);
    set = new HashSet();
    set.add("entitytemplate");
    mandatoryEle.put("entitylist", set);

    // Element: cli
    set = new HashSet();
    set.add("before-install");
    set.add("after-remove");
    possibleEle.put("cli", set);
  }

  ;

  private static void checkElement(Element element) throws Exception
  {
    HashSet set = (HashSet) possibleAttr.get(element.getName());
    if (set != null)
    {
      // Check possible attributes
      for (Iterator iter = element.attributeIterator(); iter.hasNext();)
      {
        Attribute a = (Attribute) iter.next();
        if (!set.contains(a.getName()))
          throw new Exception("Invalid attribute '" + a.getName() + "' for element '" + element.getName() + "'");
      }
      // Check the mandatory attributes
      set = (HashSet) mandatoryAttr.get(element.getName());
      if (set != null)
      {
        for (Iterator iter = set.iterator(); iter.hasNext();)
        {
          String mName = (String) iter.next();
          boolean found = false;
          for (Iterator iter2 = element.attributeIterator(); iter2.hasNext();)
          {
            Attribute a = (Attribute) iter2.next();
            if (mName.equals(a.getName()))
            {
              found = true;
              break;
            }
          }
          if (!found)
            throw new Exception("Missing mandatory attribute '" + mName + "' for element '" + element.getName() + "'");
        }
      }
    }
    //Check possible elements
    set = (HashSet) possibleEle.get(element.getName());
    if (set != null)
    {
      for (Iterator iter = element.elementIterator(); iter.hasNext();)
      {
        Element e = (Element) iter.next();
        if (!set.contains(e.getName()))
          throw new Exception("Invalid sub-element '" + e.getName() + "' for element '" + element.getName() + "'");
      }
      // Check the mandatory elements
      set = (HashSet) mandatoryEle.get(element.getName());
      if (set != null)
      {
        for (Iterator iter = set.iterator(); iter.hasNext();)
        {
          String mName = (String) iter.next();
          boolean found = false;
          for (Iterator iter2 = element.elementIterator(); iter2.hasNext();)
          {
            Element e = (Element) iter2.next();
            if (mName.equals(e.getName()))
            {
              found = true;
              break;
            }
          }
          if (!found)
            throw new Exception("Missing mandatory sub-element '" + mName + "' for element '" + element.getName() + "'");
        }
      }
    }
  }

  private static MetaData createMetaData(Element root) throws Exception
  {
    checkElement(root);
    String name = root.attributeValue("name");
    String displayName = root.attributeValue("displayname");
    String clazz = root.attributeValue("class");
    String description = root.attributeValue("description");
    String release = root.attributeValue("release");
    String vendor = root.attributeValue("vendor");
    MetaData meta = new MetaData(displayName, vendor, release, description);
    meta.setName(name);
    meta.setClassName(clazz);
    return meta;
  }

  private static Property lookupProperty(Entity entity, StringTokenizer t)
  {
    String name = t.nextToken();
    if (t.hasMoreTokens())
    {
      Entity next = entity.getEntity(name);
      if (next == null)
        return null;
      return lookupProperty(next, t);
    } else
      return entity.getProperty(name);
  }

  private static void fillEntity(Element element, Entity entity, Entity root) throws Exception
  {
    for (Iterator iter = element.elementIterator(); iter.hasNext();)
    {
      Element e = (Element) iter.next();
      checkElement(e);
      if (e.getName().equals("property"))
      {
        Property prop = new Property(e.attributeValue("name"));
        prop.setType(Class.forName(e.attributeValue("type")));
        prop.setDisplayName(e.attributeValue("display"));
        prop.setDescription(e.attributeValue("description"));
        String s = e.attributeValue("min");
        if (s != null)
          prop.setMinValue((Comparable) prop.convertToType(prop.getType(), s));
        s = e.attributeValue("max");
        if (s != null)
          prop.setMaxValue((Comparable) prop.convertToType(prop.getType(), s));
        s = e.attributeValue("default");
        if (s != null)
          prop.setDefaultValue(prop.convertToType(prop.getType(), s));
        s = e.attributeValue("choice");
        if (s != null)
        {
          ArrayList al = new ArrayList();
          StringTokenizer t = new StringTokenizer(s, " ");
          while (t.hasMoreTokens())
            al.add(t.nextToken());
          prop.setPossibleValues(al);
        }
        prop.setRebootRequired(Boolean.valueOf(e.attributeValue("reboot-required")).booleanValue());
        prop.setReadOnly(Boolean.valueOf(e.attributeValue("read-only")).booleanValue());
        prop.setMandatory(Boolean.valueOf(e.attributeValue("mandatory")).booleanValue());
        entity.addProperty(prop.getName(), prop);
      } else if (e.getName().equals("entity"))
      {
        Entity newEntity = new Entity(e.attributeValue("name"),
            e.attributeValue("display"),
            e.attributeValue("description"),
            null);
        newEntity.setIconFilename(e.attributeValue("icon"));
        entity.addEntity(newEntity);
        fillEntity(e, newEntity, root);
      } else if (e.getName().equals("entitylist"))
      {
        Element ltElement = e.element("entitytemplate");
        checkElement(ltElement);
        Entity ltEntity = new Entity(ltElement.attributeValue("name"),
            ltElement.attributeValue("display"),
            ltElement.attributeValue("description"),
            null);
        ltEntity.setIconFilename(ltElement.attributeValue("icon"));
        fillEntity(ltElement, ltEntity, root);
        EntityList entityList = new EntityList(e.attributeValue("name"),
            e.attributeValue("display"),
            e.attributeValue("description"),
            null,
            ltEntity,
            true);
        entityList.setIconFilename(e.attributeValue("icon"));
        String s = e.attributeValue("dynamic-properties");
        if (s != null)
        {
          ArrayList al = new ArrayList();
          StringTokenizer t = new StringTokenizer(s, " ");
          while (t.hasMoreTokens())
            al.add(0, t.nextToken());
          entityList.setDynamicPropNames((String[]) al.toArray(new String[al.size()]));
        }
        entity.addEntity(entityList);
      }
    }
  }

  private static void markDynamic(Entity entity)
  {
    entity.setDynamic(true);
    Map pm = entity.getProperties();
    if (pm != null)
    {
      for (Iterator iter2 = pm.entrySet().iterator(); iter2.hasNext();)
      {
        Property p = (Property) ((Map.Entry) iter2.next()).getValue();
        p.setReadOnly(true);
      }
    }
    if (entity instanceof EntityList)
    {
      EntityList el = (EntityList) entity;
      markDynamic(el.getTemplate());
    }
    Map map = entity.getEntities();
    if (map != null)
    {
      for (Iterator iter = map.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        markDynamic(e);
      }
    }
  }

  public static Document createDocument(String xml) throws Exception
  {
    SAXReader saxReader = new SAXReader();
    return saxReader.read(new StringReader(xml));
  }

  public static Document createDocument(InputStream in) throws Exception
  {
    SAXReader saxReader = new SAXReader();
    return saxReader.read(in);
  }

  public static void writeDocument(Document document, String filename) throws Exception
  {
    // Pretty print the document to System.out
    OutputFormat format = OutputFormat.createPrettyPrint();
    format.setLineSeparator(System.getProperty("line.separator"));
    format.setNewlines(true);
    XMLWriter writer = new MyXMLWriter(new FileWriter(filename), format);
    writer.write(document);
    writer.flush();
    writer.close();
  }

  public static void writeDocument(Document document, StringWriter stringWriter) throws Exception
  {
    // Pretty print the document to System.out
    OutputFormat format = OutputFormat.createPrettyPrint();
    format.setLineSeparator(System.getProperty("line.separator"));
    format.setNewlines(true);
    XMLWriter writer = new MyXMLWriter(stringWriter, format);
    writer.write(document);
    writer.flush();
    writer.close();
  }

  public static Configuration createConfigurationTemplate(String xml) throws Exception
  {
    Configuration config = null;
    Document doc = createDocument(xml);
    Element root = doc.getRootElement();
    if (!root.getName().equals("swiftlet"))
      throw new Exception("root element must be 'swiftlet', found: " + root.getName());
    config = new Configuration(createMetaData(root));
    config.setIconFilename(root.attributeValue("icon"));
    Element configElement = root.element("configuration");
    checkElement(configElement);
    fillEntity(configElement, config, config);
    Entity usage = config.getEntity("usage");
    if (usage != null)
      markDynamic(usage);
    return config;
  }

  public static List getCLICommands(String xml, String phase) throws Exception
  {
    Document doc = createDocument(xml);
    Element root = doc.getRootElement();
    Element cliElement = root.element("cli");
    if (cliElement == null)
      return null;
    checkElement(cliElement);
    Element phaseElement = cliElement.element(phase);
    if (phaseElement == null || phaseElement.getText() == null ||
        phaseElement.getText().trim().length() == 0)
      return null;
    List list = new ArrayList();
    StringTokenizer t = new StringTokenizer(phaseElement.getText(), "\n");
    while (t.hasMoreTokens())
      list.add(t.nextToken().trim());
    return list;
  }

  private static String getNodeValue(Element node, String name) throws Exception
  {
    if (node == null)
      return null;
    Attribute a = node.attribute(name);
    if (a != null)
      return a.getValue();
    Element e = node.element(name);
    if (e != null)
      return e.getText();
    return null;
  }

  private static Property fillProperty(Property template, Element node)
      throws Exception
  {
    String s = template.getName();
    String v = getNodeValue(node, s);
    if (v == null && template.isMandatory() && template.getDefaultValue() == null)
      throw new Exception("Missing mandatory property: " + s + " (no default value given)");
    Property p = null;
    try
    {
      p = (Property) ObjectCloner.copy(template, factory);
      if (v == null)
        p.setValue(p.getDefaultValue());
      else
      {
        Object value = v;
        Class type = p.getType();
        if (type == Boolean.class)
          value = Boolean.valueOf(v);
        else if (type == Integer.class)
          value = Integer.valueOf(v);
        else if (type == Long.class)
          value = Long.valueOf(v);
        else if (type == Double.class)
          value = Double.valueOf(v);
        else if (type == Float.class)
          value = Float.valueOf(v);
        p.setValue(value);
      }
    } catch (Exception e)
    {
      throw new Exception("Exception occurred while creating property '" + s +
          "': " + e);
    }
    return p;
  }

  private static EntityList fillEntityList(EntityList listTemplate, Element node)
      throws Exception
  {
    Entity template = listTemplate.getTemplate();
    if (template == null)
      throw new Exception("Missing entity template for entity list: " + listTemplate.getName());
    EntityList list = (EntityList) ObjectCloner.copy(listTemplate, factory);
    if (!list.isDynamic() && node != null)
    {
      for (Iterator iter = node.elementIterator(); iter.hasNext();)
      {
        Element e = (Element) iter.next();
        if (!e.getName().equals(template.getName()))
          throw new Exception("Invalid element found: " + e.getName() + ", expected: " + template.getName());
        Entity entry = fillEntity(template, e);
        String name = e.attributeValue("name");
        if (name == null)
          throw new Exception("Missing 'name' attribute for element: " + e.getName());
        entry.setName(name);
        list.addEntity(entry);
      }
    }
    list.createCommands();
    if (node != null)
      list.setUpgrade(node.attribute(UpgradeUtilities.UPGRADE_ATTRIBUTE) != null);
    return list;
  }

  private static Entity fillEntity(Entity template, Element node) throws Exception
  {
    Entity entity = (Entity) ObjectCloner.copy(template, factory);
    Map m = template.getProperties();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Property p = (Property) ((Map.Entry) iter.next()).getValue();
        Property pn = fillProperty(p, node);
        if (pn != null)
          entity.addProperty(pn.getName(), pn);
      }
    }
    m = template.getEntities();
    if (m.size() > 0)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        if (e instanceof EntityList)
          entity.addEntity(fillEntityList((EntityList) e, node == null ? null : node.element(e.getName())));
        else
          entity.addEntity(fillEntity(e, node == null ? null : node.element(e.getName())));
      }
    }
    entity.createCommands();
    if (node != null)
      entity.setUpgrade(node.attribute(UpgradeUtilities.UPGRADE_ATTRIBUTE) != null);
    return entity;
  }

  public static void loadIcons(Entity entity, ClassLoader loader) throws Exception
  {
    String fn = entity.getIconFilename();
    if (fn != null)
    {
      InputStream in = loader.getResourceAsStream(fn);
      if (in == null && fn.startsWith("/"))
        in = loader.getResourceAsStream(fn.substring(1));
      if (in != null)
        entity.setImageArray(SwiftUtilities.loadImageAsBytes(in));
    }
    Map m = entity.getEntities();
    if (m != null)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        loadIcons(e, loader);
        if (e instanceof EntityList)
          loadIcons(((EntityList) e).getTemplate(), loader);
      }
    }
  }

  private static Element getSwiftletElement(String searched, Element root) throws Exception
  {
    for (Iterator iter = root.elementIterator(); iter.hasNext();)
    {
      Element e = (Element) iter.next();
      if (e.getName().equals("swiftlet"))
      {
        String name = e.attributeValue("name");
        if (name == null)
          throw new Exception("Missing 'name' attribute for element: " + e.getName());
        if (name.equals(searched))
          return e;
      }
    }
    return null;
  }

  public static Configuration fillConfiguration(Configuration template, String xml) throws Exception
  {
    return fillConfiguration(template, createDocument(xml));
  }

  public static Configuration fillConfiguration(Configuration template, Document doc) throws Exception
  {
    Element root = doc.getRootElement();
    MetaData meta = template.getMetaData();
    Element swiftletNode = getSwiftletElement(meta.getName(), root);
    Configuration config = null;
    if (swiftletNode == null)
    {
      config = (Configuration) ObjectCloner.copy(template, factory);
      config.createCommands();
    } else
      config = (Configuration) fillEntity(template, swiftletNode);
    config.setMetaData(meta);
    return config;
  }

  private static void fillAttributes(Element node, Entity entity, boolean addName) throws Exception
  {
    if (addName)
    {
      node.addAttribute("name", entity.getName());
    }
    Map m = entity.getProperties();
    if (m != null)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Property p = (Property) ((Map.Entry) iter.next()).getValue();
        if (p.isStorable() &&
            p.getValue() != null &&
            (p.getDefaultValue() != null &&
                !p.getValue().equals(p.getDefaultValue())
                || p.getDefaultValue() == null))
        {
          node.addAttribute(p.getName(), p.getValue().toString());
        }
      }
    }
  }

  private static void entityToXML(Element parent, String tagName, Entity entity, boolean addName) throws Exception
  {
    Element node = DocumentHelper.createElement(addName ? tagName : entity.getName());
    fillAttributes(node, entity, addName);
    Map m = entity.getEntities();
    if (m != null)
    {
      String tplTagName = entity.getName();
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        if (e instanceof EntityList)
          entityListToXML(node, tplTagName, (EntityList) e);
        else
          entityToXML(node, tplTagName, e, false);
      }
    }
    parent.add(node);
  }

  private static void entityListToXML(Element parent, String tagName, EntityList entity) throws Exception
  {
    Element node = DocumentHelper.createElement(entity.getName());
    fillAttributes(node, entity, false);
    Map m = entity.getEntities();
    if (m != null)
    {
      String tplTagName = entity.getTemplate().getName();
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        if (e instanceof EntityList)
          entityListToXML(node, tplTagName, (EntityList) e);
        else
          entityToXML(node, tplTagName, e, true);
      }
    }
    parent.add(node);
  }

  public static void configToXML(Configuration config, Element root) throws Exception
  {
    MetaData meta = config.getMetaData();
    root.add(DocumentHelper.createComment("  " + meta.getDisplayName() + ", Release: " + meta.getVersion() + "  "));
    Element node = DocumentHelper.createElement("swiftlet");
    fillAttributes(node, config, true);
    Map m = config.getEntities();
    if (m != null)
    {
      for (Iterator iter = m.entrySet().iterator(); iter.hasNext();)
      {
        Entity e = (Entity) ((Map.Entry) iter.next()).getValue();
        if (!e.getName().equals(".metadata") && !e.isDynamic())
        {
          if (e instanceof EntityList)
            entityListToXML(node, e.getName(), (EntityList) e);
          else
            entityToXML(node, e.getName(), e, false);
        }
      }
    }
    root.add(node);
  }

  public static void elementToXML(Element ele, Element root) throws Exception
  {
    Element node = DocumentHelper.createElement(ele.getName());
    for (Iterator iter = ele.attributeIterator(); iter.hasNext();)
    {
      Attribute attr = (Attribute) iter.next();
      node.addAttribute(attr.getName(), attr.getValue());
    }
    for (Iterator iter = ele.elementIterator(); iter.hasNext();)
    {
      Element e = (Element) iter.next();
      elementToXML(e, node);
    }
    root.add(node);
  }

  private static class MyXMLWriter extends XMLWriter
  {
    public MyXMLWriter(Writer writer, OutputFormat format)
    {
      super(writer, format);
    }

    protected void writeComment(String text) throws IOException
    {
      writePrintln();
      super.writeComment(text);
    }
  }
}

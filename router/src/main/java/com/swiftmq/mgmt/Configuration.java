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

import com.swiftmq.tools.dump.Dumpalizer;

import java.io.*;


/**
 * A Configuration that extends Entity and contains the complete configuration of
 * a single Swiftlet. That is, every defined Entities, a "usage" Entity if configured,
 * and an Entity with name ".metadata" which contains the meta data of the Swiftlet. The
 * Configuration is filled by the SwiftletManager and passed to the Swiftlet as a parameter
 * to the <code>startup()</code> method.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2002, All Rights Reserved
 */
public class Configuration extends Entity
{
  public final static String ENV_ENTITY = ".env";
  public final static String META_ENTITY = ".metadata";

  MetaData meta = null;
  boolean extension = false;


  /**
   * Create a new Configuration.
   * Used by the SwiftletManager.
   * @param metaData meta data object.
   */
  public Configuration(MetaData metaData)
  {
    super(metaData.getName(), metaData.getDisplayName(), metaData.getDescription(), null);
    try
    {
      addEntity(createMetaEntity(metaData));
    } catch (Exception ignored)
    {
    }
    meta = metaData;
  }

  public Configuration()
  {
  }

  public int getDumpId()
  {
    return MgmtFactory.CONFIGURATION;
  }

  public void writeContent(DataOutput out)
    throws IOException
  {
    super.writeContent(out);
    if (meta != null)
    {
      out.writeByte(1);
      Dumpalizer.dump(out,meta);
    } else
      out.writeByte(0);
    out.writeBoolean(extension);
  }

  public void readContent(DataInput in)
    throws IOException
  {
    super.readContent(in);
    byte set = in.readByte();
    if (set == 1)
      meta = (MetaData)Dumpalizer.construct(in,new MgmtFactory());
    extension = in.readBoolean();
  }

  /**
   * Creates an Entity from a MetaData object.
   * Internal use only.
   * @param metaData meta data.
   * @return entity.
   */
  public static Entity createMetaEntity(MetaData metaData)
  {
    Entity entity = new Entity(META_ENTITY, "Swiftlet Meta Data", "Describes this Swiftlet", null);
    try
    {
      Property prop = new Property("name");
      prop.setType(String.class);
      prop.setDisplayName("Name");
      prop.setDescription("Swiftlet Name");
      prop.setValue(metaData.getName());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      prop = new Property("displayname");
      prop.setType(String.class);
      prop.setDisplayName("Display Name");
      prop.setDescription("Display Name of this Swiftlet");
      prop.setValue(metaData.getDisplayName());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      prop = new Property("description");
      prop.setType(String.class);
      prop.setDisplayName("Description");
      prop.setDescription("Description of this Swiftlet");
      prop.setValue(metaData.getDescription());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      prop = new Property("vendor");
      prop.setType(String.class);
      prop.setDisplayName("Vendor");
      prop.setDescription("Vendor of this Swiftlet");
      prop.setValue(metaData.getVendor());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      prop = new Property("version");
      prop.setType(String.class);
      prop.setDisplayName("Version");
      prop.setDescription("Version of this Swiftlet");
      prop.setValue(metaData.getVersion());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      prop = new Property("class");
      prop.setType(String.class);
      prop.setDisplayName("Class");
      prop.setDescription("Class Name of this Swiftlet");
      prop.setValue(metaData.getClassName());
      prop.setReadOnly(true);
      prop.setStorable(false);
      entity.addProperty(prop.getName(), prop);
      entity.createCommands();
    } catch (Exception ignored)
    {
    }
    return entity;
  }


  /**
   * Create a MetaData from an Entity.
   * Internal use only.
   * @param entity entity.
   * @return meta data.
   */
  public MetaData createMetaData(Entity entity)
  {
    String name = null;
    String className = null;
    String displayName = null;
    String vendor = null;
    String version = null;
    String description = null;
    try
    {
      name = (String) entity.getProperty("name").getValue();
      className = (String) entity.getProperty("class").getValue();
      displayName = (String) entity.getProperty("displayname").getValue();
      vendor = (String) entity.getProperty("vendor").getValue();
      version = (String) entity.getProperty("version").getValue();
      description = (String) entity.getProperty("description").getValue();
    } catch (Exception ignored)
    {
    }
    MetaData meta = new MetaData(displayName, vendor, version, description);
    meta.setName(name);
    meta.setClassName(className);
    return meta;
  }


  /**
   * Set the meta data.
   * Internal use only.
   * @param metaData meta data.
   */
  public void setMetaData(MetaData metaData)
  {
    try
    {
      addEntity(createMetaEntity(metaData));
    } catch (Exception ignored)
    {
    }
  }


  /**
   * Returns the meta data.
   * Internal use only.
   * @return meta data.
   */
  public MetaData getMetaData()
  {
    //  return createMetaData(getEntity(META_ENTITY));
    return meta;
  }


  /**
   * Sets whether this Swiftlet is an Extension Swiftlet.
   * Internal use only.
   * @param extension true/false.
   */
  public void setExtension(boolean extension)
  {
    this.extension = extension;
  }


  /**
   * Returns whether this Swiftlet is an Extension Swiftlet.
   * Internal use only.
   * @return true/false.
   */
  public boolean isExtension()
  {
    return extension;
  }
}


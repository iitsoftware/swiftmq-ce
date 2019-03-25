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

package com.swiftmq.tools.prop;

import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * Access class for structured properties. The structure is builded by sections.
 * A section is recognized if their property name ends with "<code>.names</code>".
 * The part before the ".names" is called the "section prefix".<BR><BR>
 *
 * <u>Example:</u><BR><BR>
 * <code>ima.comm.partner.names=partner1,partner2,partner3</code><BR>
 * (Section prefix in this case is "<code>ima.comm.partner</code>") <BR><BR>
 * <p>
 * As value of the sectipn property a list of section elements could defined.
 * The values are separated by comma.
 * <BR><BR>
 * For every section element further properties could be defined. The names of that
 * properties are a concatenation of the prefix followed by the name of the section
 * element and the property name.
 * <BR><BR>
 * <u>Example:</u><BR><BR>
 * <code>ima.comm.partner.partner1.host=www.iit.de</code><BR>
 * <code>ima.comm.partner.partner1.port=5000</code>.
 *
 * @author Andreas Mueller, IIT GmbH
 * @version 1.0
 */
public class StructuredProperties {
    public static final String SECTION_QUALIFIER = ".names";
    Properties baseProperties = null;

    /**
     * Wraps StructuredProperties around Properties.
     *
     * @param baseProperties Basis-Properties
     * @SBGen Constructor assigns baseProperties
     */
    public StructuredProperties(Properties baseProperties) {
        // SBgen: Assign variable
        this.baseProperties = baseProperties;
    }

    /**
     * Returns the base properties
     *
     * @return base properties
     * @SBGen Method get baseProperties
     */
    public Properties getBaseProperties() {
        // SBgen: Get variable
        return (baseProperties);
    }

    /**
     * Returns an array with all defined sections
     *
     * @return defined sections
     */
    public String[] getSections() {
        Vector vec = new Vector();
        Enumeration e = baseProperties.propertyNames();
        while (e.hasMoreElements()) {
            String propName = (String) e.nextElement();
            if (propName.endsWith(SECTION_QUALIFIER)) {
                vec.addElement(propName.substring(0, propName.indexOf(SECTION_QUALIFIER)));
            }
        }
        String[] rArray = new String[vec.size()];
        for (int i = 0; i < vec.size(); i++)
            rArray[i] = (String) vec.elementAt(i);

        return rArray;
    }

    /**
     * Checks if a section with that prefix is defined
     *
     * @param sectionPrefix SectionPrefix
     */
    public boolean hasSection(String sectionPrefix) {
        return baseProperties.get(sectionPrefix + SECTION_QUALIFIER) != null;
    }

    /**
     * Returns all section elements for the given section
     *
     * @param sectionPrefix SectionPrefix
     * @return section elements
     */
    public String[] getSectionElements(String sectionPrefix) {
        String propValue = (String) baseProperties.get(sectionPrefix + SECTION_QUALIFIER);
        if (propValue == null)
            return null;
        StringTokenizer t = new StringTokenizer(propValue, ",");
        String[] rArray = new String[t.countTokens()];
        int i = 0;
        while (t.hasMoreTokens())
            rArray[i++] = t.nextToken();
        return rArray;
    }

    /**
     * Returns a property value<BR><BR>
     * <u>Example:</u><BR>
     * Is the property name <code>ima.telegram.fiapas.content</code>, we get the
     * value with<BR>
     * <code>getValue("ima.telegram","fiapas","content");</code>
     *
     * @param sectionPrefix  SectionPrefix
     * @param sectionElement Name of the SectionElement
     * @param propertyName   Name of the Property
     */
    public String getValue(String sectionPrefix, String sectionElement, String propertyName) {
        return (String) baseProperties.get(sectionPrefix + "." + sectionElement + "." + propertyName);
    }
}


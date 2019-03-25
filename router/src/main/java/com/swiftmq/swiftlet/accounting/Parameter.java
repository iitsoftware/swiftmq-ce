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

package com.swiftmq.swiftlet.accounting;


/**
 * A Parameter object contains meta data about accounting parameters. It is used from the Accounting Swiftlet
 * to verify user-specified parameter values before an accounting is started.
 *
 * @author IIT GmbH, Bremen/Germany, Copyright (c) 2000-2010, All Rights Reserved
 */
public class Parameter {
    String name = null;
    String description = null;
    String defaultValue = null;
    boolean mandatory = false;
    ParameterVerifier verifier = null;

    /**
     * Creates a new Parameter object.
     *
     * @param name         Name
     * @param description  Short Description
     * @param defaultValue optional default value
     * @param mandatory    whether it is mandatory or optional
     * @param verifier     optional verifier
     */
    public Parameter(String name, String description, String defaultValue, boolean mandatory, ParameterVerifier verifier) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.mandatory = mandatory;
        this.verifier = verifier;
    }

    /**
     * Returns the name.
     *
     * @return Name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the description.
     *
     * @return description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Returns the default value (null if no default is specified).
     *
     * @return default value
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Returns whether this parameter is optional or mandatory.
     *
     * @return true / false
     */
    public boolean isMandatory() {
        return mandatory;
    }

    /**
     * Returns the optional verifier.
     *
     * @return verifier
     */
    public ParameterVerifier getVerifier() {
        return verifier;
    }

    public String toString() {
        return "Parameter, name=" + name + ", description=" + description + ", defaultValue=" + defaultValue + ", mandatory=" + mandatory + ", verifier=" + verifier + "]";
    }
}
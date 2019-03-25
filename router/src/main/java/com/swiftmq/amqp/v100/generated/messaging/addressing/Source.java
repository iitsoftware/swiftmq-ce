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

package com.swiftmq.amqp.v100.generated.messaging.addressing;

import com.swiftmq.amqp.v100.generated.messaging.delivery_state.OutcomeFactory;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.OutcomeIF;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressFactory;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AddressIF;
import com.swiftmq.amqp.v100.generated.transport.definitions.Seconds;
import com.swiftmq.amqp.v100.types.*;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * </p><p>
 * For containers which do not implement address resolution (and do not admit spontaneous
 * link attachment from their partners) but are instead only used as producers of messages,
 * it is unnecessary to provide spurious detail on the source. For this purpose it is
 * possible to use a "minimal" source in which all the fields are left unset.
 * </p><p>
 * </p><p>
 * </p>
 *
 * @author IIT Software GmbH, Bremen/Germany, (c) 2012, All Rights Reserved
 * @version AMQP Version v100. Generation Date: Wed Apr 18 14:09:32 CEST 2012
 **/

public class Source extends AMQPList
        implements SourceIF {
    public static String DESCRIPTOR_NAME = "amqp:source:list";
    public static long DESCRIPTOR_CODE = 0x00000000L << 32 | 0x00000028L;

    public AMQPDescribedConstructor codeConstructor = new AMQPDescribedConstructor(new AMQPUnsignedLong(DESCRIPTOR_CODE), AMQPTypeDecoder.UNKNOWN);
    public AMQPDescribedConstructor nameConstructor = new AMQPDescribedConstructor(new AMQPSymbol(DESCRIPTOR_NAME), AMQPTypeDecoder.UNKNOWN);

    boolean dirty = false;

    AddressIF address = null;
    TerminusDurability durable = TerminusDurability.NONE;
    TerminusExpiryPolicy expiryPolicy = TerminusExpiryPolicy.SESSION_END;
    Seconds timeout = new Seconds(0L);
    AMQPBoolean dynamic = AMQPBoolean.FALSE;
    NodeProperties dynamicNodeProperties = null;
    DistributionModeIF distributionMode = null;
    FilterSet filter = null;
    OutcomeIF defaultOutcome = null;
    AMQPArray outcomes = null;
    AMQPArray capabilities = null;

    /**
     * Constructs a Source.
     *
     * @param initValue initial value
     * @throws error during initialization
     */
    public Source(List initValue) throws Exception {
        super(initValue);
        if (initValue != null)
            decode();
    }

    /**
     * Constructs a Source.
     */
    public Source() {
        dirty = true;
    }

    /**
     * Return whether this Source has a descriptor
     *
     * @return true/false
     */
    public boolean hasDescriptor() {
        return true;
    }

    /**
     * Accept method for a Source visitor.
     *
     * @param visitor Source visitor
     */
    public void accept(SourceVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Returns the optional Address field.
     *
     * @return Address
     */
    public AddressIF getAddress() {
        return address;
    }

    /**
     * Sets the optional Address field.
     *
     * @param address Address
     */
    public void setAddress(AddressIF address) {
        dirty = true;
        this.address = address;
    }

    /**
     * Returns the optional Durable field.
     *
     * @return Durable
     */
    public TerminusDurability getDurable() {
        return durable;
    }

    /**
     * Sets the optional Durable field.
     *
     * @param durable Durable
     */
    public void setDurable(TerminusDurability durable) {
        dirty = true;
        this.durable = durable;
    }

    /**
     * Returns the optional ExpiryPolicy field.
     *
     * @return ExpiryPolicy
     */
    public TerminusExpiryPolicy getExpiryPolicy() {
        return expiryPolicy;
    }

    /**
     * Sets the optional ExpiryPolicy field.
     *
     * @param expiryPolicy ExpiryPolicy
     */
    public void setExpiryPolicy(TerminusExpiryPolicy expiryPolicy) {
        dirty = true;
        this.expiryPolicy = expiryPolicy;
    }

    /**
     * Returns the optional Timeout field.
     *
     * @return Timeout
     */
    public Seconds getTimeout() {
        return timeout;
    }

    /**
     * Sets the optional Timeout field.
     *
     * @param timeout Timeout
     */
    public void setTimeout(Seconds timeout) {
        dirty = true;
        this.timeout = timeout;
    }

    /**
     * Returns the optional Dynamic field.
     *
     * @return Dynamic
     */
    public AMQPBoolean getDynamic() {
        return dynamic;
    }

    /**
     * Sets the optional Dynamic field.
     *
     * @param dynamic Dynamic
     */
    public void setDynamic(AMQPBoolean dynamic) {
        dirty = true;
        this.dynamic = dynamic;
    }

    /**
     * Returns the optional DynamicNodeProperties field.
     *
     * @return DynamicNodeProperties
     */
    public NodeProperties getDynamicNodeProperties() {
        return dynamicNodeProperties;
    }

    /**
     * Sets the optional DynamicNodeProperties field.
     *
     * @param dynamicNodeProperties DynamicNodeProperties
     */
    public void setDynamicNodeProperties(NodeProperties dynamicNodeProperties) {
        dirty = true;
        this.dynamicNodeProperties = dynamicNodeProperties;
    }

    /**
     * Returns the optional DistributionMode field.
     *
     * @return DistributionMode
     */
    public DistributionModeIF getDistributionMode() {
        return distributionMode;
    }

    /**
     * Sets the optional DistributionMode field.
     *
     * @param distributionMode DistributionMode
     */
    public void setDistributionMode(DistributionModeIF distributionMode) {
        dirty = true;
        this.distributionMode = distributionMode;
    }

    /**
     * Returns the optional Filter field.
     *
     * @return Filter
     */
    public FilterSet getFilter() {
        return filter;
    }

    /**
     * Sets the optional Filter field.
     *
     * @param filter Filter
     */
    public void setFilter(FilterSet filter) {
        dirty = true;
        this.filter = filter;
    }

    /**
     * Returns the optional DefaultOutcome field.
     *
     * @return DefaultOutcome
     */
    public OutcomeIF getDefaultOutcome() {
        return defaultOutcome;
    }

    /**
     * Sets the optional DefaultOutcome field.
     *
     * @param defaultOutcome DefaultOutcome
     */
    public void setDefaultOutcome(OutcomeIF defaultOutcome) {
        dirty = true;
        this.defaultOutcome = defaultOutcome;
    }

    /**
     * Returns the optional Outcomes field.
     *
     * @return Outcomes
     */
    public AMQPArray getOutcomes() {
        return outcomes;
    }

    /**
     * Sets the optional Outcomes field.
     *
     * @param outcomes Outcomes
     */
    public void setOutcomes(AMQPArray outcomes) {
        dirty = true;
        this.outcomes = outcomes;
    }

    /**
     * Returns the optional Capabilities field.
     *
     * @return Capabilities
     */
    public AMQPArray getCapabilities() {
        return capabilities;
    }

    /**
     * Sets the optional Capabilities field.
     *
     * @param capabilities Capabilities
     */
    public void setCapabilities(AMQPArray capabilities) {
        dirty = true;
        this.capabilities = capabilities;
    }

    /**
     * Returns the predicted size of this Source. The predicted size may be greater than the actual size
     * but it can never be less.
     *
     * @return predicted size
     */
    public int getPredictedSize() {
        int n;
        if (dirty) {
            AMQPDescribedConstructor _c = getConstructor();
            setConstructor(null);
            n = super.getPredictedSize();
            n += codeConstructor.getPredictedSize();
            n += (address != null ? address.getPredictedSize() : 1);
            n += (durable != null ? durable.getPredictedSize() : 1);
            n += (expiryPolicy != null ? expiryPolicy.getPredictedSize() : 1);
            n += (timeout != null ? timeout.getPredictedSize() : 1);
            n += (dynamic != null ? dynamic.getPredictedSize() : 1);
            n += (dynamicNodeProperties != null ? dynamicNodeProperties.getPredictedSize() : 1);
            n += (distributionMode != null ? distributionMode.getPredictedSize() : 1);
            n += (filter != null ? filter.getPredictedSize() : 1);
            n += (defaultOutcome != null ? defaultOutcome.getPredictedSize() : 1);
            n += (outcomes != null ? outcomes.getPredictedSize() : 1);
            n += (capabilities != null ? capabilities.getPredictedSize() : 1);
            setConstructor(_c);
        } else
            n = super.getPredictedSize();
        return n;
    }

    private AMQPArray singleToArray(AMQPType t) throws IOException {
        return new AMQPArray(t.getCode(), new AMQPType[]{t});
    }

    private void decode() throws Exception {
        List l = getValue();

        AMQPType t = null;
        int idx = 0;

        // Field: address
        // Type     : AddressIF, converted: AddressIF
        // Basetype : AddressIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : AddressFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            address = AddressFactory.create(t);

        // Field: durable
        // Type     : TerminusDurability, converted: TerminusDurability
        // Basetype : uint
        // Default  : TerminusDurability.NONE
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                durable = new TerminusDurability(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'durable' in 'Source' type: " + e);
        }

        // Field: expiryPolicy
        // Type     : TerminusExpiryPolicy, converted: TerminusExpiryPolicy
        // Basetype : symbol
        // Default  : TerminusExpiryPolicy.SESSION_END
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                expiryPolicy = new TerminusExpiryPolicy(((AMQPSymbol) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'expiryPolicy' in 'Source' type: " + e);
        }

        // Field: timeout
        // Type     : Seconds, converted: Seconds
        // Basetype : uint
        // Default  : 0
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                timeout = new Seconds(((AMQPUnsignedInt) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'timeout' in 'Source' type: " + e);
        }

        // Field: dynamic
        // Type     : boolean, converted: AMQPBoolean
        // Basetype : boolean
        // Default  : AMQPBoolean.FALSE
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                dynamic = (AMQPBoolean) t;
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'dynamic' in 'Source' type: " + e);
        }

        // Field: dynamicNodeProperties
        // Type     : NodeProperties, converted: NodeProperties
        // Basetype : map
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                dynamicNodeProperties = new NodeProperties(((AMQPMap) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'dynamicNodeProperties' in 'Source' type: " + e);
        }

        // Field: distributionMode
        // Type     : DistributionModeIF, converted: DistributionModeIF
        // Basetype : DistributionModeIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : DistributionModeFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            distributionMode = DistributionModeFactory.create(t);

        // Field: filter
        // Type     : FilterSet, converted: FilterSet
        // Basetype : map
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                filter = new FilterSet(((AMQPMap) t).getValue());
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'filter' in 'Source' type: " + e);
        }

        // Field: defaultOutcome
        // Type     : OutcomeIF, converted: OutcomeIF
        // Basetype : OutcomeIF
        // Default  : null
        // Mandatory: false
        // Multiple : false
        // Factory  : OutcomeFactory
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        if (t.getCode() != AMQPTypeDecoder.NULL)
            defaultOutcome = OutcomeFactory.create(t);

        // Field: outcomes
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: false
        // Multiple : true
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                outcomes = AMQPTypeDecoder.isArray(t.getCode()) ? (AMQPArray) t : singleToArray(t);
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'outcomes' in 'Source' type: " + e);
        }

        // Field: capabilities
        // Type     : symbol, converted: AMQPSymbol
        // Basetype : symbol
        // Default  : null
        // Mandatory: false
        // Multiple : true
        // Factory  : ./.
        if (idx >= l.size())
            return;
        t = (AMQPType) l.get(idx++);
        try {
            if (t.getCode() != AMQPTypeDecoder.NULL)
                capabilities = AMQPTypeDecoder.isArray(t.getCode()) ? (AMQPArray) t : singleToArray(t);
        } catch (ClassCastException e) {
            throw new Exception("Invalid type of field 'capabilities' in 'Source' type: " + e);
        }
    }

    private void addToList(List list, Object value) {
        if (value != null)
            list.add(value);
        else
            list.add(AMQPNull.NULL);
    }

    private void encode() throws IOException {
        List l = new ArrayList();
        addToList(l, address);
        addToList(l, durable);
        addToList(l, expiryPolicy);
        addToList(l, timeout);
        addToList(l, dynamic);
        addToList(l, dynamicNodeProperties);
        addToList(l, distributionMode);
        addToList(l, filter);
        addToList(l, defaultOutcome);
        addToList(l, outcomes);
        addToList(l, capabilities);
        for (ListIterator iter = l.listIterator(l.size()); iter.hasPrevious(); ) {
            AMQPType t = (AMQPType) iter.previous();
            if (t.getCode() == AMQPTypeDecoder.NULL)
                iter.remove();
            else
                break;
        }
        setValue(l);
        dirty = false;
    }

    /**
     * Returns an array constructor (internal use)
     *
     * @return array constructor
     */
    public AMQPDescribedConstructor getArrayConstructor() throws IOException {
        if (dirty)
            encode();
        codeConstructor.setFormatCode(getCode());
        return codeConstructor;
    }

    public void writeContent(DataOutput out) throws IOException {
        if (dirty)
            encode();
        if (getConstructor() != codeConstructor) {
            codeConstructor.setFormatCode(getCode());
            setConstructor(codeConstructor);
        }
        super.writeContent(out);
    }

    public String getValueString() {
        try {
            if (dirty)
                encode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer b = new StringBuffer("[Source ");
        b.append(getDisplayString());
        b.append("]");
        return b.toString();
    }

    private String getDisplayString() {
        boolean _first = true;
        StringBuffer b = new StringBuffer();
        if (address != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("address=");
            b.append(address.getValueString());
        }
        if (durable != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("durable=");
            b.append(durable.getValueString());
        }
        if (expiryPolicy != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("expiryPolicy=");
            b.append(expiryPolicy.getValueString());
        }
        if (timeout != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("timeout=");
            b.append(timeout.getValueString());
        }
        if (dynamic != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("dynamic=");
            b.append(dynamic.getValueString());
        }
        if (dynamicNodeProperties != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("dynamicNodeProperties=");
            b.append(dynamicNodeProperties.getValueString());
        }
        if (distributionMode != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("distributionMode=");
            b.append(distributionMode.getValueString());
        }
        if (filter != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("filter=");
            b.append(filter.getValueString());
        }
        if (defaultOutcome != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("defaultOutcome=");
            b.append(defaultOutcome.getValueString());
        }
        if (outcomes != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("outcomes=");
            b.append(outcomes.getValueString());
        }
        if (capabilities != null) {
            if (!_first)
                b.append(", ");
            else
                _first = false;
            b.append("capabilities=");
            b.append(capabilities.getValueString());
        }
        return b.toString();
    }

    public String toString() {
        return "[Source " + getDisplayString() + "]";
    }
}

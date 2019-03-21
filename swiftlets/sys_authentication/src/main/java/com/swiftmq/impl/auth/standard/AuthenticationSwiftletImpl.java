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

package com.swiftmq.impl.auth.standard;

import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.SwiftletManager;
import com.swiftmq.swiftlet.auth.*;
import com.swiftmq.swiftlet.log.LogSwiftlet;
import com.swiftmq.swiftlet.queue.QueueManager;
import com.swiftmq.swiftlet.trace.TraceSpace;
import com.swiftmq.swiftlet.trace.TraceSwiftlet;
import com.swiftmq.util.SwiftUtilities;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.magicwerk.brownies.collections.GapList;

import java.util.*;

public class AuthenticationSwiftletImpl extends AuthenticationSwiftlet {
    static final String ANONYMOUS_USER = "anonymous";
    static final String PUBLIC_GROUP = "public";
    private static final String PROP_ENCRYPTED_PASSWORDS = "swiftmq.auth.encrypted.passwords";
    private static final String ENV_MASTER_PASSWORD = "SWIFTMQ_MASTER_PASSWORD";

    Configuration config = null;
    TraceSwiftlet traceSwiftlet = null;
    TraceSpace traceSpace = null;
    LogSwiftlet logSwiftlet = null;
    QueueManager queueManager = null;
    long actLoginId = 0;
    ResourceLimitGroup publicRLGroup = null;
    Group publicGroup = null;
    Map rlgroups = Collections.synchronizedMap(new HashMap());
    Map groups = Collections.synchronizedMap(new HashMap());
    Map users = Collections.synchronizedMap(new HashMap());
    boolean authenticationOff = true;
    String masterPassword = null;
    boolean useEncryption = false;
    List<AuthenticationDelegate> delegates = new GapList<AuthenticationDelegate>();

    private void logMessage(String message) {
        if (traceSpace.enabled) traceSpace.trace(getName(), message);
        logSwiftlet.logError(getName(), message);
    }

    private String decrypt(String encrypted) {
        if (useEncryption && encrypted != null) {
            StandardPBEStringEncryptor decryptor = new StandardPBEStringEncryptor();
            decryptor.setPassword(masterPassword);
            return decryptor.decrypt(encrypted);
        }
        return encrypted;
    }

    public String getPassword(String userName)
            throws AuthenticationException {
        if (userName == null)
            userName = ANONYMOUS_USER;
        User user = (User) users.get(userName);
        if (user == null) {
            logMessage("getPassword, unknown user: " + userName);
            throw new AuthenticationException("unknown user: " + userName);
        }
        return decrypt(user.getPassword());
    }

    public void verifyHostLogin(String userName, String hostname)
            throws AuthenticationException, ResourceLimitException {
        if (authenticationOff)
            return;
        if (userName == null)
            userName = ANONYMOUS_USER;
        User user = (User) users.get(userName);
        if (user == null) {
            logMessage("verifyHostLogin, unknown user: " + userName);
            throw new AuthenticationException("unknown user: " + userName);
        }
        if (!user.isHostAllowed(hostname)) {
            logMessage("verifyHostLogin, login from host '" + hostname + "' not granted for user: " + userName);
            throw new AuthenticationException("login from host '" + hostname + "' not granted for user: " + userName);
        }
        user.incNumberConnections();
    }

    public void logout(String userName, Object loginId) {
        if (authenticationOff)
            return;
        if (userName == null)
            userName = ANONYMOUS_USER;
        User user = (User) users.get(userName);
        if (user != null)
            user.decNumberConnections();
    }

    public void verifySetClientId(Object loginId)
            throws AuthenticationException {
    }

    public void verifyQueueCreation(Object loginId)
            throws AuthenticationException {
    }

    public void verifyQueueSenderSubscription(String queueName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyQueueSenderSubscription, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        QueueResourceGrant grant = group.getQueueResourceGrant(queueName);
        if (grant == null || !grant.isSenderGranted()) {
            QueueResourceGrant publicGrant = publicGroup.getQueueResourceGrant(queueName);
            if (publicGrant == null || !publicGrant.isSenderGranted()) {
                logMessage("verifyQueueSenderSubscription, sender subscription to queue '" + queueName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("sender subscription to queue '" + queueName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    public void verifyQueueReceiverSubscription(String queueName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyQueueReceiverSubscription, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        QueueResourceGrant grant = group.getQueueResourceGrant(queueName);
        if (grant == null || !grant.isReceiverGranted()) {
            QueueResourceGrant publicGrant = publicGroup.getQueueResourceGrant(queueName);
            if (publicGrant == null || !publicGrant.isReceiverGranted()) {
                logMessage("verifyQueueReceiverSubscription, receiver subscription to queue '" + queueName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("receiver subscription to queue '" + queueName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    public void verifyQueueBrowserCreation(String queueName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyQueueBrowserCreation, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        QueueResourceGrant grant = group.getQueueResourceGrant(queueName);
        if (grant == null || !grant.isBrowserGranted()) {
            QueueResourceGrant publicGrant = publicGroup.getQueueResourceGrant(queueName);
            if (publicGrant == null || !publicGrant.isBrowserGranted()) {
                logMessage("verifyQueueBrowserCreation, browser creation on queue '" + queueName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("browser creation on queue '" + queueName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    public void verifyTopicSenderSubscription(String topicName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        synchronized (delegates) {
            for (AuthenticationDelegate delegate : delegates) {
                if (delegate.isSendGranted(topicName))
                    return;
            }
        }
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyTopicSenderSubscription, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        TopicResourceGrant grant = group.getTopicResourceGrant(topicName);
        if (grant == null || !grant.isPublisherGranted()) {
            TopicResourceGrant publicGrant = publicGroup.getTopicResourceGrant(topicName);
            if (publicGrant == null || !publicGrant.isPublisherGranted()) {
                logMessage("verifyTopicSenderSubscription, publisher creation on topic '" + topicName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("publisher creation on topic '" + topicName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    public void verifyTopicReceiverSubscription(String topicName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        synchronized (delegates) {
            for (AuthenticationDelegate delegate : delegates) {
                if (delegate.isReceiveGranted(topicName))
                    return;
            }
        }
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyTopicReceiverSubscription, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        TopicResourceGrant grant = group.getTopicResourceGrant(topicName);
        if (grant == null || !grant.isSubscriberGranted()) {
            TopicResourceGrant publicGrant = publicGroup.getTopicResourceGrant(topicName);
            if (publicGrant == null || !publicGrant.isSubscriberGranted()) {
                logMessage("verifyTopicReceiverSubscription, subscriber creation on topic '" + topicName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("subscriber creation on topic '" + topicName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    public void verifyTopicDurableSubscriberCreation(String topicName, Object loginId)
            throws AuthenticationException {
        if (authenticationOff)
            return;
        synchronized (delegates) {
            for (AuthenticationDelegate delegate : delegates) {
                if (delegate.isDurableGranted(topicName))
                    return;
            }
        }
        LoginId id = (LoginId) loginId;
        User user = (User) users.get(id.getUserName());
        if (user == null) {
            logMessage("verifyTopicDurableSubscriberCreation, unknown user: " + id.getUserName());
            throw new AuthenticationException("unknown user: " + id.getUserName());
        }
        Group group = user.getGroup();
        TopicResourceGrant grant = group.getTopicResourceGrant(topicName);
        if (grant == null || !grant.isCreateDurableGranted()) {
            TopicResourceGrant publicGrant = publicGroup.getTopicResourceGrant(topicName);
            if (publicGrant == null || !publicGrant.isCreateDurableGranted()) {
                logMessage("verifyTopicDurableSubscriberCreation, create durable subscriber on topic '" + topicName + "' is not granted for user: " + id.getUserName());
                throw new AuthenticationException("create durable subscriber on topic '" + topicName + "' is not granted for user: " + id.getUserName());
            }
        }
    }

    @Override
    public void addTopicAuthenticationDelegate(AuthenticationDelegate authenticationDelegate) {
        synchronized (delegates) {
            delegates.add(authenticationDelegate);
        }
    }

    @Override
    public void removeTopicAuthenticationDelegate(AuthenticationDelegate authenticationDelegate) {
        synchronized (delegates) {
            delegates.remove(authenticationDelegate);
        }
    }

    protected Object createLoginId(String userName) {
        if (queueManager == null)
            queueManager = (QueueManager) SwiftletManager.getInstance().getSwiftlet("sys$queuemanager");
        String id = queueManager != null ? SwiftletManager.getInstance().getRouterName() + (actLoginId++) : String.valueOf(actLoginId++);
        return new LoginId(id, userName == null ? ANONYMOUS_USER : userName);
    }

    protected ResourceLimitGroup createResourceLimitGroup(String userName) {
        User user = (User) users.get(userName);
        if (user == null)
            return null;
        ResourceLimitGroup rlg = user.getResourceLimitGroup();
        return new ResourceLimitGroup(rlg.getName(),
                rlg.getMaxConnections(),
                rlg.getMaxSessions(),
                rlg.getMaxTempQueues(),
                rlg.getMaxProducers(),
                rlg.getMaxConsumers());
    }

    private QueueResourceGrant createQueueGrant(Group group, Entity queueGrantEntity) throws SwiftletException {
        String queueName = queueGrantEntity.getName();
        try {
            SwiftUtilities.verifyQueueName(queueName);
            if (queueName.indexOf('@') == -1)
                queueName += '@' + SwiftletManager.getInstance().getRouterName();
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        Property pSend = queueGrantEntity.getProperty("send-grant");
        boolean send = ((Boolean) pSend.getValue()).booleanValue();
        Property pRcv = queueGrantEntity.getProperty("receive-grant");
        boolean receive = ((Boolean) pRcv.getValue()).booleanValue();
        Property pBrowse = queueGrantEntity.getProperty("browse-grant");
        boolean browse = ((Boolean) pBrowse.getValue()).booleanValue();
        QueueResourceGrant grant = new QueueResourceGrant(queueName, receive, send, browse);
        group.addQueueResourceGrant(grant);

        pSend.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                QueueResourceGrant myGrant = (QueueResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (send grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setSenderGranted(((Boolean) newValue).booleanValue());
            }
        });
        pRcv.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                QueueResourceGrant myGrant = (QueueResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (receive grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setReceiverGranted(((Boolean) newValue).booleanValue());
            }
        });
        pBrowse.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                QueueResourceGrant myGrant = (QueueResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (browse grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setBrowserGranted(((Boolean) newValue).booleanValue());
            }
        });
        return grant;
    }

    private void createQueueGrants(Group group, EntityList queueGrantList) throws SwiftletException {
        Map r = queueGrantList.getEntities();
        if (r.size() > 0) {
            for (Iterator rIter = r.entrySet().iterator(); rIter.hasNext(); ) {
                Entity queueGrantEntity = (Entity) ((Map.Entry) rIter.next()).getValue();
                group.addQueueResourceGrant(createQueueGrant(group, queueGrantEntity));
            }
        }
        queueGrantList.setEntityAddListener(new EntityChangeAdapter(group) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                Group myGroup = (Group) configObject;
                String queueName = newEntity.getName();
                if (queueName.indexOf('@') == -1)
                    queueName += '@' + SwiftletManager.getInstance().getRouterName();
                if (myGroup.getQueueResourceGrant(queueName) != null)
                    throw new EntityAddException("Queue Grant for Queue '" + queueName + "' is already defined!");
                try {
                    QueueResourceGrant qg = createQueueGrant(myGroup, newEntity);
                    if (traceSpace.enabled)
                        traceSpace.trace(getName(), "onEntityAdd (queue grant): group=" + myGroup.getName() + ", new grant=" + qg);
                    myGroup.addQueueResourceGrant(qg);
                } catch (SwiftletException e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        queueGrantList.setEntityRemoveListener(new EntityChangeAdapter(group) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Group myGroup = (Group) configObject;
                String queueName = delEntity.getName();
                if (queueName.indexOf('@') == -1)
                    queueName += '@' + SwiftletManager.getInstance().getRouterName();
                QueueResourceGrant qg = myGroup.getQueueResourceGrant(queueName);
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityRemove (queue grant): group=" + myGroup.getName() + ", del grant=" + qg);
                myGroup.removeQueueResourceGrant(queueName);
            }
        });
    }

    private TopicResourceGrant createTopicGrant(Group group, Entity topicGrantEntity) throws SwiftletException {
        String topicName = topicGrantEntity.getName();
        Property pPub = topicGrantEntity.getProperty("publish-grant");
        boolean publish = ((Boolean) pPub.getValue()).booleanValue();
        Property pSub = topicGrantEntity.getProperty("subscribe-grant");
        boolean subscribe = ((Boolean) pSub.getValue()).booleanValue();
        Property pDur = topicGrantEntity.getProperty("durable-grant");
        boolean durable = ((Boolean) pDur.getValue()).booleanValue();
        TopicResourceGrant grant = new TopicResourceGrant(topicName, subscribe, publish, durable);

        pPub.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                TopicResourceGrant myGrant = (TopicResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (publish grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setPublisherGranted(((Boolean) newValue).booleanValue());
            }
        });
        pSub.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                TopicResourceGrant myGrant = (TopicResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (subscribe grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setSubscriberGranted(((Boolean) newValue).booleanValue());
            }
        });
        pDur.setPropertyChangeListener(new PropertyChangeAdapter(grant) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                TopicResourceGrant myGrant = (TopicResourceGrant) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (durable grant): grant=" + myGrant + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGrant.setCreateDurableGranted(((Boolean) newValue).booleanValue());
            }
        });
        return grant;
    }

    private void createTopicGrants(Group group, EntityList topicGrantList) throws SwiftletException {
        Map r = topicGrantList.getEntities();
        if (r.size() > 0) {
            for (Iterator rIter = r.entrySet().iterator(); rIter.hasNext(); ) {
                Entity topicGrantEntity = (Entity) ((Map.Entry) rIter.next()).getValue();
                group.addTopicResourceGrant(createTopicGrant(group, topicGrantEntity));
            }
        }
        topicGrantList.setEntityAddListener(new EntityChangeAdapter(group) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                Group myGroup = (Group) configObject;
                String topicName = newEntity.getName();
                if (myGroup.getTopicResourceGrant(topicName) != null)
                    throw new EntityAddException("Topic Grant for Topic '" + topicName + "' is already defined!");
                try {
                    TopicResourceGrant tg = createTopicGrant(myGroup, newEntity);
                    if (traceSpace.enabled)
                        traceSpace.trace(getName(), "onEntityAdd (topic grant): group=" + myGroup.getName() + ", new grant=" + tg);
                    myGroup.addTopicResourceGrant(tg);
                } catch (SwiftletException e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        topicGrantList.setEntityRemoveListener(new EntityChangeAdapter(group) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                Group myGroup = (Group) configObject;
                String topicName = delEntity.getName();
                TopicResourceGrant tg = myGroup.getTopicResourceGrant(topicName);
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityRemove (topic grant): group=" + myGroup.getName() + ", del grant=" + tg);
                myGroup.removeTopicResourceGrant(topicName);
            }
        });
    }

    private Group createGroup(String groupName, Entity groupEntity) throws SwiftletException {
        if (groups.get(groupName) != null)
            throw new SwiftletException("Group '" + groupName + "' is already defined!");
        Group group = new Group(groupName);

        createQueueGrants(group, (EntityList) groupEntity.getEntity("queue-grants"));
        createTopicGrants(group, (EntityList) groupEntity.getEntity("topic-grants"));

        if (traceSpace.enabled) traceSpace.trace(getName(), "createGroup: " + group);
        groups.put(group.getName(), group);
        if (group.getName().equals(PUBLIC_GROUP))
            publicGroup = group;
        return group;
    }

    private List getUsersForGroup(Group group) {
        ArrayList ulist = null;
        for (Iterator iter = users.entrySet().iterator(); iter.hasNext(); ) {
            User user = (User) ((Map.Entry) iter.next()).getValue();
            if (user.getGroup() == group) {
                if (ulist == null)
                    ulist = new ArrayList();
                ulist.add(user);
            }
        }
        return ulist;
    }

    private List getUsersForRLGroup(ResourceLimitGroup group) {
        ArrayList ulist = null;
        for (Iterator iter = users.entrySet().iterator(); iter.hasNext(); ) {
            User user = (User) ((Map.Entry) iter.next()).getValue();
            if (user.getResourceLimitGroup() == group) {
                if (ulist == null)
                    ulist = new ArrayList();
                ulist.add(user);
            }
        }
        return ulist;
    }

    private void createGroups(EntityList groupList) throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "creating groups ...");
        Map m = groupList.getEntities();
        if (m.size() > 0) {
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String groupName = (String) iter.next();
                Entity groupEntity = groupList.getEntity(groupName);
                createGroup(groupName, groupEntity);
            }
        }
        // Ensure to have always a public group
        if (publicGroup == null) {
            publicGroup = new Group(PUBLIC_GROUP); // empty public group
            groups.put(PUBLIC_GROUP, publicGroup);
        }

        groupList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String groupName = newEntity.getName();
                try {
                    createGroup(groupName, newEntity);
                    if (traceSpace.enabled) traceSpace.trace(getName(), "onEntityAdd (group): new group=" + groupName);
                } catch (SwiftletException e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        groupList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String groupName = delEntity.getName();
                if (groupName.equals(PUBLIC_GROUP))
                    throw new EntityRemoveException("Can't remove 'public' Group.");
                List ulist = getUsersForGroup((Group) groups.get(groupName));
                if (ulist != null) {
                    StringBuffer b = new StringBuffer();
                    b.append("Can't remove Group; it's assigned to User(s) ");
                    for (int i = 0; i < ulist.size(); i++) {
                        if (i > 0)
                            b.append(", ");
                        b.append(((User) ulist.get(i)).getName());
                    }
                    throw new EntityRemoveException(b.toString());
                }
                if (traceSpace.enabled) traceSpace.trace(getName(), "onEntityRemove (group): del group=" + groupName);
                groups.remove(groupName);
            }
        });
    }

    private ResourceLimitGroup createRLGroup(String groupName, Entity groupEntity) throws SwiftletException {
        if (rlgroups.get(groupName) != null)
            throw new SwiftletException("Resource Limit Group '" + groupName + "' is already defined!");
        Property maxConnectionsProp = groupEntity.getProperty("max-connections");
        Property maxSessionsProp = groupEntity.getProperty("max-sessions");
        Property maxTempQueuesProp = groupEntity.getProperty("max-tempqueues");
        Property maxProducersProp = groupEntity.getProperty("max-producers");
        Property maxConsumersProp = groupEntity.getProperty("max-consumers");
        ResourceLimitGroup group = new ResourceLimitGroup(groupName,
                ((Integer) maxConnectionsProp.getValue()).intValue(),
                ((Integer) maxSessionsProp.getValue()).intValue(),
                ((Integer) maxTempQueuesProp.getValue()).intValue(),
                ((Integer) maxProducersProp.getValue()).intValue(),
                ((Integer) maxConsumersProp.getValue()).intValue());

        maxConnectionsProp.setPropertyChangeListener(new PropertyChangeAdapter(group) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                ResourceLimitGroup myGroup = (ResourceLimitGroup) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (max connections): resource limit group=" + myGroup + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGroup.setMaxConnections(((Integer) newValue).intValue());
            }
        });

        maxSessionsProp.setPropertyChangeListener(new PropertyChangeAdapter(group) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                ResourceLimitGroup myGroup = (ResourceLimitGroup) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (max sessions): resource limit group=" + myGroup + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGroup.setMaxSessions(((Integer) newValue).intValue());
            }
        });

        maxTempQueuesProp.setPropertyChangeListener(new PropertyChangeAdapter(group) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                ResourceLimitGroup myGroup = (ResourceLimitGroup) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (max temp queues): resource limit group=" + myGroup + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGroup.setMaxTempQueues(((Integer) newValue).intValue());
            }
        });

        maxProducersProp.setPropertyChangeListener(new PropertyChangeAdapter(group) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                ResourceLimitGroup myGroup = (ResourceLimitGroup) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (max producers): resource limit group=" + myGroup + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGroup.setMaxProducers(((Integer) newValue).intValue());
            }
        });

        maxConsumersProp.setPropertyChangeListener(new PropertyChangeAdapter(group) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                ResourceLimitGroup myGroup = (ResourceLimitGroup) configObject;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (max consumers): resource limit group=" + myGroup + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myGroup.setMaxConsumers(((Integer) newValue).intValue());
            }
        });

        if (traceSpace.enabled) traceSpace.trace(getName(), "createResourceLimitGroup: " + group);
        rlgroups.put(group.getName(), group);
        if (group.getName().equals(PUBLIC_GROUP))
            publicRLGroup = group;
        return group;
    }

    private void createRLGroups(EntityList groupList) throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "creating resource limit groups ...");
        Map m = groupList.getEntities();
        Entity pgEntity = (Entity) m.get(PUBLIC_GROUP);
        if (pgEntity == null) {
            pgEntity = groupList.createEntity();
            pgEntity.setName(PUBLIC_GROUP);
            pgEntity.createCommands();
            try {
                groupList.addEntity(pgEntity);
            } catch (Exception e) {
                throw new SwiftletException(e.toString());
            }
            m = groupList.getEntities();
        }
        if (m.size() > 0) {
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String groupName = (String) iter.next();
                Entity groupEntity = groupList.getEntity(groupName);
                createRLGroup(groupName, groupEntity);
            }
        }
        // Ensure to have always a public group
        if (publicRLGroup == null) {
            publicRLGroup = new ResourceLimitGroup(PUBLIC_GROUP, -1, 10, 10, 10, 10);
            rlgroups.put(PUBLIC_GROUP, publicRLGroup);
        }
        groupList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String groupName = newEntity.getName();
                try {
                    createRLGroup(groupName, newEntity);
                    if (traceSpace.enabled)
                        traceSpace.trace(getName(), "onEntityAdd (resource limit group): new group=" + groupName);
                } catch (SwiftletException e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        groupList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String groupName = delEntity.getName();
                if (groupName.equals(PUBLIC_GROUP))
                    throw new EntityRemoveException("Can't remove 'public' Resource Limit Group.");
                List ulist = getUsersForRLGroup((ResourceLimitGroup) rlgroups.get(groupName));
                if (ulist != null) {
                    StringBuffer b = new StringBuffer();
                    b.append("Can't remove Resource Limit Group; it's assigned to User(s) ");
                    for (int i = 0; i < ulist.size(); i++) {
                        if (i > 0)
                            b.append(", ");
                        b.append(((User) ulist.get(i)).getName());
                    }
                    throw new EntityRemoveException(b.toString());
                }
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityRemove (resource limit group): del group=" + groupName);
                rlgroups.remove(groupName);
            }
        });
    }

    private void createHostAccessList(User user, EntityList haEntitiy) {
        Map h = haEntitiy.getEntities();
        if (h.size() > 0) {
            for (Iterator hIter = h.keySet().iterator(); hIter.hasNext(); ) {
                user.addHost((String) hIter.next());
            }
        }

        haEntitiy.setEntityAddListener(new EntityChangeAdapter(user) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                User myUser = (User) configObject;
                String predicate = newEntity.getName();
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityAdd (host access list): user=" + myUser + ",new host=" + predicate);
                myUser.addHost(predicate);
            }
        });
        haEntitiy.setEntityRemoveListener(new EntityChangeAdapter(user) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                User myUser = (User) configObject;
                String predicate = delEntity.getName();
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "onEntityRemove (host access list): user=" + myUser + ",del host=" + predicate);
                myUser.removeHost(predicate);
            }
        });
    }

    private User createUser(String userName, Entity userEntity) throws SwiftletException {
        if (users.get(userName) != null)
            throw new SwiftletException("User '" + userName + "' is already defined!");
        Property groupProp = userEntity.getProperty("group");
        String groupName = (String) groupProp.getValue();
        Group group = (Group) groups.get(groupName);
        if (group == null)
            throw new SwiftletException("User '" + userName + "', group '" + groupName + "' is unknown!");
        Property rlgroupProp = userEntity.getProperty("resource-limit-group");
        String rlgroupName = (String) rlgroupProp.getValue();
        ResourceLimitGroup rlgroup = (ResourceLimitGroup) rlgroups.get(rlgroupName);
        if (rlgroup == null)
            throw new SwiftletException("User '" + userName + "', resource limit group '" + rlgroupName + "' is unknown!");
        Property passwProp = userEntity.getProperty("password");
        String password = (String) passwProp.getValue();
        User user = new User(userName, password, group, rlgroup);
        EntityList gh = (EntityList) userEntity.getEntity("host-access-list");
        createHostAccessList(user, gh);
        if (traceSpace.enabled) traceSpace.trace(getName(), "createUser: " + user);
        users.put(user.getName(), user);

        groupProp.setPropertyChangeListener(new PropertyChangeAdapter(user) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                User myUser = (User) configObject;
                Group myGroup = (Group) groups.get(newValue);
                if (myGroup == null)
                    throw new PropertyChangeException("Group '" + (String) newValue + "' is unknown!");
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (group): user=" + myUser + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myUser.setGroup(myGroup);
            }
        });

        rlgroupProp.setPropertyChangeListener(new PropertyChangeAdapter(user) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                User myUser = (User) configObject;
                ResourceLimitGroup myGroup = (ResourceLimitGroup) rlgroups.get(newValue);
                if (myGroup == null)
                    throw new PropertyChangeException("Resource Limit Group '" + (String) newValue + "' is unknown!");
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (resource limit group): user=" + myUser + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myUser.setResourceLimitGroup(myGroup);
            }
        });

        passwProp.setPropertyChangeListener(new PropertyChangeAdapter(user) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                User myUser = (User) configObject;
                String myPassword = (String) newValue;
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (password): user=" + myUser + ", oldValue=" + oldValue + ", newValue=" + newValue);
                myUser.setPassword(myPassword);
            }
        });

        return user;
    }

    private void createUsers(EntityList userList) throws SwiftletException {
        if (traceSpace.enabled) traceSpace.trace(getName(), "creating users ...");
        Map m = userList.getEntities();
        if (m.size() > 0) {
            for (Iterator iter = m.keySet().iterator(); iter.hasNext(); ) {
                String userName = (String) iter.next();
                Entity userEntity = userList.getEntity(userName);
                createUser(userName, userEntity);
            }
        }
        if (users.get(ANONYMOUS_USER) == null)
            users.put(ANONYMOUS_USER, new User(ANONYMOUS_USER, null, publicGroup, publicRLGroup));

        userList.setEntityAddListener(new EntityChangeAdapter(null) {
            public void onEntityAdd(Entity parent, Entity newEntity)
                    throws EntityAddException {
                String myName = newEntity.getName();
                try {
                    createUser(myName, newEntity);
                    if (traceSpace.enabled) traceSpace.trace(getName(), "onEntityAdd (user): new user=" + myName);
                } catch (Exception e) {
                    throw new EntityAddException(e.getMessage());
                }
            }
        });
        userList.setEntityRemoveListener(new EntityChangeAdapter(null) {
            public void onEntityRemove(Entity parent, Entity delEntity)
                    throws EntityRemoveException {
                String myName = delEntity.getName();
                if (myName.equals(ANONYMOUS_USER))
                    throw new EntityRemoveException("Can't remove " + myName + " User");
                if (traceSpace.enabled) traceSpace.trace(getName(), "onEntityRemove (user): del user=" + myName);
                users.remove(myName);
            }
        });
    }

    protected void startup(Configuration config)
            throws SwiftletException {
        this.config = config;
        logSwiftlet = (LogSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$log");
        traceSwiftlet = (TraceSwiftlet) SwiftletManager.getInstance().getSwiftlet("sys$trace");
        traceSpace = traceSwiftlet.getTraceSpace(TraceSwiftlet.SPACE_KERNEL);
        if (traceSpace.enabled) traceSpace.trace(getName(), "startup ...");

        useEncryption = Boolean.valueOf(System.getProperty(PROP_ENCRYPTED_PASSWORDS, "false"));
        if (useEncryption) {
            masterPassword = System.getenv(ENV_MASTER_PASSWORD);
            if (masterPassword == null)
                throw new SwiftletException("Encrypted Passwords require to set the master password with environment var " + ENV_MASTER_PASSWORD);
        }

        createGroups((EntityList) config.getEntity("groups"));
        createRLGroups((EntityList) config.getEntity("resource-limit-groups"));
        createUsers((EntityList) config.getEntity("users"));

        Property authProp = config.getProperty("authentication-enabled");
        authenticationOff = !((Boolean) authProp.getValue()).booleanValue();
        if (traceSpace.enabled)
            traceSpace.trace(getName(), "startup, authentication is " + (authenticationOff ? "OFF" : "ON"));
        authProp.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "propertyChanged (authentication.enabled): oldValue=" + oldValue + ", newValue=" + newValue);
                authenticationOff = !((Boolean) newValue).booleanValue();
                if (traceSpace.enabled)
                    traceSpace.trace(getName(), "authentication is " + (authenticationOff ? "OFF" : "ON"));
            }
        });
        if (traceSpace.enabled) traceSpace.trace(getName(), "startup: done.");
    }

    protected void shutdown()
            throws SwiftletException {
        // true if shutdown while standby
        if (config == null)
            return;
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown ...");
        rlgroups.clear();
        groups.clear();
        users.clear();
        publicRLGroup = null;
        publicGroup = null;
        authenticationOff = true;
        config = null;
        if (traceSpace.enabled) traceSpace.trace(getName(), "shutdown: done.");
    }
}


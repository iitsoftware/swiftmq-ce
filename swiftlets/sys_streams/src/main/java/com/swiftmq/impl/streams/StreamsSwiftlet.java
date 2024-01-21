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

package com.swiftmq.impl.streams;

import com.swiftmq.impl.streams.jobs.JobRegistrar;
import com.swiftmq.mgmt.*;
import com.swiftmq.swiftlet.Swiftlet;
import com.swiftmq.swiftlet.SwiftletException;
import com.swiftmq.swiftlet.auth.AuthenticationDelegate;
import com.swiftmq.swiftlet.mgmt.event.MgmtListener;
import com.swiftmq.swiftlet.timer.event.TimerListener;
import com.swiftmq.tools.sql.LikeComparator;
import com.swiftmq.util.SwiftUtilities;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StreamsSwiftlet extends Swiftlet implements TimerListener, AuthenticationDelegate {
    SwiftletContext ctx;
    EntityListEventAdapter domainAdapter = null;
    JobRegistrar jobRegistrar = null;
    boolean collectOn = false;
    long collectInterval = -1;
    boolean isStartup = false;
    boolean isShutdown = false;
    RepositorySupport repositorySupport = new RepositorySupport();
    StreamLibDeployer streamLibDeployer = null;

    private void collectChanged(long oldInterval, long newInterval) {
        if (!collectOn)
            return;
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "collectChanged: old interval: " + oldInterval + " new interval: " + newInterval);
        if (oldInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: removeTimerListener for interval " + oldInterval);
            ctx.timerSwiftlet.removeTimerListener(this);
        }
        if (newInterval > 0) {
            if (ctx.traceSpace.enabled)
                ctx.traceSpace.trace(getName(), "collectChanged: addTimerListener for interval " + newInterval);
            ctx.timerSwiftlet.addTimerListener(newInterval, this);
        }
    }

    @Override
    public void performTimeAction() {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "collecting ...");
        Map domains = ctx.config.getEntity("domains").getEntities();
        if (domains != null && domains.size() > 0) {
            for (Iterator diter = domains.entrySet().iterator(); diter.hasNext(); ) {
                Map packages = ((Entity) ((Map.Entry) diter.next()).getValue()).getEntity("packages").getEntities();
                if (packages != null && packages.size() > 0) {
                    for (Iterator piter = packages.entrySet().iterator(); piter.hasNext(); ) {
                        Map streams = ((Entity) ((Map.Entry) piter.next()).getValue()).getEntity("streams").getEntities();
                        if (streams != null && streams.size() > 0) {
                            for (Iterator iter = streams.entrySet().iterator(); iter.hasNext(); ) {
                                StreamController streamController = (StreamController) ((Entity) ((Map.Entry) iter.next()).getValue()).getUserObject();
                                if (ctx.traceSpace.enabled)
                                    ctx.traceSpace.trace(getName(), "collecting, streamController=" + streamController);
                                if (streamController != null)
                                    streamController.collect(collectInterval);
                            }
                        }

                    }
                }

            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "collecting DONE");
    }

    private void createDomainAdapter(EntityList list) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createDomainAdapter ...");
        domainAdapter = new EntityListEventAdapter(list, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    final String domainName = newEntity.getName();
                    EntityListEventAdapter packageAdapter = new EntityListEventAdapter((EntityList) newEntity.getEntity("packages"), true, true) {
                        @Override
                        public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                            try {
                                final String packageName = newEntity.getName();
                                newEntity.setUserObject(createStreamAdapter((EntityList) newEntity.getEntity("streams"), domainName, packageName));
                            } catch (SwiftletException e) {
                                ctx.logSwiftlet.logError(getName(), "Error starting stream: " + e);
                                System.err.println("Error starting stream: " + e.getMessage());
                            }
                        }

                        @Override
                        public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                            try {
                                ((EntityListEventAdapter) delEntity.getUserObject()).close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    };
                    packageAdapter.init();
                    newEntity.setUserObject(packageAdapter);
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                EntityListEventAdapter packageAdapter = (EntityListEventAdapter) delEntity.getUserObject();
                if (packageAdapter != null) {
                    try {
                        packageAdapter.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    delEntity.setUserObject(null);
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            domainAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createDomainAdapter done");
    }

    private EntityListEventAdapter createStreamAdapter(EntityList list, final String domainName, final String packageName) throws SwiftletException {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createStreamAdapter ...");
        EntityListEventAdapter streamAdapter = new EntityListEventAdapter(list, true, true) {
            public void onEntityAdd(Entity parent, Entity newEntity) throws EntityAddException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " ...");
                try {
                    StreamController streamController = new StreamController(ctx, repositorySupport, newEntity, domainName, packageName);
                    if (!isStartup)
                        streamController.init();
                    newEntity.setUserObject(streamController);
                } catch (Exception e) {
                    throw new EntityAddException(e.toString());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityAdd: " + newEntity.getName() + " done");
            }

            public void onEntityRemove(Entity parent, Entity delEntity) throws EntityRemoveException {
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " ...");
                StreamController streamController = (StreamController) delEntity.getUserObject();
                if (streamController != null) {
                    streamController.close();
                    delEntity.setUserObject(null);
                    if (!isShutdown)
                        streamLibDeployer.removeStreamLibs(streamController.fqn());
                }
                if (ctx.traceSpace.enabled)
                    ctx.traceSpace.trace(getName(), "onEntityRemove: " + delEntity.getName() + " done");
            }
        };
        try {
            streamAdapter.init();
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "createStreamAdapter done");
        return streamAdapter;
    }

    private void startStreams(EntityList domainList) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startStreams ...");
        Map domains = domainList.getEntities();
        if (domains != null) {
            for (Iterator iterDomain = domains.entrySet().iterator(); iterDomain.hasNext(); ) {
                Map pgks = ((Entity) ((Map.Entry) iterDomain.next()).getValue()).getEntity("packages").getEntities();
                if (pgks != null) {
                    for (Iterator iterPkgs = pgks.entrySet().iterator(); iterPkgs.hasNext(); ) {
                        Map streams = ((Entity) ((Map.Entry) iterPkgs.next()).getValue()).getEntity("streams").getEntities();
                        if (streams != null) {
                            for (Iterator iterStreams = streams.entrySet().iterator(); iterStreams.hasNext(); ) {
                                try {
                                    ((StreamController) ((Entity) ((Map.Entry) iterStreams.next()).getValue()).getUserObject()).init();
                                } catch (Exception e) {
                                    ctx.logSwiftlet.logError(getName(), "Error starting stream: " + e);
                                    System.err.println("Error starting stream: " + e.getMessage());
                                }
                            }
                        }

                    }
                }
            }
        }

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startStreams done");
    }

    private Entity findStream(String fqn) {
        String[] tokens = SwiftUtilities.tokenize(fqn, ".");
        if (tokens.length != 3)
            return null;
        Entity domain = ctx.root.getEntity("domains").getEntity(tokens[0]);
        if (domain != null) {
            Entity pkg = domain.getEntity("packages").getEntity(tokens[1]);
            if (pkg != null)
                return pkg.getEntity("streams").getEntity(tokens[2]);
        }
        return null;
    }

    private void buildDependencies(String fqn, TreeNode<String> depList) throws Exception {
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "buildDependencies, stream=" + fqn + " ...");
        Entity streamEntity = findStream(fqn);
        if (streamEntity == null)
            throw new Exception("Stream '" + fqn + "' not found while building dependencies");
        EntityList entityList = (EntityList) streamEntity.getEntity("dependencies");
        Map entities = entityList.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                String depFQN = ((Entity) ((Map.Entry) iter.next()).getValue()).getName();
                if (depList.hasParent(depFQN))
                    throw new Exception("Circular reference for Stream '" + depFQN + "' in dependencies of Stream '" + fqn + "' detected");
                TreeNode<String> child = new TreeNode<String>(depFQN);
                depList.addChild(child);
                buildDependencies(depFQN, child);
            }
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "buildDependencies, stream=" + fqn + " done, depList=" + depList);
    }

    private boolean isIncluded(String fqn, EntityList entityList) {
        Map entities = entityList.getEntities();
        if (entities != null && entities.size() > 0) {
            for (Iterator iter = entities.entrySet().iterator(); iter.hasNext(); ) {
                String depFQN = ((Entity) ((Map.Entry) iter.next()).getValue()).getName();
                if (fqn.equals(depFQN))
                    return true;
            }
        }
        return false;
    }

    protected void startDependencies(String fqn) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startDependencies, stream=" + fqn + " ...");
        TreeNode<String> depList = new TreeNode<String>(fqn);
        buildDependencies(fqn, depList);

        for (TreeNode<String> node : depList.getChildren()) {
            String depFQN = node.getData();
            Entity streamEntity = findStream(depFQN);
            if (streamEntity == null)
                throw new Exception("Stream '" + depFQN + "' not found while starting dependencies of Stream '" + fqn + "'");
            if (isStartup)
                ((StreamController) streamEntity.getUserObject()).init();
            boolean enabled = (Boolean) streamEntity.getProperty("enabled").getValue();
            if (!enabled)
                streamEntity.getProperty("enabled").setValue(true);
        }
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "startDependencies, stream=" + fqn + " done");

    }

    protected void stopDependencies(String fqn) throws Exception {
        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopDependencies, stream=" + fqn + " ...");
        Map domains = ctx.root.getEntity("domains").getEntities();
        if (domains != null) {
            for (Iterator iterDomain = domains.entrySet().iterator(); iterDomain.hasNext(); ) {
                Entity domain = (Entity) ((Map.Entry) iterDomain.next()).getValue();
                Map pgks = domain.getEntity("packages").getEntities();
                if (pgks != null) {
                    for (Iterator iterPkgs = pgks.entrySet().iterator(); iterPkgs.hasNext(); ) {
                        Entity pkg = (Entity) ((Map.Entry) iterPkgs.next()).getValue();
                        Map streams = pkg.getEntity("streams").getEntities();
                        if (streams != null) {
                            for (Iterator iterStreams = streams.entrySet().iterator(); iterStreams.hasNext(); ) {
                                Entity stream = (Entity) ((Map.Entry) iterStreams.next()).getValue();
                                if ((Boolean) stream.getProperty("enabled").getValue()) {
                                    if (isIncluded(fqn, (EntityList) stream.getEntity("dependencies"))) {
                                        StreamController streamController = (StreamController) stream.getUserObject();
                                        if (isShutdown) {
                                            if (streamController != null)
                                                streamController.close();
                                        } else
                                            stream.getProperty("enabled").setValue(false);
                                    }
                                }
                            }
                        }

                    }
                }
            }
        }

        if (ctx.traceSpace.enabled)
            ctx.traceSpace.trace(getName(), "stopDependencies, stream=" + fqn + " done");
    }

    private boolean isGranted(String topicName) {
        String predicates = (String) ctx.config.getProperty("stream-grant-predicates").getValue();
        if (predicates != null && predicates.trim().length() > 0) {
            String[] tokens = SwiftUtilities.tokenize(predicates, " ");
            for (String token : tokens) {
                if (LikeComparator.compare(topicName, token, '\\'))
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSendGranted(String topicName) {
        return isGranted(topicName);
    }

    @Override
    public boolean isReceiveGranted(String topicName) {
        return isGranted(topicName);
    }

    @Override
    public boolean isDurableGranted(String topicName) {
        return isGranted(topicName);
    }

    protected void startup(Configuration config) throws SwiftletException {
        ctx = new SwiftletContext(config, this);
        if (!ctx.HASENGINE) {
            ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "You are using Java " + ctx.JAVAVERSION + " but not GraalVM. Cannot start Streams Swiftlet. Please use GraalVM: https://graalvm.org");
            ctx.logSwiftlet.logWarning(ctx.streamsSwiftlet.getName(), "You are using Java " + ctx.JAVAVERSION + " but not GraalVM. Cannot start Streams Swiftlet. Please use GraalVM: https://graalvm.org");
            return;
        }
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup ...");
        isStartup = true;
        ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "starting, available Scripting Engines:");
        ScriptEngineManager manager = new ScriptEngineManager();
        List<ScriptEngineFactory> factories = manager.getEngineFactories();
        for (int i = 0; i < factories.size(); i++) {
            ctx.logSwiftlet.logInformation(ctx.streamsSwiftlet.getName(), "name=" + factories.get(i).getEngineName() +
                    ", version=" + factories.get(i).getEngineVersion() + ", language name=" + factories.get(i).getLanguageName() +
                    ", language version=" + factories.get(i).getLanguageVersion() +
                    ", names=" + factories.get(i).getNames());
        }

        ctx.authenticationSwiftlet.addTopicAuthenticationDelegate(this);

        createDomainAdapter((EntityList) config.getEntity("domains"));
        try {
            startStreams((EntityList) config.getEntity("domains"));
        } catch (Exception e) {
            throw new SwiftletException(e.getMessage());
        }
        /*${evalstartupmark}*/

        jobRegistrar = new JobRegistrar(ctx);
        jobRegistrar.register();
        Property prop = ctx.root.getProperty("collect-interval");
        prop.setPropertyChangeListener(new PropertyChangeAdapter(null) {
            public void propertyChanged(Property property, Object oldValue, Object newValue)
                    throws PropertyChangeException {
                collectInterval = ((Long) newValue).longValue();
                collectChanged(((Long) oldValue).longValue(), collectInterval);
            }
        });

        collectInterval = ((Long) prop.getValue()).longValue();

        ctx.mgmtSwiftlet.addMgmtListener(new MgmtListener() {
            public void adminToolActivated() {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "adminToolActivated");
                collectOn = true;
                collectChanged(-1, collectInterval);
            }

            public void adminToolDeactivated() {
                if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "adminToolDeactivated");
                collectChanged(collectInterval, -1);
                collectOn = false;
            }
        });
        try {
            streamLibDeployer = new StreamLibDeployer(ctx);
        } catch (Exception e) {
            throw new SwiftletException(e.toString());
        }
        isStartup = false;

        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "startup done.");
    }

    protected void shutdown() throws SwiftletException {
        if (ctx == null || !ctx.HASENGINE)
            return;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown ...");
        isShutdown = true;

        ctx.authenticationSwiftlet.removeTopicAuthenticationDelegate(this);

        if (collectOn && collectInterval > 0)
            ctx.timerSwiftlet.removeTimerListener(this);

        jobRegistrar.unregister();
        try {
            domainAdapter.close();
        } catch (Exception e) {
        }
        ctx.eventLoopMUX.close();
        ctx.evalScriptLoop.close();
        isShutdown = false;
        if (ctx.traceSpace.enabled) ctx.traceSpace.trace(getName(), "shutdown done.");
    }

    private class TreeNode<T> {
        private T data = null;
        private List<TreeNode> children = new ArrayList<TreeNode>();
        private TreeNode parent = null;

        public TreeNode(T data) {
            this.data = data;
        }

        public void addChild(TreeNode child) {
            child.setParent(this);
            this.children.add(child);
        }

        public void addChild(T data) {
            TreeNode<T> newChild = new TreeNode<T>(data);
            newChild.setParent(this);
            children.add(newChild);
        }

        public void addChildren(List<TreeNode> children) {
            for (TreeNode t : children) {
                t.setParent(this);
            }
            this.children.addAll(children);
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        public boolean hasParent(T data) {
            if (parent == null)
                return false;
            if (parent.getData().equals(data))
                return true;
            return parent.hasParent(data);
        }

        public T getData() {
            return data;
        }

        public void setData(T data) {
            this.data = data;
        }

        private void setParent(TreeNode parent) {
            this.parent = parent;
        }

        public TreeNode getParent() {
            return parent;
        }
    }
}

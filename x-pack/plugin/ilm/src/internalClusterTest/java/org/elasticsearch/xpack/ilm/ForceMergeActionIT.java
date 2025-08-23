/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@TestLogging(
    reason = "Enabling ILM trace logs to aid potential future debugging",
    value = "org.elasticsearch.xpack.ilm:TRACE,org.elasticsearch.xpack.core.ilm:TRACE"
)
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class ForceMergeActionIT extends ESIntegTestCase {
    // TODO: test security on cloned index

    private String policy;
    private String dataStream;

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
        dataStream = "logs-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        logger.info("--> running [{}] with data stream [{}] and policy [{}]", getTestName(), dataStream, policy);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s")
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .build();
    }

    /**
     * Tests a simple force merge action on a rolled over data stream index, without specifying an index codec.
     */
    public void testSimpleCompleteForceMergeWithoutCodec() {
        // TODO: Add codec variant
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        createSingletonPolicy(forceMergeAction);
        createDataStream();
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        ensureTwoSegments();
        String forceMergedIndex = awaitForceMergedIndexName(firstBackingIndex);
        assertForceMergeComplete(forceMergedIndex, false);
    }

    public void testRollingUpgrade() throws Exception {
        ForceMergeAction forceMergeAction = new ForceMergeAction(1, null);
        createSingletonPolicy(forceMergeAction);
        createDataStream();
        String firstBackingIndex = getDataStreamBackingIndexNames(dataStream).getFirst();
        awaitStep(firstBackingIndex, "hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);

        ensureTwoSegments();
        boolean upgradeBeforeForceMerge = randomBoolean();
        if (upgradeBeforeForceMerge) {
            internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());
        }
        String forceMergedIndex = awaitForceMergedIndexName(firstBackingIndex);
        if (upgradeBeforeForceMerge == false) {
            internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());
        }

        assertForceMergeComplete(forceMergedIndex, false);
    }

    /**
     * Creates a singleton ILM policy with the given force merge action, and a rollover action that rolls over at 2 primary shard documents.
     * The force merge action randomly executes either in the hot or warm phase, as the phase shouldn't matter.
     */
    private void createSingletonPolicy(ForceMergeAction forceMergeAction) {
        RolloverAction rolloverAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        // Force merge is only allowed in hot and warm phases. We randomly force merge in warm or hot tier, as the phase shouldn't matter.
        Map<String, Phase> phases;
        if (randomBoolean()) {
            phases = Map.of(
                "hot",
                new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, rolloverAction, ForceMergeAction.NAME, forceMergeAction))
            );
        } else {
            phases = Map.of(
                "hot",
                new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, rolloverAction)),
                "warm",
                new Phase("warm", TimeValue.ZERO, Map.of(ForceMergeAction.NAME, forceMergeAction))
            );
        }
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest));
    }

    /**
     * Creates a data stream with the given name and an index template that applies the given policy to its backing indices.
     */
    private void createDataStream() {
        final var composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStream))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .template(Template.builder().settings(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy)))
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("template-" + dataStream).indexTemplate(composableIndexTemplate)
            )
        );
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest));
    }

    /**
     * Indexes two documents into the data stream's write index, refreshing in between, to ensure that the write index has two segments.
     * Note, this only works if the write index has a single (primary) shard.
     */
    private void ensureTwoSegments() {
        prepareIndex(dataStream).setCreate(true)
            .setSource("@timestamp", "2025-08-23", "field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        prepareIndex(dataStream).setCreate(true)
            .setSource("@timestamp", "2025-08-23", "field", "value")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        final var writeIndex = getDataStreamBackingIndexNames(dataStream).getLast();
        // TODO: looks like we can't reliably get two segments here
        // assertNumberOfSegments(writeIndex, 2);
    }

    /**
     * Asserts that the given index has the expected number of segments in every shard.
     */
    private void assertNumberOfSegments(String writeIndex, int expectedNrOfSegments) {
        getShardSegments(writeIndex).forEach(s -> assertEquals(expectedNrOfSegments, s.getSegments().size()));
    }

    /**
     * Waits until the given index is at the specified step in the specified phase. If phase is null, only action and step are checked.
     */
    private static void awaitStep(String indexName, @Nullable String phase, String action, String step) {
        awaitClusterState(state -> {
            final var indexMetadata = state.metadata().getProject(ProjectId.DEFAULT).index(indexName);
            if (indexMetadata == null) {
                return false;
            }
            final var executionState = indexMetadata.getLifecycleExecutionState();
            if (executionState == null) {
                return false;
            }
            return (phase == null || phase.equals(executionState.phase()))
                && action.equals(executionState.action())
                && step.equals(executionState.step());
        });
    }

    /**
     * Waits until a clone of the given index exists that was created by the force merge action, and returns its name.
     */
    private String awaitForceMergedIndexName(String indexName) {
        SetOnce<String> forceMergedIndexName = new SetOnce<>();
        awaitClusterState(state -> {
            final var foundIndex = state.metadata()
                .getProject(ProjectId.DEFAULT)
                .indices()
                .keySet()
                .stream()
                .filter(i -> i.startsWith(ForceMergeAction.FORCE_MERGED_INDEX_PREFIX) && i.endsWith(indexName))
                .findFirst();
            if (foundIndex.isEmpty()) {
                return false;
            }
            forceMergedIndexName.set(foundIndex.get());
            return true;
        });
        logger.info("--> found force merge index [{}]", forceMergedIndexName.get());
        return forceMergedIndexName.get();
    }

    private void assertForceMergeComplete(String indexName, boolean withCodec) {
        awaitStep(indexName, null, PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);
        logger.info("--> force merge complete on index [{}]", indexName);
        assertNumberOfSegments(indexName, 1);

        Settings indexSettings = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get()
            .getIndexToSettings()
            .get(indexName);
        int actualPrimaries = indexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, -1);
        int actualReplicas = indexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1);
        assertEquals("Unexpected number of primary shards", 1, actualPrimaries);
        assertEquals("Unexpected number of replica shards", 1, actualReplicas);
        assertNull(indexSettings.get(IndexMetadata.APIBlock.WRITE.settingName()));
        String codec = indexSettings.get(EngineConfig.INDEX_CODEC_SETTING.getKey());
        assertEquals(withCodec ? CodecService.BEST_COMPRESSION_CODEC : null, codec);

        String sourceIndexName = indexSettings.get(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY);
        assertNotNull(sourceIndexName);
        // Ensure the original backing index is deleted
        awaitIndexNotExists(sourceIndexName);
    }
}

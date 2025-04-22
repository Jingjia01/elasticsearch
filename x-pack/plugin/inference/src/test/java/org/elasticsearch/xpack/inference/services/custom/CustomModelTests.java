/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.apache.http.HttpHeaders;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CustomModelTests extends ESTestCase {
    public static String taskSettingsKey = "test_taskSettings_key";
    public static String taskSettingsValue = "test_taskSettings_value";

    public static String secretSettingsKey = "test_secret_key";
    public static SerializableSecureString secretSettingsValue = new SerializableSecureString("test_secret_value");
    public static String url = "http://www.abc.com";
    public static String path = "/endpoint";

    public void testOverride_DoesNotModifiedFields_TaskSettingsIsEmpty() {
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            CustomServiceSettingsTests.createRandom(),
            CustomTaskSettingsTests.createRandom(),
            CustomSecretSettingsTests.createRandom()
        );

        var overriddenModel = CustomModel.of(model, Map.of());
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverride() {
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            CustomServiceSettingsTests.createRandom(),
            new CustomTaskSettings(Map.of("key", "value")),
            CustomSecretSettingsTests.createRandom()
        );

        var overriddenModel = CustomModel.of(
            model,
            new HashMap<>(Map.of(CustomTaskSettings.PARAMETERS, new HashMap<>(Map.of("key", "different_value"))))
        );
        MatcherAssert.assertThat(
            overriddenModel,
            is(
                createModel(
                    "service",
                    TaskType.TEXT_EMBEDDING,
                    model.getServiceSettings(),
                    new CustomTaskSettings(Map.of("key", "different_value")),
                    model.getSecretSettings()
                )
            )
        );
    }

    public static CustomModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new CustomModel(modelId, taskType, CustomService.NAME, serviceSettings, taskSettings, secrets, null);
    }

    public static CustomModel createModel(
        String modelId,
        TaskType taskType,
        CustomServiceSettings serviceSettings,
        CustomTaskSettings taskSettings,
        @Nullable CustomSecretSettings secretSettings
    ) {
        return new CustomModel(modelId, taskType, CustomService.NAME, serviceSettings, taskSettings, secretSettings);
    }

    public static CustomModel getTestModel() {
        return getTestModel(TaskType.TEXT_EMBEDDING, new TextEmbeddingResponseParser("$.result.embeddings[*].embedding"));
    }

    public static CustomModel getTestModel(TaskType taskType, CustomResponseParser responseParser) {
        Integer dims = 1536;
        Integer maxInputTokens = 512;
        Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, "${" + secretSettingsKey + "}");
        String requestContentString = "\"input\":\"${input}\"";

        CustomServiceSettings serviceSettings = new CustomServiceSettings(
            SimilarityMeasure.DOT_PRODUCT,
            dims,
            maxInputTokens,
            url,
            headers,
            requestContentString,
            responseParser,
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message")
        );

        CustomTaskSettings taskSettings = new CustomTaskSettings(Map.of(taskSettingsKey, taskSettingsValue));
        CustomSecretSettings secretSettings = new CustomSecretSettings(Map.of(secretSettingsKey, secretSettingsValue));

        return CustomModelTests.createModel("service", taskType, serviceSettings, taskSettings, secretSettings);
    }
}

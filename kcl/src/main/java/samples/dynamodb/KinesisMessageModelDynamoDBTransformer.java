/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package samples.dynamodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.arnx.jsonic.JSON;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import samples.KinesisMessageModel;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.BasicJsonTransformer;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBTransformer;

/**
 * A custom transfomer for {@link KinesisMessageModel} records in JSON format. The output is in a format
 * usable for insertions to Amazon DynamoDB.
 */
public class KinesisMessageModelDynamoDBTransformer extends
        BasicJsonTransformer<KinesisMessageModel, Map<String, AttributeValue>> implements
        DynamoDBTransformer<KinesisMessageModel> {

    private static final Log LOG = LogFactory.getLog(KinesisMessageModelDynamoDBTransformer.class);

    /**
     * Creates a new KinesisMessageModelDynamoDBTransformer.
     */
    public KinesisMessageModelDynamoDBTransformer() {
        super(KinesisMessageModel.class);
    }

    @Override
    public KinesisMessageModel toClass(Record record) throws IOException {
        try {
            String json = new String(record.getData().array(), "UTF-8");
            Map<String, Object> tweet = JSON.decode(json);
            Map<String, Object> user = (Map)tweet.get("user");
            KinesisMessageModel model = new KinesisMessageModel();
            model.setId(tweet.get("id").toString());
            model.setCreatedAt(tweet.get("created_at").toString());
            model.setText(tweet.get("text").toString());
            model.setFollowersCount(Integer.valueOf(user.get("followers_count").toString()));
            model.setFriendsCount(Integer.valueOf(user.get("friends_count").toString()));
            System.out.println(model);
            return model;
        } catch (Exception e) {
            String message = "Error parsing record from JSON: " + new String(record.getData().array());
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

    @Override
    public Map<String, AttributeValue> fromClass(KinesisMessageModel message) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        putStringIfNonempty(item, "id", message.getId());
        putStringIfNonempty(item, "text", message.getText());
        putIntegerIfNonempty(item, "friends_count", message.getFriendsCount());
        putIntegerIfNonempty(item, "followers_count", message.getFollowersCount());
        putStringIfNonempty(item, "created_at", message.getCreatedAt());
        return item;
    }

    /**
     * Helper method to map nonempty String attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to check before inserting into the item map
     */
    private void putStringIfNonempty(Map<String, AttributeValue> item, String key, String value) {
        if (value != null && !value.isEmpty()) {
            item.put(key, new AttributeValue().withS(value));
        }
    }

    /**
     * Helper method to map boolean attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to insert into the item map
     */
    private void putBoolIfNonempty(Map<String, AttributeValue> item, String key, Boolean value) {
        putStringIfNonempty(item, key, Boolean.toString(value));
    }

    /**
     * Helper method to map nonempty Integer attributes to an AttributeValue.
     * 
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to insert into the item map
     */
    private void putIntegerIfNonempty(Map<String, AttributeValue> item, String key, Integer value) {
        putStringIfNonempty(item, key, Integer.toString(value));
    }

    protected KinesisMessageModel toModel() {
        return null;
    }
}

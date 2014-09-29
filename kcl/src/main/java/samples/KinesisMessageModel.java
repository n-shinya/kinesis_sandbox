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
package samples;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.arnx.jsonic.JSON;

/**
 * 
 * This is the data model for the objects being sent through the Amazon Kinesis streams in the samples
 * 
 */
public class KinesisMessageModel implements Serializable {

    /**
     * Default constructor for Jackson JSON mapper - uses bean pattern.
     */
    public KinesisMessageModel() {
    }

    protected String id;
    protected String createdAt;
    protected String text;
    protected Integer friendsCount;
    protected Integer followersCount;
    protected Integer favoriteCount;
    protected Integer retweetCount;
    protected String source;
    protected String name;
    protected Long timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Integer getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(Integer friendsCount) {
        this.friendsCount = friendsCount;
    }

    public Integer getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(Integer followersCount) {
        this.followersCount = followersCount;
    }

    public Integer getFavoriteCount() {
        return favoriteCount;
    }

    public void setFavoriteCount(Integer favoriteCount) {
        this.favoriteCount = favoriteCount;
    }
    public Integer getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(Integer retweetCount) {
        this.retweetCount = retweetCount;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @SuppressWarnings("unchecked")
    public static KinesisMessageModel newInstance(String json) {
        KinesisMessageModel model = new KinesisMessageModel();

        Map<String, Object> tweet = JSON.decode(json);
        model.setId(tweet.get("id").toString());
        model.setCreatedAt(tweet.get("created_at").toString());
        model.setText(tweet.get("text").toString());
        model.setSource(tweet.get("source").toString());
        model.setTimestamp(Long.valueOf(tweet.get("timestamp_ms").toString()));

        if (tweet.get("retweeted_status") instanceof Map) {
            Map<String, Object> retweet = (Map)tweet.get("retweeted_status");
            model.setFavoriteCount(Integer.valueOf(retweet.get("favorite_count").toString()));
            model.setRetweetCount(Integer.valueOf(retweet.get("retweet_count").toString()));
        }

        Map<String, Object> user = (Map)tweet.get("user");
        model.setFollowersCount(Integer.valueOf(user.get("followers_count").toString()));
        model.setFriendsCount(Integer.valueOf(user.get("friends_count").toString()));
        model.setName(user.get("name").toString());

        return model;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }
    }

}

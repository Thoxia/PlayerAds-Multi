package com.thoxia.playerads.multiserver;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.thoxia.playerads.ad.Ad;
import com.thoxia.playerads.module.Module;
import com.thoxia.playerads.multiserver.manager.MultiAdManager;
import com.thoxia.playerads.multiserver.packet.AdPacket;
import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import lombok.Getter;

import java.util.UUID;

public final class MultiServerModule extends Module {

    public static final String ADD_CHANNEL = "playerads:add";
    public static final String REMOVE_CHANNEL = "playerads:remove";
    public static final String MAP_KEY = "playerads:ads";
    public static final String LAST_AD_KEY = "playerads:last-ad";

    public static final Gson GSON = new GsonBuilder().create();

    private RedisClient redisClient;
    private MultiAdManager adManager;

    @Getter private UUID serverId;

    private RedisPubSubAsyncCommands<String, String> asyncRedisConnection;

    @Override
    public void onLoad() {
        adManager = new MultiAdManager(this);
        getPlugin().setAdManager(adManager);
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();

        serverId = UUID.randomUUID();

        redisClient = RedisClient.create(getConfig().getString("redis-uri"));

        asyncRedisConnection = redisClient.connectPubSub().async();

        asyncRedisConnection.getStatefulConnection().addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                switch (channel) {
                    case ADD_CHANNEL -> {
                        AdPacket packet = GSON.fromJson(message, AdPacket.class);
                        Ad ad = packet.ad;
                        // we have to get the spot on the current server inorder to guarantee object safety
                        ad.setSpot(adManager.getSpot(ad.getSpot().getSlot()));
                        adManager.handleAdd(ad, packet.serverId.equals(serverId));
                    }

                    case REMOVE_CHANNEL -> {
                        Ad ad = GSON.fromJson(message, Ad.class);
                        adManager.handleRemove(ad);
                    }
                }
            }
        });

        asyncRedisConnection.subscribe(ADD_CHANNEL, REMOVE_CHANNEL);

    }

    @Override
    public void onDisable() {
        if (redisClient != null) {
            this.redisClient.shutdown();
            this.asyncRedisConnection.shutdown(false);
        }
    }

    public RedisPubSubAsyncCommands<String, String> getRedisConnection() {
        return asyncRedisConnection;
    }

}

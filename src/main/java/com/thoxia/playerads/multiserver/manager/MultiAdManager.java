package com.thoxia.playerads.multiserver.manager;

import com.thoxia.playerads.PlayerAdsPlugin;
import com.thoxia.playerads.ad.Ad;
import com.thoxia.playerads.ad.AdSpot;
import com.thoxia.playerads.ad.LocalAdManager;
import com.thoxia.playerads.multiserver.MultiServerModule;
import com.thoxia.playerads.multiserver.packet.AdPacket;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class MultiAdManager extends LocalAdManager {

    private final MultiServerModule module;

    private final Set<Ad> cachedAds = new HashSet<>();

    public MultiAdManager(MultiServerModule module) {
        super(module.getPlugin());

        this.module = module;
    }

    @Override
    public Set<Ad> getCachedAds() {
        return cachedAds;
    }

    @Override
    public boolean addToCache(Ad ad) {
        return this.cachedAds.add(ad);
    }

    @Override
    public boolean removeFromCache(Ad ad) {
        return this.cachedAds.remove(ad);
    }

    @Override
    public void clearCache() {
        this.cachedAds.clear();
    }

    @Override
    public void addAd(Ad ad) {
        this.cachedAds.add(ad);
        this.module.getRedisConnection().hset(MultiServerModule.MAP_KEY, ad.getUniqueId().toString(), MultiServerModule.GSON.toJson(ad));
    }

    @Override
    public CompletableFuture<Collection<Ad>> getAds(String player) {
        CompletableFuture<Collection<Ad>> future = getAds().thenApply(ads -> ads.stream()
                .filter(ad -> ad.getPlayerName().equals(player)).toList());
        return future.toCompletableFuture();
    }

    @Override
    public CompletableFuture<Collection<Ad>> getAds() {
        return this.module.getRedisConnection().hgetall(MultiServerModule.MAP_KEY)
                .exceptionally(throwable -> {
                    PlayerAdsPlugin.getInstance().getLogger().log(Level.SEVERE,
                            "An exception was found whilst fetching ads!", throwable);
                    return new HashMap<>();
                })
                .thenApply(Map::values)
                .thenApply(jsons -> {
                    Collection<Ad> ads = new HashSet<>();
                    for (String json : jsons) {
                        ads.add(MultiServerModule.GSON.fromJson(json, Ad.class));
                    }
                    return ads;
                }).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Ad> getAd(AdSpot spot) {
        CompletableFuture<Ad> future = getAds().thenApply(ads -> ads.stream()
                .filter(ad -> ad.getSpot().equals(spot)).findFirst().orElse(null));
        return future.toCompletableFuture();
    }

    @Override
    public void postAd(Ad ad, boolean postWebhooks) {
        addAd(ad);
        this.module.getRedisConnection().set(MultiServerModule.LAST_AD_KEY, ad.getCreationTime() + "");
        this.module.getRedisConnection().publish(MultiServerModule.ADD_CHANNEL,
                MultiServerModule.GSON.toJson(new AdPacket(module.getServerId(), ad)));
    }

    @Override
    public CompletableFuture<Boolean> removeAds(Ad... ads) {
        for (Ad ad : ads) {
            this.module.getRedisConnection().hdel(MultiServerModule.MAP_KEY, ad.getUniqueId().toString());
            this.module.getRedisConnection().publish(MultiServerModule.REMOVE_CHANNEL, MultiServerModule.GSON.toJson(ad));
        }

        return CompletableFuture.completedFuture(true);
    }

    public void handleAdd(Ad ad, boolean postWebhook) {
        super.postAd(ad, postWebhook);
    }

    public void handleRemove(Ad ad) {
        this.cachedAds.remove(ad);
        super.removeAds(ad);
    }

    @Override
    public CompletableFuture<Long> getLastAdvertisementTime() {
        return this.module.getRedisConnection().get(MultiServerModule.LAST_AD_KEY).thenApply(str -> {
            if (str == null) return 0L;

            return Long.parseLong(str);
        }).toCompletableFuture();
    }

}

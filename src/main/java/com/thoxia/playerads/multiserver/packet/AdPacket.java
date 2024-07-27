package com.thoxia.playerads.multiserver.packet;

import com.thoxia.playerads.ad.Ad;
import lombok.RequiredArgsConstructor;

import java.util.UUID;

@RequiredArgsConstructor
public class AdPacket {

    public final UUID serverId;
    public final Ad ad;

}

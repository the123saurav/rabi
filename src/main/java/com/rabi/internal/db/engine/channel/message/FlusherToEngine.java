package com.rabi.internal.db.engine.channel.message;

import com.rabi.internal.db.engine.Index;
import com.rabi.internal.db.engine.channel.Message;

public class FlusherToEngine extends Message {
    private final Index index;

    public FlusherToEngine(Index i){
        index = i;
    }

    public Index getIndex(){
        return index;
    }
}

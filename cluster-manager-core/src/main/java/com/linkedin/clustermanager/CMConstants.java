package com.linkedin.clustermanager;

public interface CMConstants
{

    enum ZNAttribute
    {
        SESSION_ID, CURRENT_STATE
    }

    enum ChangeType
    {
        IDEAL_STATE, CONFIG, LIVE_INSTANCE, CURRENT_STATE, MESSAGE, EXTERNAL_VIEW
    }
}

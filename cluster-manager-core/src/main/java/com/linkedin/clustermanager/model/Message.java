package com.linkedin.clustermanager.model;



/**
 * Message class basically extends ZNRecord but provides additional fields
 * @author kgopalak
 */

public class Message extends ZNRecord
{
    enum MessageType
    {
        STATE_TRANSITION
    };

    public enum Attributes
    {
        MSG_ID, TGT_SESSION_ID, SRC_NAME, TGT_NAME, MSG_STATE, STATE_UNIT_KEY, STATE_UNIT_GROUP, FROM_STATE, TO_STATE;
    }


    public Message()
    {
        super();
    }

    public Message(ZNRecord record)
    {
        super(record);
    }

    public String getTgtSessionId()
    {
        return getSimpleFieldAsString(Attributes.TGT_SESSION_ID.toString());
    }

    public void setTgtSessionId(String tgtSessionId)
    {
        setSimpleField(Attributes.TGT_SESSION_ID.toString(), tgtSessionId);
    }

    public String getMsgSrc()
    {
        return getSimpleFieldAsString(Attributes.SRC_NAME.toString());
    }

    public void setSrcName(String msgSrc)
    {
        setSimpleField(Attributes.SRC_NAME.toString(), msgSrc);
    }

    public String getTgtName()
    {
        return getSimpleFieldAsString(Attributes.TGT_NAME.toString());
    }

    public void setMsgState(String msgState)
    {
        setSimpleField(Attributes.MSG_STATE.toString(), msgState);
    }

    public String getMsgState()
    {
        return getSimpleFieldAsString(Attributes.MSG_STATE.toString());
    }

    public void setStateUnitKey(String stateUnitKey)
    {
        setSimpleField(Attributes.STATE_UNIT_KEY.toString(), stateUnitKey);
    }

    public String getStateUnitKey()
    {
        return getSimpleFieldAsString(Attributes.STATE_UNIT_KEY.toString());
    }

    public String getMsgId()
    {
        return getSimpleFieldAsString(Attributes.MSG_ID.toString());
    }

    public void setMsgId(String msgId)
    {
        setSimpleField(Attributes.MSG_ID.toString(), msgId);
    }

    public void setFromState(String state)
    {
        setSimpleField(Attributes.FROM_STATE.toString(), state);
    }

    public String getFromState()
    {
        return getSimpleFieldAsString(Attributes.FROM_STATE.toString());
    }

    public void setToState(String state)
    {
        setSimpleField(Attributes.TO_STATE.toString(), state);
    }

    public String getToState()
    {
        return getSimpleFieldAsString(Attributes.TO_STATE.toString());
    }

    private String getSimpleFieldAsString(String key)
    {
        Object ret = getSimpleField(key);
        return (ret != null) ? ret.toString() : null;
    }

    public void setTgtName(String msgTgt)
    {
        setSimpleField(Attributes.TGT_NAME.toString(), msgTgt);

    }


    public Boolean getDebug()
    {
        // TODO Auto-generated method stub
        return false;
    }

    public Integer getGeneration()
    {
        // TODO Auto-generated method stub
        return 1;
    }


    public void setStateUnitGroup(String stateUnitGroup)
    {
        setSimpleField(Attributes.STATE_UNIT_GROUP.toString(), stateUnitGroup);

    }

    public String getStateUnitGroup()
    {
        return getSimpleFieldAsString(Attributes.STATE_UNIT_GROUP.toString());
    }
}

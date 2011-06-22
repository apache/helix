package com.linkedin.clustermanager.statemachine;

public class CMTaskResult
{

    private boolean _success;
    private String _message;

    public boolean isSucess()
    {
        return _success;
    }

    public void setSuccess(boolean success)
    {
        this._success = success;
    }

    public String getMessage()
    {
        return _message;
    }

    public void setMessage(String message)
    {
        this._message = message;
    }

}

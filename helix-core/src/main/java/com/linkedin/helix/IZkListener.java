package com.linkedin.helix;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;

public interface IZkListener extends IZkChildListener, IZkDataListener, IZkStateListener
{

}

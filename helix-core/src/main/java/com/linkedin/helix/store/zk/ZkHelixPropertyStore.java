package com.linkedin.helix.store.zk;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkCachedDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.HelixPropertyListener;

public class ZkHelixPropertyStore<T> extends
    ZkCachedDataAccessor<T>
{

  public ZkHelixPropertyStore(ZkBaseDataAccessor<T> accessor, String root,
      List<String> subscribedPaths)
  {
    super(accessor, root, subscribedPaths, null);
  }

  // temp test
  public static void main(String[] args) throws Exception
  {
    // clean up zk
    String root = "/ZkHPS";
    ZkClient zkClient = new ZkClient("localhost:2191");
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive(root);

    List<String> subscribedPaths = new ArrayList<String>();
    subscribedPaths.add(root);
    ZkHelixPropertyStore<ZNRecord> store = new ZkHelixPropertyStore<ZNRecord>(
        new ZkBaseDataAccessor<ZNRecord>(zkClient), root, subscribedPaths);

    store.subscribe("/", new HelixPropertyListener()
    {

      @Override
      public void onDataChange(String path)
      {
        System.out.println("onDataChange:" + path);
      }

      @Override
      public void onDataCreate(String path)
      {
        System.out.println("onDataCreate:" + path);
      }

      @Override
      public void onDataDelete(String path)
      {
        System.out.println("onDataDelete:" + path);
      }

    });

    // test back to back add-delete-add
    store.set("/child0", new ZNRecord("child0"),
        BaseDataAccessor.Option.PERSISTENT);
    System.out.println("1:cache:" + store._zkCache);

    ZNRecord record = store.get("/child0", null, 0); // will put the record in
                                                     // cache
    System.out.println("1:get:" + record);

    String child0Path = root + "/child0";
    for (int i = 0; i < 2; i++)
    {
      zkClient.delete(child0Path);
      zkClient.createPersistent(child0Path, new ZNRecord("child0-new-" + i));
    }

    Thread.sleep(500); // should wait for zk callback to add "/child0" into
                       // cache
    System.out.println("2:cache:" + store._zkCache);

    record = store.get("/child0", null, 0);
    System.out.println("2:get:" + record);

    zkClient.delete(child0Path);
    Thread.sleep(500); // should wait for zk callback to remove "/child0" from
                       // cache
    System.out.println("3:cache:" + store._zkCache);
    record = store.get("/child0", null, 0);
    System.out.println("3:get:" + record);

    zkClient.close();
  }
}

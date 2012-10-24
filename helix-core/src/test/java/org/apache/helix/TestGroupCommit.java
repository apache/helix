package org.apache.helix;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.GroupCommit;
import org.apache.helix.ZNRecord;


public class TestGroupCommit
{
  // @Test
  public void testGroupCommit() throws InterruptedException
  {
    final BaseDataAccessor<ZNRecord> accessor = new Mocks.MockBaseDataAccessor();
    final GroupCommit commit = new GroupCommit();
    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(400);
    for (int i = 0; i < 2400; i++)
    {
      Runnable runnable = new MyClass(accessor, commit, i);
      newFixedThreadPool.submit(runnable);
    }
    Thread.sleep(10000);
    System.out.println(accessor.get("test", null, 0));
    System.out.println(accessor.get("test", null, 0).getSimpleFields().size());
  }

}

class MyClass implements Runnable
{
  private final BaseDataAccessor<ZNRecord> store;
  private final GroupCommit                commit;
  private final int                        i;

  public MyClass(BaseDataAccessor<ZNRecord> store, GroupCommit commit, int i)
  {
    this.store = store;
    this.commit = commit;
    this.i = i;
  }

  @Override
  public void run()
  {
    // System.out.println("START " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
    ZNRecord znRecord = new ZNRecord("test");
    znRecord.setSimpleField("test_id" + i, "" + i);
    commit.commit(store, 0, "test", znRecord);
    store.get("test", null, 0).getSimpleField("");
    // System.out.println("END " + System.currentTimeMillis() + " --"
    // + Thread.currentThread().getId());
  }

}

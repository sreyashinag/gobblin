package com.linkedin.uif.runtime;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.linkedin.uif.runtime.FileBasedJobLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.runtime.JobLock;

/**
 * Unit test for {@link com.linkedin.uif.runtime.FileBasedJobLock}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.runtime"})
public class FileBasedJobLockTest {

    private FileSystem fs;
    private Path path;

    @BeforeClass
    public void setUp() throws IOException {
        this.fs = FileSystem.getLocal(new Configuration());
        this.path = new Path("MRJobLockTest");
        if (!this.fs.exists(this.path)) {
            this.fs.mkdirs(this.path);
        }
    }

    public void testLocalJobLock() throws Exception {
        final JobLock lock = new FileBasedJobLock(this.fs, this.path.getName(), "MRJobLockTest");
        final CountDownLatch latch = new CountDownLatch(2);

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Assert.assertTrue(lock.tryLock());
                    Thread.sleep(2000);
                    lock.unlock();
                    latch.countDown();
                } catch (Exception e) {
                    // Ignored
                }
            }
        });
        thread1.start();

        Thread.sleep(1000);

        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Assert.assertFalse(lock.tryLock());
                    Thread.sleep(2000);
                    Assert.assertTrue(lock.tryLock());
                    Thread.sleep(1000);
                    lock.unlock();
                    latch.countDown();
                } catch (Exception e) {
                    // Ignored
                }
            }
        });
        thread2.start();

        latch.await();
    }

    @AfterClass
    public void tearDown() throws IOException {
        if (this.fs.exists(this.path)) {
            this.fs.delete(this.path, true);
        }
    }
}
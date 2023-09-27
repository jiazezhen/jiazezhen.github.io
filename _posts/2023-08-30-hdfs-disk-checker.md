---
title: Hadoop DiskChecker
author: jiazz
date: 2023-08-30 14:00:00 +0800
categories: [hadoop]
tags: [hadoop, hdfs]
---

## hadoop 2.7.3

### 何时检查

1. DataNode启动初始化BlockPool时，会检查一次
2. Starting CheckDiskError Thread， 每隔5s检查一次，下面介绍何始启动磁盘异常检查线程。

```java
// 1. BlockPool初始化时
org.apache.hadoop.hdfs.server.datanode.DataNode#initBlockPool
	// Exclude failed disks before initializing the block pools to avoid startup
    // failures.
    checkDiskError();


// 2. 线程异步检查

/**
   * Check if there is a disk failure asynchronously and if so, handle the error
   */
  public void checkDiskErrorAsync() {
    synchronized(checkDiskErrorMutex) {
      checkDiskErrorFlag = true;
      if(checkDiskErrorThread == null) {
        startCheckDiskErrorThread();
        checkDiskErrorThread.start();
        LOG.info("Starting CheckDiskError Thread");
      }
    }
  }

/**
   * Starts a new thread which will check for disk error check request 
     * every 5 sec
   */
org.apache.hadoop.hdfs.server.datanode.DataNode#startCheckDiskErrorThread
	checkDiskError();
```

**何时会启动磁盘异步检查线程？**

1. BlockReceiver初始化发生IOException时
2. BlockReceiver在close block file发生IOException时
3. BlockReceiver在receivePacket发生IOException时
4. BlockReceiver处理incoming acks响应线程，运行过程出现IOException时
5. DataNode的DataTransfer线程向其它节点发送block发生IOException时
6. DirectoryScanner在扫目录，对目录执行listFile(dir)发生IOException时
7. FsDatasetImpl中在查看block对应文件时，如果返回文件不为null，但是文件不存在时，可能磁盘坏了

启动线程，需要先拿到锁checkDiskErrorMutex

```java
// 构造函数
org.apache.hadoop.hdfs.server.datanode.BlockReceiver#BlockReceiver
	datanode.checkDiskErrorAsync();

  /**
   * close files and release volume reference.
   */
org.apache.hadoop.hdfs.server.datanode.BlockReceiver#close
	datanode.checkDiskErrorAsync();
  /** 
   * Receives and processes a packet. It can contain many chunks.
   * returns the number of data bytes that the packet has.
   */
org.apache.hadoop.hdfs.server.datanode.BlockReceiver#receivePacket
	datanode.checkDiskErrorAsync();

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
org.apache.hadoop.hdfs.server.datanode.BlockReceiver.PacketResponder#run
	datanode.checkDiskErrorAsync();

  /**
   * DataTransfer
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
org.apache.hadoop.hdfs.server.datanode.DataNode.DataTransfer#run
    // check if there are any disk problem
    checkDiskErrorAsync();

/**
 * DirectoryScanner：
 * Periodically scans the data directories for block and block metadata files.
 * Reconciles the differences with block information maintained in the dataset.
 */
/** Compile list {@link ScanInfo} for the blocks in the directory <dir> */
org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler#compileReport
        // Initiate a check on disk failure.
        datanode.checkDiskErrorAsync();

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl#validateBlockFile
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskErrorAsync();
```

### 如何检查

#### hdfs检查主流程

会遍历本机每个Volume，逐个检查
有专门的工具类DiskChecker去检查，如果抛DiskErrorException，表示坏盘
最终是通过java.io接口调用native方法(java调非java方法的一种方式)去检查。

```java
/**
   * Check the disk error
   */
org.apache.hadoop.hdfs.server.datanode.DataNode#checkDiskError
Set<File> unhealthyDataDirs = data.checkDataDir();

/**
     * Check if all the data directories are healthy
     * @return A set of unhealthy data directories.
     */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl#checkDataDir
return volumes.checkDirs();


/**
   * Calls {@link FsVolumeImpl#checkDirs()} on each volume.
   * 
   * Use checkDirsMutext to allow only one instance of checkDirs() call
   *
   * @return list of all the failed volumes.
   */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#checkDirs
fsv.checkDirs();

org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl#checkDirs
for(BlockPoolSlice s : bpSlices.values()) {
    s.checkDirs();
}

// 会抛DiskErrorException
/**
* // directory where finalized replicas are stored 
* finalizedDir = new File(currentDir, DataStorage.STORAGE_DIR_FINALIZED);
* // directory store RBW replica 
* rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);
* // directory store Temporary replica
* tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
**/
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice#checkDirs
DiskChecker.checkDir(finalizedDir);
DiskChecker.checkDir(tmpDir);
DiskChecker.checkDir(rbwDir);


/**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
org.apache.hadoop.util.DiskChecker#checkDir(java.io.File){
    if (!mkdirsWithExistsCheck(dir)) {
      throw new DiskErrorException("Cannot create directory: "
                                   + dir.toString());
    }
    checkDirAccess(dir);
}

  /**
   * Checks that the given file is a directory and that the current running
   * process can read, write, and execute it.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not a directory, not readable, not
   *   writable, or not executable
   */
org.apache.hadoop.util.DiskChecker#checkDirAccess{
    if (!dir.isDirectory()) {
      throw new DiskErrorException("Not a directory: "
                                   + dir.toString());
    }

    checkAccessByFileMethods(dir);
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by using methods of the File object.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
org.apache.hadoop.util.DiskChecker#checkAccessByFileMethods
{
    if (!FileUtil.canRead(dir)) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    if (!FileUtil.canWrite(dir)) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }

    if (!FileUtil.canExecute(dir)) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString());
    }
  }

```

#### 底层检查流程

以canRead为例

```java
  /**
   * Platform independent implementation for {@link File#canRead()}
   * @param f input file
   * @return On Unix, same as {@link File#canRead()}
   *         On Windows, true if process has read access on the path
   */
org.apache.hadoop.fs.FileUtil#canRead
	f.canRead()


    /**
     * Tests whether the application can read the file denoted by this
     * abstract pathname. On some platforms it may be possible to start the
     * Java virtual machine with special privileges that allow it to read
     * files that are marked as unreadable. Consequently this method may return
     * {@code true} even though the file does not have read permissions.
     *
     * @return  <code>true</code> if and only if the file specified by this
     *          abstract pathname exists <em>and</em> can be read by the
     *          application; <code>false</code> otherwise
     *
     * @throws  SecurityException
     *          If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *          method denies read access to the file
     */
	java.io.File#canRead
    public boolean canRead() {
        SecurityManager security = System.getSecurityManager();
        if (security != null) {
            security.checkRead(path);
        }
        if (isInvalid()) {
            return false;
        }
        return fs.checkAccess(this, FileSystem.ACCESS_READ);
    }

    /**
     * Check if the file has an invalid path. Currently, the inspection of
     * a file path is very limited, and it only covers Nul character check.
     * Returning true means the path is definitely invalid/garbage. But
     * returning false does not guarantee that the path is valid.
     *
     * @return true if the file path is invalid.
     */
	java.io.File#isInvalid
    final boolean isInvalid() {
        if (status == null) {
            status = (this.path.indexOf('\u0000') < 0) ? PathStatus.CHECKED
                                                       : PathStatus.INVALID;
        }
        return status == PathStatus.INVALID;
    }

    /**
     * Check whether the file or directory denoted by the given abstract
     * pathname may be accessed by this process.  The second argument specifies
     * which access, ACCESS_READ, ACCESS_WRITE or ACCESS_EXECUTE, to check.
     * Return false if access is denied or an I/O error occurs
     */
	java.io.UnixFileSystem#checkAccess
    public native boolean checkAccess(File f, int access);
```

#### 附：hdfs数据目录

```java
$ ll /data/disk1/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577
total 12
drwxrwxr-x 4 hadoop hadoop 4096 Aug 17 12:17 current
-rw-rw-r-- 1 hadoop hadoop  166 Aug 17 12:17 scanner.cursor
drwxrwxr-x 2 hadoop hadoop 4096 Aug 17 12:17 tmp
$ ll current/
total 12
drwxrwxr-x 7 hadoop hadoop 4096 Aug 22 07:04 finalized
drwxrwxr-x 2 hadoop hadoop 4096 Aug 22 14:22 rbw
-rw-rw-r-- 1 hadoop hadoop  130 Aug 17 12:17 VERSION
$ ll current/finalized/
total 20
drwxrwxr-x  68 hadoop hadoop 4096 Aug 18 06:41 subdir64
drwxrwxr-x 126 hadoop hadoop 4096 Aug 19 14:06 subdir65
drwxrwxr-x 124 hadoop hadoop 4096 Aug 20 21:22 subdir66
drwxrwxr-x 124 hadoop hadoop 4096 Aug 22 06:12 subdir67
drwxrwxr-x  35 hadoop hadoop 4096 Aug 22 14:22 subdir68
```

### 如何下盘

#### 磁盘检查过程中

- volumeList.remove(target) // 先从volumeList中去除
- blockScanner.removeVolumeScanner(target) // Stops and removes a volume scanner.
- target.closeAndWait(); // Close this volume and wait all other threads to release the reference count
- target.shutdown(); // 
- 见到Removed volume: xxx日志，表示下盘完成

```java
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#checkDirs
	removeVolume(fsv);

  /**
   * Dynamically remove a volume in the list.
   * @param target the volume instance to be removed.
   */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#removeVolume(org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl)
private void removeVolume(FsVolumeImpl target) {
    while (true) {
      final FsVolumeImpl[] curVolumes = volumes.get();
      final List<FsVolumeImpl> volumeList = Lists.newArrayList(curVolumes);
      if (volumeList.remove(target)) {
        if (volumes.compareAndSet(curVolumes,
            volumeList.toArray(new FsVolumeImpl[volumeList.size()]))) {
          if (blockScanner != null) {
            blockScanner.removeVolumeScanner(target);
          }
          try {
            target.closeAndWait();
          } catch (IOException e) {
            FsDatasetImpl.LOG.warn(
                "Error occurs when waiting volume to close: " + target, e);
          }
          target.shutdown();
          FsDatasetImpl.LOG.info("Removed volume: " + target);
          break;
        } else {
          if (FsDatasetImpl.LOG.isDebugEnabled()) {
            FsDatasetImpl.LOG.debug(
                "The volume list has been changed concurrently, " +
                "retry to remove volume: " + target);
          }
        }
      } else {
        if (FsDatasetImpl.LOG.isDebugEnabled()) {
          FsDatasetImpl.LOG.debug("Volume " + target +
              " does not exist or is removed by others.");
        }
        break;
      }
    }
  }
```

#### 磁盘检查后

- Remove volumes and block info from FsDataset
- Remove volumes from DataStorage
- Reset configuration DATA_DIR and {@link dataDirs} to represent active volumes

```java
org.apache.hadoop.hdfs.server.datanode.DataNode#checkDiskError
    // Remove all unhealthy volumes from DataNode.
    removeVolumes(unhealthyDataDirs, false);

  /**
   * Remove volumes from DataNode.
   *
   * It does three things:
   * <li>
   *   <ul>Remove volumes and block info from FsDataset.</ul>
   *   <ul>Remove volumes from DataStorage.</ul>
   *   <ul>Reset configuration DATA_DIR and {@link dataDirs} to represent
   *   active volumes.</ul>
   * </li>
   * @param absoluteVolumePaths the absolute path of volumes.
   * @param clearFailure if true, clears the failure information related to the
   *                     volumes.
   * @throws IOException
   */
org.apache.hadoop.hdfs.server.datanode.DataNode#removeVolumes(java.util.Set<java.io.File>, boolean)
	// Remove volumes and block infos from FsDataset.
    data.removeVolumes(absoluteVolumePaths, clearFailure);
	
    // Remove volumes from DataStorage.
    try {
      storage.removeVolumes(absoluteVolumePaths);
    } catch (IOException e) {
      ioe = e;
    }

    // Set configuration and dataDirs to reflect volume changes.
    for (Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext(); ) {
      StorageLocation loc = it.next();
      if (absoluteVolumePaths.contains(loc.getFile().getAbsoluteFile())) {
        it.remove();
      }
    }
    conf.set(DFS_DATANODE_DATA_DIR_KEY, Joiner.on(",").join(dataDirs));
	
```

手动下盘方式：
在配置dfs.datanode.data.dir中去掉坏盘目录，然后刷新配置 `hdfs dfsadmin -reconfig datanode 10.92.175.34:50020 start`

```xml
<property>
  <name>dfs.datanode.data.dir</name>
  <value>file:///data/disk1/hadoop/hdfs/data,file:///data/disk2/hadoop/hdfs/data,file:///data/disk3/hadoop/hdfs/data,file:///data/disk4/hadoop/hdfs/data,file:///data/disk5/hadoop/hdfs/data,file:///data/disk6/hadoop/hdfs/data,file:///data/disk7/hadoop/hdfs/data,file:///data/disk8/hadoop/hdfs/data,file:///data/disk9/hadoop/hdfs/data,file:///data/disk10/hadoop/hdfs/data,file:///data/disk11/hadoop/hdfs/data,file:///data/disk12/hadoop/hdfs/data</value>
</property
```

### 下盘日志

#### namenode

```shell
2023-08-15 02:02:42,699 WARN org.apache.hadoop.hdfs.server.namenode.NameNode: Disk error on DatanodeRegistration(10.92.105.62:10040, datanodeUuid=44c3a0b8-6bad-4d6a-9bbf-06a735295ce2, infoPort=10060, infoSecurePort=0, ipcPort=50020, storageInfo=lv=-56;cid=bjrw-001;nsid=442760267;c=0): DataNode failed volumes:/data/disk10/hadoop/hdfs/data;
2023-08-15 02:02:42,701 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: Number of failed storage changes from 0 to 1
2023-08-15 02:02:42,701 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: [DISK]DS-11651b55-446b-4fd8-b09a-8859cf7d570a:NORMAL:10.92.105.62:10040 failed.
2023-08-15 02:02:42,704 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: DS-11651b55-446b-4fd8-b09a-8859cf7d570a had lastBlockReportId 0x0, but curBlockReportId = 0x2ed368382503
2023-08-15 02:02:42,704 WARN org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: processReport 0x2ed368382503: removing zombie storage DS-11651b55-446b-4fd8-b09a-8859cf7d570a, which no longer exists on the DataNode.
2023-08-15 02:02:42,704 WARN org.apache.hadoop.hdfs.server.blockmanagement.BlockManager: processReport 0x2ed368382503: removed 0 replicas from storage DS-11651b55-446b-4fd8-b09a-8859cf7d570a, which no longer exists on the DataNode.
```

#### datanode

```shell
2023-08-15 02:02:40,008 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: Slow BlockReceiver write data to disk cost:43101ms (threshold=300ms)
2023-08-15 02:02:40,159 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Starting CheckDiskError Thread
...
2023-08-15 02:02:40,160 WARN org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Removing failed volume /data/disk10/hadoop/hdfs/data/current: 
org.apache.hadoop.util.DiskChecker$DiskErrorException: Directory is not writable: /data/disk10/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577/current/finalized
        at org.apache.hadoop.util.DiskChecker.checkAccessByFileMethods(DiskChecker.java:193)
        at org.apache.hadoop.util.DiskChecker.checkDirAccess(DiskChecker.java:174)
        at org.apache.hadoop.util.DiskChecker.checkDir(DiskChecker.java:108)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice.checkDirs(BlockPoolSlice.java:304)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl.checkDirs(FsVolumeImpl.java:798)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList.checkDirs(FsVolumeList.java:241)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl.checkDataDir(FsDatasetImpl.java:2030)
        at org.apache.hadoop.hdfs.server.datanode.DataNode.checkDiskError(DataNode.java:3141)
        at org.apache.hadoop.hdfs.server.datanode.DataNode.access$800(DataNode.java:244)
        at org.apache.hadoop.hdfs.server.datanode.DataNode$7.run(DataNode.java:3174)
        at java.lang.Thread.run(Thread.java:745)
2023-08-15 02:02:40,161 INFO org.apache.hadoop.hdfs.server.datanode.BlockScanner: Removing scanner for volume /data/disk10/hadoop/hdfs/data (StorageID DS-11651b55-446b-4fd8-b09a-8859cf7d570a)
2023-08-15 02:02:40,161 INFO org.apache.hadoop.hdfs.server.datanode.VolumeScanner: VolumeScanner(/data/disk10/hadoop/hdfs/data, DS-11651b55-446b-4fd8-b09a-8859cf7d570a) exiting.
2023-08-15 02:02:40,161 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: opWriteBlock BP-844174450-10.92.101.62-1523246230577:blk_1144955377_71215312 received exception java.io.IOException: Premature EOF from inputStream
2023-08-15 02:02:40,161 WARN org.apache.hadoop.hdfs.server.datanode.VolumeScanner: VolumeScanner(/data/disk10/hadoop/hdfs/data, DS-11651b55-446b-4fd8-b09a-8859cf7d570a): error saving org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl$BlockIteratorImpl@5fb9d7de.
java.io.FileNotFoundException: /data/disk10/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577/scanner.cursor.tmp (Read-only file system)
        at java.io.FileOutputStream.open0(Native Method)
        at java.io.FileOutputStream.open(FileOutputStream.java:270)
        at java.io.FileOutputStream.<init>(FileOutputStream.java:213)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl$BlockIteratorImpl.save(FsVolumeImpl.java:681)
        at org.apache.hadoop.hdfs.server.datanode.VolumeScanner.saveBlockIterator(VolumeScanner.java:314)
        at org.apache.hadoop.hdfs.server.datanode.VolumeScanner.run(VolumeScanner.java:633)
2023-08-15 02:02:40,256 WARN org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Failed to write dfsUsed to /data/disk10/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577/current/dfsUsed
java.io.FileNotFoundException: /data/disk10/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577/current/dfsUsed (Read-only file system)
at java.io.FileOutputStream.open0(Native Method)
        at java.io.FileOutputStream.open(FileOutputStream.java:270)
        at java.io.FileOutputStream.<init>(FileOutputStream.java:213)
        at java.io.FileOutputStream.<init>(FileOutputStream.java:162)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice.saveDfsUsed(BlockPoolSlice.java:243)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice.shutdown(BlockPoolSlice.java:678)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl.shutdown(FsVolumeImpl.java:827)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList.removeVolume(FsVolumeList.java:327)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList.checkDirs(FsVolumeList.java:249)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl.checkDataDir(FsDatasetImpl.java:2030)
        at org.apache.hadoop.hdfs.server.datanode.DataNode.checkDiskError(DataNode.java:3141)
        at org.apache.hadoop.hdfs.server.datanode.DataNode.access$800(DataNode.java:244)
        at org.apache.hadoop.hdfs.server.datanode.DataNode$7.run(DataNode.java:3174)
        at java.lang.Thread.run(Thread.java:745)
2023-08-15 02:02:40,259 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Removed volume: /data/disk10/hadoop/hdfs/data/current
2023-08-15 02:02:40,259 WARN org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Completed checkDirs. Found 1 failure volumes.
2023-08-15 02:02:40,259 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Deactivating volumes (clear failure=false): /data/disk10/hadoop/hdfs/data
2023-08-15 02:02:40,260 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Removing /data/disk10/hadoop/hdfs/data from FsDataset.
2023-08-15 02:02:40,262 INFO org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry: Block 1144933273_BP-844174450-10.92.101.62-1523246230577 has been invalidated.  Marking short-circuit slots as invalid: Slot(slotIdx=40, shm=RegisteredShm(38bf9abc5a2436e5e69d94822c34bec0))
...
2023-08-15 02:02:40,262 INFO org.apache.hadoop.hdfs.server.common.Storage: Removing block level storage: /data/disk10/hadoop/hdfs/data/current/BP-844174450-10.92.101.62-1523246230577
2023-08
```

## hadoop 2.9.2

### 何时检查

hbase 2.9以后磁盘检查有些变化
[https://issues.apache.org/jira/browse/HDFS-11274](https://issues.apache.org/jira/browse/HDFS-11274)
This is a performance improvement that is possible after [HDFS-11182](https://issues.apache.org/jira/browse/HDFS-11182). The goal is to trigger async volume check with throttling only on suspected volume upon datanode file IO errors.


**何时检查：**

1. DataNode启动初始化BlockPool时，会检查一次，检查所有盘

```java
org.apache.hadoop.hdfs.server.datanode.DataNode#initBlockPool
    // Exclude failed disks before initializing the block pools to avoid startup
    // failures.
    checkDiskError();

org.apache.hadoop.hdfs.server.datanode.DataNode#checkDiskError
	unhealthyVolumes = volumeChecker.checkAllVolumes(data);
```

2. 异常检查单个volume
   1. DirectoryScanner在扫目录，对目录执行listFile(dir)发生IOException时
   2. FileIoProvider在进行File IO操作有Exception时
   3. FsDatasetImpl中在查看block对应文件时，如果返回文件不为null，但是文件不存在时，可能磁盘坏了

```java
  /**
   * Check a single volume asynchronously, returning a {@link ListenableFuture}
   * that can be used to retrieve the final result.
   *
   * If the volume cannot be referenced then it is already closed and
   * cannot be checked. No error is propagated to the callback.
   *
   * @param volume the volume that is to be checked.
   * @param callback callback to be invoked when the volume check completes.
   * @return true if the check was scheduled and the callback will be invoked.
   *         false otherwise.
   */
org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker#checkVolume


  /**
   * Check if there is a disk failure asynchronously
   * and if so, handle the error.
   */
org.apache.hadoop.hdfs.server.datanode.DataNode#checkDiskErrorAsync
    volumeChecker.checkVolume(
        volume, new DatasetVolumeChecker.Callback() { // 回调函数，检查完处理逻辑
          @Override
          public void call(Set<FsVolumeSpi> healthyVolumes,
                           Set<FsVolumeSpi> failedVolumes) {
            if (failedVolumes.size() > 0) {
              LOG.warn("checkDiskErrorAsync callback got {} failed volumes: {}",
                  failedVolumes.size(), failedVolumes);
            } else {
              LOG.debug("checkDiskErrorAsync: no volume failures detected");
            }
            lastDiskErrorCheck = Time.monotonicNow();
            handleVolumeFailures(failedVolumes); 
          }
        });


    /**
     * Compile a list of {@link ScanInfo} for the blocks in the directory
     * given by {@code dir}.
     *
     * @param vol the volume that contains the directory to scan
     * @param bpFinalizedDir the root directory of the directory to scan
     * @param dir the directory to scan
     * @param report the list onto which blocks reports are placed
     */
org.apache.hadoop.hdfs.server.datanode.DirectoryScanner.ReportCompiler#compileReport
		// Initiate a check on disk failure.
        datanode.checkDiskErrorAsync(volume);


// file io 操作失败时：flush delete crateFile....
org.apache.hadoop.hdfs.server.datanode.FileIoProvider#onFailure
	datanode.checkDiskErrorAsync(volume);

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl#validateBlockFile
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskErrorAsync(info.getVolume());

```

### 如何检查

这里只介绍checkVolume，checkVolumes只是遍历每块盘检查，逻辑一致
引入DatasetVolumeChecker检查磁盘
**主要步骤：**

- 先获取到volumeReference

FsVolumeReference volumeReference = volume.obtainReference();

- 再通过AsyncChecker调度任务去检查该volume

​	Optional<ListenableFuture<VolumeCheckResult>> olf = delegateChecker.schedule(volume, IGNORED_CONTEXT);

- 执行异步检查结果的回调函数，即检查后处理逻辑，在如何下盘部分介绍

Futures.addCallback(olf.get(),
    new ResultHandler(volumeReference, new HashSet<FsVolumeSpi>(),
    new HashSet<FsVolumeSpi>(),
    new AtomicLong(1), callback));

```java
  /**
   * Check a single volume asynchronously, returning a {@link ListenableFuture}
   * that can be used to retrieve the final result.
   *
   * If the volume cannot be referenced then it is already closed and
   * cannot be checked. No error is propagated to the callback.
   *
   * @param volume the volume that is to be checked.
   * @param callback callback to be invoked when the volume check completes.
   * @return true if the check was scheduled and the callback will be invoked.
   *         false otherwise.
   */
org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker#checkVolume
```

**AsyncChecker是如何schedule检查的？**
这里用到用到了guava工具包的ListenableFuture

1. 会有一个Map保存每个volume的检查结果，如果900000ms即15min钟内检查过该volume，就不用再继续检查该盘了，直接就结束了
2. ListenableFuture异步检查，如果抛出DiskErrorException表示坏盘，否则返回一个VolumeCheckResult.HEALTHY的状态值，异常检查超时时间是10min
3. 通过DiskChecker去检查目录finalizedDir、tmpDir、rbwDir 是否可读可写可执行，否则会抛出异常DiskErrorException

```java
  /**
   * See {@link AsyncChecker#schedule}
   *
   * If the object has been checked recently then the check will
   * be skipped. Multiple concurrent checks for the same object
   * will receive the same Future.
   */
  @Override
org.apache.hadoop.hdfs.server.datanode.checker.ThrottledAsyncChecker#schedule
    final ListenableFuture<V> lfWithoutTimeout = executorService.submit(
        new Callable<V>() {
          @Override
          public V call() throws Exception {
            return target.check(context);
          }
        });


  /**
   * Query the health of this object. This method may hang
   * indefinitely depending on the status of the target resource.
   *
   * @param context for the probe operation. May be null depending
   *                on the implementation.
   *
   * @return result of the check operation.
   *
   * @throws Exception encountered during the check operation. An
   *                   exception indicates that the check failed.
   */
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl#check
    for(BlockPoolSlice s : bpSlices.values()) {
      s.checkDirs();
    }

org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice#checkDirs
    DiskChecker.checkDir(finalizedDir);
    DiskChecker.checkDir(tmpDir);
    DiskChecker.checkDir(rbwDir);
```

DiskChecker检查调用链：

```java
org.apache.hadoop.util.DiskChecker#checkDir(java.io.File)
  /**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {
    checkDirInternal(dir);
  }

  private static void checkDirInternal(File dir)
      throws DiskErrorException {    
    if (!mkdirsWithExistsCheck(dir)) {
      throw new DiskErrorException("Cannot create directory: "
                                   + dir.toString());
    }
    checkAccessByFileMethods(dir);
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by using methods of the File object.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
  private static void checkAccessByFileMethods(File dir)
      throws DiskErrorException {
    if (!dir.isDirectory()) {
      throw new DiskErrorException("Not a directory: "
          + dir.toString());
    }

    if (!FileUtil.canRead(dir)) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    if (!FileUtil.canWrite(dir)) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }

    if (!FileUtil.canExecute(dir)) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString());
    }
  }
```

### 如何下盘

在检查磁盘使用了ListenableFuture异步检查，检查结果的回调函数，即为下盘逻辑

```java
org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker#checkVolume
	Futures.addCallback(olf.get(),
          new ResultHandler(volumeReference, new HashSet<FsVolumeSpi>(),
          new HashSet<FsVolumeSpi>(),
          new AtomicLong(1), callback));
```

如果异常检查过程有异常表示检查到坏盘，如果有检查结果状态码表示成功。打印相关日志：如下就是在日志中看到的逻辑

```java
org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker.ResultHandler#ResultHandler

	@Override
    public void onSuccess(@Nonnull VolumeCheckResult result) {
      switch(result) {
      case HEALTHY:
      case DEGRADED:
        LOG.debug("Volume {} is {}.", reference.getVolume(), result);
        markHealthy(); //将volume添加到healthyVolumes列表中
        break;
      case FAILED:
        LOG.warn("Volume {} detected as being unhealthy",
            reference.getVolume());
        markFailed();
        break;
      default:
        LOG.error("Unexpected health check result {} for volume {}",
            result, reference.getVolume());
        markHealthy();
        break;
      }
      cleanup();
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
      Throwable exception = (t instanceof ExecutionException) ?
          t.getCause() : t;
      LOG.warn("Exception running disk checks against volume " +
          reference.getVolume(), exception);
      markFailed(); // 将volume添加到failedVolumes列表
      cleanup(); //调callback函数下盘 handleVolumeFailures
    }
```

以看到成功，将volume添加到healthyVolumes列表中，失败就添加到 handleVolumeFailures列表
然后调用callback函数handleVolumeFailures

```java
org.apache.hadoop.hdfs.server.datanode.DataNode#handleVolumeFailures
  private void handleVolumeFailures(Set<FsVolumeSpi> unhealthyVolumes) {
    if (unhealthyVolumes.isEmpty()) {
      LOG.debug("handleVolumeFailures done with empty " +
          "unhealthyVolumes");
      return;
    }

    data.handleVolumeFailures(unhealthyVolumes); // 下盘
    final Set<File> unhealthyDirs = new HashSet<>(unhealthyVolumes.size());

    StringBuilder sb = new StringBuilder("DataNode failed volumes:");
    for (FsVolumeSpi vol : unhealthyVolumes) {
      unhealthyDirs.add(new File(vol.getBasePath()).getAbsoluteFile());
      sb.append(vol).append(";");
    }

    try {
      // Remove all unhealthy volumes from DataNode.
      removeVolumes(unhealthyDirs, false);
    } catch (IOException e) {
      LOG.warn("Error occurred when removing unhealthy storage dirs: "
          + e.getMessage(), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(sb.toString());
    }
      // send blockreport regarding volume failure
    handleDiskError(sb.toString());
  }
```

#### data.handleVolumeFailures(unhealthyVolumes)

- volumes.remove(target) // 先从volumeList中去除
- blockScanner.removeVolumeScanner(target) // Stops and removes a volume scanner.
- target.setClosed(); // Close this volume and wait all other threads to release the reference count
- target.shutdown(); // 
- volumesBeingRemoved.add(target)
- 见到Removed volume: xxx日志，表示下盘完成

```java
org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeList#handleVolumeFailures
          removeVolume(fsv);

  /**
   * Dynamically remove a volume in the list.
   * @param target the volume instance to be removed.
   */
  private void removeVolume(FsVolumeImpl target) {
    if (volumes.remove(target)) {
      if (blockScanner != null) {
        blockScanner.removeVolumeScanner(target);
      }
      try {
        target.setClosed();
      } catch (IOException e) {
        FsDatasetImpl.LOG.warn(
            "Error occurs when waiting volume to close: " + target, e);
      }
      target.shutdown();
      volumesBeingRemoved.add(target);
      FsDatasetImpl.LOG.info("Removed volume: " + target);
    } else {
      if (FsDatasetImpl.LOG.isDebugEnabled()) {
        FsDatasetImpl.LOG.debug("Volume " + target +
            " does not exist or is removed by others.");
      }
    }
  }
```

#### removeVolumes

同上2.7.3不缀述

```java
  /**
   * Remove volumes from DataNode.
   *
   * It does three things:
   * <li>
   *   <ul>Remove volumes and block info from FsDataset.</ul>
   *   <ul>Remove volumes from DataStorage.</ul>
   *   <ul>Reset configuration DATA_DIR and {@link #dataDirs} to represent
   *   active volumes.</ul>
   * </li>
   * @param storageLocations the absolute path of volumes.
   * @param clearFailure if true, clears the failure information related to the
   *                     volumes.
   * @throws IOException
   */
org.apache.hadoop.hdfs.server.datanode.DataNode#removeVolumes(java.util.Set<java.io.File>, boolean)

```

### 下盘日志

#### namenode

```shell
2023-08-25 18:24:38,768 WARN org.apache.hadoop.hdfs.server.namenode.NameNode: Disk error on DatanodeRegistration(10.92.100.37:10040, datanodeUuid=247f0771-dbe1-4fe0-b93b-8d7630
88cdf3, infoPort=10060, infoSecurePort=0, ipcPort=50020, storageInfo=lv=-57;cid=bjrw-009;nsid=799046926;c=1584421416376): DataNode failed volumes:/data/disk6/hadoop/hdfs/data/c
urrent;
2023-08-25 18:24:40,425 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: Number of failed storages changes from 0 to 1
2023-08-25 18:24:40,425 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: [DISK]DS-249063b4-d9ea-4978-aaa1-7b506b80c3aa:NORMAL:10.92.100.37:10040 failed.
2023-08-25 18:24:40,425 INFO org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor: Removed storage [DISK]DS-249063b4-d9ea-4978-aaa1-7b506b80c3aa:FAILED:10.92.100.37
:10040 from DataNode 10.92.100.37:10040
```

#### datanode

```shell
2023-08-25 18:24:38,295 WARN org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker: Exception running disk checks against volume /data/disk6/hadoop/hdfs/data/current
org.apache.hadoop.util.DiskChecker$DiskErrorException: Directory is not writable: /data/disk6/hadoop/hdfs/data/current/BP-1051756431-10.92.184.51-1584421416376/current/finalized
        at org.apache.hadoop.util.DiskChecker.checkAccessByFileMethods(DiskChecker.java:167)
        at org.apache.hadoop.util.DiskChecker.checkDirInternal(DiskChecker.java:100)
        at org.apache.hadoop.util.DiskChecker.checkDir(DiskChecker.java:77)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice.checkDirs(BlockPoolSlice.java:350)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl.check(FsVolumeImpl.java:920)
        at org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl.check(FsVolumeImpl.java:84)
        at org.apache.hadoop.hdfs.server.datanode.checker.ThrottledAsyncChecker$1.call(ThrottledAsyncChecker.java:142)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
2023-08-25 18:24:38,296 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: checkDiskErrorAsync callback got 1 failed volumes: [/data/disk6/hadoop/hdfs/data/current]
2023-08-25 18:24:38,296 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: checkDiskErrorAsync callback got 1 failed volumes: [/data/disk6/hadoop/hdfs/data/current]
2023-08-25 18:24:38,296 INFO org.apache.hadoop.hdfs.server.datanode.BlockScanner: Removing scanner for volume /data/disk6/hadoop/hdfs/data (StorageID DS-249063b4-d9ea-4978-aaa1-7b506b80c3aa)
2023-08-25 18:24:38,299 INFO org.apache.hadoop.hdfs.server.datanode.VolumeScanner: VolumeScanner(/data/disk6/hadoop/hdfs/data, DS-249063b4-d9ea-4978-aaa1-7b506b80c3aa) exiting.
2023-08-25 18:24:38,324 INFO org.apache.hadoop.hdfs.server.datanode.checker.ThrottledAsyncChecker: Scheduling a check for /data/disk6/hadoop/hdfs/data/current
2023-08-25 18:24:38,324 WARN org.apache.hadoop.hdfs.server.datanode.VolumeScanner: VolumeScanner(/data/disk6/hadoop/hdfs/data, DS-249063b4-d9ea-4978-aaa1-7b506b80c3aa): error saving org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl$BlockIteratorImpl@672e328a.
...
23-08-25 18:24:38,345 WARN org.apache.hadoop.fs.CachingGetSpaceUsed: Thread Interrupted waiting to refresh disk information: sleep interrupted
2023-08-25 18:24:38,346 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Removed volume: /data/disk6/hadoop/hdfs/data/current
2023-08-25 18:24:38,351 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Volume reference is released.
2023-08-25 18:24:38,352 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Volume reference is released.
2023-08-25 18:24:38,352 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Deactivating volumes (clear failure=false): /data/disk6/hadoop/hdfs/data
2023-08-25 18:24:38,352 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Removing /data/disk6/hadoop/hdfs/data from FsDataset.
2023-08-25 18:24:38,352 INFO org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl: Volume reference is released.
...   
# Marking short-circuit slots as invalid
2023-08-25 18:24:38,378 INFO org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry: Block 1245048997_BP-1051756431-10.92.184.51-1584421416376 has been invalidated.  Marking short-circuit slots as invalid: Slot(slotIdx=81, shm=RegisteredShm(e9c509600491831541e7e473a215cb3f))
2023-08-25 18:24:38,379 INFO org.apache.hadoop.hdfs.server.common.Storage: Removing block level storage: /data/disk6/hadoop/hdfs/data/current/BP-1051756431-10.92.184.51-1584421416376
2023-08-25 18:24:38,589 INFO org.apache.hadoop.hdfs.server.datanode.DataNode: Deactivating volumes (clear failure=false): /data/disk6/hadoop/hdfs/data
2023-08-25 18:24:38,589 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode.handleDiskError on : [DataNode failed volumes:/data/disk6/hadoop/hdfs/data/current;] Keep Running: true
2023-08-25 18:24:38,589 WARN org.apache.hadoop.hdfs.server.datanode.DataNode: DataNode.handleDiskError on : [DataNode failed volumes:/data/disk6/hadoop/hdfs/data/current;] Keep Running: true

```
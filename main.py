import csv
import os
import math 
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import datetime

# Const
MB_IN_BYTES = 1000 * 1000
GB_IN_BYTES = 1000 * MB_IN_BYTES
GB_IN_KB = 1000 * 1000
MINUTE_IN_SECONDS = 60
SECONDS_IN_MS = 1000

# Task 1 Const
MIN_SNAPSHOT_DURATION_FOR_TASK_1 = 60
TASK_1_LOOK_BACK_TIME = 60

# Task 3 Const
MIN_SNAPSHOT_TIME_LENGTH_TASK_3 = 60

# Task 4 Const
MIN_OPLOG_BYTES_PER_SECOND = 277777 # When converted into Oplog GB / hours, this 277777 is exact 1gb/hr 
MIN_SNAPSHOT_DURATION_FOR_TASK_4 = 15 

# Populate jobId to Atlas/CM cache
backupJobIdToEnv = {}
def populateBackupJobIdToEnvCache():
  for file in os.listdir("./allStats"):
    if (file.startswith("task5") or file.startswith("task6")):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        for row in spamreader:
          if (row[0] == "SnapshotId"):
            # ignore the first row
            continue
          
          jobId = row[1]
          if jobId not in backupJobIdToEnv:
            backupJobIdToEnv[jobId] = row[3]

          print("Finished Processing file: ", "./allStats/" + file)
populateBackupJobIdToEnvCache()

## Helpers

# Parses the snapshot metadata row and then returns the number of minutes the snapshot took.
def computeSnapshotFinishTime(snapshotMetadataRow):
  startTime = snapshotMetadataRow[2] # "TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1"
  startTimeDateObj = datetime.datetime.strptime(startTime, 'TS time:%a %b %d %H:%M:%S GMT %Y inc:1')

  endTime = snapshotMetadataRow[3] # " Wed Mar 24 15:52:51 GMT 2021"]
  endTimeDateObj = datetime.datetime.strptime(endTime, '%a %b %d %H:%M:%S GMT %Y')

  # print('Start Date-time:', startTimeDateObj)
  # print('End Date-time:', endTimeDateObj)

  diffSeconds = (endTimeDateObj - startTimeDateObj).total_seconds()
  return math.ceil(diffSeconds / 60)
# Sanity check test case
# print(computeSnapshotFinishTime(["605b603812f4a64f7b5cc766", "5d109ba8c56c98ab46a51b8d", "TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1", "Wed Mar 24 15:52:51 GMT 2021"]))

#########################################################
#########################################################
#### Task 1
#  * task1 will be in the following format for each snapshot:
#  *
# 605b603812f4a64f7b5cc766	5d109ba8c56c98ab46a51b8d	TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1	Wed Mar 24 15:52:51 GMT 2021			
# Snapshot Node Stats:	Wed Mar 24 15:51:56 GMT 2021	Wed Mar 24 15:52:56 GMT 2021	Wed Mar 24 15:53:56 GMT 2021	Wed Mar 24 15:54:56 GMT 2021	Wed Mar 24 15:55:56 GMT 2021	Wed Mar 24 15:56:56 GMT 2021
# Snapshot Node Disk Space Free:	7173742592	7178293248	7278637056	7278632960	7278739456	7278735360
# Snapshot Node Disk Space Used:	3553189888	3548639232	3448295424	3448299520	3448193024	3448197120
# Secondary Node Stats:	Wed Mar 24 15:51:48 GMT 2021	Wed Mar 24 15:52:48 GMT 2021	Wed Mar 24 15:53:48 GMT 2021	Wed Mar 24 15:54:48 GMT 2021	Wed Mar 24 15:55:48 GMT 2021	Wed Mar 24 15:56:48 GMT 2021
# Secondary Node Disk Space Free:	7215312896	7215308800	7215013888	7215026176	7210758144	7210774528
# Secondary Node Disk Space Used:	3511619584	3511623680	3511918592	3511906304	3516174336	3516157952
# Difference Between Node Disk Space Free Timestamp:	Wed Mar 24 15:51:56 GMT 2021	Wed Mar 24 15:52:56 GMT 2021	Wed Mar 24 15:53:56 GMT 2021	Wed Mar 24 15:54:56 GMT 2021	Wed Mar 24 15:55:56 GMT 2021	Wed Mar 24 15:56:56 GMT 2021
# Difference Between Node Disk Space Free:	41570304	37015552	63623168	63606784	67981312	67960832
# Difference Between Node Disk Space Used Timestamp:	Wed Mar 24 15:51:56 GMT 2021	Wed Mar 24 15:52:56 GMT 2021	Wed Mar 24 15:53:56 GMT 2021	Wed Mar 24 15:54:56 GMT 2021	Wed Mar 24 15:55:56 GMT 2021	Wed Mar 24 15:56:56 GMT 2021
# Difference Between Node Disk Space Used:	41570304	37015552	63623168	63606784	67981312	67960832
#########################################################
#########################################################
def analysisForTask1(nthGraph):
  totalSnapshots = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task1"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          if (len(logicalStatGrouping) == 11):
            #analyze only when logical group is complete
            filteredStatGroup = filterForTask1(logicalStatGrouping)
            if ("Throwable observed while computing stats. Stats may be incomplete for this row." in filteredStatGroup[0]):
              print("Observed invalid data row. Continuing")
              logicalStatGrouping = []
              continue

            # Plot and save graph
            if (nthGraph == None or totalSnapshots < nthGraph):
              plotTask1(filteredStatGroup, totalSnapshots)

            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1

            if (totalSnapshots % 100 == 0):
              print("Finished processing through " + str(totalSnapshots) + " snapshots")

  print("Total number of snapshots for task1 is: ", totalSnapshots)
  return totalSnapshots

def filterForTask1(statGroup):
  filteredGroup = []
  filteredGroup.append(statGroup[0]) # the first row needs no filtering

  timeRow = statGroup[7]
  diffDiskSpaceFree = statGroup[8]

  filteredTimeRow = []
  filteredDiffDiskSpaceFree = []

  validPoint = 1
  for i in range(1, len(timeRow)):
    filteredTimeRow.append(validPoint)
    filteredDiffDiskSpaceFree.append(int(diffDiskSpaceFree[i]) / MB_IN_BYTES)
    validPoint += 1

  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filteredDiffDiskSpaceFree)
  return filteredGroup


# TASK_1_LOOK_BACK_TIME references the amount of time the metric was collected 
# even after the $backupCursor was closed

def plotTask1(filteredStatGroup, nthGraphWorkedOn):
  # print("Plotting for", filteredStatGroup[0])
  snapshotEndMinute = computeSnapshotFinishTime(filteredStatGroup[0])

  timeRow = filteredStatGroup[1]
  diffDiskSpaceFree = filteredStatGroup[2]

  if (len(diffDiskSpaceFree) < (MIN_SNAPSHOT_DURATION_FOR_TASK_1 + TASK_1_LOOK_BACK_TIME)):
    print("Observed dataset without any datapoints for diffDiskSpaceFree. Continuing")
    return

  fig, ax1 = plt.subplots(1, 1)

  titleText = "Task 1 - Secondary Disk Usage Diff\n SnapshotID: {}, JobID: {}, Env: {}".format(filteredStatGroup[0][0], filteredStatGroup[0][1], backupJobIdToEnv[filteredStatGroup[0][1]])
  fig.suptitle(titleText, fontsize="small")

  ax1.plot(timeRow[1:], diffDiskSpaceFree[1:], '.-')
  ax1.set_ylabel('Disk Space Diff (MB)')
  ax1.set_xlabel('time (mins from snapshot start)')

  ax1.axvline(x=snapshotEndMinute, color="green")

  plt.tight_layout()
  plt.savefig("img/task1/" + backupJobIdToEnv[filteredStatGroup[0][1]] + "/" + str(nthGraphWorkedOn) + "_" + filteredStatGroup[0][0])

  plt.close()


#########################################################
#########################################################
#### Task 2
#  * task2 will be in the following format for each snapshot:
#  *
#  * Snapshot info....
#  * Time Row, T1, T2, T3 ....
#  * Queries Per Second, q1, q2, q3 ...
#  * Average OpExecution Time (ms / cmd), o1, o2, o3 ....
#  * Queued Requests (r & w combined), r1, r2, r3 ....
#  *
#########################################################
#########################################################
def analysisForTask2(nthGraph):
  totalSnapshotsWithSecondaryWorkload = 0
  totalSnapshots = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task2"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          #analyze when logical group is complete
          if (len(logicalStatGrouping) == 5):
            # if this snapshot contains a secondary workload
            if (snapshotContainsSecondaryWorkload(logicalStatGrouping)):
              totalSnapshotsWithSecondaryWorkload += 1

              if (nthGraph == None or totalSnapshotsWithSecondaryWorkload < nthGraph):
                plotTask2(logicalStatGrouping, totalSnapshotsWithSecondaryWorkload)

            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1

  print("Total number of snapshots is: ", totalSnapshots)
  print("Total number of snapshots with secondary workload is: ", totalSnapshotsWithSecondaryWorkload)
  return totalSnapshotsWithSecondaryWorkload

def snapshotContainsSecondaryWorkload(statGroup):
  for i in range(len(statGroup)):
    row = statGroup[i]

    if "Number of queued requests (read & write combined)" in row:
      for dataPoint in row:
        if (dataPoint == "NA" or dataPoint == "0"):
          continue
        
        if ("queued requests" in dataPoint): 
          continue

        if ("No Measurements Available" in dataPoint):
          continue

        dpVal = int(dataPoint)
        if (dpVal >= 10):
          return True
  return False

def plotTask2(statGroup, nthGraphWorkedOn):
  print("Plotting for", statGroup[0])
  filteredStatGroup = filterForTask2(statGroup)

  snapshotEndMinute = computeSnapshotFinishTime(filteredStatGroup[0])

  timeRow = filteredStatGroup[1]
  queriesPerSecond = filteredStatGroup[2]
  opExecutionTime = filteredStatGroup[3]
  queuedRequests = filteredStatGroup[4]

  fig, (ax1, ax2, ax3) = plt.subplots(3, 1)
  titleText = "Task 2 - Performance impact on Snapshot Secondary\n SnapshotID: {}, JobID: {}, Env: {}".format(filteredStatGroup[0][0], filteredStatGroup[0][1], backupJobIdToEnv[filteredStatGroup[0][1]])
  fig.suptitle(titleText, fontsize="small")

  ax1.ticklabel_format(axis="x", style="plain", scilimits=(0,0))
  ax1.plot(timeRow[1:], queriesPerSecond[1:], '.')
  ax1.set_yticks([min(queriesPerSecond[1:]), max(queriesPerSecond[1:])])
  ax1.set_ylabel('Query / Sec', fontsize="small")

  ax2.ticklabel_format(axis="x", style="plain", scilimits=(0,0))
  ax2.plot(timeRow[1:], opExecutionTime[1:], '.')
  ax2.set_yticks([min(opExecutionTime[1:]), max(opExecutionTime[1:])])
  ax2.set_ylabel('Op Execution (sec)', fontsize="small")

  ax3.plot(timeRow[1:], queuedRequests[1:], '.-')
  ax3.set_xlabel('time (mins from snapshot start)')
  ax3.set_ylabel('Queued Requests', fontsize="small")

  ax1.axvline(x=snapshotEndMinute, color="green")
  ax2.axvline(x=snapshotEndMinute, color="green")
  ax3.axvline(x=snapshotEndMinute, color="green")

  plt.tight_layout()
  plt.savefig("img/task2/" + backupJobIdToEnv[filteredStatGroup[0][1]] + "/" + str(nthGraphWorkedOn) + "_" + filteredStatGroup[0][0])
  plt.close()

def filterForTask2(statGroup):
  filteredGroup = []
  filteredGroup.append(statGroup[0]) # the first row needs no filtering

  timeRow = statGroup[1]
  queriesPerSecond = statGroup[2]
  opExecutionTimes = statGroup[3]
  queuedReadRequests = statGroup[4]

  filteredTimeRow = []
  filterdQueriesPerSecond = []
  filteredOpExecution = []
  filteredQueuedReadRequests = []

  validPoint = 1
  for i in range(1, len(timeRow)):
    if queriesPerSecond[i] == "NA" or opExecutionTimes[i] == "NA" or queuedReadRequests[i] == "NA":
      # skip this point
      continue
    
    filteredTimeRow.append(validPoint)
    filterdQueriesPerSecond.append(int(queriesPerSecond[i]))
    filteredOpExecution.append(int(opExecutionTimes[i]) / SECONDS_IN_MS)
    filteredQueuedReadRequests.append(int(queuedReadRequests[i]))

    validPoint += 1
  
  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filterdQueriesPerSecond)
  filteredGroup.append(filteredOpExecution)
  filteredGroup.append(filteredQueuedReadRequests)  

  return filteredGroup


#########################################################
#########################################################
#### Task 3
#  * task3 will be in the following format for each snapshot:
#  *
# 606fce7349d09650dee1d6c9	5d80aab779358e6447e199b6	TS time:Fri Apr 09 03:47:00 GMT 2021 inc:1	Fri Apr 09 03:48:52 GMT 2021
# Memory Type	Fri Apr 09 03:47:24 GMT 2021	Fri Apr 09 03:47:33 GMT 2021	Fri Apr 09 03:48:33 GMT 2021	Fri Apr 09 03:48:39 GMT 2021
# Virtual Memory	NA	2372	2372	NA
# Resident Memory	NA	486	486	NA	
#  *
#########################################################
#########################################################
def analysisForTask3(nthGraph):
  totalSnapshots = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task3"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          #analyze when logical group is complete
          if (len(logicalStatGrouping) == 4):
            filteredStatGroup = filterForTask3(logicalStatGrouping)
            if (filteredStatGroup == None):
              print("Observed invalid snapshot row grouping: ", logicalStatGrouping)
              logicalStatGrouping = []
              continue

            if (nthGraph == None or totalSnapshots < nthGraph):
              plotTask3(filteredStatGroup, totalSnapshots)

            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1

  print("Total number of snapshots is: ", totalSnapshots)
  return totalSnapshots

def filterForTask3(statGroup):
  filteredGroup = []
  filteredGroup.append(statGroup[0]) # the first row needs no filtering

  timeRow = statGroup[1]
  virtualMemoryRow = statGroup[2]
  residentMemoryRow = statGroup[3]

  filteredTimeRow = []
  filteredVirtualMemoryRow = []
  filteredResidentMemoryRow = []

  validPoint = 1
  for i in range(1, len(timeRow)):
    if virtualMemoryRow[i] == "NA" or residentMemoryRow[i] == "NA":
      # skip this point
      continue
    
    # if there is a exception warning or no measrement warning, return a None, and in the caller handle if
    if (virtualMemoryRow[i] == "No Measurements Available"):
      return None

    filteredTimeRow.append(validPoint)
    filteredVirtualMemoryRow.append(int(virtualMemoryRow[i]))
    filteredResidentMemoryRow.append(int(residentMemoryRow[i]))

    validPoint += 1
  
  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filteredVirtualMemoryRow)
  filteredGroup.append(filteredResidentMemoryRow)
  return filteredGroup

def plotTask3(filteredStatGroup, nthGraphWorkedOn):
  print("Plotting for", filteredStatGroup[0])

  snapshotEndMinute = computeSnapshotFinishTime(filteredStatGroup[0])

  timeRow = filteredStatGroup[1]
  virtualMemoryRow = filteredStatGroup[2]
  residentMemoryRow = filteredStatGroup[3]

  if (len(virtualMemoryRow) < MIN_SNAPSHOT_TIME_LENGTH_TASK_3 or len(residentMemoryRow) < MIN_SNAPSHOT_TIME_LENGTH_TASK_3):
    print("Observed dataset without any datapoints for diffDiskSpaceFree. Continuing")
    return

  fig, (ax1, ax2) = plt.subplots(2, 1)
  titleText = "Task 3 - Snapshotted MongoDB node Memory\n SnapshotID: {}, JobID: {}, Env: {}".format(filteredStatGroup[0][0], filteredStatGroup[0][1], backupJobIdToEnv[filteredStatGroup[0][1]])
  fig.suptitle(titleText, fontsize="small")

  ax1.plot(timeRow[1:], virtualMemoryRow[1:], '.')
  ax1.set_yticks([min(virtualMemoryRow[1:]), max(virtualMemoryRow[1:])])
  ax1.set_ylabel('Virtual Memory (mb)', fontsize="small")

  ax2.plot(timeRow[1:], residentMemoryRow[1:], '.')
  ax2.set_yticks([min(residentMemoryRow[1:]), max(residentMemoryRow[1:])])
  ax2.set_ylabel('Resident Memory (mb)', fontsize="small")
  ax2.set_xlabel('time (mins from snapshot start)')

  ax1.axvline(x=snapshotEndMinute, color="green")
  ax2.axvline(x=snapshotEndMinute, color="green")

  plt.tight_layout()
  plt.savefig("img/task3/" + backupJobIdToEnv[filteredStatGroup[0][1]] + "/" + str(nthGraphWorkedOn) + "_" + filteredStatGroup[0][0])
  plt.close()


snapshotHasSufficientOplogWorkload = {}

#########################################################
#########################################################
#### Task 4 - memory
# task4-memory csv files will be in the following format for each snapshot:
#
# 605b603812f4a64f7b5cc766	5d109ba8c56c98ab46a51b8d	TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1	Wed Mar 24 15:52:51 GMT 2021	
# OplogBytes / Second	Wed Mar 24 15:51:51 GMT 2021	Wed Mar 24 15:52:02 GMT 2021		
# Oplog Bytes / Second	143	143		
#########################################################
#########################################################
def analysisForTask4_OplogSize(nthGraph):
  totalSnapshots = 0
  totalSnapshotWithValidOplogWorkload = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task4-oplog"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          #analyze when logical group is complete
          if (len(logicalStatGrouping) == 3):
            filteredStatGroup = filterForTask4_oplog(logicalStatGrouping)
            
            if (filteredStatGroup == None):
              # print("Observed invalid snapshot row grouping: ", logicalStatGrouping)
              logicalStatGrouping = []
              continue
            
            if (nthGraph == None or totalSnapshots < nthGraph):
              oplogSufficient = isOplogWorkloadSufficient(filteredStatGroup, totalSnapshots)
              if (oplogSufficient):
                totalSnapshotWithValidOplogWorkload += 1
                snapshotHasSufficientOplogWorkload[filteredStatGroup[0][0]] = True
            
            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1
  print("Total number of snapshots is: ", totalSnapshots)
  print("Total number of snapshots with 1GB of Oplog / hour is: ", totalSnapshotWithValidOplogWorkload)
  return totalSnapshots

# 605b603812f4a64f7b5cc766	5d109ba8c56c98ab46a51b8d	TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1	Wed Mar 24 15:52:51 GMT 2021	
# OplogBytes / Second	Wed Mar 24 15:51:51 GMT 2021	Wed Mar 24 15:52:02 GMT 2021		
# Oplog Bytes / Second	143	143		
def filterForTask4_oplog(statGroup):
  filteredGroup = []
  filteredGroup.append(statGroup[0]) # the first row needs no filtering

  timeRow = statGroup[1]
  oplogBytesPerSecondRow = statGroup[2]

  filteredTimeRow = []
  filteredOplogBytesPerSecondRow = []

  validPoint = 1
  for i in range(1, len(timeRow)):
    if (oplogBytesPerSecondRow[i] == "No Measurements Available"):
      return None
    if (oplogBytesPerSecondRow[i] == "Throwable observed while computing stats. Stats may be incomplete for this row."):
      return None

    filteredTimeRow.append(validPoint)
    filteredOplogBytesPerSecondRow.append(int(oplogBytesPerSecondRow[i]))

    validPoint += 1
  
  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filteredOplogBytesPerSecondRow)
  return filteredGroup

def isOplogWorkloadSufficient(filteredStatGroup, totalSnapshots):
  # print("Analyzing for", filteredStatGroup[0])
  oplogRow = filteredStatGroup[2]
  if (len(oplogRow) < 1):
    return False

  return max(oplogRow) > MIN_OPLOG_BYTES_PER_SECOND

#########################################################
#########################################################
#### Task 4
# task4-memory will be in the following format for each snapshot:
#
# 605b735d9ebbd160585f7c06	5c1b4a5c014b765a2bb3d5da	TS time:Wed Mar 24 17:12:59 GMT 2021 inc:1	Wed Mar 24 17:14:40 GMT 2021
# Category	Wed Mar 24 17:13:38 GMT 2021	Wed Mar 24 17:14:38 GMT 2021
# Total System Memory	1843056	1843056
# Free System Memory	151740	25952
# Buffered System Memory	3124	3124
# Cache System Memory	625784	752220
#########################################################
#########################################################
def analysisForTask4_memory(nthGraph):
  analysisForTask4_OplogSize(nthGraph)

  totalSnapshots = 0
  totalSnapshotsWithOplogWorkload = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task4-memory"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          # analyze when logical group is complete
          if (len(logicalStatGrouping) == 6):
            filteredStatGroup = filterForTask4_memory(logicalStatGrouping)

            if (filteredStatGroup == None):
              # print("Observed invalid snapshot row grouping: ", logicalStatGrouping)
              logicalStatGrouping = []
              continue

            if (nthGraph == None or totalSnapshots < nthGraph):
              plottedWithWorkload = plotTask4_memory(filteredStatGroup, totalSnapshots)
              if (plottedWithWorkload):
                totalSnapshotsWithOplogWorkload += 1


            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1
  print("Total number of snapshots is: ", totalSnapshots)
  print("Total number of snapshot with 1gb oplog workload and at least 15 mins long is: ", totalSnapshotsWithOplogWorkload)
  return totalSnapshots

def filterForTask4_memory(statGroup):
  filteredGroup = []
  filteredGroup.append(statGroup[0]) # the first row needs no filtering

  timeRow = statGroup[1]
  totalSystemMemory = statGroup[2]
  freeSystemMemory = statGroup[3]
  bufferedSystemMemory = statGroup[4]
  cacheSystemMemory = statGroup[5]

  filteredTimeRow = []
  filteredTotalSystemMemory = []
  filteredFreeSystemMemory = []
  filteredBufferedSystemMemory = []
  filteredCacheSystemMemory = []

  validPoint = 1
  for i in range(1, len(timeRow)):
    if totalSystemMemory[i] == "NA" or freeSystemMemory[i] == "NA" or bufferedSystemMemory[i] == "NA" or cacheSystemMemory[i] == "NA":
      # skip this point
      continue
    
    # if there is a exception warning or no measrement warning, return a None, and in the caller handle if
    if (totalSystemMemory[i] == "No Measurements Available"):
      return None

    if (totalSystemMemory[i] == "Throwable observed while computing stats. Stats may be incomplete for this row."):
      return None

    filteredTimeRow.append(validPoint)
    filteredTotalSystemMemory.append(int(totalSystemMemory[i]) / GB_IN_KB) # System Memory is returned in MB
    filteredFreeSystemMemory.append(int(freeSystemMemory[i]) / GB_IN_KB)
    filteredBufferedSystemMemory.append(int(bufferedSystemMemory[i]) / GB_IN_KB)
    filteredCacheSystemMemory.append(int(cacheSystemMemory[i]) / GB_IN_KB)

    validPoint += 1
  
  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filteredTotalSystemMemory)
  filteredGroup.append(filteredFreeSystemMemory)
  filteredGroup.append(filteredBufferedSystemMemory)
  filteredGroup.append(filteredCacheSystemMemory)
  return filteredGroup

def plotTask4_memory(filteredStatGroup, nthGraphWorkedOn):
  # print("Plotting for", filteredStatGroup[0])

  snapshotEndMinute = computeSnapshotFinishTime(filteredStatGroup[0])

  timeRow = filteredStatGroup[1]
  if (len(timeRow) < MIN_SNAPSHOT_DURATION_FOR_TASK_4):
    # print("Snapshot didnt contain enough datapoints. Had: ", len(totalSystemMemory))
    return False

  totalSystemMemory = filteredStatGroup[2]
  freeSystemMemory = filteredStatGroup[3]

  usedSystemMemory = ["Used System Memory"]
  for i in range(1, len(totalSystemMemory)):
    usedSystemMemory.append(totalSystemMemory[i] - freeSystemMemory[i])

  snapshotId = filteredStatGroup[0][0] 
  snapshotHasOplogWorkload = snapshotId in snapshotHasSufficientOplogWorkload    
  subDirName = "insufficientOplogWorkload"
  if (snapshotHasOplogWorkload):
    subDirName = "validOplogWorkload"

  titleText = "Task 4 - MongoDB Agent Memory\n SnapshotID: {}, JobID: {}, Env: {}\n Snapshot has at least 1GB of oplog workload: {}".format(filteredStatGroup[0][0], filteredStatGroup[0][1], backupJobIdToEnv[filteredStatGroup[0][1]], snapshotHasOplogWorkload)

  fig, ax1 = plt.subplots(1, 1)
  fig.suptitle(titleText, fontsize="small")

  # ax1.plot(timeRow[1:], totalSystemMemory[1:], '.-', label="Total System Memory")
  # ax1.plot(timeRow[1:], freeSystemMemory[1:], '.-', label="Free System Memory")
  # Only plot the used system memory
  ax1.plot(timeRow[1:], usedSystemMemory[1:], '.-', label="Used System Memory")  
  ax1.set_ylabel('Memory Usage (GB)', fontsize="small")
  ax1.set_xlabel('time (mins from snapshot start)')

  ax1.legend()
  ax1.axvline(x=snapshotEndMinute, color="green")

  plt.tight_layout()
  plt.savefig("img/task4/" + backupJobIdToEnv[filteredStatGroup[0][1]] + "/" + subDirName + "/" + str(nthGraphWorkedOn) + "_" + filteredStatGroup[0][0])
  plt.close()
  return snapshotHasOplogWorkload


#########################################################
#########################################################
#### Task 5
##### So, for the output of task5, we will have a total of 4 graphs
###### 1. Atlas on 4.2.7+ and 4.4.0+
###### 2. Atlas on ~ 4.2.6 (should be none)
###### 3. CM on 4.2.7+ and 4.4.0+
###### 4. CM on ~ 4.2.6
##########
# task5 will be in the following format for each snapshot:
# SnapshotId	JobId	MongodVersion	Atlas/CM	Replica Set / Sharded Cluster	Snapshot Start Time	Snapshot End Time	Snapshot Duration	Snapshot Size	Number of Files in Snapshot																
# 605b76fee780250c268b6af8	5a95e7b29d0e9b0759e02517	4.2.13	Atlas	Replica Set	TS time:Wed Mar 24 17:28:34 GMT 2021 inc:1	Wed Mar 24 17:33:46 GMT 2021	312	5115825411	47																
# 605b75fc9ebbd16058600b4f	5d07ae09553855198e441337	4.2.13	Atlas	Replica Set	TS time:Wed Mar 24 17:24:09 GMT 2021 inc:1	Wed Mar 24 17:43:24 GMT 2021	1155	Missing Compressed Size	247																
#########################################################
#########################################################
def analysisForTask5():
  totalSnapshots = iterateAndPlotForTask5(True, True)
  print("Total number of non-incremental snapshots on Atlas before 4.2.7 is: ", totalSnapshots)
  totalSnapshots = iterateAndPlotForTask5(True, False)
  print("Total number of non-incremental snapshots on Atlas after  4.2.7 is: ", totalSnapshots)
  totalSnapshots = iterateAndPlotForTask5(False, True)
  print("Total number of non-incremental snapshots on CM before 4.2.7 is: ", totalSnapshots)
  totalSnapshots = iterateAndPlotForTask5(False, False)
  print("Total number of non-incremental snapshots on CM after 4.2.7 is: ", totalSnapshots)

def iterateAndPlotForTask5(atlas, old42Version):
  totalCompletedSnapshots = 0
  snapshotSizes = []
  snapshotTimes = []

  for file in os.listdir("./allStats"):
    if file.startswith("task5"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        for row in spamreader:
          if (row[0] == "SnapshotId"):
            # ignore the first row
            continue
          
          if (row[8] == "Missing Compressed Size"):
            # Ignore snapshots that weren't finished
            continue

          if not (isValidSnapshotRowForTask5or6(row, atlas, old42Version)):
            continue

          snapshotSizes.append(int(row[8]) / GB_IN_BYTES)
          snapshotTimes.append(int(row[7]) / MINUTE_IN_SECONDS)
          
          totalCompletedSnapshots += 1  
      print("Finished Processing file: ", "./allStats/" + file)

  saveGraphAs = "img/task5/"
  if (atlas and old42Version):
    plt.suptitle('Task 5: Non Incremental Snapshots - Atlas, pre-4.2.7')
    saveGraphAs += "atlas-pre427"
  elif (atlas and not old42Version):
    plt.suptitle('Task 5: Non Incremental Snapshots - Atlas, post-4.2.7')
    saveGraphAs += "atlas-post427"
  elif (not atlas and old42Version):
    plt.suptitle('Task 5: Non Incremental Snapshots - CM, pre-4.2.7')
    saveGraphAs += "cm-pre427"
  elif (not atlas and not old42Version):
    plt.suptitle('Task 5: Non Incremental Snapshots - CM, post-4.2.7')
    saveGraphAs += "cm-post427"

  plt.xlabel('Snapshot Duration (minutes)')
  plt.ylabel('Snapshot size (GB)')

  plt.plot(snapshotTimes, snapshotSizes, 'bo')
  # plt.savefig(saveGraphAs)
  plt.close()

  computeStandardDeviationTask5(snapshotSizes, snapshotTimes)

  return totalCompletedSnapshots

def isValidSnapshotRowForTask5or6(row, atlas, old42Version):
  rowIsAtlas = (row[3] == "Atlas")
  rowIsOld42Version = (row[2] in ["4.2.0", "4.2.1", "4.2.2", "4.2.3", "4.2.4", "4.2.5", "4.2.6"]) 
  return ((rowIsAtlas == atlas) and (rowIsOld42Version == old42Version))

def computeStandardDeviationTask5(snapshotSizes, snapshotTimes):
  avgTransferGBPerMin = []
  for i in range(len(snapshotSizes)):
    avgTransferGBPerMin.append(snapshotSizes[i] / snapshotTimes[i])
  
  r1 = np.average(avgTransferGBPerMin)
  print("\nMean: ", r1)
    
  r2 = np.sqrt(np.mean((avgTransferGBPerMin - np.mean(avgTransferGBPerMin)) ** 2))
  print("\nstd: ", r2)
    
  r3 = np.mean((avgTransferGBPerMin - np.mean(avgTransferGBPerMin)) ** 2)
  print("\nvariance: ", r3)


#########################################################
#########################################################
#### Task 6
###### Just 2 graphs
###### 1. Atlas on 4.2.7+ and 4.4.0+
###### 2. CM on 4.2.7+ and 4.4.0+
# Example csv
# SnapshotId	JobId	MongodVersion	Atlas/CM	Replica Set / Sharded Cluster	Snapshot Start Time	Snapshot End Time	Snapshot Duration	Snapshot Size	Number of Files in Snapshot	Number of Dirty Blocks	Number of Clean Blocks														
# 605b5f2d45c22e32cd938bf5	5cd09dbff2a30b57d91b3f7f	4.2.12	Atlas	Replica Set	TS time:Wed Mar 24 15:46:55 GMT 2021 inc:1	Wed Mar 24 16:33:25 GMT 2021	2790	204035372768	267	6594	13986														
#########################################################
#########################################################
def analysisForTask6():
  totalSnapshots = iterateAndPlotForTask6(True)
  print("Total number of incremental snapshots on Atlas is: ", totalSnapshots)
  totalSnapshots = iterateAndPlotForTask6(False)
  print("Total number of incremental snapshots on CM is: ", totalSnapshots)

def iterateAndPlotForTask6(atlas):
  totalCompletedSnapshots = 0
  numDirtyBlocks = []
  snapshotTimes = []

  for file in os.listdir("./allStats"):
    if file.startswith("task6"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        for row in spamreader:
          if (row[0] == "SnapshotId"):
            # ignore the first row
            continue
          
          if (row[8] == "Missing Compressed Size"):
            # Ignore snapshots that weren't finished
            continue

          if not (isValidSnapshotRowForTask5or6(row, atlas, False)):
            continue

          numDirtyBlocks.append(int(row[10]))
          snapshotTimes.append(int(row[7]) / MINUTE_IN_SECONDS)
          
          totalCompletedSnapshots += 1  
      print("Finished Processing file: ", "./allStats/" + file)

  saveGraphAs = "img/task6/"
  if (atlas):
    plt.suptitle('Task 6: Incremental Snapshots - Atlas')
    saveGraphAs += "atlas"
  elif (not atlas):
    plt.suptitle('Task 6: Incremental Snapshots - CM')
    saveGraphAs += "cm"

  plt.xlabel('Snapshot Duration (minutes)')
  plt.ylabel('Number of Dirty Blocks')

  plt.plot(snapshotTimes, numDirtyBlocks, 'bo')
  plt.savefig(saveGraphAs)
  plt.close()

  computeStandardDeviationTask6(numDirtyBlocks, snapshotTimes)

  return totalCompletedSnapshots

def computeStandardDeviationTask6(numDirtyBlocks, snapshotTimes):
  avgTransferBlockPerMin = []
  for i in range(len(numDirtyBlocks)):
    avgTransferBlockPerMin.append(numDirtyBlocks[i] / snapshotTimes[i])
  
  r1 = np.average(avgTransferBlockPerMin)
  print("\nMean: ", r1)
    
  r2 = np.sqrt(np.mean((avgTransferBlockPerMin - np.mean(avgTransferBlockPerMin)) ** 2))
  print("\nstd: ", r2)
    
  r3 = np.mean((avgTransferBlockPerMin - np.mean(avgTransferBlockPerMin)) ** 2)
  print("\nvariance: ", r3)




# Callers 
# analysisForTask1(None) 
# analysisForTask2(None)
# analysisForTask3(None)
# analysisForTask4_OplogSize(None)
# analysisForTask4_memory(None)
# analysisForTask5()
# analysisForTask6()

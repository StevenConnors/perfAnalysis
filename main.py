import csv
import os
import math 
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import datetime


def analysisForTask2(nthGraph):
  #  * task2 will be in the following format for each snapshot:
  #  *
  #  * Snapshot info....
  #  * Time Row, T1, T2, T3 ....
  #  * Queries Per Second, q1, q2, q3 ...
  #  * Average OpExecution Time (ms / cmd), o1, o2, o3 ....
  #  * Queued Requests (r & w combined), r1, r2, r3 ....
  #  *
  totalSnapshotsWithSecondaryWorkload = 0
  totalSnapshots = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task2"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          if (len(logicalStatGrouping) == 5):
            #analyze when logical group is complete
            if (snapshotContainsSecondaryWorkload(logicalStatGrouping)):
              totalSnapshotsWithSecondaryWorkload += 1

              if (totalSnapshotsWithSecondaryWorkload == nthGraph):
                # Take a sample and plot it
                print("Found one! Gonna chart and return")
                plotTask2(logicalStatGrouping)
                return

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

def plotTask2(statGroup):
  print("Plotting for", statGroup[0])
  filteredStatGroup = filterForTask2(statGroup)

  snapshotEndMinute = computeSnapshotFinishTime(filteredStatGroup[0])

  timeRow = filteredStatGroup[1]
  queriesPerSecond = filteredStatGroup[2]
  opExecutionTime = filteredStatGroup[3]
  queuedRequests = filteredStatGroup[4]

  fig, (ax1, ax2, ax3) = plt.subplots(3, 1)
  fig.suptitle('A tale of 2 subplots')

  ax1.plot(timeRow[1:], queriesPerSecond[1:], '.-')
  ax1.set_yticks([min(queriesPerSecond[1:]), max(queriesPerSecond[1:])])
  ax1.set_ylabel('Queries (per second)')

  ax2.plot(timeRow[1:], opExecutionTime[1:], '.-')
  ax2.set_yticks([min(opExecutionTime[1:]), max(opExecutionTime[1:])])
  ax2.set_ylabel('opExecutionTime (ms)')

  ax3.plot(timeRow[1:], queuedRequests[1:], '.-')
  ax3.set_xlabel('time (mins from snapshot start)')
  ax3.set_ylabel('queuedRequests')

  ax1.axvline(x=snapshotEndMinute)
  ax2.axvline(x=snapshotEndMinute)
  ax3.axvline(x=snapshotEndMinute)
  plt.show()

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
    filteredOpExecution.append(int(opExecutionTimes[i]))
    filteredQueuedReadRequests.append(int(queuedReadRequests[i]))

    validPoint += 1
  
  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filterdQueriesPerSecond)
  filteredGroup.append(filteredOpExecution)
  filteredGroup.append(filteredQueuedReadRequests)  

  return filteredGroup

# Parses the snapshot metadata row and then returns the number of minutes the snapshot took.
def computeSnapshotFinishTime(snapshotMetadataRow):
  startTime = snapshotMetadataRow[2] # "TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1"
  startTimeDateObj = datetime.datetime.strptime(startTime, 'TS time:%a %b %d %H:%M:%S GMT %Y inc:1')

  endTime = snapshotMetadataRow[3] # " Wed Mar 24 15:52:51 GMT 2021"]
  endTimeDateObj = datetime.datetime.strptime(endTime, '%a %b %d %H:%M:%S GMT %Y')

  print('Start Date-time:', startTimeDateObj)
  print('End Date-time:', endTimeDateObj)

  diffSeconds = (endTimeDateObj - startTimeDateObj).total_seconds()
  return math.ceil(diffSeconds / 60)
# print(computeSnapshotFinishTime(["605b603812f4a64f7b5cc766", "5d109ba8c56c98ab46a51b8d", "TS time:Wed Mar 24 15:51:21 GMT 2021 inc:1", "Wed Mar 24 15:52:51 GMT 2021"]))


# analysisForTask2(13)















# Appendix


def testPlot():
  t = np.arange(0.01, 5.0, 0.01)
  s1 = np.sin(2 * np.pi * t)
  s2 = np.exp(-t)
  s3 = np.sin(4 * np.pi * t)

  ax1 = plt.subplot(311)
  plt.plot(t, s1)
  plt.setp(ax1.get_xticklabels(), fontsize=6)

  # share x only
  ax2 = plt.subplot(312, sharex=ax1)
  plt.plot(t, s2)
  # make these tick labels invisible
  plt.setp(ax2.get_xticklabels(), visible=False)

  # share x and y
  ax3 = plt.subplot(313, sharex=ax1, sharey=ax1)
  plt.plot(t, s3)
  plt.xlim(0.01, 5.0)
  plt.show()


def plotTask2_attempt1(statGroup):
  print("Plotting for", statGroup[0])
  filteredStatGroup = filter(statGroup)

  # print(statGroup)
  timeRow = filteredStatGroup[1]
  queriesPerSecond = filteredStatGroup[2]
  opExecutionTime = filteredStatGroup[3]
  queuedRequests = filteredStatGroup[4]

  print(queuedRequests)

  fig, ax = plt.subplots()
  ax.plot(timeRow, opExecutionTime,  'ro')

  ax.set(xlabel='minutes since snapshot start', ylabel='Queries (ms / command)',
        title='Task2, Queries')
  # ax.grid()

  # fig.savefig("test.png")
  plt.show()

def plotTask2_star(statGroup):
  print("Plotting for", statGroup[0])
  filteredStatGroup = filter(statGroup)

  # print(statGroup)
  timeRow = filteredStatGroup[1]
  queriesPerSecond = filteredStatGroup[2]
  opExecutionTime = filteredStatGroup[3]
  queuedRequests = filteredStatGroup[4]

  fig, ax = plt.subplots()

  ax.set(xlabel='minutes since snapshot start', ylabel='Queries (ms / command)',
        title='Task2, Queries')
  # ax.grid()
  
  plt.figure()
  plt.subplot(211)
  plt.plot(timeRow[1:], opExecutionTime[1:], 'bo')

  plt.subplot(212)
  plt.plot(timeRow[1:], queuedRequests[1:], 'bo')
  plt.show()

















def analysisForTask1():
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

  totalSnapshots = 0

  for file in os.listdir("./allStats"):
    if file.startswith("task1"):
      with open("./allStats/" + file, newline='')  as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')

        logicalStatGrouping = [] 
        for row in spamreader:
          logicalStatGrouping.append(row)

          if (len(logicalStatGrouping) == 11):
            #analyze when logical group is complete
            print("Found one! Gonna chart and return")
            plotTask1(logicalStatGrouping)

            return

            # initialize
            logicalStatGrouping = []
            totalSnapshots += 1

  print("Total number of snapshots is: ", totalSnapshots)
  return totalSnapshotsWithSecondaryWorkload




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

    diffIntVal = int(diffDiskSpaceFree[i])
    filteredDiffDiskSpaceFree.append(diffIntVal)

    validPoint += 1


  filteredGroup.append(filteredTimeRow)
  filteredGroup.append(filteredDiffDiskSpaceFree)
  return filteredGroup



def plotTask1(statGroup):
  print("Plotting for", statGroup[0])

  filteredStatGroup = filterForTask1(statGroup)

  print(filteredStatGroup)

  timeRow = filteredStatGroup[1]
  diffDiskSpaceFree = filteredStatGroup[2]

  fig, ax1 = plt.subplots(1, 1)
  fig.suptitle('Task 1 Disk Usage')

  ax1.plot(timeRow[1:], diffDiskSpaceFree[1:], '.-')
  ax1.set_yticks([min(diffDiskSpaceFree[1:]), max(diffDiskSpaceFree[1:])])
  ax1.set_ylabel('Disk Space Diff (bytes)')
  ax1.set_xlabel('time (mins from snapshot start)')

  # plt.axvline(x=300) # how to plot a vertical line
  plt.show()



# analysisForTask1()
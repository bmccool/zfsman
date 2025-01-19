#!/usr/bin/env python3
"""
Generates the report files that snailback.py uses to determine most recent common backups
Run this on the backup after receiving new backups
"""
import pyznap.pyzfs as zfs
from os import listdir, path
from os.path import isfile, join
import subprocess as sp
import json
import datetime

#TODO
#zfs list -o space -r superior | grep tankbackup | grep -v @
#^^^ This command shows all spaced use by dataset vs snapshots.  Use it to provide a before&after space savings after pruning
#Note, need to have listsnaapshots=on, zpool set listsnapshots=on superior

def get_report_directory():
    #TODO hardcoded
    return "/mnt/snailback/report/"

def get_date(snapshot):
    #TODO assuming string
    #tankbackup/vmstorage/limited/subvol-107-disk-0@autosnap_2019-11-24_21:00:08_hourly
    # We can assume this date format... so
    
    # 1. Split on underscore and take last three pieces as date, time, type (hourly etc.)
    date = snapshot.snapname().split('_')[-3]
    time = snapshot.snapname().split('_')[-2]
    period = snapshot.snapname().split('_')[-1]

    return date, time, period

def get_period(snapshot):
    #TODO assuming string
    #tankbackup/vmstorage/limited/subvol-107-disk-0@autosnap_2019-11-24_21:00:08_hourly
    # We can assume this date format... so
    
    # 1. Split on underscore and take last three pieces as date, time, type (hourly etc.)
    date = snapshot.snapname().split('_')[-3]
    time = snapshot.snapname().split('_')[-2]
    period = snapshot.snapname().split('_')[-1]

    return period

def to_datetime(snapshot):
    """ Return datetime object of snapshot date and time """
    date, time, period = get_date(snapshot)
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    day = int(date.split('-')[2])
    hour = int(time.split(':')[0])
    minute = int(time.split(':')[1])
    second = int(time.split(':')[2])
    return datetime.datetime(year, month, day, hour, minute, second)

def is_more_recent(snap1, snap2):
    """ Return true if snap1 is more recent than snap2 """
    dt1 = to_datetime(snap1)
    dt2 = to_datetime(snap2)
    if dt1 > dt2:
        return True
    return False
    

def get_most_recent(snapshots, period="monthly"):
    """ Get the most recent snapshot of a period type in a list of snapshots """
    most_recent = None
    #print("There are {} snapshots, looking for most recent {} snapshot".format(len(snapshots), str(period)))
    for snapshot in snapshots:
        if get_period(snapshot) == period:
            # This snapshot is the correct period
            if most_recent == None:
                # We don't have any snapshots yet, so this is the most recent
                most_recent = snapshot
            elif is_more_recent(snapshot, most_recent):
                # This snapshot is more recent
                most_recent = snapshot
    return most_recent

def prune_except(snapshots, snapshot_to_keep, period):
    """
    Prune all snapshots of a given period EXCEPT snapshot to keep 
    snapshots: all snapshots of a given filesystem
    snapshot_to_keep: the snapshot of this period to keep
    period: 'yearly', 'monthly', etc
    """

    for snapshot in snapshots:
        if get_period(snapshot) == period:
            if snapshot != snapshot_to_keep:
                print("{} is not the snapshot to keep {}, destroying...".format(str(snapshot), str(snapshot_to_keep)))
                snapshot.destroy()

def prune_syncoid(snapshots):
    """ prune syncoid snapshots.  Call only once they are no longer needed """
    for snapshot in snapshots:
        if "syncoid" in str(snapshot):
            print("Pruning syncoid snapshot {}...".format(str(snapshot)))
            snapshot.destroy() 

source_children = zfs.find(path="superior/tankbackup", types=['filesystem', 'volume'])

print("Checking that report directory exists...")
if not path.isdir('/mnt/snailback/report') or not path.exists('/mnt/snailback/report'):
    raise NotADirectoryError("/mnt/snailback/report does not exist, verify drive is mounted: mount /dev/sdg1 /mnt/snailback")
print("Opening /mnt/snailback/report/backups.txt")
with open("/mnt/snailback/report/backups.txt", "w") as backup_file:
    print("Most recent montly snapshots in superior/tankbackup are:")
    for child in source_children:
        snapshots = child.snapshots()
        if len(snapshots) > 0:
            for period in ['yearly', 'monthly', 'weekly', 'daily', 'hourly']:
                most_recent = get_most_recent(snapshots, period)
                if most_recent:
                    print(most_recent.__str__().split('tankbackup/')[1])
                    backup_file.write(most_recent.__str__().split('tankbackup/')[1] + "\n")
                    prune_except(snapshots, most_recent, period)
                else:
                    print("No {} backup found for {}!".format(period, str(child)))
            #prune_syncoid(snapshots)

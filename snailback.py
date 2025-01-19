#!/usr/bin/env python3

import pyznap.pyzfs as zfs
from os import listdir, path, remove
from os.path import isfile, join
import subprocess as sp
import datetime

#TODO TODO TODO
#Delete .gzips before this runs
def find_snapshot_files():
    #TODO make this work
    return "/mnt/snailback"

def get_snapshot_files(path, suffix="gzip"):
    #TODO this is slightly different, we are getting the full path here instead of relative
    files = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and suffix in f]
    return files
    

def get_date(snapshot):
    #TODO assuming string
    #tankbackup/vmstorage/limited/subvol-107-disk-0@autosnap_2019-11-24_21:00:08_hourly
    # We can assume this date format... so
    
    # 1. Split on underscore and take last three pieces as date, time, type (hourly etc.)
    try:
        # Assume we are passed a ZFS snapshot
        date = snapshot.snapname().split('_')[-3]
        time = snapshot.snapname().split('_')[-2]
        period = snapshot.snapname().split('_')[-1]
    except:
        # If not, then we were surely passed a string...
        date = snapshot.split('_')[-3]
        time = snapshot.split('_')[-2]
        period = snapshot.split('_')[-1]

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

def get_most_recent_common_snapshot(child):
    if not path.isdir('/mnt/snailback/report') or not path.exists('/mnt/snailback/report'):
        #TODO what if the drive is mounted but the report directory hasn't been created?  Error for now, workaround later
        raise NotADirectoryError("/mnt/snailback/report does not exist, verify drive is mounted: mount /dev/sdg1 /mnt/snailback")
    most_recent_common_snapshot = None
    snapshots = [str(snapshot) for snapshot in child.snapshots()]
    with open("/mnt/snailback/report/backups.txt", "r") as backup_file:
        for line in backup_file:
            line = line.strip()
            # If the destination snapshot filesystem name matches our filesystem name...
            if line.split('@')[0] == str(child):
                # If our filesystem has the same snapshot available...
                if line in snapshots:
                    #This is a common snapshot, see if it is the most recent one.
                    if not most_recent_common_snapshot:
                        most_recent_common_snapshot = line
                    else:
                        if is_more_recent(line, most_recent_common_snapshot):
                            most_recent_common_snapshot = line
    if not most_recent_common_snapshot:
        print("No match found for " + str(child) + "!")
    else:
        most_recent_common_snapshot = most_recent_common_snapshot.split('@')[1]
    return most_recent_common_snapshot

def fill_harddrive(args):
    files = get_snapshot_files(find_snapshot_files())
    blacklist = args.blacklist
    whitelist = args.whitelist

    if args.append and (args.whitelist is not None):
        # Only remove files we will end up writing again
        files = [file for file in files if True in [acceptedItem in str(file) for acceptedItem in args.whitelist]]
                
    for each in files:
        print("Removing old backup file: {}".format(str(each)))
        remove(each)

    source_children = zfs.find(path="tank", types=['filesystem', 'volume'])
    #Source children is literally just the filesystems in TANK
    #a child is just one filesystem

    #Apply whitelist to source_children
    if whitelist is not None:
        source_children = [child for child in source_children if True in [acceptedItem in str(child) for acceptedItem in args.whitelist]]

    #Apply blacklist to source_children
    if blacklist is not None:
        source_children = [child for child in source_children if not True in [excludedItem in str(child) for excludedItem in args.blacklist]]
    

    """
    for child in source_children:
        #if ("brendon" not in str(child)) and ("vmbackup" not in str(child)):
        #if str(child) not in blacklist:
        if not True in (excludedItem in str(child) for excludedItem in blacklist):
            print("Backing up {}!".format(str(child)))
        else:
            print("Not backing up {}...".format(str(child)))
    """
    with open("/mnt/snailback/report/noMatch.txt", "w") as no_match_file:
        picker = backup_picker()
        for child in source_children:
            common_snap_name = get_most_recent_common_snapshot(child)
            if common_snap_name:
                # There is a common snapshot for this filesystem, send an incremental stream
                print("Backup: " + str(child) + "@" + str(common_snap_name))
                common_snap = None
                for snapshot in child.snapshots():
                    if common_snap_name in snapshot.snapname():
                        common_snap = snapshot
                print("Source: " + str(common_snap))
                snapshot = child.snapshots()[-1]
                if args.dryrun:
                    picker.add_backup(snapshot, common_snap)
                else:
                    filename = '/mnt/snailback/' + snapshot.name.split('@')[0].split('/')[-1] + '.gzip'
                    snapshot.send_to_file(filename, base=common_snap, intermediates=True)
            else:
                # There is not common snapshot for this filesystem, send the latest monthly snapshot
                snapshot = get_most_recent(child.snapshots(), "monthly")
                if args.dryrun:
                    picker.add_backup(snapshot)
                else:
                    no_match_file.write(str(snapshot.name) + "\n")
                    filename = '/mnt/snailback/' + snapshot.name.split('@')[0].split('/')[-1] + '.gzip'
                    snapshot.send_to_file(filename)

    print("Found {} total backups to make".format(str(picker.num_children)))
    print("Need {} bytes disk space to backup".format(str(picker.size_total)))
    print("Need {} KB disk space to backup".format(str(picker.size_total/1000)))
    print("Need {} MB disk space to backup".format(str(picker.size_total/1000000)))
    print("Need {} GB disk space to backup".format(str(picker.size_total/1000000000)))
    print("Need {} TB disk space to backup".format(str(picker.size_total/1000000000000)))

    picker.show_data()

    

class backup_picker(object):
    def __init__(self):
        self.size_total = 0 #bytes
        self.num_children = 0 #filesystems to backup
        self.backups=[]

    class backup(object):
        def __init__(self, snapshot, common_snap=None):
            self.size = snapshot.stream_size(base=common_snap)
            self.name = str(snapshot) #TODO make it so I can print backup and get name
            #TODO self.age = ????

    def add_backup(self, snapshot, common_snap=None):
        new_backup = self.backup(snapshot, common_snap)
        self.size_total = self.size_total + new_backup.size
        self.num_children = self.num_children + 1
        self.backups.append(new_backup)

    def show_data(self):
        for each in self.backups:
            print("{} needs {:10.2f} GB".format(each.name, (each.size/1000000000)))
            

if __name__ == '__main__':
    print("IN main")
    print("DOING nothing")
    import argparse
    parser = argparse.ArgumentParser(prog="Snailback write application")
    parser.add_argument("-w", "--whitelist")
    parser.add_argument("-b", "--blacklist")
    parser.add_argument("-a", "--append", help="Don't delete the files already on the drive first, or only delete the ones we are writing newly", action="store_true")
    parser.add_argument("-d", "--dryrun", help="Don't actually do anything, just show how much space is needed for the operations listed", action="store_true")
    args = parser.parse_args()

    if args.whitelist:
        args.whitelist = args.whitelist.split(',')
    if args.blacklist is None:
        args.blacklist = "brendon"
    if args.blacklist:
        args.blacklist = args.blacklist.split(',')

    if args.dryrun:
        print("DRY RUN")
    else:
        print("NO DRY RUN")

    #TODO not filling the hard drive if its a dry run... rename?
    fill_harddrive(args) 

# External drive...
#ata-WDC_WD20EADS-00W4B0_WD-WCAVY6248374        ata-WDC_WD20EADS-00W4B0_WD-WCAVY6248374-part1
# mount /dev/sdu1 /mnt/snailback/
#TODO change prints to loggers

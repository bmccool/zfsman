#!/usr/bin/env python3

import pyznap.pyzfs as zfs
from os import listdir
from os.path import isfile, join
import subprocess as sp

# External drive...
#ata-WDC_WD20EADS-00W4B0_WD-WCAVY6248374        ata-WDC_WD20EADS-00W4B0_WD-WCAVY6248374-part1
# mount /dev/sdu1 /mnt/snailback/
#TODO change prints to loggers

def find_snapshot_files():
    #TODO make this work
    return "/mnt/snailback"

def get_snapshot_files(path, suffix="gzip"):
    files = [f for f in listdir(path) if isfile(join(path, f)) and suffix in f]
    return files
    

files = get_snapshot_files(find_snapshot_files())
#TODO assume there is no proper order to recieve the snapshots in i.e. child first, parent first...

def receive(snapshot, filesystem, new=False):
    """zfs receive -d -F pool/fs < /snaps/fs@all-I"""
    # gunzip -c -d /backup/03152013/tank-originalfilesystem.zsnd.gz | zfs receive tank/copyfilesystem
    #get abspath of snapshot
    snapshot = join(find_snapshot_files(), snapshot)
    cmd = ['gunzip', '-c', '-d', snapshot, '|']
    if new:
        cmd += ['zfs', 'receive', '-v', '-F']
    else:
        cmd += ['zfs', 'receive', '-v', '-d', '-F']
    cmd += [filesystem]
    cmd = [' '.join(cmd)]
    print(cmd)
    return sp.run(cmd, shell=True, check=True, stdout=sp.PIPE)

def destroy(full_zfs_path):
    cmd = ['zfs', 'destroy', '-r', full_zfs_path]
    cmd = [' '.join(cmd)]
    print(cmd)
    #TODO we should have this in a try block, set check to True, and verify that the CalledProcessError exception matches what we think it should be... 
    #['zfs destroy -r superior/tankbackup/tank/vmstorage/limited/subvol-112-disk-0']
    #cannot open 'superior/tankbackup/tank/vmstorage/limited/subvol-112-disk-0': dataset does not exist
    return sp.run(cmd, shell=True, check=False, stdout=sp.PIPE)

def get_new_filesystems():
    no_match_list = []
    with open('/mnt/snailback/report/noMatch.txt', 'r') as no_match_file:
        for line in no_match_file:
            line = line.strip()
            no_match_list.append(line)
    return no_match_list

no_match_list = get_new_filesystems()

for snapshot in files:
    print("{}: Recieving backup!".format(str(snapshot)))
    snapshot_name = snapshot.split(".gzip")[0]
    print("{}: Looking for filesystem: {} to receive the backup...".format(snapshot, snapshot_name))
    
    source_children = zfs.find(path="superior/tankbackup", types=['filesystem', 'volume'])

    # Is this a new filesystem?
    new_filesystem = False
    for line in no_match_list:
        if line.split('@')[0].split('/')[-1] == snapshot_name:
            # This is a new filesystem
            new_filesystem = True

    if new_filesystem:
        backup_prefix = 'superior/tankbackup/'
        backup_filesystem_name = backup_prefix + str(line.split('@')[0])
        print("{}: New ZFS dataset found.  Creating ZFS dataset {}".format(str(snapshot), str(backup_filesystem_name)))
        destroy(backup_filesystem_name)
        receive(snapshot, backup_filesystem_name, True)
    else:
        for child in source_children:
            # Check that the last part of the zfs filesystem name is equal to the snapshot name
            if child.name.split('/')[-1] == snapshot_name:
                print("{}: Applying snapshot to ZFS dataset {}...".format(snapshot, child))
                receive(snapshot, child.name, False)

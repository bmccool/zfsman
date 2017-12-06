"""
Created on Wed Dec 06 2017

@author: yboetz

Clean snapshots
"""

from datetime import datetime
from subprocess import CalledProcessError
from utils import Remote, parse_name
import pyzfs as zfs
from process import DatasetBusyError, DatasetNotFoundError


def clean_snap(filesystem, conf):
    """Deletes snapshots of a single filesystem according to conf"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')

    ssh = filesystem.ssh
    if ssh:
        name_log = '{:s}@{:s}:{:s}'.format(ssh.user, ssh.host, filesystem.name)
    else:
        name_log = filesystem.name

    print('{:s} INFO: Cleaning snapshots on {:s}...'.format(logtime(), name_log))

    snapshots = {'hourly': [], 'daily': [], 'weekly': [], 'monthly': [], 'yearly': []}
    for snap in filesystem.snapshots():
        # Ignore snapshots not taken with pyznap or sanoid
        if not snap.name.split('@')[1].startswith(('pyznap', 'autosnap')):
            continue
        snap_type = snap.name.split('_')[-1]

        try:
            snapshots[snap_type].append(snap)
        except KeyError:
            continue

    for snaps in snapshots.values():
        snaps.reverse()

    for snap in snapshots['yearly'][conf['yearly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['monthly'][conf['monthly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['weekly'][conf['weekly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['daily'][conf['daily']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))

    for snap in snapshots['hourly'][conf['hourly']:]:
        print('{:s} INFO: Deleting snapshot {:s}'.format(logtime(), snap.name))
        try:
            snap.destroy()
        except (DatasetBusyError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))


def clean_config(config):
    """Deletes old snapshots according to strategy given in config"""

    logtime = lambda: datetime.now().strftime('%b %d %H:%M:%S')
    print('{:s} INFO: Cleaning snapshots...'.format(logtime()))

    for conf in config:
        if not conf.get('clean', None):
            continue

        name = conf['name']
        try:
            _type, fsname, user, host, port = parse_name(name)
        except ValueError as err:
            print('{:s} ERROR: Could not parse {:s}: {}...'
                  .format(logtime(), name, err))
            continue

        if _type == 'ssh':
            name = name.split(':', maxsplit=2)[-1]
            try:
                ssh = Remote(user, host, port, conf['key'])
            except FileNotFoundError as err:
                print('{:s} ERROR: {} is not a valid ssh key file...'.format(logtime(), err))
                continue
            if not ssh.test():
                continue
        else:
            ssh = None

        try:
            # filesystem = zfs.open(fsname, ssh=ssh)
            # Children includes the base filesystem (filesystem)
            children = zfs.find(path=fsname, types=['filesystem', 'volume'], ssh=ssh)
        except (ValueError, DatasetNotFoundError, CalledProcessError) as err:
            print('{:s} ERROR: {}'.format(logtime(), err))
            continue

        # Clean snapshots of parent filesystem
        clean_snap(children[0], conf)
        # Clean snapshots of all children that don't have a seperate config entry
        for child in children[1:]:
            if ssh:
                child_name = 'ssh:{:d}:{:s}@{:s}:{:s}'.format(port, user, host, child.name)
            else:
                child_name = child.name
            # Skip if entry already in config
            if child_name in [entry['name'] for entry in config]:
                continue
            else:
                clean_snap(child, conf)

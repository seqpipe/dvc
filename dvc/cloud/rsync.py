import os
from subprocess import check_output, STDOUT, CalledProcessError

from dvc.logger import Logger
from dvc.cloud.base import DataCloudBase
from dvc.progress import progress


class RsyncKey(object):
    def __init__(self, bucket, name):
        self.name = name
        self.bucket = bucket

    @property
    def path(self):
        return os.path.join(self.bucket, self.name)


class DataCloudRsync(DataCloudBase):
    """
    Driver for remote storage over rsync.
    """
    REGEX = r'^rsync://((?P<remote>.+):)?(?P<path>/+.*)$'

    def connect(self):
        pass

    def disconnect(self):
        pass

    def rsync(self, src, dest='', dry=False):
        args = ['rsync', '-azP']

        if dry:
            args.append('-n')

        if dest != '' and self.remote in dest and not dry:
            mkdir = dest.split(':')[-1].rsplit('/', 1)[0]
            args.extend(['--rsync-path="mkdir -p {} && rsync"'.format(mkdir)])

        args.extend([src, dest])
        return check_output(' '.join(args), stderr=STDOUT, shell=True)

    def cache_file_key(self, path):
        relpath = os.path.relpath(
            os.path.abspath(path),
            self._cloud_settings.cache.cache_dir)
        return relpath.replace('\\', '/')

    @property
    def remote(self):
        return self.group('remote')

    def _in_remote(self, path):
        try:
            return self.remote + ':' + path
        except Exception:
            return path

    def _isfile_remote(self, path):
        try:
            self.rsync(self._in_remote(path), dry=True)
            return True
        except CalledProcessError:
            return False

    def _cmp_checksum(self, key, path):
        path_exists = os.path.exists(path)
        key_exists = self._isfile_remote(key.path)

        if path_exists and key_exists:
            return True

        return False

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        key = RsyncKey(self.path, key_name)
        if self._isfile_remote(key.path):
            return key
        return None

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        key = RsyncKey(self.path, key_name)
        return key

    def _push_key(self, key, path):
        try:
            self.rsync(path, self._in_remote(key.path))
            progress.finish_target(key.name)
        except CalledProcessError as exc:
            Logger.error('Failed to push "{}": {}'.format(key.path, exc))
        return path

    def _pull_key(self, key, path, no_progress_bar=False):
        self._makedirs(path)

        tmp_file = self.tmp_file(path)
        try:
            self.rsync(self._in_remote(key.path), tmp_file)
            os.rename(tmp_file, path)
            progress.finish_target(key.name)
            return path
        except CalledProcessError as exc:
            Logger.error('Failed to copy "{}": {}'.format(key.path, exc))
            return None

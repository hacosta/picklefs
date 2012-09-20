#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import errno
import fuse
import stat
import os
import time
import json

fuse.fuse_python_api = (0, 2)

# Use same timestamp for all files
_file_timestamp = int(time.time())


def path_to_entry(path, json):
	try:
		if path == "/":
			return json
		else:
			tmp = json
			for i in path.split('/')[1:]:
				tmp = tmp[i]
		
		return tmp
	except Exception, e:
		return None

def is_file(entry):
	return "deposit_uuid" in entry

def is_dir(entry):
	return not is_file(entry)

def get_parent_path(path):
	return "/".join(path.split("/")[:-1])


class MyStat(fuse.Stat):
	"""
	Convenient class for Stat objects.
	Set up the stat object with appropriate
	values depending on constructor args.
	"""
	def __init__(self, is_dir, size):
		fuse.Stat.__init__(self)
		if is_dir:
			self.st_mode = stat.S_IFDIR | 0555
			self.st_nlink = 2
		else:
			self.st_mode = stat.S_IFREG | 0444
			self.st_nlink = 1
			self.st_size = size
		self.st_atime = _file_timestamp
		self.st_mtime = _file_timestamp
		self.st_ctime = _file_timestamp


class ZtrusteeFS(fuse.Fuse):
	"""
	The underlying structure of the fs looks like this:

	{
		'file1': {'deposit_uuid': 'xxxxxx'}, # Omited data for brevity
		'file2': {'deposit_uuid': 'yyyyyy'},
		'dir1': {
			'dir2': {
				'dir3': {}
			},
			'dir21': {
			}
		}
	}
	"""

	class ZtrusteeFile:
		"""
		File class, used for all file-related operations
		"""

		def read(self, path, size, offset):
			entry = path_to_entry(path, self.tree)
			content = "hello world!"
			file_size = len(content)
			if offset < file_size:
				if offset + size > file_size:
					size = file_size - offset
				return content[offset:offset+size]
			else:
				return ''

		def write(self, buf, offset):
			return len(buf)


	def __init__(self, tree_path, *args, **kw):
		fuse.Fuse.__init__(self, *args, **kw)
		self.tree_path = tree_path
		fp = open(tree_path)
		self.tree = json.load(fp)
		fp.close()


	def main(self, *args, **kwargs):
		self.file_class = self.ZtrusteeFile
		return fuse.Fuse.main(self, *args, **kwargs)

	def flush_tree(self):
		fp = open(self.tree_path, "w")
		json.dump(self.tree, fp)
		fp.close()

	def getattr(self, path):
		entry = path_to_entry(path, self.tree)

		if entry is None:
			return -errno.ENOENT
		else:
			size = 1
			return MyStat(is_dir(entry), size)

	def readdir(self, path, offset):
		yield fuse.Direntry('.')
		yield fuse.Direntry('..')
		tmp = path_to_entry(path, self.tree)
		for e in tmp:
			yield fuse.Direntry(str(e))

	def open(self, path, flags):
		# Only support for 'READ ONLY' flag
		access_flags = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
		if flags & access_flags != os.O_RDONLY:
			return -errno.EACCES
		else:
			return 0
	
	def mkdir(self, path, mode):
		#remove the last path
		split = path.split("/")
		dir = split[-1]
		curr_entry = split[:-1]
		curr_path = "/".join(curr_entry)
		entry = path_to_entry(curr_path, self.tree)

		if entry == None:
			return -errno.ENOENT
		elif is_file(entry):
			return -errno.ENOTDIR

		entry[dir] = {}
		self.flush_tree()
	
	def rmdir(self, path):
		entry = path_to_entry(path, self.tree)
		if entry == None:
			return -errno.ENOENT
		elif is_file(entry):
			return -errno.ENOTDIR
		elif len(entry) != 0:
			return -errno.EEXIST

		parent_entry = path_to_entry(get_parent_path(path), self.tree)
		name = path.split("/")[-1]
		del(parent_entry[name])
		self.flush_tree()

	### Incomplete templates
	def create(self, path, flags, mode):
		print '*** create', path, flags, oct(mode)
		return -errno.ENOSYS

	def getdir(self, path):
		"""
		return: [[('file1', 0), ('file2', 0), ... ]]
		"""
		print '*** getdir', path
		return -errno.ENOSYS

	def mythread ( self ):
		print '*** mythread'
		return -errno.ENOSYS

	def chmod ( self, path, mode ):
		print '*** chmod', path, oct(mode)
		return -errno.ENOSYS

	def chown ( self, path, uid, gid ):
		print '*** chown', path, uid, gid
		return -errno.ENOSYS

	def fsync ( self, path, isFsyncFile ):
		print '*** fsync', path, isFsyncFile
		return -errno.ENOSYS

	def link ( self, targetPath, linkPath ):
		print '*** link', targetPath, linkPath
		return -errno.ENOSYS

	def mknod ( self, path, mode, dev ):
		print '*** mknod', path, oct(mode), dev
		return -errno.ENOSYS

	def readlink ( self, path ):
		print '*** readlink', path
		return -errno.ENOSYS

	def release ( self, path, flags ):
		print '*** release', path, flags
		return -errno.ENOSYS

	def rename ( self, oldPath, newPath ):
		print '*** rename', oldPath, newPath
		return -errno.ENOSYS

	def statfs ( self ):
		print '*** statfs'
		return -errno.ENOSYS

	def symlink ( self, targetPath, linkPath ):
		print '*** symlink', targetPath, linkPath
		return -errno.ENOSYS

	def truncate ( self, path, size ):
		print '*** truncate', path, size
		return -errno.ENOSYS

	def unlink ( self, path ):
		print '*** unlink', path
		return -errno.ENOSYS

	def utime ( self, path, times ):
		print '*** utime', path, times
		return -errno.ENOSYS


if __name__ == '__main__':
	fs = ZtrusteeFS('tree.json')
	fs.parse(errex=1)
	fs.main()

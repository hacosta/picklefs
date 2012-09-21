#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import errno
import fuse
import stat
import os
import time
import pickle
import json

fuse.fuse_python_api = (0, 2)

# Use same timestamp for all files
_file_timestamp = int(time.time())

def is_file(entry):
	return type(entry) is list

def is_dir(entry):
	return type(entry) is dict

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

	# Helper functions
	def _path_to_entry(self, path):
		"""
		Returns the corresponding entry in the in-memory
		data struct
		"""
		try:
			if path == "/":
				return self.tree
			else:
				ret = self.tree
				for i in path.split('/')[1:]:
					ret = ret[i]
			return ret
		except Exception, e:
			return None

	def _flush_tree(self):
		fp = open(self.tree_path, "w")
		pickle.dump(self.tree, fp)
		dfp = open(self.tree_path + ".json", "w")
		json.dump(self.tree, dfp, indent=4)
		fp.close()
		dfp.close()

	def __init__(self, tree_path, *args, **kw):
		fuse.Fuse.__init__(self, *args, **kw)
		self.tree_path = tree_path
		fp = open(tree_path)
		try:
			self.tree = pickle.load(fp)
		except EOFError:
			self.tree = {}
		fp.close()


	def main(self, *args, **kwargs):
		return fuse.Fuse.main(self, *args, **kwargs)


	"""
	The underlying structure of the fs looks like this:

	"""
	def open(self, path, flags):
		print '*** open', path

		access_flags = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
		entry = self._path_to_entry(path)
		if entry == None:
			return -errno.ENOENT
		elif flags & access_flags != os.O_RDONLY:
			return -errno.EACCES
		else:
			return 0

	def read(self, path, size, offset):
		print "*** read"
		entry = self._path_to_entry(path)

		if entry == None:
			return -errno.ENOENT
		else:
			content = "hello world!"
			file_size = len(content)
			buf = ''
			if offset < file_size:
				if offset + size > file_size:
					size = file_size - offset
				buf = content[offset:offset+size]
			return buf

	def write(self, buf, offset):
		print '*** write', path, buf, offset
		return len(buf)

	def create(self, path, flags, mode):
		print '*** create', path, flags, oct(mode)
		pp = get_parent_path(path)
		entry = self._path_to_entry(pp)

		if entry == None:
			print "No ent"
			return -errno.ENOENT
		elif is_file(entry):
			print "Notdir"
			return -errno.ENOTDIR
		filename = os.path.split(path)[-1]
		entry[filename] = [ { 'deposit_uuid' : 'zzzzz'}, {'flags' : flags } ]

		self._flush_tree()

	def getattr(self, path):
		print '*** getattr', path
		entry = self._path_to_entry(path)

		if entry is None:
			return -errno.ENOENT
		else:
			size = len("hello, world")
			return MyStat(is_dir(entry), size)

	def readdir(self, path, offset):
		yield fuse.Direntry('.')
		yield fuse.Direntry('..')
		for e in self._path_to_entry(path):
			yield fuse.Direntry(str(e))


	
	def mkdir(self, path, mode):
		print '*** mkdir', path
		#remove the last path
		split = path.split("/")
		dir = split[-1]
		curr_entry = split[:-1]
		curr_path = "/".join(curr_entry)
		entry = self._path_to_entry(curr_path)

		if entry == None:
			return -errno.ENOENT
		elif is_file(entry):
			return -errno.ENOTDIR

		entry[dir] = {}
		self._flush_tree()
	
	def rmdir(self, path):
		print '*** rmdir', path
		entry = self._path_to_entry(path)
		if entry == None:
			return -errno.ENOENT
		elif is_file(entry):
			return -errno.ENOTDIR
		elif len(entry) != 0:
			return -errno.EEXIST

		parent_entry = self._path_to_entry(get_parent_path(path))
		name = path.split("/")[-1]
		del(parent_entry[name])
		self._flush_tree()
	
	def utime(self, path, time):
		print '*** utime', path, time
		entry = self._path_to_entry(path)
		entry.append({'time': time})
		return 0

	### Incomplete templates (Taken from NullFS)
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



if __name__ == '__main__':
	fs = ZtrusteeFS('tree.pickle')
	fs.parse(errex=1)
	fs.main()

package gfarmClient

// #cgo CFLAGS: -g -Wall -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lgfarm -Wl,-rpath,/usr/local/lib
// #include <stdlib.h>
// #include <gfarm/gfarm.h>
// inline int gfarm_s_isdir(gfarm_mode_t m) { return GFARM_S_ISDIR(m); }
// inline void gflog_debug2(int msg_no, const char *format) { gflog_debug(msg_no, format); }
import "C"

import (
	"io"
	"os"
	"path"
	"time"
	"unsafe"
	"fmt"
)

const (
	o_accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
)

type File struct {
	gf C.GFS_File
	path string
	flags int
}

type FileInfo struct {
	name string
	st_size C.gfarm_off_t
	st_mode C.gfarm_mode_t
	st_mtimespec C.struct_gfarm_timespec
}

func Stat(path string) (FileInfo, error) {
	var sb C.struct_gfs_stat

	err := gfs_stat(path, &sb)
	if err != nil {
		return FileInfo{}, err
	}
	defer gfs_stat_free(&sb)
	return FileInfo{path, sb.st_size, sb.st_mode, sb.st_mtimespec}, nil
}

func OpenFile(path string, flags int, perm os.FileMode) (*File, error) {
	var gf C.GFS_File
	var err error

gflog_debug(GFARM_MSG_UNFIXED, "openFile");

	if (flags & os.O_CREATE) != 0 {
		err = gfs_pio_create(path, flags, perm, &gf)
	} else {
		err = gfs_pio_open(path, flags, &gf)
	}

	if err != nil {
		return nil, err
	}
	uncache_path(path)
	return &File{gf, path, flags}, nil
}

func (f *File) Close() error {
	err := gfs_pio_close(f.gf)
	if err != nil {
		return err
	}
	if (f.flags & o_accmode) == os.O_WRONLY ||
	   (f.flags & o_accmode) == os.O_RDWR ||
	   (f.flags & os.O_TRUNC) != 0 {
		uncache_path(f.path)
	}
	return nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	var n C.int
	err := gfs_pio_pread(f.gf, &b[0], len(b), off, &n)
	if err != nil {
		return 0, err
	}
	if int(n) == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

func (f *File) Read(b []byte) (int, error) {
	var n C.int
	err := gfs_pio_read(f.gf, &b[0], len(b), &n)
	if err != nil {
		return 0, err
	}
	if int(n) == 0 {
		return 0, io.EOF
	}
	return int(n), nil
}

func (f *File) Write(b []byte) (int, error) {
	var n C.int
	err := gfs_pio_write(f.gf, &b[0], len(b), &n)
	if err != nil {
		return 0, err
	}
	uncache_path(f.path)
	return int(n), nil
}

func Rename(from, to string) error {
	err := gfs_rename(from, to)
	if err != nil {
		return err
	}
	uncache_path(from)
	uncache_parent(from)
	uncache_path(to)
	uncache_parent(to)
	return nil
}

func Remove(path string) error {
	var sb C.struct_gfs_stat

	err := gfs_stat(path, &sb)
	if err != nil {
		return err
	}
	defer gfs_stat_free(&sb)
	if gfarm_s_isdir(sb.st_mode) {
		err = gfs_rmdir(path)
	} else {
		err = gfs_unlink(path)
	}
	if err != nil {
		return err
	}
	uncache_path(path)
	uncache_parent(path)
	return nil
}

func Mkdir(path string, perm os.FileMode) error {
	err := gfs_mkdir(path, perm)
	if err != nil {
		return nil
	}
	uncache_parent(path)
	return nil
}

func MkdirAll(path string, perm os.FileMode) error {
	err := gfs_mkdir_p(path, perm, 0)
	if err != nil {
		return nil
	}
	return nil
}

func ReadDir(dirname string) ([]FileInfo, error) {
	var d C.GFS_Dir
	var entry *C.struct_gfs_dirent
	var r []FileInfo
        err := gfs_opendir_caching(dirname, &d)
        if err != nil {
		return nil, err
	}
	defer gfs_closedir(d)

	for {
		err = gfs_readdir(d, &entry)
		if err != nil {
			return nil, err
		}
		if entry == (*C.struct_gfs_dirent)(C.NULL) {
			break
		}
		basename := C.GoString((*C.char)(&entry.d_name[0]))
		if basename == "." || basename == ".." {
			continue
		}
		sb, err := Stat(path.Join(dirname, basename))
		if err != nil {
			return nil, err
		}
		fi := FileInfo{basename, sb.st_size, sb.st_mode, sb.st_mtimespec}
		r = append(r, fi)
        }
	return r, nil
}

func (r FileInfo) Name() string {
	return r.name
}

func (r FileInfo) Size() int64 {
	return int64(r.st_size)
}

func (r FileInfo) Mode() os.FileMode {
	return os.FileMode(r.st_mode)
}

func (r FileInfo) ModTime() time.Time {
	return time.Unix(int64(r.st_mtimespec.tv_sec), int64(r.st_mtimespec.tv_nsec))
}

func (r FileInfo) IsDir() bool {
	return gfarm_s_isdir(r.st_mode)
}

type gfError struct {
	code int
}

func (e gfError) Error() string {
	return C.GoString(C.gfarm_error_string(C.int(e.code)))
}

func gfCheckError(code C.int) error {
	if code != C.GFARM_ERR_NO_ERROR {
		return &gfError{int(code)}
	}
	return nil
}

func Gfarm_initialize() error {
	syslog_priority := gflog_syslog_name_to_priority(GFARM2FS_SYSLOG_PRIORITY_DEBUG)
	gflog_set_priority_level(syslog_priority)
	syslog_facility := gflog_syslog_name_to_facility(GFARM2FS_SYSLOG_FACILITY_DEFAULT)
	gflog_syslog_open(C.LOG_PID, syslog_facility)
	return gfCheckError(C.gfarm_initialize((*C.int)(C.NULL), (***C.char)(C.NULL)))
}

func Gfarm_terminate() error {
	return gfCheckError(C.gfarm_terminate())
}

func gfs_stat(path string, sb *C.struct_gfs_stat) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_stat(cpath, (*C.struct_gfs_stat)(unsafe.Pointer(sb))))
}

func gfs_stat_free(sb *C.struct_gfs_stat) () {
	C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(sb)))
}

func gfarm_s_isdir(mode C.gfarm_mode_t) bool {
	return C.gfarm_s_isdir(mode) != C.int(0)
}

func gfs_pio_open(path string, flags int, gf *C.GFS_File) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cflags := gfs_hook_open_flags_gfarmize(flags)
	if cflags == C.int(-1) {
		return &gfError{-1}
	}
	return gfCheckError(C.gfs_pio_open(cpath, cflags, (*C.GFS_File)(unsafe.Pointer(gf))))
}

func gfs_pio_create(path string, flags int, mode os.FileMode, gf *C.GFS_File) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cflags := gfs_hook_open_flags_gfarmize(flags)
	if cflags == C.int(-1) {
		return &gfError{-1}
	}
	return gfCheckError(C.gfs_pio_create(cpath, cflags, C.gfarm_mode_t(mode) & C.GFARM_S_ALLPERM, (*C.GFS_File)(unsafe.Pointer(gf))))
}

func gfs_pio_close(gf C.GFS_File) error {
	return gfCheckError(C.gfs_pio_close(gf))
}

func gfs_pio_pread(gf C.GFS_File, b *byte, len int, off int64, n *C.int) error {
	return gfCheckError(C.gfs_pio_pread(gf, unsafe.Pointer(b), C.int(len), C.long(off), (*C.int)(unsafe.Pointer(n))))
}

func gfs_pio_read(gf C.GFS_File, b *byte, len int, n *C.int) error {
	return gfCheckError(C.gfs_pio_read(gf, unsafe.Pointer(b), C.int(len), (*C.int)(unsafe.Pointer(n))))
}

func gfs_pio_write(gf C.GFS_File, b *byte, len int, n *C.int) error {
	return gfCheckError(C.gfs_pio_write(gf, unsafe.Pointer(b), C.int(len), (*C.int)(unsafe.Pointer(n))))
}

func gfs_rename(from, to string) error {
	src := C.CString(from)
	defer C.free(unsafe.Pointer(src))
	dst := C.CString(to)
	defer C.free(unsafe.Pointer(dst))
	return gfCheckError(C.gfs_rename(src, dst))
}

func gfs_unlink(path string) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_unlink(cpath))
}

func gfs_rmdir(path string) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_rmdir(cpath))
}

func gfs_mkdir(path string, mode os.FileMode) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_mkdir(cpath, C.gfarm_mode_t(mode) & C.GFARM_S_ALLPERM))
}

func gfarm_url_dir(path string) string {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cparent := C.gfarm_url_dir(cpath)
	defer C.free(unsafe.Pointer(cparent))
	return C.GoString(cparent)
}

func gfs_mkdir_p(path string, mode os.FileMode, lv int) error {
	var sb C.struct_gfs_stat
	parent := gfarm_url_dir(path)

	err := gfs_stat(parent, &sb)
	if err == nil {
		defer gfs_stat_free(&sb)
		if gfarm_s_isdir(sb.st_mode) {
			// FALLTHRU
		} else {
			return &gfError{C.GFARM_ERR_NOT_A_DIRECTORY}
		}
	} else if err.(*gfError).code == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
		err = gfs_mkdir_p(parent, mode, lv + 1)
		if err != nil {
			return err
		}
	}
	err = gfs_mkdir(path, mode)
	if err != nil {
		return err
	}
	uncache_parent(path)
	return nil
}

func gfs_opendir_caching(path string, d *C.GFS_Dir) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
        return gfCheckError(C.gfs_opendir_caching(cpath, (*C.GFS_Dir)(unsafe.Pointer(d))))
}

func gfs_readdir(d C.GFS_Dir, entry **C.struct_gfs_dirent) error {
	return gfCheckError(C.gfs_readdir(d, (**C.struct_gfs_dirent)(unsafe.Pointer(entry))))
}

func gfs_closedir(d C.GFS_Dir) error {
        return gfCheckError(C.gfs_closedir(d))
}

func gfs_hook_open_flags_gfarmize(open_flags int) C.int {
	var gfs_flags C.int

	switch open_flags & o_accmode {
	case os.O_RDONLY:
		gfs_flags = C.GFARM_FILE_RDONLY
	case os.O_WRONLY:
		gfs_flags = C.GFARM_FILE_WRONLY
	case os.O_RDWR:
		gfs_flags = C.GFARM_FILE_RDWR
	default:
		return C.int(-1)
	}

	if (open_flags & os.O_TRUNC) != 0 {
		gfs_flags |= C.GFARM_FILE_TRUNC
	}
	if (open_flags & os.O_APPEND) != 0 {
		gfs_flags |= C.GFARM_FILE_APPEND
	}
	if (open_flags & os.O_EXCL) != 0 {
		gfs_flags |= C.GFARM_FILE_EXCLUSIVE
	}
	if (open_flags & os.O_CREATE) != 0 {
		// DO NOTHING
	}
	return gfs_flags
}

func uncache_path(path string) () {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	C.gfs_stat_cache_purge(cpath)
}

func uncache_parent(path string) () {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cparent := C.gfarm_url_dir(cpath)
	defer C.free(unsafe.Pointer(cparent))
	C.gfs_stat_cache_purge(cparent)
}

//func gfs_stat_cache_expiration_set() () { }

//void gflog_initialize(void);
//void gflog_terminate(void);

const (
	GFARM2FS_SYSLOG_PRIORITY_DEBUG = "debug"
	GFARM2FS_SYSLOG_FACILITY_DEFAULT = "local0"
	GFARM_MSG_UNFIXED = C.GFARM_MSG_UNFIXED
)

func gflog_syslog_name_to_priority(name string) C.int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return C.gflog_syslog_name_to_priority(cname)
}

func gflog_syslog_name_to_facility(name string) C.int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return C.gflog_syslog_name_to_facility(cname)
}

func gflog_set_priority_level(syslog_priority C.int) () {
	C.gflog_set_priority_level(syslog_priority)
}

func gflog_syslog_open(syslog_flags, syslog_priority C.int) () {
	C.gflog_syslog_open(syslog_flags, syslog_priority)
}

func gflog_debug(msg_no C.int, format string) () {
	cformat := C.CString(format)
	defer C.free(unsafe.Pointer(cformat))
fmt.Fprintf(os.Stderr, "@@@ gflog_debug %q\n", format)
	C.gflog_debug2(msg_no, cformat)
}

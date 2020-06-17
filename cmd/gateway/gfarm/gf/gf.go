package gf

// #cgo CFLAGS: -g -Wall -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lgfarm -Wl,-rpath,/usr/local/lib
// #include <stdlib.h>
// #include <gfarm/gfarm.h>
// inline int gfarm_s_isdir(gfarm_mode_t m) { return GFARM_S_ISDIR(m); }
import "C"

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"
	"unsafe"
)

/* C. は、めんばのかた、へんすうのかた、ていすう、じつひきすうのかた のみに しよう */
/*  れいがい: ReadDirのなかのキャスト
              gfs_file_info のかりひきすうのかた */
/* unsafe.Pointer はしよう しない */
/* きゃすとは きょくりょく さける:
ReadAt, Read, Write のもどりち int(n)
gfs_file_infoのなか

ReadDir:
                if entry == (*C.struct_gfs_dirent)(C.NULL) {
                basename := C.GoString((*C.char)(&entry.d_name[0]))      
*/

type File struct {
	gf C.GFS_File
	path string
}

func (g *File) String() string {
	return fmt.Sprintf("#{File %p %q}", g.gf, g.path)
}

type FileInfo struct {
	name string
	st_size C.gfarm_off_t
	st_mode C.gfarm_mode_t
	st_atimespec, st_mtimespec C.struct_gfarm_timespec
}

func Stat(path string) (FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ Stat: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ Stat EXIT: %q\n", path)
	var sb C.struct_gfs_stat

	err := gfs_stat(path, &sb)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Stat gfs_stat ERROR: %q %v\n", path, err)
		return FileInfo{}, err
	}
	defer gfs_stat_free(&sb)
	return FileInfo{path, sb.st_size, sb.st_mode, sb.st_atimespec, sb.st_mtimespec}, nil
}

func OpenFile(path string, flag int, perm os.FileMode) (*File, error) {
fmt.Fprintf(os.Stderr, "@@@ Open: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ Open EXIT: %q\n", path)
	var gf C.GFS_File
	var err error
	if flag == os.O_RDONLY {
		err = gfs_pio_open(path, C.GFARM_FILE_RDONLY, &gf)
	} else {
		err = gfs_pio_create(path, C.GFARM_FILE_WRONLY | C.GFARM_FILE_TRUNC, perm, &gf)
	}
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Open ERROR: %q %v\n", path, err)
		return nil, err
	}
	return &File{gf, path}, nil
}

func (f *File) Close() error {
fmt.Fprintf(os.Stderr, "@@@ Close: %q\n", f.path)
defer fmt.Fprintf(os.Stderr, "@@@ Close EXIT: %q\n", f.path)
	err := gfs_pio_close(f.gf)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Close ERROR: %q %v\n", f.path, err)
		return err
	}
	return nil
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	var n C.int
	err := gfs_pio_pread(f.gf, &b[0], len(b), off, &n)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ ReadAt ERROR: %q %d %v\n", f.path, off, err)
		return 0, err
	}
	if int(n) == 0 {
fmt.Fprintf(os.Stderr, "@@@ ReadAt: EOF\n", int(n))
		return 0, io.EOF
	}
	return int(n), nil
}

func (f *File) Read(b []byte) (int, error) {
	var n C.int
	err := gfs_pio_read(f.gf, &b[0], len(b), &n)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Read ERROR: %q %v\n", f.path, err)
		return 0, err
	}
	if int(n) == 0 {
fmt.Fprintf(os.Stderr, "@@@ Read: EOF\n", int(n))
		return 0, io.EOF
	}
	return int(n), nil
}

func (f *File) Write(b []byte) (int, error) {
	var n C.int
	err := gfs_pio_write(f.gf, &b[0], len(b), &n)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Write ERROR: %q %v\n", f.path, err)
		return 0, err
	}
	return int(n), nil
}

func Rename(from, to string) error {
fmt.Fprintf(os.Stderr, "@@@ Rename: %q %q\n", from, to)
defer fmt.Fprintf(os.Stderr, "@@@ Rename EXIT: %q %q\n", from, to)
	err := gfs_rename(from, to)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Rename ERROR: %q %q %v\n", from, to, err)
		return err
	}
	return nil
}

func Remove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ Remove: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ Remove EXIT: %q\n", path)
	var sb C.struct_gfs_stat

	err := gfs_stat(path, &sb)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Remove gfs_stat ERROR: %q %v\n", path, err)
		return err
	}
	defer gfs_stat_free(&sb)
	if gfarm_s_isdir(sb.st_mode) {
		return gfs_rmdir(path)
	}
	return gfs_unlink(path)
}

func RemoveOneLV(dirname string) error {
fmt.Fprintf(os.Stderr, "@@@ RemoveOneLV: %q\n", dirname)
defer fmt.Fprintf(os.Stderr, "@@@ RemoveOneLV EXIT: %q\n", dirname)
	entries, err := ReadDir(dirname)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ RemoveOneLV ReadDir ERROR: %q\n", dirname)
		return err
	}
	for _, entry := range entries {
		err = gfs_unlink(path.Join(dirname, entry.Name()))
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ RemoveOneLV gfs_unlink ERROR: %q\n", path.Join(dirname, entry.Name()))
			return err
		}
	}
	return gfs_rmdir(dirname)
}

func Mkdir(path string, perm os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ Mkdir: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ Mkdir EXIT: %q\n", path)
	err := gfs_mkdir(path, perm)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Mkdir ERROR(ignored): %q %v\n", path, err)
		return nil
	}
	return nil
}

func MkdirAll(path string, perm os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ MkdirAll: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ MkdirAll EXIT: %q\n", path)
	err := gfs_mkdir_p(path, perm, 0)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ MkdirAll ERROR(ignored): %q, %v\n", path, err)
		return nil
	}
	return nil
}

func ReadDir(dirname string) ([]FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ Readir: %q\n", dirname)
defer fmt.Fprintf(os.Stderr, "@@@ Readir EXIT: %q\n", dirname)
	var d C.GFS_Dir
	var entry *C.struct_gfs_dirent
	var r []FileInfo
        err := gfs_opendir_caching(dirname, &d)
        if err != nil {
fmt.Fprintf(os.Stderr, "@@@ ReadDir ERROR: %q %v\n", dirname, err)
		return nil, err
	}
	defer gfs_closedir(d)

	for {
		err = gfs_readdir(d, &entry)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ ReadDir ERROR: %q %v\n", dirname, err)
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
fmt.Fprintf(os.Stderr, "@@@ ReadDir ERROR: STAT %q %v\n", path.Join(dirname, basename), err)
			return nil, err
		}
		fi := FileInfo{basename, sb.st_size, sb.st_mode, sb.st_atimespec, sb.st_mtimespec}
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

/* ここから、ていレベルそうさかんすう */
/*
おやくそく:
go で、C.String を うけとる かんすう を つくらない
C.CStringしたら、defer C.free する   (C.CString のextent を、かんすうない に せいげん する)
  => CString は、(たしょう こうりつが わるくても) かんすうの そとに ださない
  => したうけ かんすう に わたすのは OK だが、C.CString を わたす さきの かんすう は
     C.* のみなので、じっしつ てきに、したうけ かんすう にも わたさないことに なる
gfs_stat、 gfs_pio_stat、 gfs_fstat したら、 defer gfs_stat_free する
    (sb の extent を ditto.)
gfs_opendir_caching, したら、defer gfs_closedir する
    (open した directory の extent を ditto.)
gfs_pio_open したら、defer gfs_pio_close する (gfOpenFileをのぞく)
 gfs_pio_createしたら、defer gfs_pio_close する (gfCreateFileをのぞく)

いかのパターンで、「かた」はいっちしなければならない
  こんぱいらはちぇっくしてくれないので、ぷろぐらまがほしょうする
  var へんすう かた
  (かた)(unsafe.Pointer(へんすう)))

きゃすとは、きょくりょく さける
  れいがい: unsafe.Pointer はきゃすとがひっす

Cでint(エラー)をかえすかんすうは、すべてgfCheckErrorでくるむ
れいがい:
  もどりかたがvoid のかんすう(gfs_stat_freeのみ)はgoでもvoidとする
  えらーをかえさないかんすうは、いかのふたつ
    gfarm_url_dir(string)、 gfarm_s_isdir(bool)

たいぷあさーしょんは、いかのいっかしょのみ
	} else if err.(*gfError).code == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
*/

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

func gfs_pio_stat(f C.GFS_File, sb *C.struct_gfs_stat) error {
	return gfCheckError(C.gfs_pio_stat(f, (*C.struct_gfs_stat)(unsafe.Pointer(sb))))
}

func gfs_fstat(f C.GFS_File, sb *C.struct_gfs_stat) error {
	return gfCheckError(C.gfs_fstat(f, (*C.struct_gfs_stat)(unsafe.Pointer(sb))))
}

func gfs_stat_free(sb *C.struct_gfs_stat) () {
	C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(sb)))
}

func gfarm_s_isdir(mode C.gfarm_mode_t) bool {
	return C.gfarm_s_isdir(mode) != C.int(0)
}

func gfs_pio_open(path string, flags C.int, gf *C.GFS_File) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_pio_open(cpath, flags, (*C.GFS_File)(unsafe.Pointer(gf))))
}

func gfs_pio_create(path string, flags C.int, mode os.FileMode, gf *C.GFS_File) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfCheckError(C.gfs_pio_create(cpath, flags, C.gfarm_mode_t(mode), (*C.GFS_File)(unsafe.Pointer(gf))))
}

func gfs_pio_close(f C.GFS_File) error {
	return gfCheckError(C.gfs_pio_close(f))
}

func gfs_pio_pread(f C.GFS_File, b *byte, len int, off int64, n *C.int) error {
	return gfCheckError(C.gfs_pio_pread(f, unsafe.Pointer(b), C.int(len), C.long(off), (*C.int)(unsafe.Pointer(n))))
}

func gfs_pio_read(f C.GFS_File, b *byte, len int, n *C.int) error {
	return gfCheckError(C.gfs_pio_read(f, unsafe.Pointer(b), C.int(len), (*C.int)(unsafe.Pointer(n))))
}

func gfs_pio_write(f C.GFS_File, b *byte, len int, n *C.int) error {
	return gfCheckError(C.gfs_pio_write(f, unsafe.Pointer(b), C.int(len), (*C.int)(unsafe.Pointer(n))))
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
	return gfCheckError(C.gfs_mkdir(cpath, C.gfarm_mode_t(mode)))
}

func gfarm_url_dir(path string) string {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cparent := C.gfarm_url_dir(cpath)
	defer C.free(unsafe.Pointer(cparent))
	return C.GoString(cparent)
}

func gfs_mkdir_p(path string, mode os.FileMode, lv int) error {
fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p: %d %q\n", lv, path)
defer fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p EXIT: %d %q\n", lv, path)
	var sb C.struct_gfs_stat
	parent := gfarm_url_dir(path)

fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p: parent = %q\n", parent)
	err := gfs_stat(parent, &sb)
	if err == nil {
		defer gfs_stat_free(&sb)
		if gfarm_s_isdir(sb.st_mode) {
			fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p PARENT OK: %q: already exists\n", parent)
			//FALLTHRU
		} else {
			fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p FAIL: parent is not a directory %q: not a directory\n", parent)
			return &gfError{C.GFARM_ERR_NOT_A_DIRECTORY}
		}
	} else if err.(*gfError).code == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
		err = gfs_mkdir_p(parent, mode, lv + 1)
		if err != nil {
			return err
		}
	}
fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir: %q\n", path)
	err = gfs_mkdir(path, mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p FAIL: %q gfs_mkdir failed\n", path)
		return err
	}
	fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir_p SUCCESS: %q gfs_mkdir succeeded\n", path)
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

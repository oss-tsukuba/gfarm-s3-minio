package gfarm

// #cgo CFLAGS: -g -Wall -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lgfarm -Wl,-rpath,/usr/local/lib
// #include <stdlib.h>
// #include <gfarm/gfarm.h>
// inline int gfarm_s_isdir(gfarm_mode_t m) { return GFARM_S_ISDIR(m); }
import "C"

/*
 * os を、gf におきかえると、さくっといれかわるようにする

 * type Client          common
 * type ClientOptions   common
 * type FileReadWriter *os.File / *gfFile
 * type FileInfo        os.FileInfo / gfFileInfo
 * type FsInfo          common
 * type GfarmError      common
 * ClientOptionsFromConf common
 * GfarmError.Error     -
 * NewClient
 * Close
 *
 *	type os.File
 *			gfFile { gf *C.GFS_File }
 *	type os.FileInfo
 *			type gfFileInfo
 *	type os.FileMode
 *			gfarm_mode_t
 *
 * Stat
 *	os.Stat(string)
 *			gfStat(string)
 * Open
 *	os.OpenFile(string, os.O_RDONLY, mode_t)
 *			gfOpenFile(string)
 * Create
 *	os.OpenFile(string, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode_t)
 *			gfCreateFile(string)
 * FileReadWriter.Close
 *	os.File.Close()
 *			gfClose()
 * FileReadWriter.ReadAt
 *	os.File.ReadAt([]byte, int64)
 *			gfFile.ReadAt()
 * FileReadWriter.Read
 *	os.File.Read([]byte)
 *			gfFile.Read()
 * FileReadWriter.Write
 *	os.File.Write([]byte)
 *			gfFile.Write()
 * Rename
 *	os.Rename(string, string)
 *			gfRename()
 * Remove
 *	os.Remove(string)
 *                      gfRemove()
 * RemoveAll
 *	os.RemoveAll(string)
 *			gfRemoveOneLV()
 * Mkdir
 *	os.Mkdir(string, mode_t)
 *			gfMkdir()
 * MkdirAll
 *	os.MkdirAll(string, mode_t)
 *			gfMkdirAll()
 * ReadDir []FileInfo
 *	ioutil.ReadDir(string) []os.FileInfo
 *			gfReadDir() []gfFileInfo
 *
 * FileInfo.Name()
 *	os.FileInfo.Name()
 *			gfFileInfo.Name()
 * FileInfo.Size()
 *	os.FileInfo.Size()
 *			gfFileInfo.Size()
 * FileInfo.Mode()
 *	os.FileInfo.Mode()
 *			gfFileInfo.Mode()
 * FileInfo.AccessTime()
 *	os.FileInfo.ModTime()  <<<<<
 *			gfFileInfo.AccessTime()
 * FileInfo.ModTime()
 *	os.FileInfo.ModTime()
 *			gfFileInfo.ModTime()
 * FileInfo.IsDir()
 *	os.FileInfo.IsDir()
 *			gfFileInfo.IsDir()
 *
 * StatFs.Used
 *	0
 *			0
 * 	osPathJoin
 *			gfPathJoin
 */

/* ファイルシステムばん ここから
@import (
@	"fmt"
@	"io/ioutil"
@	"os"
@	"time"
@	"unsafe"
@)
@
@type Client struct {
@	opts ClientOptions
@}
@
@type ClientOptions struct {
@	User string
@	Rootdir string
@}
@
@type FileReadWriter struct {
@	f *os.File
@}
@
@type FileInfo struct {
@	i os.FileInfo
@}
@
@type FsInfo struct {
@	Used uint64
@}
@
@type GfarmError struct {
@	code int
@}
@
@func ClientOptionsFromConf() ClientOptions {
@	return ClientOptions{"", "/mnt/data/nas1"}
@}
@
@func (e *GfarmError) Error() string {
@	return fmt.Sprintf("error code: %d", e.code)
@}
@
@func NewClient(o ClientOptions) (*Client, error) {
@	return &Client{o}, nil
@}
@
@func (clnt *Client) Close() error {
@	return nil
@}
@
@func (clnt *Client) Stat(path string) (FileInfo, error) {
@	v, err := os.Stat(osPathJoin(clnt.opts.Rootdir, path))
@	if err != nil {
@		return FileInfo{}, err
@	}
@	return FileInfo{v}, err
@}
@
@func (clnt *Client) Open(path string) (*FileReadWriter, error) {
@	f, err := os.OpenFile(osPathJoin(clnt.opts.Rootdir, path), os.O_RDONLY, 0666)
@	if err != nil {
@		return nil, err
@	}
@	return &FileReadWriter{f}, nil
@}
@
@func (clnt *Client) Create(path string) (*FileReadWriter, error) {
@	f, err := os.OpenFile(osPathJoin(clnt.opts.Rootdir, path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
@	if err != nil {
@		return nil, err
@	}
@	return &FileReadWriter{f}, nil
@}
@
@//ファイルシステム版のOpen2, Create2 は、それぞれOpen, Createと同じ
@func (clnt *Client) Open2(dirname, ignore, path string) (*FileReadWriter, error) {
@	return clnt.Open(osPathJoin(dirname, path))
@}
@func (clnt *Client) Create2(dirname, ignore, path string) (*FileReadWriter, error) {
@	return clnt.Create(osPathJoin(dirname, path))
@}
@
@func (f *FileReadWriter) Close() (error) {
@	return f.f.Close()
@}
@
@func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
@	return r.f.ReadAt(p, off)
@}
@
@func (r *FileReadWriter) Read(p []byte) (int, error) {
@	return r.f.Read(p)
@}
@
@func (w *FileReadWriter) Write(p []byte) (int, error) {
@	return w.f.Write(p)
@}
@
@func (clnt *Client) Rename(from, to string) error {
@	return os.Rename(osPathJoin(clnt.opts.Rootdir, from), osPathJoin(clnt.opts.Rootdir, to))
@}
@
@func (clnt *Client) Remove(path string) error {
@	return os.Remove(osPathJoin(clnt.opts.Rootdir, path))
@}
@
@func (clnt *Client) RemoveAll(path string) error {
@	return os.RemoveAll(osPathJoin(clnt.opts.Rootdir, path))
@}
@
@func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
@	return os.Mkdir(osPathJoin(clnt.opts.Rootdir, path), mode)
@}
@
@func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
@	return os.MkdirAll(osPathJoin(clnt.opts.Rootdir, path), mode)
@}
@
@func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
@	v, err := ioutil.ReadDir(osPathJoin(clnt.opts.Rootdir, path))
@	if err != nil {
@		return nil, err
@	}
@	var r []FileInfo
@	for _, s := range v {
@		r = append(r, FileInfo{s})
@	}
@	return r, err
@}
@
@func (fi *FileInfo) Name() string {
@	return fi.i.Name()
@}
@
@func (fi *FileInfo) Size() int64 {
@	return fi.i.Size()
@}
@
@func (fi *FileInfo) Mode() os.FileMode {
@	return fi.i.Mode()
@}
@
@func (fi *FileInfo) AccessTime() time.Time {
@	return fi.i.ModTime()
@}
@
@func (fi *FileInfo) ModTime() time.Time {
@	return fi.i.ModTime()
@}
@
@func (fi *FileInfo) IsDir() bool {
@	return fi.i.IsDir()
@}
@
@func (clnt *Client) StatFs() (FsInfo, error) {
@	return FsInfo{0}, nil
@}
@
@func osPathJoin(dirname, basename string) string {
@	return path.Join(dirname, basename)
@}
@ここまで ファイルシステムばん */

/******************************************************************/

/* ここから gfarmばん */

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"
	"unsafe"
)

type Client struct {
	opts ClientOptions
}

type ClientOptions struct {
	User string
	Rootdir string
}

type FileReadWriter struct {
	f *gfFile
}

type FileReadWriter2 struct {
	f, g *gfFile
}

type FileInfo struct {
	i gfFileInfo
}

type FsInfo struct {
	Used uint64
} 

type GfarmError struct {
	code int
}

func ClientOptionsFromConf() ClientOptions {
	return ClientOptions{"", "/home/hp120273/hpci005858/tmp/nas1"}
/* XXX コマンドライン引数から 受け取る  */
}

func (e *GfarmError) Error() string {
	return fmt.Sprintf("error code: %d", e.code)
}

func NewClient(o ClientOptions) (*Client, error) {
	err := gfarm_initialize()
	if err != nil {
		return nil, err
	}
	return &Client{o}, nil
}

func (clnt *Client) Close() error {
	return gfarm_terminate()
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
	v, err := gfStat(gfPathJoin(clnt.opts.Rootdir, path))
	if err != nil {
		return FileInfo{}, err
	}
	return FileInfo{v}, err
}

func (clnt *Client) Open(path string) (*FileReadWriter, error) {
	f, err := gfOpenFile(gfPathJoin(clnt.opts.Rootdir, path))
	if err != nil {
		return nil, err
	}
	return &FileReadWriter{f}, nil
}

func (clnt *Client) Create(path string) (*FileReadWriter, error) {
	f, err := gfCreateFile(gfPathJoin(clnt.opts.Rootdir, path), 0666)
	if err != nil {
		return nil, err
	}
	return &FileReadWriter{f}, nil
}

func (f *FileReadWriter) Close() (error) {
	return f.f.gfClose()
}

func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
	return r.f.ReadAt(p, off)
}

func (r *FileReadWriter) Read(p []byte) (int, error) {
	return r.f.Read(p)
}

func (w *FileReadWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

/* Multipart はさいあっぷでうわがき できるので、
  openのじてんで、local と gfarm のりょうほう に からの ふぁいるを
  さくせい する ひつよう が ある
 */
func (clnt *Client) Open2(dirname, chaceDirname, path string) (*FileReadWriter2, error) {
	f, err := gfOpenFile(gfPathJoin(clnt.opts.Rootdir, gfPathJoin(dirname, path)))
	if err != nil {
		return nil, err
	}
/* ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく */
	return &FileReadWriter2{f, nil}, nil
}

func (clnt *Client) Create2(dirname, chaceDirname, path string) (*FileReadWriter2, error) {
	f, err := gfCreateFile(gfPathJoin(clnt.opts.Rootdir, gfPathJoin(dirname, path)), 0666)
	if err != nil {
		return nil, err
	}
/* ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく */
	return &FileReadWriter2{f, nil}, nil
}

func (f *FileReadWriter2) Close() (error) {
/* gもクローズする*/
	return f.f.gfClose()
}

func (r *FileReadWriter2) ReadAt(p []byte, off int64) (int, error) {
/* gからよめるうちは、gからよむ*/
	return r.f.ReadAt(p, off)
}

func (r *FileReadWriter2) Read(p []byte) (int, error) {
/* gからよめるうちは、gからよむ*/
	return r.f.Read(p)
}

func (w *FileReadWriter2) Write(p []byte) (int, error) {
/* gにかけるうちは、gにかく*/
	return w.f.Write(p)
}

func (clnt *Client) Rename(from, to string) error {
	return gfRename(gfPathJoin(clnt.opts.Rootdir, from), gfPathJoin(clnt.opts.Rootdir, to))
}

func (clnt *Client) Remove(path string) error {
	return gfRemove(gfPathJoin(clnt.opts.Rootdir, path))
}

func (clnt *Client) RemoveAll(path string) error {
	return gfRemoveOneLV(gfPathJoin(clnt.opts.Rootdir, path))
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
	return gfMkdir(gfPathJoin(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
	return gfMkdirAll(gfPathJoin(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
	v, err := gfReadDir(gfPathJoin(clnt.opts.Rootdir, path))
	if err != nil {
		return nil, err
	}
	var r []FileInfo
	for _, s := range v {
		r = append(r, FileInfo{s})
	}
	return r, err
}

func (fi *FileInfo) Name() string {
	return fi.i.Name()
}

func (fi *FileInfo) Size() int64 {
	return fi.i.Size()
}

func (fi *FileInfo) Mode() os.FileMode {
	return fi.i.Mode()
}

func (fi *FileInfo) AccessTime() time.Time {
	return fi.i.AccessTime()
}

func (fi *FileInfo) ModTime() time.Time {
	return fi.i.ModTime()
}

func (fi *FileInfo) IsDir() bool {
	return fi.i.IsDir()
}

func (clnt *Client) StatFs() (FsInfo, error) {
	return FsInfo{0}, nil
}

/* ここまで gfarmばん */

/******************************************************************/

/* ここから、osのかわりのgfぱっけーじ */
/* C. は、めんばのかた、へんすうのかた、ていすう、じつひきすうのかた のみに しよう */
/*  れいがい: gfReadDirのなかのキャスト
              gfs_file_info のかりひきすうのかた */
/* unsafe.Pointer はしよう しない */
/* きゃすとは きょくりょく さける:
ReadAt, Read, Write のもどりち int(n)
gfs_file_infoのなか

gfReadDir:
                if entry == (*C.struct_gfs_dirent)(C.NULL) {
                basename := C.GoString((*C.char)(&entry.d_name[0]))      

*/

type gfFile struct {
	gf C.GFS_File
	path string
}

func (g *gfFile) String() string {
	return fmt.Sprintf("#{gfFile %p %q}", g.gf, g.path)
}

type gfFileInfo struct {
	name string
	st_size C.gfarm_off_t
	st_mode C.gfarm_mode_t
	st_atimespec, st_mtimespec C.struct_gfarm_timespec
}

func gfStat(path string) (gfFileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ gfStat: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfStat EXIT: %q\n", path)
//@var sb0, sb1, sb2 C.struct_gfs_stat
	var sb C.struct_gfs_stat
//@var gf C.GFS_File

	err := gfs_stat(path, &sb)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfStat gfs_stat ERROR: %q %v\n", path, err)
		return gfFileInfo{}, err
	}
	defer gfs_stat_free(&sb)
//@ if gfarm_s_isdir(sb0.st_mode) {
	return gfFileInfo{path, sb.st_size, sb.st_mode, sb.st_atimespec, sb.st_mtimespec}, nil
//@}

/*
@	err = gfs_pio_open(path, C.GFARM_FILE_RDONLY, &gf)
@	if err != nil {
@fmt.Fprintf(os.Stderr, "@@@ gfStat gfs_pio_open ERROR: %q %v\n", path, err)
@		return gfFileInfo{}, err
@	}
@	defer gfs_pio_close(gf)
@
@	err = gfs_pio_stat(gf, &sb1)
@	if err != nil {
@fmt.Fprintf(os.Stderr, "@@@ gfStat gfs_pio_stat ERROR: %q %v\n", path, err)
@		return gfFileInfo{}, err
@	}
@	defer gfs_stat_free(&sb1)
@
@	err = gfs_fstat(gf, &sb2)
@	if err != nil {
@fmt.Fprintf(os.Stderr, "@@@ gfStat gfs_fstat ERROR: %q %v\n", path, err)
@		return gfFileInfo{}, err
@	}
@	defer gfs_stat_free(&sb2)
@
@	return gfFileInfo{path, sb1.st_size, sb1.st_mode, sb2.st_atimespec, sb2.st_mtimespec}, nil
*/
}

func gfOpenFile(path string) (*gfFile, error) {
fmt.Fprintf(os.Stderr, "@@@ gfOpenFile: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfOpenFile EXIT: %q\n", path)
	var gf C.GFS_File
	err := gfs_pio_open(path, C.GFARM_FILE_RDONLY, &gf)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfOpenFile ERROR: %q %v\n", path, err)
		return nil, err
	}
	return &gfFile{gf, path}, nil
}

func gfCreateFile(path string, mode os.FileMode) (*gfFile, error) {
fmt.Fprintf(os.Stderr, "@@@ gfCreateFile: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfCreateFile EXIT: %q\n", path)
	var gf C.GFS_File
	err := gfs_pio_create(path, C.GFARM_FILE_WRONLY | C.GFARM_FILE_TRUNC, mode, &gf)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfCreateFile ERROR: %q %v\n", path, err)
		return nil, err
	}
	return &gfFile{gf, path}, nil
}

func (f *gfFile) gfClose() error {
fmt.Fprintf(os.Stderr, "@@@ gfCloseFile: %q\n", f.path)
defer fmt.Fprintf(os.Stderr, "@@@ gfCloseFile EXIT: %q\n", f.path)
	err := gfs_pio_close(f.gf)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfClose ERROR: %q %v\n", f.path, err)
		return err
	}
	return nil
}

func (f *gfFile) ReadAt(b []byte, off int64) (int, error) {
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

func (f *gfFile) Read(b []byte) (int, error) {
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

func (f *gfFile) Write(b []byte) (int, error) {
	var n C.int
	err := gfs_pio_write(f.gf, &b[0], len(b), &n)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ Write ERROR: %q %v\n", f.path, err)
		return 0, err
	}
	return int(n), nil
}

func gfRename(from, to string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRename: %q %q\n", from, to)
defer fmt.Fprintf(os.Stderr, "@@@ gfRename EXIT: %q %q\n", from, to)
	err := gfs_rename(from, to)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRename ERROR: %q %q %v\n", from, to, err)
		return err
	}
	return nil
}

func gfRemove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRemove: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfRemove EXIT: %q\n", path)
	var sb C.struct_gfs_stat

	err := gfs_stat(path, &sb)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRemove gfs_stat ERROR: %q %v\n", path, err)
		return err
	}
	defer gfs_stat_free(&sb)
	if gfarm_s_isdir(sb.st_mode) {
		return gfs_rmdir(path)
	}
	return gfs_unlink(path)
}

func gfRemoveOneLV(dirname string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV: %q\n", dirname)
defer fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV EXIT: %q\n", dirname)
	entries, err := gfReadDir(dirname)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV gfReadDir ERROR: %q\n", dirname)
		return err
	}
	for _, entry := range entries {
		err = gfs_unlink(gfPathJoin(dirname, entry.Name()))
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV gfs_unlink ERROR: %q\n", gfPathJoin(dirname, entry.Name()))
			return err
		}
	}
	return gfs_rmdir(dirname)
}

func gfMkdir(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ gfMkdir: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfMkdir EXIT: %q\n", path)
	err := gfs_mkdir(path, mode)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfMkdir ERROR(ignored): %q %v\n", path, err)
		return nil
	}
	return nil
}

func gfMkdirAll(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll EXIT: %q\n", path)
	err := gfs_mkdir_p(path, mode, 0)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll ERROR(ignored): %q, %v\n", path, err)
		return nil
	}
	return nil
}

func gfReadDir(dirname string) ([]gfFileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ gfReadir: %q\n", dirname)
defer fmt.Fprintf(os.Stderr, "@@@ gfReadir EXIT: %q\n", dirname)
	var d C.GFS_Dir
	var entry *C.struct_gfs_dirent
	var r []gfFileInfo
        err := gfs_opendir_caching(dirname, &d)
        if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: %q %v\n", dirname, err)
		return nil, err
	}
	defer gfs_closedir(d)

	for {
		err = gfs_readdir(d, &entry)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: %q %v\n", dirname, err)
			return nil, err
		}
		if entry == (*C.struct_gfs_dirent)(C.NULL) {
			break
		}
		basename := C.GoString((*C.char)(&entry.d_name[0]))
		if basename == "." || basename == ".." {
			continue
		}
		sb, err := gfStat(gfPathJoin(dirname, basename))
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: STAT %q %v\n", gfPathJoin(dirname, basename), err)
			return nil, err
		}
		fi := gfFileInfo{basename, sb.st_size, sb.st_mode, sb.st_atimespec, sb.st_mtimespec}
		r = append(r, fi)
        }
	return r, nil
}

func gfPathJoin(dirname, basename string) string {
fmt.Fprintf(os.Stderr, "@@@ gfPathJoin: %q %q => %q\n", dirname, basename, path.Join(dirname, basename))
	return path.Join(dirname, basename)
}

func (r gfFileInfo) Name() string {
	return r.name
}

func (r gfFileInfo) Size() int64 {
	return int64(r.st_size)
}

func (r gfFileInfo) Mode() os.FileMode {
	return os.FileMode(r.st_mode)
}

func (r gfFileInfo) AccessTime() time.Time {
	return time.Unix(int64(r.st_atimespec.tv_sec), int64(r.st_atimespec.tv_nsec))
}

func (r gfFileInfo) ModTime() time.Time {
	return time.Unix(int64(r.st_mtimespec.tv_sec), int64(r.st_mtimespec.tv_nsec))
}

func (r gfFileInfo) IsDir() bool {
	return gfarm_s_isdir(r.st_mode)
}

func (r gfFileInfo) Sys() interface{} {
	return nil
}

func (r gfFileInfo) String() string {
	return fmt.Sprintf("#{gfFileInfo Name: %q  Size: %d  Mode: %v  ModTime: %v  AccessTime: %v  IsDir: %v}", r.Name(), r.Size(), r.Mode(), r.ModTime(), r.AccessTime(), r.IsDir())
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

func gfarm_initialize() error {
	return gfCheckError(C.gfarm_initialize((*C.int)(C.NULL), (***C.char)(C.NULL)))
}

func gfarm_terminate() error {
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

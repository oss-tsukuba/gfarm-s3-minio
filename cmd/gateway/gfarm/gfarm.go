package gfarm

// #cgo CFLAGS: -g -Wall -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lgfarm -Wl,-rpath,/usr/local/lib
// #include <stdlib.h>
// #include <gfarm/gfarm.h>
// inline int gfarm_s_isdir(gfarm_mode_t m) { return GFARM_S_ISDIR(m); }
import "C"

/*
 * os を、gf におきかえると、さくっといれかわるようにする
 *
 *	type os.File
 *			gfFile { gf *C.GFS_File }
 *	type os.FileInfo
 *			type gfFileInfo
 *	type os.FileMode
 *			gfarm_mode_t
 *	os.Stat(string)
 *			gfStat(string)
 *	os.OpenFile(string, os.O_RDONLY, mode_t)
 *			gfOpenFile(string)
 *	os.OpenFile(string, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode_t)
 *			gfCreateFile(string)
 *	os.File.Close()
 *			gfClose()
 *	os.File.ReadAt([]byte, int64)
 *			gfReadAt()
 *	os.File.Read([]byte)
 *			gfRead()
 *	os.File.Write([]byte)
 *			gfWrite()
 *	os.Rename(string, string)
 *			gfRename()
 *	os.Remove(string)
 *                        @gfs_unlink()
 *			@gfs_rmdir()
 *	os.RemoveAll(string)
 *			gfRemoveOneLV()
 *			@@@ いちかいそうのみ、readdir, unlink, rmdir
 *	os.Mkdir(string, mode_t)
 *			gfMkdir()
 *	os.MkdirAll(string, mode_t)
 *			gfMkdirAll()
 *	ioutil.ReadDir(string) []os.FileInfo
 *			gfReadDir()
 *
 *	os.FileInfo.Name()
 *			gfFileInfo.Name()
 *	os.FileInfo.Size()
 *			gfFileInfo.Size()
 *	os.FileInfo.Mode()
 *			gfFileInfo.Mode()
 *	os.FileInfo.ModTime()
 *			gfFileInfo.ModTime()
 *	os.FileInfo.IsDir()
 *			gfFileInfo.IsDir()
 *	os.FileInfo.AccessTime()
 *			_
 *
 *	StatFs.Used
 */

/* ファイルシステムばん ここから
@import (
@	"os"
@	"time"
@	"fmt"
@	"io/ioutil"
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
@//fmt.Fprintf(os.Stderr, "@@@ NewClient %v\n", o)
@	var c *Client
@	c = &Client{o}
@	return c, nil
@}
@
@func (clnt *Client) Close() error {
@//fmt.Fprintf(os.Stderr, "@@@ Close\n")
@	return nil
@}
@
@func (clnt *Client) Stat(path string) (FileInfo, error) {
@//fmt.Fprintf(os.Stderr, "@@@ Stat %q\n", path)
@	v, err := os.Stat(clnt.opts.Rootdir + path)
@//fmt.Fprintf(os.Stderr, "@@@ Stat %v\n", v)
@	if err != nil { return FileInfo{}, err }
@	return FileInfo{v}, err
@}
@
@func (clnt *Client) Open(path string) (*FileReadWriter, error) {
@//fmt.Fprintf(os.Stderr, "@@@ Open %q\n", path)
@	var r *FileReadWriter
@	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0666)
@	if err != nil { return nil, err }
@	r = &FileReadWriter{f}
@	return r, nil
@}
@
@func (clnt *Client) Create(path string) (*FileReadWriter, error) {
@//fmt.Fprintf(os.Stderr, "@@@ Create %q\n", path)
@	var w *FileReadWriter
@	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
@	if err != nil { return nil, err }
@	w = &FileReadWriter{f}
@	return w, nil
@}
@
@
@//@func (clnt *Client) Append(path string) (*FileReadWriter, error) {
@//@//fmt.Fprintf(os.Stderr, "@@@ Append %q\n", path)
@//@//@	var w *FileReadWriter
@//@	//f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_APPEND, 0666)
@//@	w, err := clnt.Create(path)
@//@//@	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
@//@//@	if err != nil { return nil, err }
@//@//@	w = &FileReadWriter{f}
@//@//fmt.Fprintf(os.Stderr, "@@@ Append => %v\n", w)
@//@	return w, err
@//@}
@
@func (f *FileReadWriter) Close() (error) {
@//fmt.Fprintf(os.Stderr, "@@@ Close\n")
@//fmt.Fprintf(os.Stderr, "@@@ Close => %v\n", f)
@	return f.f.Close()
@}
@
@//@func (clnt *Client) CreateEmptyFile(path string) error {
@//@fmt.Fprintf(os.Stderr, "@@@ CreateEmptyFile %q\n", path)
@//@	f, err := clnt.Create(path)
@//@	if err != nil { return err }
@//@	f.Close()
@//@	return nil
@//@}
@
@func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
@	n, e := r.f.ReadAt(p, off)
@//fmt.Fprintf(os.Stderr, "@@@ ReadAt(%d) => %d %v  ", off, n, e)
@	return n, e
@}
@
@func (r *FileReadWriter) Read(p []byte) (int, error) {
@	n, e := r.f.Read(p)
@//fmt.Fprintf(os.Stderr, "@@@ Read() => %d %v  ", n, e)
@	return n, e
@}
@
@func (w *FileReadWriter) Write(p []byte) (int, error) {
@	n, e := w.f.Write(p)
@//fmt.Fprintf(os.Stderr, "@@@ Write => %d %v  ", n, e)
@	return n, e
@}
@
@func (clnt *Client) Rename(from, to string) error {
@//fmt.Fprintf(os.Stderr, "@@@ Rename %q %q\n", from, to)
@	return os.Rename(clnt.opts.Rootdir + from, clnt.opts.Rootdir + to)
@	return nil
@}
@
@func (clnt *Client) Remove(path string) error {
@fmt.Fprintf(os.Stderr, "@@@ Remove %q\n", path)
@	return os.Remove(clnt.opts.Rootdir + path)
@}
@
@func (clnt *Client) RemoveAll(path string) error {
@fmt.Fprintf(os.Stderr, "@@@ RemoveAll %q\n", path)
@	return os.RemoveAll(clnt.opts.Rootdir + path)
@}
@
@func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
@//fmt.Fprintf(os.Stderr, "@@@ Mkdir %q %v\n", path, mode)
@	return os.Mkdir(clnt.opts.Rootdir + path, mode)
@}
@
@func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
@//fmt.Fprintf(os.Stderr, "@@@ MkdirAll %q %v\n", path, mode)
@	return os.MkdirAll(clnt.opts.Rootdir + path, mode)
@}
@
@func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
@	v, err := ioutil.ReadDir(clnt.opts.Rootdir + path)
@	if err != nil { return nil, err }
@	var r []FileInfo
@	for _, s := range v {
@		r = append(r, FileInfo{s})
@	}
@fmt.Fprintf(os.Stderr, "@@@ ReadDir %q => %v\n", path, r)
@	return r, err
@}
@
@//fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
@//	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0755)
@//	if err != nil { return nil, err }
@//	g := FileReadWriter{f}
@//	defer g.Close()
@//	return g.Readdir(0)
@//fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
@//fmt.Fprintf(os.Stderr, "@@@ ReadDir => %v\n", v)
@//@func (f *FileReadWriter) Readdir(n int) ([]FileInfo, error) {
@//@fmt.Fprintf(os.Stderr, "@@@ Readdir %v\n", n)
@//@	v, err := f.f.Readdir(n)
@//@//fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", v)
@//@	if err != nil { return nil, err }
@//@	var r []FileInfo
@//@	for _, s := range v {
@//@		r = append(r, FileInfo{s})
@//@	}
@//@fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", r)
@//@	return r, err
@//@}
@
@func (fi *FileInfo) Name() string {
@//fmt.Fprintf(os.Stderr, "@@@ Name: %q\n", fi.i.Name())
@	return fi.i.Name()
@}
@
@func (fi *FileInfo) Size() int64 {
@//fmt.Fprintf(os.Stderr, "@@@ Size: %d\n", fi.i.Size())
@	return fi.i.Size()
@}
@
@func (fi *FileInfo) Mode() os.FileMode {
@//fmt.Fprintf(os.Stderr, "@@@ Mode: %v\n", fi.i.Mode())
@	return fi.i.Mode()
@}
@
@func (fi *FileInfo) ModTime() time.Time {
@//fmt.Fprintf(os.Stderr, "@@@ ModTime: %v\n", fi.i.ModTime())
@	return fi.i.ModTime()
@}
@
@func (fi *FileInfo) IsDir() bool {
@//fmt.Fprintf(os.Stderr, "@@@ IsDir: %v\n", fi.i.IsDir())
@	return fi.i.IsDir()
@}
@
@func (fi *FileInfo) AccessTime() time.Time {
@//fmt.Fprintf(os.Stderr, "@@@ AccessTime: %v\n", fi.i.ModTime())
@	return fi.i.ModTime()
@}
@
@func (clnt *Client) StatFs() (FsInfo, error) {
@//fmt.Fprintf(os.Stderr, "@@@ StatFs %q\n")
@	return FsInfo{0}, nil
@}
@
@ここまで ファイルシステムばん */

/******************************************************************/

/* ここから gfarmばん */

import (
	"os"
	"time"
	"fmt"
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
}

func (e *GfarmError) Error() string {
	return fmt.Sprintf("error code: %d", e.code)
}

func NewClient(o ClientOptions) (*Client, error) {
//fmt.Fprintf(os.Stderr, "@@@ NewClient %v\n", o)
	var c *Client
	err := gfarm_initialize()
	if err != nil {
		return nil, err
	}
	c = &Client{o}
	return c, nil
}

func (clnt *Client) Close() error {
//fmt.Fprintf(os.Stderr, "@@@ Close\n")
	gfarm_terminate()
	return nil
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
//fmt.Fprintf(os.Stderr, "@@@ Stat %q\n", path)
	v, err := gfStat(clnt.opts.Rootdir + path)
//fmt.Fprintf(os.Stderr, "@@@ Stat %v\n", v)
	if err != nil { return FileInfo{}, err }
	return FileInfo{v}, err
}

func (clnt *Client) Open(path string) (*FileReadWriter, error) {
//fmt.Fprintf(os.Stderr, "@@@ Open %q\n", path)
	var r *FileReadWriter
	f, err := gfOpenFile(clnt.opts.Rootdir + path)
	if err != nil { return nil, err }
	r = &FileReadWriter{f}
	return r, nil
}

func (clnt *Client) Create(path string) (*FileReadWriter, error) {
//fmt.Fprintf(os.Stderr, "@@@ Create %q\n", path)
	var w *FileReadWriter
	f, err := gfCreateFile(clnt.opts.Rootdir + path, 0666)
	if err != nil { return nil, err }
	w = &FileReadWriter{f}
	return w, nil
}


//@func (clnt *Client) Append(path string) (*FileReadWriter, error) {
//@//fmt.Fprintf(os.Stderr, "@@@ Append %q\n", path)
//@//@	var w *FileReadWriter
//@	//f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_APPEND, 0666)
//@	w, err := clnt.Create(path)
//@//@	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
//@//@	if err != nil { return nil, err }
//@//@	w = &FileReadWriter{f}
//@//fmt.Fprintf(os.Stderr, "@@@ Append => %v\n", w)
//@	return w, err
//@}

func (f *FileReadWriter) Close() (error) {
//fmt.Fprintf(os.Stderr, "@@@ Close\n")
//fmt.Fprintf(os.Stderr, "@@@ Close => %v\n", f)
	return f.f.gfClose()
}

//@func (clnt *Client) CreateEmptyFile(path string) error {
//@fmt.Fprintf(os.Stderr, "@@@ CreateEmptyFile %q\n", path)
//@	f, err := clnt.Create(path)
//@	if err != nil { return err }
//@	f.Close()
//@	return nil
//@}

func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
	n, e := r.f.gfReadAt(p, off)
//fmt.Fprintf(os.Stderr, "@@@ ReadAt(%d) => %d %v  ", off, n, e)
	return n, e
}

func (r *FileReadWriter) Read(p []byte) (int, error) {
	n, e := r.f.gfRead(p)
//fmt.Fprintf(os.Stderr, "@@@ Read() => %d %v  ", n, e)
	return n, e
}

func (w *FileReadWriter) Write(p []byte) (int, error) {
	n, e := w.f.gfWrite(p)
//fmt.Fprintf(os.Stderr, "@@@ Write => %d %v  ", n, e)
	return n, e
}

func (clnt *Client) Rename(from, to string) error {
//fmt.Fprintf(os.Stderr, "@@@ Rename %q %q\n", from, to)
	return gfRename(clnt.opts.Rootdir + from, clnt.opts.Rootdir + to)
	return nil
}

func (clnt *Client) Remove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ Remove %q\n", path)
	return gfRemove(clnt.opts.Rootdir + path)
}

func (clnt *Client) RemoveAll(path string) error {
fmt.Fprintf(os.Stderr, "@@@ RemoveAll %q\n", path)
	return gfRemoveOneLV(clnt.opts.Rootdir + path)
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
//fmt.Fprintf(os.Stderr, "@@@ Mkdir %q %v\n", path, mode)
	return gfMkdir(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
//fmt.Fprintf(os.Stderr, "@@@ MkdirAll %q %v\n", path, mode)
	return gfMkdirAll(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
	r, err := gfReadDir(clnt.opts.Rootdir + path)
	if err != nil { return nil, err }

//	var r []FileInfo
//	for _, s := range v {
//		r = append(r, FileInfo{s})
//	}

fmt.Fprintf(os.Stderr, "@@@ ReadDir %q => %v\n", path, r)
	return r, err
}

//fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
//	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0755)
//	if err != nil { return nil, err }
//	g := FileReadWriter{f}
//	defer g.Close()
//	return g.Readdir(0)
//fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
//fmt.Fprintf(os.Stderr, "@@@ ReadDir => %v\n", v)
//@func (f *FileReadWriter) Readdir(n int) ([]FileInfo, error) {
//@fmt.Fprintf(os.Stderr, "@@@ Readdir %v\n", n)
//@	v, err := f.f.Readdir(n)
//@//fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", v)
//@	if err != nil { return nil, err }
//@	var r []FileInfo
//@	for _, s := range v {
//@		r = append(r, FileInfo{s})
//@	}
//@fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", r)
//@	return r, err
//@}

func (fi *FileInfo) Name() string {
//fmt.Fprintf(os.Stderr, "@@@ Name: %q\n", fi.i.Name())
	return fi.i.Name()
}

func (fi *FileInfo) Size() int64 {
//fmt.Fprintf(os.Stderr, "@@@ Size: %d\n", fi.i.Size())
	return fi.i.Size()
}

func (fi *FileInfo) Mode() os.FileMode {
//fmt.Fprintf(os.Stderr, "@@@ Mode: %v\n", fi.i.Mode())
	return fi.i.Mode()
}

func (fi *FileInfo) ModTime() time.Time {
//fmt.Fprintf(os.Stderr, "@@@ ModTime: %v\n", fi.i.ModTime())
	return fi.i.ModTime()
}

func (fi *FileInfo) IsDir() bool {
//fmt.Fprintf(os.Stderr, "@@@ IsDir: %v\n", fi.i.IsDir())
	return fi.i.IsDir()
}

func (fi *FileInfo) AccessTime() time.Time {
//fmt.Fprintf(os.Stderr, "@@@ AccessTime: %v\n", fi.i.ModTime())
	return fi.i.ModTime()
}

func (clnt *Client) StatFs() (FsInfo, error) {
//fmt.Fprintf(os.Stderr, "@@@ StatFs %q\n")
	return FsInfo{0}, nil
}

/* ここまで gfarmばん */

/******************************************************************/

/* ここから、osのかわりのgfぱっけーじ */

type gfFile struct {
	gf C.GFS_File
	path string
}

func (g *gfFile) String() string {
	return fmt.Sprintf("#{gfFile %p %q}", g.gf, g.path)
}

type gfFileInfo struct {
	name string
	size int64
	mode os.FileMode
	modTime time.Time
	isDir bool
}

type gfError struct {
	code int
}

func (e gfError) Error() string {
//	return C.GoString(C.gfarm_error_string(C.int(e.code)))
	errmsg := C.gfarm_error_string(C.int(e.code))
	//defer C.free(unsafe.Pointer(errmsg))
	return fmt.Sprintf("%s", C.GoString(errmsg))
}

func gfCheckError(code C.int) error {
	if code != C.GFARM_ERR_NO_ERROR {
		return &gfError{int(code)}
	}
	return nil
}

func gfarm_initialize() error {
fmt.Fprintf(os.Stderr, "@@@ gfarm_initialize\n")
defer fmt.Fprintf(os.Stderr, "@@@ gfarm_initialize EXIT\n")
	err := gfCheckError(C.gfarm_initialize((*C.int)(C.NULL), (***C.char)(C.NULL)))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfarm_initialize ERROR: %v\n", err)
		return err
	}
	return nil
}

func gfarm_terminate() error {
fmt.Fprintf(os.Stderr, "@@@ gfarm_terminate\n")
defer fmt.Fprintf(os.Stderr, "@@@ gfarm_terminate EXIT\n")
	err := gfCheckError(C.gfarm_terminate())
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfarm_terminate ERROR: %v\n", err)
		return err
	}
	return nil
}

func gfStat(path string) (gfFileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ gfStat: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfStat EXIT: %q\n", path)
	var r gfFileInfo
	var sb C.struct_gfs_stat

	cpath := C.CString(path)
	//defer C.free(unsafe.Pointer(cpath))
	err := gfCheckError(C.gfs_stat(cpath, (*C.struct_gfs_stat)(unsafe.Pointer(&sb))))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfStat ERROR: %q %v\n", path, err)
		return gfFileInfo{}, err
	}
	//defer C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))

	r = gfFileInfo{path, int64(sb.st_size), os.FileMode(sb.st_mode), time.Unix(int64(sb.st_mtimespec.tv_sec), int64(sb.st_mtimespec.tv_nsec)), C.gfarm_s_isdir(C.gfarm_mode_t(sb.st_mode)) != C.int(0)}

//@	r.name = path
//@	r.size = int64(sb.st_size)
//@	r.mode = os.FileMode(sb.st_mode)
//@	r.modTime = time.Unix(int64(sb.st_mtimespec.tv_sec), int64(sb.st_mtimespec.tv_nsec))
//@	r.isDir = C.gfarm_s_isdir((C.gfarm_mode_t)(sb.st_mode)) != C.int(0)
//

	fmt.Fprintf(os.Stderr, "@@@ gfStat: %q => %s\n", path, r.String())
	return r, nil
}

func gfOpenFile(path string) (*gfFile, error) {
fmt.Fprintf(os.Stderr, "@@@ gfOpenFile: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfOpenFile EXIT: %q\n", path)
	var gf C.GFS_File
	gfarm_url := C.CString(path)
	//defer C.free(unsafe.Pointer(gfarm_url))
	err := gfCheckError(C.gfs_pio_open(gfarm_url, C.GFARM_FILE_RDONLY, (*C.GFS_File)(unsafe.Pointer(&gf))))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfOpenFile ERROR: %q %v\n", path, err)
		return nil, err
	}
	r := &gfFile{gf, path}
fmt.Fprintf(os.Stderr, "@@@ gfOpenFile: return %v\n", r)
	return r, nil
}

func gfCreateFile(path string, mode os.FileMode) (*gfFile, error) {
fmt.Fprintf(os.Stderr, "@@@ gfCreateFile: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfCreateFile EXIT: %q\n", path)
	var gf C.GFS_File
	gfarm_url := C.CString(path)
	//defer C.free(unsafe.Pointer(gfarm_url))
	err := gfCheckError(C.gfs_pio_create(gfarm_url, C.GFARM_FILE_WRONLY | C.GFARM_FILE_TRUNC, C.gfarm_mode_t(mode), (*C.GFS_File)(unsafe.Pointer(&gf))))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfCreateFile ERROR: %q %v\n", path, err)
		return nil, err
	}
	r := &gfFile{gf, path}
fmt.Fprintf(os.Stderr, "@@@ gfCreateFile: return %v\n", r)
	return r, nil
}

func (f *gfFile) gfClose() error {
fmt.Fprintf(os.Stderr, "@@@ gfCloseFile %v\n", f)
	err := gfCheckError(C.gfs_pio_close(f.gf))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfClose ERROR: %q %v\n", f.path, err)
		return err
	}
	return nil
}

func (f *gfFile) gfReadAt(b []byte, off int64) (int, error) {
fmt.Fprintf(os.Stderr, "@@@ gfReadAt %v\n", f)
	var n C.int
	err := gfCheckError(C.gfs_pio_pread(f.gf, unsafe.Pointer(&b[0]), C.int(len(b)), C.long(off), (*C.int)(unsafe.Pointer(&n))))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadAt ERROR: %q %d %v\n", f.path, off, err)
		return 0, err
	}
	return int(n), nil
}

func (f *gfFile) gfRead(b []byte) (int, error) {
fmt.Fprintf(os.Stderr, "@@@ gfRead %v\n", f)
	var n C.int
	err := gfCheckError(C.gfs_pio_read(f.gf, unsafe.Pointer(&b[0]), C.int(len(b)), (*C.int)(unsafe.Pointer(&n))))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRead ERROR: %q %v\n", f.path, err)
		return 0, err
	}
	return int(n), nil
}

func (f *gfFile) gfWrite(b []byte) (int, error) {
fmt.Fprintf(os.Stderr, "@@@ gfWrite %v\n", f)
	var n C.int
fmt.Fprintf(os.Stderr, "@@@ gfWrite gfs_pio_write f=%v b=%v len=%v &n=%v\n", f, unsafe.Pointer(&b[0]), C.int(len(b)), (*C.int)(unsafe.Pointer(&n)))
	err := gfCheckError(C.gfs_pio_write(f.gf, unsafe.Pointer(&b[0]), C.int(len(b)), (*C.int)(unsafe.Pointer(&n))))
	if err != nil { 
fmt.Fprintf(os.Stderr, "@@@ gfWrite ERROR: %q %v\n", f.path, err)
		return 0, err
	}
	return int(n), nil
}

func gfRename(from, to string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRename: %q %q\n", from, to)
defer fmt.Fprintf(os.Stderr, "@@@ gfRename EXIT: %q %q\n", from, to)
	src := C.CString(from)
	//defer C.free(unsafe.Pointer(src))
	dst := C.CString(to)
	//defer C.free(unsafe.Pointer(dst))
	err := gfCheckError(C.gfs_rename(src, dst))
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfRename ERROR: %q %q %v\n", from, to, err)
		return err
	}
	return nil
}

func gfRemove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRemove: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfRemove EXIT: %q\n", path)
	return nil
}

func gfRemoveOneLV(path string) error {
fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfRemoveOneLV EXIT: %q\n", path)
	return nil
}

func gfMkdir(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ gfMkdir: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfMkdir EXIT: %q\n", path)
	cpath := C.CString(path)
//	defer C.free(unsafe.Pointer(cpath))
	err := gfCheckError(C.gfs_mkdir(cpath, C.gfarm_mode_t(mode)))
	if err != nil {
		// return err
fmt.Fprintf(os.Stderr, "@@@ gfMkdir ERROR(ignored): %q %v\n", path, err)
		return nil
	}
	return nil
}

func gfMkdirAll(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll EXIT: %q\n", path)
	cpath := C.CString(path)
//	defer C.free(unsafe.Pointer(cpath))
	err := gf_mkdir_p(cpath, mode, 0)
	//if err != nil { return &gfError{(int)(e)} }
	if err != nil {
		// return err
fmt.Fprintf(os.Stderr, "@@@ gfMkdirAll ERROR(ignored): %q, %v\n", path, err)
		return nil
	}
	return nil
}

func gf_mkdir_p(cpath *C.char, mode os.FileMode, lv int) error {
fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p: %d %q\n", lv, C.GoString(cpath))
defer fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p EXIT: %d %q\n", lv, C.GoString(cpath))
	var sb C.struct_gfs_stat
	//parent := C.gfarm_url_dir((*C.char)(cpath))
	parent := C.gfarm_url_dir(cpath)
//	defer C.free(unsafe.Pointer(&parent))

fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p: parent = %q\n", C.GoString(parent))
//	fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ 0\n")
	e := C.gfs_stat(parent, (*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
	err := gfCheckError(e)
//fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ A\n")
//fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ B\n")
	//if e == C.GFARM_ERR_NO_ERROR {
	if err == nil {
//		defer C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
//fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ C\n")
		if C.gfarm_s_isdir(C.gfarm_mode_t(sb.st_mode)) != C.int(0) {
			fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p PARENT OK: %q: already exists\n", C.GoString(parent))
			//XXX return nil
			//FALLTHRU
		} else {
			fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p FAIL: parent is not a directory %q: not a directory\n", C.GoString(parent))
			return &gfError{C.GFARM_ERR_NOT_A_DIRECTORY}
		}
	//} else if e == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
	} else if e == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
//fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ D\n")
		err = gf_mkdir_p(parent, mode, lv + 1)
		if err != nil {
//			fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gf_mkdir_p: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
			return err
		}
	}
//fmt.Fprintf(os.Stderr, "@@@: gfs_mkdir: @@@ @@@ @@@ DDD %q\n", C.GoString(parent))
fmt.Fprintf(os.Stderr, "@@@ gfs_mkdir: %q\n", C.GoString(cpath))
	err = gfCheckError(C.gfs_mkdir(cpath, C.gfarm_mode_t(mode)))
	if err != nil {
		//fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gf_mkdir_p: %q %s\n", C.GoString(cpath), C.gfarm_error_string(C.int(e)))
		fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p FAIL: %q gfs_mkdir failed\n", C.GoString(cpath))
		return err
	}
	fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p SUCCESS: %q gfs_mkdir succeeded\n", C.GoString(cpath))
	return nil
//fmt.Fprintf(os.Stderr, "@@@: gf_mkdir_p: @@@ @@@ @@@ E\n")

	//fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gf_mkdir_p: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
	//fmt.Fprintf(os.Stderr, "@@@ gf_mkdir_p FAIL: %q UNKNOWN ERROR\n", C.GoString(parent))
	//return &gfError{1}
}

func gfReadDir(path string) ([]FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ gfReadir: %q\n", path)
defer fmt.Fprintf(os.Stderr, "@@@ gfReadir EXIT: %q\n", path)
	var d C.GFS_Dir
	var entry *C.struct_gfs_dirent
	var r []FileInfo
	cpath := C.CString(path)
        err := gfCheckError(C.gfs_opendir_caching(cpath, (*C.GFS_Dir)(unsafe.Pointer(&d))))
        if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: %q %v\n", path, err)
		return nil, err
	}

	for {
		err = gfCheckError(C.gfs_readdir(d, (**C.struct_gfs_dirent)(unsafe.Pointer(&entry))))
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: %q %v\n", path, err)
			return nil, err
		}
		if entry == (*C.struct_gfs_dirent)(C.NULL) { break }
		basename := C.GoString((*C.char)(&entry.d_name[0]))
		if basename == "." || basename == ".." {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir skip: %q\n", basename)
			continue
		}
		sb, err := gfStat(path + "/" + basename)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: STAT %q %v\n", path + "/" + basename, err)
			return nil, err
		}
		rr := gfFileInfo{basename, sb.Size(), sb.Mode(), sb.ModTime(), sb.IsDir()}
fmt.Fprintf(os.Stderr, "@@@ gfReadDir rr = %v\n", rr)
		r = append(r, FileInfo{rr})
        }
        err = gfCheckError(C.gfs_closedir(d))
        if err != nil {
fmt.Fprintf(os.Stderr, "@@@ gfReadDir ERROR: %q %v\n", path, err)
		return nil, err
	}
	return r, nil
}

func (r gfFileInfo) Name() string {
	return r.name
}

func (r gfFileInfo) Size() int64 {
	return r.size
}

func (r gfFileInfo) Mode() os.FileMode {
	return r.mode
}

func (r gfFileInfo) ModTime() time.Time {
	return r.modTime
}

func (r gfFileInfo) IsDir() bool {
	return r.isDir
}

func (r gfFileInfo) Sys() interface{} {
	return nil
}

func (r gfFileInfo) String() string {
	return fmt.Sprintf("#{gfFileInfo name: %q  size: %d  mode: %v modTime: %v  isDir: %v}", r.name, r.size, r.mode, r.modTime, r.isDir)
}


/*
@func tst_write_to_gfarm(filePath string, reader io.Reader, buf []byte, fallocSize int64) (int64, error) {
@fmt.Fprintf(os.Stderr, "@@@: TST_WRITE_TO_GFARM => gfreg_d => gfreg_main\n")
@
@	c := make(chan int)
@
@	r0, writer, err := os.Pipe()
@	if err != nil { panic(err) }
@
@	go func() {
@		gfreg(filePath, r0.Fd())
@		r0.Close()
@		c<- 0
@	} ()
@
@	_, err = io.CopyBuffer(writer, io.LimitReader(reader, fallocSize), buf)
@
@	writer.Close()
@
@	r := <-c
@	if r != 0 { panic("panic") }
@
@	return fallocSize, nil
@}
@
@type gfarmController struct {
@	p *os.File
@	c chan int
@}
@
@func (g gfarmController) Close() error {
@fmt.Fprintf(os.Stderr, "@@@: gfarm.Close CALLED\n")
@	r := <-g.c
@	if r != 0 {
@		panic("!chan")
@	}
@	return g.p.Close()
@}
@
@func (g gfarmController) Read(b []byte) (n int, err error) {
@fmt.Fprintf(os.Stderr, "@@@: gfarm.Reader CALLED\n")
@	return g.p.Read(b)
@}
@
@func tst_read_from_gfarm(fsObjPath string) (io.ReadCloser, int64, error) {
@fmt.Fprintf(os.Stderr, "@@@: TST_READ_FROM_GFARM == gfexport_main\n")
@	var size int64
@	var g gfarmController
@
@	fi, e := gfarmFsStatFile(fsObjPath)
@	if e != nil { return nil, 0, e }
@
@	reader, w1, err := os.Pipe()
@	if err != nil { return nil, 0, err }
@
@	size = fi.Size()
@
@	c := make(chan int)
@
@	go func() {
@		gfexport(fsObjPath, w1.Fd())
@		w1.Close()
@		c<- 0
@	} ()
@
@//XXX  Close should wait until <-c is available
@	//C.gfarm_terminate()
@
@	g.p = reader
@	g.c = c
@	return g, size, nil
@}
@
@func gfreg(path string, d uintptr) {
@	var gf C.GFS_File
@fmt.Fprintf(os.Stderr, "##################### GFREG ##################\n")
@	gfarm_url := C.CString(path)
@	defer C.free(unsafe.Pointer(gfarm_url))
@
@	flags := C.GFARM_FILE_WRONLY | C.GFARM_FILE_TRUNC
@	C.gfs_pio_create(gfarm_url, C.int(flags), 0600, (*C.GFS_File)(&gf))
@	C.gfs_pio_sendfile(gf, 0, C.int(d), 0, -1, (*C.long)(C.NULL))
@	C.gfs_pio_close(gf)
@}
@
@func gfexport(path string, d uintptr) {
@	var gf C.GFS_File
@	var sb C.struct_gfs_stat
@fmt.Fprintf(os.Stderr, "##################### GFEXPORT ##################\n")
@	gfarm_url := C.CString(path)
@	defer C.free(unsafe.Pointer(gfarm_url))
@	C.gfs_pio_open(gfarm_url, C.GFARM_FILE_RDONLY, (*C.GFS_File)(unsafe.Pointer(&gf)))
@	C.gfs_fstat(gf, (*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
@	size := sb.st_size
@	C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
@	C.gfs_pio_recvfile(gf, 0, C.int(d), 0, size, (*C.long)(C.NULL))
@	C.gfs_pio_close(gf)
@}
*/

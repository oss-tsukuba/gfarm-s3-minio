package gfarm

// #cgo CFLAGS: -g -Wall -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lgfarm -Wl,-rpath,/usr/local/lib
// #include <stdlib.h>
// #include <gfarm/gfarm.h>
// inline int gfarm_s_isdir(gfarm_mode_t m) { return GFARM_S_ISDIR(m); }
import "C"

/*
os を、gf におきかえると、さくっといれかわるようにする

	type os.File
	type os.FileInfo
	type os.FileMode
	os.Stat(string)
	os.OpenFile(string, os.O_RDONLY, mode_t)
	os.OpenFile(string, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode_t)
	os.File.Close()
	os.File.ReadAt([]byte, int64)
	os.File.Read([]byte)
	os.File.Write([]byte)
XXX os.File.Readdir(int) []os.FileInfo
	os.Rename(string, string)
	os.Remove(string)
	os.RemoveAll(string)
	os.Mkdir(string, mode_t)
	os.MkdirAll(string, mode_t)
	ioutil.ReadDir(string) []os.FileInfo

	os.FileInfo.Name()
	os.FileInfo.Size()
	os.FileInfo.Mode()
	os.FileInfo.ModTime()
	os.FileInfo.IsDir()
	os.FileInfo.AccessTime()
*/

import (
	"os"
	"time"
	"fmt"
	"io/ioutil"
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
	f *os.File
}

type FileInfo struct {
	i os.FileInfo
}

type FsInfo struct {
	Used uint64
}

type GfarmError struct {
	code int
}

func ClientOptionsFromConf() ClientOptions {
	return ClientOptions{"", "/mnt/data/nas1"}
}

func (e *GfarmError) Error() string {
	return fmt.Sprintf("error code: %d", e.code)
}

func NewClient(o ClientOptions) (*Client, error) {
//fmt.Fprintf(os.Stderr, "@@@ NewClient %v\n", o)
	var c *Client
//fmt.Fprintf(os.Stderr, "@@@ gfarm_initialize()\n")
	err := gfarm_initialize()
	if err != nil {
//fmt.Fprintf(os.Stderr, "@@@ gfarm_initialize() => %v\n", err)
		return nil, err
	}
	c = &Client{o}
	return c, nil
}

func (clnt *Client) Close() error {
//fmt.Fprintf(os.Stderr, "@@@ Close\n")
//fmt.Fprintf(os.Stderr, "@@@ gfarm_terminate()\n")
	gfarm_terminate()
	return nil
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
//fmt.Fprintf(os.Stderr, "@@@ Stat %q\n", path)
	v, err := os.Stat(clnt.opts.Rootdir + path)
//fmt.Fprintf(os.Stderr, "@@@ Stat %v\n", v)
	if err != nil { return FileInfo{}, err }
	return FileInfo{v}, err
}

func (clnt *Client) Open(path string) (*FileReadWriter, error) {
//fmt.Fprintf(os.Stderr, "@@@ Open %q\n", path)
	var r *FileReadWriter
	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0666)
	if err != nil { return nil, err }
	r = &FileReadWriter{f}
	return r, nil
}

func (clnt *Client) Create(path string) (*FileReadWriter, error) {
//fmt.Fprintf(os.Stderr, "@@@ Create %q\n", path)
	var w *FileReadWriter
	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil { return nil, err }
	w = &FileReadWriter{f}
	return w, nil
}

func (clnt *Client) Append(path string) (*FileReadWriter, error) {
//fmt.Fprintf(os.Stderr, "@@@ Append %q\n", path)
//@	var w *FileReadWriter
	//f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_APPEND, 0666)
	w, err := clnt.Create(path)
//@	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
//@	if err != nil { return nil, err }
//@	w = &FileReadWriter{f}
//fmt.Fprintf(os.Stderr, "@@@ Append => %v\n", w)
	return w, err
}

func (f *FileReadWriter) Close() (error) {
//fmt.Fprintf(os.Stderr, "@@@ Close\n")
//fmt.Fprintf(os.Stderr, "@@@ Close => %v\n", f)
	return f.f.Close()
}

/*
@func (clnt *Client) CreateEmptyFile(path string) error {
@fmt.Fprintf(os.Stderr, "@@@ CreateEmptyFile %q\n", path)
@	f, err := clnt.Create(path)
@	if err != nil { return err }
@	f.Close()
@	return nil
@}
*/

func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
	n, e := r.f.ReadAt(p, off)
//fmt.Fprintf(os.Stderr, "@@@ ReadAt(%d) => %d %v  ", off, n, e)
	return n, e
}

func (r *FileReadWriter) Read(p []byte) (int, error) {
	n, e := r.f.Read(p)
//fmt.Fprintf(os.Stderr, "@@@ Read() => %d %v  ", n, e)
	return n, e
}

func (w *FileReadWriter) Write(p []byte) (int, error) {
	n, e := w.f.Write(p)
//fmt.Fprintf(os.Stderr, "@@@ Write => %d %v  ", n, e)
	return n, e
}

func (clnt *Client) Rename(from, to string) error {
//fmt.Fprintf(os.Stderr, "@@@ Rename %q %q\n", from, to)
	return os.Rename(clnt.opts.Rootdir + from, clnt.opts.Rootdir + to)
	return nil
}

func (clnt *Client) Remove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ Remove %q\n", path)
	return os.Remove(clnt.opts.Rootdir + path)
}

func (clnt *Client) RemoveAll(path string) error {
fmt.Fprintf(os.Stderr, "@@@ RemoveAll %q\n", path)
	return os.RemoveAll(clnt.opts.Rootdir + path)
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
//fmt.Fprintf(os.Stderr, "@@@ Mkdir %q %v\n", path, mode)
	return os.Mkdir(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
//fmt.Fprintf(os.Stderr, "@@@ MkdirAll %q %v\n", path, mode)
	return os.MkdirAll(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
//	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0755)
//	if err != nil { return nil, err }
//	g := FileReadWriter{f}
//	defer g.Close()
//	return g.Readdir(0)
fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", path)
	v, err := ioutil.ReadDir(path)
fmt.Fprintf(os.Stderr, "@@@ ReadDir => %v\n", v)
	if err != nil { return nil, err }
	var r []FileInfo
	for _, s := range v {
		r = append(r, FileInfo{s})
	}
fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", r)
	return r, err
}

func (f *FileReadWriter) Readdir(n int) ([]FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ Readdir %v\n", n)
	v, err := f.f.Readdir(n)
fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", v)
	if err != nil { return nil, err }
	var r []FileInfo
	for _, s := range v {
		r = append(r, FileInfo{s})
	}
fmt.Fprintf(os.Stderr, "@@@ Readdir => %v\n", r)
	return r, err
}

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


/*
func tst_write_to_gfarm(filePath string, reader io.Reader, buf []byte, fallocSize int64) (int64, error) {
fmt.Fprintf(os.Stderr, "@@@: TST_WRITE_TO_GFARM => gfreg_d => gfreg_main\n")

	c := make(chan int)

	r0, writer, err := os.Pipe()
	if err != nil { panic(err) }

	go func() {
		gfreg(filePath, r0.Fd())
		r0.Close()
		c<- 0
	} ()

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, fallocSize), buf)

	writer.Close()

	r := <-c
	if r != 0 { panic("panic") }

	return fallocSize, nil
}

type gfarmController struct {
	p *os.File
	c chan int
}

func (g gfarmController) Close() error {
fmt.Fprintf(os.Stderr, "@@@: gfarm.Close CALLED\n")
	r := <-g.c
	if r != 0 {
		panic("!chan")
	}
	return g.p.Close()
}

func (g gfarmController) Read(b []byte) (n int, err error) {
fmt.Fprintf(os.Stderr, "@@@: gfarm.Reader CALLED\n")
	return g.p.Read(b)
}

func tst_read_from_gfarm(fsObjPath string) (io.ReadCloser, int64, error) {
fmt.Fprintf(os.Stderr, "@@@: TST_READ_FROM_GFARM == gfexport_main\n")
	var size int64
	var g gfarmController

	fi, e := gfarmFsStatFile(fsObjPath)
	if e != nil { return nil, 0, e }

	reader, w1, err := os.Pipe()
	if err != nil { return nil, 0, err }

	size = fi.Size()

	c := make(chan int)

	go func() {
		gfexport(fsObjPath, w1.Fd())
		w1.Close()
		c<- 0
	} ()

//XXX  Close should wait until <-c is available
	//C.gfarm_terminate()

	g.p = reader
	g.c = c
	return g, size, nil
}

func gfreg(path string, d uintptr) {
	var gf C.GFS_File
fmt.Fprintf(os.Stderr, "##################### GFREG ##################\n")
	gfarm_url := C.CString(path)
	defer C.free(unsafe.Pointer(gfarm_url))

	flags := C.GFARM_FILE_WRONLY | C.GFARM_FILE_TRUNC
	C.gfs_pio_create(gfarm_url, C.int(flags), 0600, (*C.GFS_File)(&gf))
	C.gfs_pio_sendfile(gf, 0, C.int(d), 0, -1, (*C.long)(C.NULL))
	C.gfs_pio_close(gf)
}

func gfexport(path string, d uintptr) {
	var gf C.GFS_File
	var sb C.struct_gfs_stat
fmt.Fprintf(os.Stderr, "##################### GFEXPORT ##################\n")
	gfarm_url := C.CString(path)
	defer C.free(unsafe.Pointer(gfarm_url))
	C.gfs_pio_open(gfarm_url, C.GFARM_FILE_RDONLY, (*C.GFS_File)(unsafe.Pointer(&gf)))
	C.gfs_fstat(gf, (*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
	size := sb.st_size
	C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
	C.gfs_pio_recvfile(gf, 0, C.int(d), 0, size, (*C.long)(C.NULL))
	C.gfs_pio_close(gf)
}
*/

func gfarm_initialize() error {
	e := C.gfarm_initialize((*C.int)(C.NULL), (***C.char)(C.NULL))
	if e != C.GFARM_ERR_NO_ERROR {
		return &GfarmError{1}
		fmt.Fprintf(os.Stderr, "%s\n", C.gfarm_error_string(C.int(e)))
	}
	return nil
}

func gfarm_terminate() {
	e := C.gfarm_terminate()
	if e != C.GFARM_ERR_NO_ERROR {
		fmt.Fprintf(os.Stderr, "gfarm_terminate(): %s\n", C.gfarm_error_string(e))
	}
}

func gfarm_mkdir_p(path string, mode int) bool {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return gfarm_mkdir_pc(cpath, mode)
}

func gfarm_mkdir_pc(cpath *C.char, mode int) bool {
	var sb C.struct_gfs_stat
	parent := C.gfarm_url_dir((*C.char)(cpath))
//	defer C.free(unsafe.Pointer(&parent))

fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ %q => %q\n", C.GoString(cpath), C.GoString(parent))

fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ 0\n")
	e := C.gfs_stat(parent, (*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ A\n")
fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ B\n")
	if e == C.GFARM_ERR_NO_ERROR {
		defer C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ C\n")
		if C.gfarm_s_isdir((C.gfarm_mode_t)(sb.st_mode)) != C.int(0) {
			fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ %q: exists\n", C.GoString(parent))
			return true
		} else {
			fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gfarm_mkdir_pc: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
			return false
		}
	} else if e == C.GFARM_ERR_NO_SUCH_FILE_OR_DIRECTORY {
fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ D\n")
		if !gfarm_mkdir_pc(parent, mode) {
			fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gfarm_mkdir_pc: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
			return false
		}
fmt.Fprintf(os.Stderr, "@@@: gfs_mkdir: @@@ @@@ @@@ DDD %s\n", C.GoString(parent))
		e = C.gfs_mkdir((*C.char)(parent), (C.gfarm_mode_t)(mode))
		if e != C.GFARM_ERR_NO_ERROR {
			fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gfarm_mkdir_pc: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
			return false
		}
		return true
	}
fmt.Fprintf(os.Stderr, "@@@: gfarm_mkdir_pc: @@@ @@@ @@@ E\n")

	fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gfarm_mkdir_pc: %q %s\n", C.GoString(parent), C.gfarm_error_string(C.int(e)))
	return false
}

func gfarmFsStatFile(path string) (os.FileInfo, error) {
	var r gfsFileInfo
	var sb C.struct_gfs_stat
	var sbp *C.struct_gfs_stat

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	sbp = (*C.struct_gfs_stat)(unsafe.Pointer(&sb))
	//e := C.gfs_stat(cpath, (*C.struct_gfs_stat)(unsafe.Pointer(&sb)))
	e := C.gfs_stat(cpath, sbp)
	if e != C.GFARM_ERR_NO_ERROR {
		errmsg := C.gfarm_error_string(C.int(e))
//		defer C.free(unsafe.Pointer(errmsg))
		r.e = C.GoString(errmsg)
		return nil, r
	}
	defer C.gfs_stat_free((*C.struct_gfs_stat)(unsafe.Pointer(&sb)))

	r.name = path
	r.size = int64(sb.st_size)
	r.mode = os.FileMode(sb.st_mode)
	r.modTime = time.Unix(int64(sb.st_mtimespec.tv_sec), int64(sb.st_mtimespec.tv_nsec))
	r.isDir = C.gfarm_s_isdir((C.gfarm_mode_t)(sb.st_mode)) != C.int(0)

	fmt.Fprintf(os.Stderr, "@@@ @@@ @@@ gfarmFsStatFile: %s => %s\n", path, r.String())
	return r, nil
}

type gfsFileInfo struct {
	name string
	size int64
	mode os.FileMode
	modTime time.Time
	isDir bool
	e string
}

func (r gfsFileInfo) Error() string {
	return r.e
}

func (r gfsFileInfo) Name() string {
fmt.Fprintf(os.Stderr, "Name: %q", r.name)
	return r.name
}

func (r gfsFileInfo) Size() int64 {
fmt.Fprintf(os.Stderr, "Size: %d", r.size)
	return r.size
}

func (r gfsFileInfo) Mode() os.FileMode {
fmt.Fprintf(os.Stderr, "Mode: %v", r.mode)
	return r.mode
}

func (r gfsFileInfo) ModTime() time.Time {
fmt.Fprintf(os.Stderr, "ModTime: %v", r.modTime)
	return r.modTime
}

func (r gfsFileInfo) IsDir() bool {
fmt.Fprintf(os.Stderr, "isDir: %v", r.isDir)
	return r.isDir
}

func (r gfsFileInfo) Sys() interface{} {
	return nil
}

func (r gfsFileInfo) String() string {
	return fmt.Sprintf("<gfsFileInfo name: %q  size: %d  mode: %v modTime: %v  isDir: %v>", r.name, r.size, r.mode, r.modTime, r.isDir)
}

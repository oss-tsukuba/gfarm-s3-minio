package gfarm

import (
	"os"
	"time"
	"fmt"
)

type Client struct {
	opts ClientOptions
}

type FileReadWriter struct {
	f *os.File
}

type FsInfo struct {
	Used uint64
}

type FileInfo struct {
	i os.FileInfo
}

type ClientOptions struct {
	User string
	Rootdir string
}

func ClientOptionsFromConf() ClientOptions {
	return ClientOptions{"", "/mnt/data/nas1"}
}

func NewClient(o ClientOptions) (*Client, error) {
fmt.Fprintf(os.Stderr, "@@@ NewClient %v\n", o)
	var c *Client
	c = &Client{o}
	return c, nil
}

func (r *FileReadWriter) ReadAt(p []byte, off int64) (int, error) {
fmt.Fprintf(os.Stderr, "@@@ ReadAt %v %v\n", p, off)
	return r.f.ReadAt(p, off)
}

func (w *FileReadWriter) Write(p []byte) (int, error) {
fmt.Fprintf(os.Stderr, "@@@ Write %v\n", p)
	return w.f.Write(p)
}

func (clnt *Client) Rename(from, to string) error {
fmt.Fprintf(os.Stderr, "@@@ Rename %q %q\n", from, to)
	return os.Rename(clnt.opts.Rootdir + from, clnt.opts.Rootdir + to)
	return nil
}

func (clnt *Client) Remove(path string) error {
fmt.Fprintf(os.Stderr, "@@@ Remove %q\n", path)
	return os.Remove(clnt.opts.Rootdir + path)
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ Mkdir %q %v\n", path, mode)
	return os.Mkdir(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
fmt.Fprintf(os.Stderr, "@@@ MkdirAll %q %v\n", path, mode)
	return os.MkdirAll(clnt.opts.Rootdir + path, mode)
}

func (clnt *Client) ReadDir(sep string) ([]FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ ReadDir %q\n", sep)
	f, err := os.OpenFile(clnt.opts.Rootdir, os.O_RDONLY, 0755)
	if err != nil { return nil, err }
	g := FileReadWriter{f}
	defer g.Close()
	return g.Readdir(0)
}

func (clnt *Client) Open(path string) (*FileReadWriter, error) {
fmt.Fprintf(os.Stderr, "@@@ Open %q\n", path)
	var r *FileReadWriter
	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDONLY, 0755)
	if err != nil { return nil, err }
	r = &FileReadWriter{f}
	return r, nil
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

func (f *FileReadWriter) Close() (error) {
fmt.Fprintf(os.Stderr, "@@@ Close\n")
	return f.f.Close()
}

func (clnt *Client) Create(path string) (*FileReadWriter, error) {
fmt.Fprintf(os.Stderr, "@@@ Create %q\n", path)
	var w *FileReadWriter
	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil { return nil, err }
	w = &FileReadWriter{f}
	return w, nil
}

func (clnt *Client) Close() error {
fmt.Fprintf(os.Stderr, "@@@ Close\n")
	return nil
}

func (clnt *Client) CreateEmptyFile(path string) error {
fmt.Fprintf(os.Stderr, "@@@ CreateEmptyFile %q\n", path)
	f, err := clnt.Create(path)
	if err != nil { return err }
	f.Close()
	return nil
}

func (clnt *Client) Append(path string) (*FileReadWriter, error) {
fmt.Fprintf(os.Stderr, "@@@ Append %q\n", path)
	var w *FileReadWriter
	f, err := os.OpenFile(clnt.opts.Rootdir + path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil { return nil, err }
	w = &FileReadWriter{f}
	return w, nil
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ Stat %q\n", path)
	v, err := os.Stat(clnt.opts.Rootdir + path)
fmt.Fprintf(os.Stderr, "@@@ Stat %v\n", v)
	if err != nil { return FileInfo{}, err }
	return FileInfo{v}, err
}

func (clnt *Client) StatFs() (FsInfo, error) {
fmt.Fprintf(os.Stderr, "@@@ StatFs %q\n")
	return FsInfo{0}, nil
}

func (fi *FileInfo) Name() string {
fmt.Fprintf(os.Stderr, "@@@ Name\n")
	return fi.i.Name()
}

func (fi *FileInfo) Size() int64 {
fmt.Fprintf(os.Stderr, "@@@ Size\n")
	return fi.i.Size()
}

func (fi *FileInfo) Mode() os.FileMode {
fmt.Fprintf(os.Stderr, "@@@ Size\n")
	return fi.i.Mode()
}

func (fi *FileInfo) ModTime() time.Time {
fmt.Fprintf(os.Stderr, "@@@ ModTime\n")
	return fi.i.ModTime()
}

func (fi *FileInfo) IsDir() bool {
fmt.Fprintf(os.Stderr, "@@@ IsDir\n")
	return fi.i.IsDir()
}

func (fi *FileInfo) AccessTime() time.Time {
fmt.Fprintf(os.Stderr, "@@@ AccessTime\n")
	return fi.i.ModTime()
}

package gfarm

import (
	"io"
//	"io/ioutil"
	"os"
	pathutil "path"
	"time"
	gf "github.com/minio/minio/cmd/gateway/gfarm/gf"
)

type Client struct {
	opts ClientOptions
}

type ClientOptions struct {
	User string
	Rootdir string
}

type FileReadAtWriter interface {
	Close() error
	ReadAt(b []byte, off int64) (int, error)
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
}

type FileReadWriter interface {
	Close() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
}

type FileReadWriterWithLocalCache struct {
	f, g FileReadWriter
}

type FileInfo interface {
	Name() string
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
}

type FsInfo struct {
	Used uint64
} 

/* ここから gfarmばん */

func ClientOptionsFromConf() ClientOptions {
	return ClientOptions{"", "/home/hp120273/hpci005858/tmp/nas1"}
// XXX コマンドライン引数から 受け取る
}

func NewClient(o ClientOptions) (*Client, error) {
	err := gf.Gfarm_initialize()
	if err != nil {
		return nil, err
	}
	return &Client{o}, nil
}

func (clnt *Client) Close() error {
	return gf.Gfarm_terminate()
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
	return gf.Stat(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) Open(path string) (FileReadAtWriter, error) {
	return gf.OpenFile(pathutil.Join(clnt.opts.Rootdir, path), os.O_RDONLY, 0666)
}

func (clnt *Client) Create(path string) (FileReadAtWriter, error) {
	return gf.OpenFile(pathutil.Join(clnt.opts.Rootdir, path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (clnt *Client) OpenWithLocalCache(dirname, cacheDirname, path string) (FileReadWriter, error) {
	f, err := gf.OpenFile(pathutil.Join(clnt.opts.Rootdir, pathutil.Join(dirname, path)), os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
// ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく
	return &FileReadWriterWithLocalCache{f, nil}, nil
}

func (clnt *Client) CreateWithLocalCache(dirname, cacheDirname, path string) (FileReadWriter, error) {
	f, err := gf.OpenFile(pathutil.Join(clnt.opts.Rootdir, pathutil.Join(dirname, path)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
// ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく
	return &FileReadWriterWithLocalCache{f, nil}, nil
}

func (f *FileReadWriterWithLocalCache) Close() error {
	var gerr error
	gerr = nil
	if f.g != nil {
		gerr = f.g.Close()
	}
	ferr := f.f.Close()
	if gerr != nil {
		return gerr
	}
	return ferr
}

func (r *FileReadWriterWithLocalCache) Read(p []byte) (int, error) {
	if r.g != nil {
		n, err := r.g.Read(p)
		if err == io.EOF {
			err = r.g.Close()
			r.g = nil
			if err != nil {
				return 0, err
			}
			// FALLTHRU
		} else if err != nil {
			return 0, err
		} else {
			return n, nil
		}
	}
	return r.f.Read(p)
}

func (w *FileReadWriterWithLocalCache) Write(p []byte) (int, error) {
// gにかけるうちは、gにかく
	return w.f.Write(p)
}

func (clnt *Client) Rename(from, to string) error {
	return gf.Rename(pathutil.Join(clnt.opts.Rootdir, from), pathutil.Join(clnt.opts.Rootdir, to))
}

func (clnt *Client) Remove(path string) error {
	return gf.Remove(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) RemoveAll(path string) error {
	return gf.RemoveOneLV(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
	return gf.Mkdir(pathutil.Join(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
	return gf.MkdirAll(pathutil.Join(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
	v, err := gf.ReadDir(pathutil.Join(clnt.opts.Rootdir, path))
	if err != nil {
		return nil, err
	}
	var r []FileInfo
	for _, s := range v {
		r = append(r, s)
	}
	return r, err
}

func (clnt *Client) StatFs() (FsInfo, error) {
	return FsInfo{0}, nil
}

/* ここまで gfarmばん */
/* ここからファイルシステムばん *

func ClientOptionsFromConf() ClientOptions {
	return ClientOptions{"", "/mnt/data/nas1"}
// XXX コマンドライン引数から 受け取る
}

func NewClient(o ClientOptions) (*Client, error) {
	return &Client{o}, nil
}

func (clnt *Client) Close() error {
	return nil
}

func (clnt *Client) Stat(path string) (FileInfo, error) {
	return os.Stat(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) Open(path string) (FileReadAtWriter, error) {
	return os.OpenFile(pathutil.Join(clnt.opts.Rootdir, path), os.O_RDONLY, 0666)
}

func (clnt *Client) Create(path string) (FileReadAtWriter, error) {
	return os.OpenFile(pathutil.Join(clnt.opts.Rootdir, path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (clnt *Client) OpenWithLocalCache(dirname, cacheDirname, path string) (FileReadWriter, error) {
	f, err := os.OpenFile(pathutil.Join(clnt.opts.Rootdir, pathutil.Join(dirname, path)), os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
// ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく
	return &FileReadWriterWithLocalCache{f, nil}, nil
}

func (clnt *Client) CreateWithLocalCache(dirname, cacheDirname, path string) (FileReadWriter, error) {
	f, err := os.OpenFile(pathutil.Join(clnt.opts.Rootdir, pathutil.Join(dirname, path)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
// ここで、g に、ふぁいるしすてむのどういつぱすをひらいておく
	return &FileReadWriterWithLocalCache{f, nil}, nil
}

func (f *FileReadWriterWithLocalCache) Close() error {
	var gerr error
	gerr = nil
	if f.g != nil {
		gerr = f.g.Close()
	}
	ferr := f.f.Close()
	if gerr != nil {
		return gerr
	}
	return ferr
}

func (r *FileReadWriterWithLocalCache) Read(p []byte) (int, error) {
	if r.g != nil {
		n, err := r.g.Read(p)
		if err == io.EOF {
			err = r.g.Close()
			r.g = nil
			if err != nil {
				return 0, err
			}
			// FALLTHRU
		} else if err != nil {
			return 0, err
		} else {
			return n, nil
		}
	}
	return r.f.Read(p)
}

func (w *FileReadWriterWithLocalCache) Write(p []byte) (int, error) {
// gにかけるうちは、gにかく
	return w.f.Write(p)
}

func (clnt *Client) Rename(from, to string) error {
	return os.Rename(pathutil.Join(clnt.opts.Rootdir, from), pathutil.Join(clnt.opts.Rootdir, to))
}

func (clnt *Client) Remove(path string) error {
	return os.Remove(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) RemoveAll(path string) error {
	return os.RemoveAll(pathutil.Join(clnt.opts.Rootdir, path))
}

func (clnt *Client) Mkdir(path string, mode os.FileMode) error {
	return os.Mkdir(pathutil.Join(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) MkdirAll(path string, mode os.FileMode) error {
	return os.MkdirAll(pathutil.Join(clnt.opts.Rootdir, path), mode)
}

func (clnt *Client) ReadDir(path string) ([]FileInfo, error) {
	v, err := ioutil.ReadDir(pathutil.Join(clnt.opts.Rootdir, path))
	if err != nil {
		return nil, err
	}
	var r []FileInfo
	for _, s := range v {
		r = append(r, s)
	}
	return r, err
}

func (clnt *Client) StatFs() (FsInfo, error) {
	return FsInfo{0}, nil
}

* ここまでファイルシステムばん */

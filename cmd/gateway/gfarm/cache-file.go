package gfarm

import (
	"bytes"
//	"context"
	"crypto/md5"
//	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
//	"net/http"
	"os"
//	"path"
//	"strconv"
//	"sort"
//	"sync"
//	"strings"
//	"syscall"
//	"time"
//	"unsafe"
//	"github.com/minio/minio/pkg/env"

	gf "github.com/minio/minio/pkg/gfarm"
//	"github.com/minio/cli"
//	"github.com/minio/minio-go/v6/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
//	"github.com/minio/minio/cmd/logger"
//	"github.com/minio/minio/pkg/auth"
//	humanize "github.com/dustin/go-humanize"
)

func (n *gfarmObjects) createMetaTmpBucketGfarm(minioMetaTmpBucket string) error {
fmt.Fprintf(os.Stderr, "@@@ createMetaTmpBucketGfarm: %q\n", minioMetaTmpBucket)
	gfarm_url_minioMetaTmpBucket := n.gfarm_url_PathJoin(gfarmSeparator, minioMetaTmpBucket)
	if err := gf.MkdirAll(gfarm_url_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewGatewayLayer", "MkdirAll", gfarm_url_minioMetaTmpBucket, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMetaTmpBucketCache(minioMetaTmpBucket string) error {
fmt.Fprintf(os.Stderr, "@@@ createMetaTmpBucketCache: %q\n", minioMetaTmpBucket)
	if n.cachectl == nil {
		return nil
	}
	gfarm_cache_minioMetaTmpBucket := n.gfarm_cache_PathJoin(gfarmSeparator, minioMetaTmpBucket)
	if err := os.MkdirAll(gfarm_cache_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) createMultipartUploadDirGfarm(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ createMultipartUploadDirGfarm: %q\n", dirName)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	if err := gf.Mkdir(gfarm_url_dirName, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewMultipartUpload", "Mkdir", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMultipartUploadDirCache(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ createMultipartUploadDirCache: %q\n", dirName)
	if n.cachectl == nil {
		return nil
	}
	gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
	if err := os.Mkdir(gfarm_cache_dirName, os.FileMode(0755)); err != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) checkUploadIDExistsGfarm(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExistsGfarm: %q\n", dirName)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	_, err := gf.Stat(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "checkUploadIDExists", "Stat", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) checkUploadIDExistsCache(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExistsCache: %q\n", dirName)
	if n.cachectl == nil {
		return nil
	}
	gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
	_, err := os.Stat(gfarm_cache_dirName)
	if err != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) removeMultipartCacheWorkdir(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ removeMultipartCacheWorkdir: %q\n", dirName)
	if n.cachectl == nil {
		return nil
	}
	gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
	entries, err := ioutil.ReadDir(gfarm_cache_dirName)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		gfarm_cache_dirName_EntryName := n.gfarm_cache_PathJoin(dirName, entry.Name())
//		err = os.Remove(gfarm_cache_dirName_EntryName)
		err = n.removeOsFileIfExists(gfarm_cache_dirName_EntryName)
		if err != nil {
			return err
		}
	}
	if os.Remove(gfarm_cache_dirName) != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) removeMultipartGfarmWorkdir(dirName string) error {
fmt.Fprintf(os.Stderr, "@@@ removeMultipartGfarmWorkdir: %q\n", dirName)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	entries, err := gf.ReadDir(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "ReadDir", gfarm_url_dirName, err)
		return err
	}
	for _, entry := range entries {
		gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
		//err = gf.Remove(gfarm_url_dirName_entryName)
		err = n.removeGfarmFileIfExists(gfarm_url_dirName_entryName)
		if err != nil {
			gf.LogError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "Remove", gfarm_url_dirName_entryName, err)
			return err
		}
	}
	err = gf.Remove(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "Remove", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) copyToPartFileTruncateOrCreate(partName string, r *minio.PutObjReader) error {
fmt.Fprintf(os.Stderr, "@@@ copyToPartFileTruncateOrCreate: %q\n", partName)
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	n.removeGfarmFileIfExists(gfarm_url_partName)
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		n.removeOsFileIfExists(gfarm_cache_partName)
		return n.copyToCachedFile(gfarm_url_partName, gfarm_cache_partName, r)
	} else {
		return n.copyToGfarmFile(gfarm_url_partName, r)
	}
}

func (n *gfarmObjects) copyFromPartFileAppendOrCreate(w *gf.File, partName string) error {
fmt.Fprintf(os.Stderr, "@@@ copyFromPartFileAppendOrCreate: %q\n", partName)
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		return n.copyFromCachedFile(w, gfarm_url_partName, gfarm_cache_partName)
	} else {
		return n.copyFromGfarmFile(w, gfarm_url_partName)
	}
}

func (n *gfarmObjects) copyToCachedFile(gfarm_url_partName, gfarm_cache_partName string, r *minio.PutObjReader) error {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile: %q %q\n", gfarm_url_partName, gfarm_cache_partName)
	var total int64 = 0
	buf := make([]byte, 1024 * 1024 * 32)
	var len int

	c := n.cachectl

	var hash hash.Hash = nil
	if c.enable_partfile_digest {
		hash = md5.New()
	}

	w_cache, err := n.createOsFile(gfarm_cache_partName)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile A: %v\n", err)
		return err
	}
	//defer w_cache.Close()

	/* write to OS FILE */
	for {
		len, err = r.Read(buf)
		if err == io.EOF {
			// cache only!
			myAssert(len == 0, "len == 0")
			if err := w_cache.Close(); err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile B: %v\n", err)
				return err
			}
			break
		} else if err != nil {
			// fatal error!
			w_cache.Close()
			c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile C: %v\n", err)
			return err
		}
		myAssert(len != 0, "len != 0")

		if c.fasterLimit < c.fasterTotal + int64(len) {
			// cache full => switch to gfarm
			if err := w_cache.Close(); err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile D: %v\n", err)
				return err
			}
			break
		}

		n, err := myWrite(w_cache, hash, buf[:len])
		if n != len {
			// partial write to os file => switch to gfarm
			if err := w_cache.Close(); err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile E: %v\n", err)
				return err
			}
			break
		} /* else */
		if err != nil {
			// write failed to os file => switch to gfarm
			if err := w_cache.Close(); err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile F: %v\n", err)
				return err
			}
			break
		}

		c.updateCacheUsage(int64(len))

		total += int64(len)
	}

	n.registerFileSize(gfarm_cache_partName, total)

	if len != 0 {
		w_gfarm, err := n.createGfarmFile(gfarm_url_partName)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile G: %v\n", err)
			return err
		}
		//defer w_gfarm.Close()

		for {
			wrote_bytes, err := myWrite(w_gfarm, hash, buf[:len])
			if err != nil {
				w_gfarm.Close()
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile H: %v\n", err)
				return err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")

			len, err = r.Read(buf)
			if err == io.EOF {
				break
			} else if err != nil {
				// fatal error!
				w_gfarm.Close()
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile I: %v\n", err)
				return err
			}
			myAssert(len != 0, "len != 0")

		}

		if err := w_gfarm.Close(); err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile J: %v\n", err)
			return err
		}
	}

	if hash != nil {
		n.registerHashValue(gfarm_url_partName, hash)
	}

	return nil
}

func (n *gfarmObjects) copyFromCachedFile(w *gf.File, gfarm_url_partName, gfarm_cache_partName string) error {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile: %q %q\n", gfarm_url_partName, gfarm_cache_partName)
	c := n.cachectl

	buf := make([]byte, 1024 * 1024 * 32)

	r_cache, err := n.openOsFile(gfarm_cache_partName)
	if err != nil {
		return err
	}
	defer r_cache.Close()

	var hash hash.Hash = nil
	if c.enable_partfile_digest {
		hash = md5.New()
	}

	_, err = gf.Stat(gfarm_url_partName)
	if err == nil {
		r_gfarm, err := n.openGfarmFile(gfarm_url_partName)
		if err != nil {
			return err
		}

		defer r_gfarm.Close()

		cacheFileSize := n.retrieveFileSize(gfarm_cache_partName)

		total, err := myCopyHashUpto(w, r_cache, hash, cacheFileSize, buf)

		if total != cacheFileSize {
//XXX
			return err
		}

		_, err = myCopyHash(w, r_gfarm, hash, buf)
		if err != nil {
			return err
		}

	} else {
		_, err = myCopyHash(w, r_cache, hash, buf)
		if err != nil {
			return err
		}
	}

	if hash != nil {
		hb := hash.Sum(nil)
		ha, _ := n.retrieveHashValue(gfarm_url_partName)

		if bytes.Compare(hb, ha) != 0 {
			return io.ErrNoProgress
		}
	}

	return nil
}

func (n *gfarmObjects) copyToGfarmFile(gfarm_url_partName string, r *minio.PutObjReader) error {
fmt.Fprintf(os.Stderr, "@@@ copyToGfarmFile: %q\n", gfarm_url_partName)
	w, err := n.createGfarmFile(gfarm_url_partName)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToGfarmFile: %q\n", err)
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r.Reader)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToGfarmFile: %q\n", err)
		return err
	}
	return nil
}

func (n *gfarmObjects) copyFromGfarmFile(w *gf.File, gfarm_url_partName string) error {
fmt.Fprintf(os.Stderr, "@@@ copyFromGfarmFile: %q\n", gfarm_url_partName)
	r, err := n.openGfarmFile(gfarm_url_partName)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) createGfarmFile(gfarm_url_path string) (*gf.File, error) {
fmt.Fprintf(os.Stderr, "@@@ createGfarmFile: %q\n", gfarm_url_path)
	w, err := gf.OpenFile(gfarm_url_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "createGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return w, err
}

func (n *gfarmObjects) openGfarmFile(gfarm_url_path string) (*gf.File, error) {
fmt.Fprintf(os.Stderr, "@@@ openGfarmFile: %q\n", gfarm_url_path)
	r, err := gf.OpenFile(gfarm_url_path, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "openGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return r, err
}

func (n *gfarmObjects) createOsFile(gfarm_cache_path string) (*os.File, error) {
fmt.Fprintf(os.Stderr, "@@@ createOsFile: %q\n", gfarm_cache_path)
	return os.OpenFile(gfarm_cache_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
}

func (n *gfarmObjects) registerFileSize(gfarm_cache_partName string, value int64) () {
fmt.Fprintf(os.Stderr, "@@@ registerFileSize: %q %v\n", gfarm_cache_partName, value)
	n.cachectl.sizes[gfarm_cache_partName] = value
//	err := gf.LSetXattr(gfarm_url_path, gfarmS3OffsetKey, unsafe.Pointer(&value), unsafe.Sizeof(value), gf.GFS_XATTR_CREATE)
// GFS_XATTR_REPLACE
//	if err != nil {
//		gf.LogError(GFARM_MSG_UNFIXED, "copyToCachedFile", "LSetXattr", gfarm_url_path, err)
//	}
//	return err
}

func (n *gfarmObjects) removeGfarmFileIfExists(gfarm_url_path string) error {
fmt.Fprintf(os.Stderr, "@@@ removeGfarmFileIfExists: %q\n", gfarm_url_path)
	if n.cachectl != nil {
		n.deleteHashValue(gfarm_url_path)
	}
	err := gf.Remove(gfarm_url_path)
	if err == nil || gf.IsNotExist(err) {
		return nil
	}
	return err
}

func (n *gfarmObjects) removeOsFileIfExists(gfarm_cache_path string) error {
fmt.Fprintf(os.Stderr, "@@@ removeOsFileIfExists: %q\n", gfarm_cache_path)
	myAssert(n.cachectl != nil, "n.cachectl != nil")
	cacheFileSize := n.retrieveFileSize(gfarm_cache_path)
	n.cachectl.updateCacheUsage(-cacheFileSize)
	n.deleteFileSize(gfarm_cache_path)
	err := os.Remove(gfarm_cache_path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return err
}

func (n *gfarmObjects) registerHashValue(gfarm_url_path string, hash hash.Hash) () {
fmt.Fprintf(os.Stderr, "@@@ registerHashValue: %q %v\n", gfarm_url_path, hash)
//	var hash_size uintptr = uintptr(hash.Size())
	ha := hash.Sum(nil)
	n.cachectl.hashes[gfarm_url_path] = ha
//	err = gf.LSetXattr(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), hash_size, gf.GFS_XATTR_CREATE)
//	if err != nil {
//		gf.LogError(GFARM_MSG_UNFIXED, "copyToCachedFile", "LSetXattr", gfarm_url_partName, err)
//		return err
//	}
}

func (n *gfarmObjects) openOsFile(gfarm_cache_path string) (*os.File, error) {
fmt.Fprintf(os.Stderr, "@@@ openOsFile: %q\n", gfarm_cache_path)
	return os.OpenFile(gfarm_cache_path, os.O_RDONLY, os.FileMode(0644))
}

func (n *gfarmObjects) retrieveFileSize(gfarm_cache_partName string) int64 {
fmt.Fprintf(os.Stderr, "@@@ retrieveFileSize: %q\n", gfarm_cache_partName)
	return n.cachectl.sizes[gfarm_cache_partName]
//	var value int64
//	size := unsafe.Sizeof(value)
//	err = gf.LGetXattrCached(gfarm_url_path, gfarmS3OffsetKey, unsafe.Pointer(&value), &size)
//	if err != nil {
//		gf.LogError(GFARM_MSG_UNFIXED, "copyFromCachedFile", "LGetXattrCached", gfarm_url_path, err)
//		return 0, err
//	}
//	return value, nil
}

func (n *gfarmObjects) deleteFileSize(gfarm_cache_partName string) () {
fmt.Fprintf(os.Stderr, "@@@ deleteFileSize: %q\n", gfarm_cache_partName)
	delete(n.cachectl.sizes, gfarm_cache_partName)
}

func (n *gfarmObjects) retrieveHashValue(gfarm_url_partName string) ([]byte, error) {
fmt.Fprintf(os.Stderr, "@@@ retrieveHashValue: %q\n", gfarm_url_partName)
	return n.cachectl.hashes[gfarm_url_partName], nil
//	var hash_size uintptr = uintptr(hash.Size())
//	ha := make([]byte, hash_size)
//	hb := hash.Sum(nil)
//
//	err = gf.LGetXattrCached(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), &hash_size)
//	if err != nil {
//		gf.LogError(GFARM_MSG_UNFIXED, "copyFromCachedFile", "LGetXattrCached", gfarm_url_partName, err)
//		return nil, err
//	}
//	return ha, nil
}

func (n *gfarmObjects) deleteHashValue(gfarm_url_partName string) () {
fmt.Fprintf(os.Stderr, "@@@ deleteHashValue: %q\n", gfarm_url_partName)
	delete(n.cachectl.hashes, gfarm_url_partName)
}

func myWrite(w io.Writer, hash hash.Hash, buf []byte) (int, error) {
//fmt.Fprintf(os.Stderr, "@@@ myWrite\n")
	if hash != nil {
		_, _ = io.Copy(hash, bytes.NewReader(buf))
	}
	return w.Write(buf)
}

func myCopyHash(w io.Writer, r io.Reader, hash hash.Hash, buf []byte) (int64, error) {
//fmt.Fprintf(os.Stderr, "@@@ myCopyHash\n")
	var total int64 = 0
	for {
		len, err := r.Read(buf)
		if err == io.EOF {
			return total, nil
		} else if err != nil {
			return total, err
		}
		wrote_bytes, err := myWrite(w, hash, buf[:len])
		if err != nil {
			return total, err
		}
		myAssert(wrote_bytes == len, "wrote_bytes == len")

		total += int64(len)
	}
}

func myCopyHashUpto(w io.Writer, r io.Reader, hash hash.Hash, limit int64, buf []byte) (int64, error) {
//fmt.Fprintf(os.Stderr, "@@@ myCopyHashUpto\n")
	var total int64 = 0
	for {
		len, err := r.Read(buf)
		if err == io.EOF {
			return total, nil
		} else if err != nil {
			return total, err
		}

		if limit < total + int64(len) {
			len = int(limit - total)
		}

		wrote_bytes, err := myWrite(w, hash, buf[:len])
		if err != nil {
			return total, err
		}
		myAssert(wrote_bytes == len, "wrote_bytes == len")

		total += int64(len)

		if limit <= total {
			return total, nil
		}
	}
}

func (c *cacheController) updateCacheUsage(len int64) () {
//fmt.Fprintf(os.Stderr, "@@@ updateCacheUsage\n")
	c.mutex.Lock()
	c.fasterTotal += len
	if c.fasterMax < c.fasterTotal {
		c.fasterMax = c.fasterTotal
	}
	c.mutex.Unlock()
}

func myAssert(c bool, m string) () {
	if !c {
		fmt.Fprintf(os.Stderr, "Assertion failed: %s\n", m)
	}
}

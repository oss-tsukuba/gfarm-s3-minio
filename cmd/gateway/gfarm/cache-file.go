package gfarm

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"

	gf "github.com/minio/minio/pkg/gfarm"
	minio "github.com/minio/minio/cmd"
)

func (n *gfarmObjects) createMetaTmpBucketGfarm(minioMetaTmpBucket string) error {
	gfarm_url_minioMetaTmpBucket := n.gfarm_url_PathJoin(gfarmSeparator, minioMetaTmpBucket)
	if err := gf.MkdirAll(gfarm_url_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewGatewayLayer", "MkdirAll", gfarm_url_minioMetaTmpBucket, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMetaTmpBucketCache(minioMetaTmpBucket string) error {
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
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	if err := gf.Mkdir(gfarm_url_dirName, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewMultipartUpload", "Mkdir", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMultipartUploadDirCache(dirName string) error {
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
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	_, err := gf.Stat(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "checkUploadIDExists", "Stat", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) checkUploadIDExistsCache(dirName string) error {
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

func (n *gfarmObjects) removeMultipartGfarmWorkdir(dirName string) error {
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	entries, err := gf.ReadDir(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "ReadDir", gfarm_url_dirName, err)
		return err
	}
	for _, entry := range entries {
		gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
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

func (n *gfarmObjects) removeMultipartCacheWorkdir(dirName string) error {
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

func (n *gfarmObjects) copyToPartFileTruncateOrCreate(partName string, r *minio.PutObjReader) error {
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
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		return n.copyFromCachedFile(w, gfarm_url_partName, gfarm_cache_partName)
	} else {
		return n.copyFromGfarmFile(w, gfarm_url_partName)
	}
}

func (n *gfarmObjects) copyToCachedFile(gfarm_url_partName, gfarm_cache_partName string, r *minio.PutObjReader) error {
	var total int64 = 0
	buf := make([]byte, myCopyBufsize)
	var len int = 0

	c := n.cachectl

	var hash hash.Hash = nil
	if c.enable_partfile_digest {
		hash = md5.New()
	}

	w_cache, err := n.createOsFile(gfarm_cache_partName)
	if err != nil {
		return err
	}
	//defer w_cache.Close()  -- we should close w_cache on demand

	var read_err error = nil
	/* write to OS FILE */
	for {
		len, read_err = r.Read(buf)

		if read_err != nil && read_err != io.EOF {
			// fatal error!
			w_cache.Close()
			c.updateCacheUsage(-total)
			return read_err
		}

		if c.cacheLimit < c.cacheTotal + int64(len) {
			if close_err := w_cache.Close(); close_err != nil {
				c.updateCacheUsage(-total)
				return close_err
			}
			read_err = nil
			break // cache full => switch to gfarm
		}

		wrote_bytes, write_err := myWriteWithHash(w_cache, hash, buf[:len])
		if wrote_bytes != len {
			if write_err := w_cache.Close(); write_err != nil {
				c.updateCacheUsage(-total)
				return write_err
			}
			read_err = nil
			break // partial write to cache => switch to gfarm
		}

		if write_err != nil {
			if close_err := w_cache.Close(); close_err != nil {
				c.updateCacheUsage(-total)
				return close_err
			}
			read_err = nil
			break // write to cache failed => switch to gfarm
		}

		myAssert(wrote_bytes == len, "wrote_bytes == len")

		len = 0		// don't care
		total += int64(wrote_bytes)
		c.updateCacheUsage(int64(wrote_bytes))

		if read_err == io.EOF {
			// cache only!
			if close_err := w_cache.Close(); close_err != nil {
				c.updateCacheUsage(-total)
				return close_err
			}
			break
		}
	}

	n.registerFileSize(gfarm_cache_partName, total)

	if read_err == nil {	// <=> read_err != io.EOF
		w_gfarm, err := n.createGfarmFile(gfarm_url_partName)
		if err != nil {
			return err
		}
		//defer w_gfarm.Close()  -- we should close w_cache on demand

		if len != 0 {
			wrote_bytes, write_err := myWriteWithHash(w_gfarm, hash, buf[:len])
			total += int64(wrote_bytes)
			if write_err != nil {
				w_gfarm.Close()
				return write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
		}

		total_gfarm, err := myCopyWithHash(w_gfarm, r, hash, buf)
		total += total_gfarm
		if err != nil {
			// fatal error!
			w_gfarm.Close()
			return err
		}

		if close_err := w_gfarm.Close(); close_err != nil {
			return close_err
		}
	}

	if hash != nil {
		n.registerHashValue(gfarm_url_partName, hash)
	}

	return nil
}

func (n *gfarmObjects) copyFromCachedFile(w *gf.File, gfarm_url_partName, gfarm_cache_partName string) error {
	c := n.cachectl

	buf := make([]byte, myCopyBufsize)

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

		total, err := myCopyWithHashLimit(w, r_cache, hash, cacheFileSize, buf)

		if total != cacheFileSize {
			return io.ErrUnexpectedEOF
		}

		_, err = myCopyWithHash(w, r_gfarm, hash, buf)
		if err != nil {
			return err
		}
	} else {
		_, err = myCopyWithHash(w, r_cache, hash, buf)
		if err != nil {
			return err
		}
	}

	if hash != nil {
		hb := hash.Sum(nil)
		ha := n.retrieveHashValue(gfarm_url_partName)

		if bytes.Compare(hb, ha) != 0 {
			return io.ErrNoProgress
		}
	}

	return nil
}

func (n *gfarmObjects) copyToGfarmFile(gfarm_url_partName string, r *minio.PutObjReader) error {
	w, err := n.createGfarmFile(gfarm_url_partName)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r.Reader)
	if err != nil {
		return err
	}
	return nil
}

func (n *gfarmObjects) copyFromGfarmFile(w *gf.File, gfarm_url_partName string) error {
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
	w, err := gf.OpenFile(gfarm_url_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "createGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return w, err
}

func (n *gfarmObjects) openGfarmFile(gfarm_url_path string) (*gf.File, error) {
	r, err := gf.OpenFile(gfarm_url_path, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "openGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return r, err
}

func (n *gfarmObjects) removeGfarmFileIfExists(gfarm_url_path string) error {
	if n.cachectl != nil {
		n.deleteHashValue(gfarm_url_path)
	}
	err := gf.Remove(gfarm_url_path)
	if err == nil || gf.IsNotExist(err) {
		return nil
	}
	return err
}

func (n *gfarmObjects) createOsFile(gfarm_cache_path string) (*os.File, error) {
	return os.OpenFile(gfarm_cache_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
}

func (n *gfarmObjects) openOsFile(gfarm_cache_path string) (*os.File, error) {
	return os.OpenFile(gfarm_cache_path, os.O_RDONLY, os.FileMode(0644))
}

func (n *gfarmObjects) removeOsFileIfExists(gfarm_cache_path string) error {
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

func (n *gfarmObjects) registerFileSize(gfarm_cache_partName string, value int64) () {
	c := n.cachectl
	c.mutex.Lock()
	c.sizes[gfarm_cache_partName] = value
	c.mutex.Unlock()
}

func (n *gfarmObjects) registerHashValue(gfarm_url_path string, hash hash.Hash) () {
	ha := hash.Sum(nil)
	c := n.cachectl
	c.mutex.Lock()
	c.hashes[gfarm_url_path] = ha
	c.mutex.Unlock()
}

func (n *gfarmObjects) retrieveFileSize(gfarm_cache_partName string) int64 {
	return n.cachectl.sizes[gfarm_cache_partName]
}

func (n *gfarmObjects) retrieveHashValue(gfarm_url_partName string) ([]byte) {
	return n.cachectl.hashes[gfarm_url_partName]
}

func (n *gfarmObjects) deleteFileSize(gfarm_cache_partName string) () {
	delete(n.cachectl.sizes, gfarm_cache_partName)
}

func (n *gfarmObjects) deleteHashValue(gfarm_url_partName string) () {
	delete(n.cachectl.hashes, gfarm_url_partName)
}

func myWriteWithHash(w io.Writer, hash hash.Hash, buf []byte) (int, error) {
	if hash != nil {
		_, _ = io.Copy(hash, bytes.NewReader(buf))
	}
	return w.Write(buf)
}

func myCopyWithHash(w io.Writer, r io.Reader, hash hash.Hash, buf []byte) (int64, error) {
	var total int64 = 0
	for {
		len, read_err := r.Read(buf)
		if read_err != nil && read_err != io.EOF {
			return total, read_err
		}
		if len != 0 {
			wrote_bytes, write_err := myWriteWithHash(w, hash, buf[:len])
			total += int64(wrote_bytes)
			if write_err != nil {
				return total, write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
		}
		if read_err == io.EOF {
			return total, nil
		}
	}
}

func myCopyWithHashLimit(w io.Writer, r io.Reader, hash hash.Hash, limit int64, buf []byte) (int64, error) {
	var total int64 = 0

	for {
		len, read_err := r.Read(buf)
		if read_err != nil && read_err != io.EOF {
			return total, read_err
		}

		if len != 0 {
			if limit < total + int64(len) {
				len = int(limit - total)
			}
			wrote_bytes, write_err := myWriteWithHash(w, hash, buf[:len])

			total += int64(wrote_bytes)
			if write_err != nil {
				return total, write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
			if limit <= total {
				myAssert(limit == total, "limit == total")
				return total, nil
			}
		}
		if read_err == io.EOF {
			myAssert(limit == total, "limit == total")
			return total, nil
		}
	}
}

func (c *cacheController) updateCacheUsage(len int64) () {
	c.mutex.Lock()
	c.cacheTotal += len
	if c.cacheMax < c.cacheTotal {
		c.cacheMax = c.cacheTotal
	}
	c.mutex.Unlock()
}

func myAssert(c bool, m string) () {
	if !c {
		fmt.Fprintf(os.Stderr, "Assertion failed: %s\n", m)
	}
}

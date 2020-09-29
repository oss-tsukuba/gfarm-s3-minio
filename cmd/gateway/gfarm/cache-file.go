package gfarm

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"time"

	gf "github.com/minio/minio/pkg/gfarm"
	minio "github.com/minio/minio/cmd"
)

func (n *gfarmObjects) createMetaTmpBucketGfarm(minioMetaTmpBucket string) error {
//fmt.Fprintf(os.Stderr, "@@@ createMetaTmpBucketGfarm: %q\n", minioMetaTmpBucket)
	gfarm_url_minioMetaTmpBucket := n.gfarm_url_PathJoin(gfarmSeparator, minioMetaTmpBucket)
fmt.Fprintf(os.Stderr, "@@@ createMetaTmpBucketGfarm gf.MkdirAll %q\n", gfarm_url_minioMetaTmpBucket)
	if err := gf.MkdirAll(gfarm_url_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewGatewayLayer", "MkdirAll", gfarm_url_minioMetaTmpBucket, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMetaTmpBucketCache(minioMetaTmpBucket string) error {
//fmt.Fprintf(os.Stderr, "@@@ createMetaTmpBucketCache: %q\n", minioMetaTmpBucket)
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
//fmt.Fprintf(os.Stderr, "@@@ createMultipartUploadDirGfarm: %q\n", dirName)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
fmt.Fprintf(os.Stderr, "@@@ createMultipartUploadDirGfarm gf.Mkdir %q\n", gfarm_url_dirName)
	if err := gf.Mkdir(gfarm_url_dirName, os.FileMode(0755)); err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "NewMultipartUpload", "Mkdir", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) createMultipartUploadDirCache(dirName string) error {
//fmt.Fprintf(os.Stderr, "@@@ createMultipartUploadDirCache: %q\n", dirName)
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
//fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExistsGfarm: %q\n", dirName)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	_, err := gf.Stat(gfarm_url_dirName)
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "checkUploadIDExists", "Stat", gfarm_url_dirName, err)
		return err
	}
	return nil
}

func (n *gfarmObjects) checkUploadIDExistsCache(dirName string) error {
//fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExistsCache: %q\n", dirName)
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
//fmt.Fprintf(os.Stderr, "@@@ removeMultipartGfarmWorkdir: %q\n", dirName)
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

c := n.cachectl
fmt.Fprintf(os.Stderr, "@@@ cacheLimit: %v cacheTotal: %v cacheMax: %v\n", c.cacheLimit, c.cacheTotal, c.cacheMax)

	return nil
}

func (n *gfarmObjects) removeMultipartCacheWorkdir(dirName string) error {
//fmt.Fprintf(os.Stderr, "@@@ removeMultipartCacheWorkdir: %q\n", dirName)
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

func (n *gfarmObjects) copyToPartFileTruncateOrCreate(partName string, r *minio.PutObjReader) error {
//fmt.Fprintf(os.Stderr, "@@@ copyToPartFileTruncateOrCreate: %q\n", partName)
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
//fmt.Fprintf(os.Stderr, "@@@ copyFromPartFileAppendOrCreate: %q\n", partName)
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		return n.copyFromCachedFile(w, gfarm_url_partName, gfarm_cache_partName)
	} else {
		return n.copyFromGfarmFile(w, gfarm_url_partName)
	}
}

func (n *gfarmObjects) copyToCachedFile(gfarm_url_partName, gfarm_cache_partName string, r *minio.PutObjReader) error {
//fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile: %q %q\n", gfarm_url_partName, gfarm_cache_partName)
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
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile A: %v\n", err)
		return err
	}
	//defer w_cache.Close()

	var read_err error = nil
	/* write to OS FILE */
	for {
		len, read_err = r.Read(buf)

		if read_err != nil && read_err != io.EOF {
			// fatal error!
			w_cache.Close()
			c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile C: %v\n", read_err)
			return read_err
		}

		if c.cacheLimit < c.cacheTotal + int64(len) {
			if close_err := w_cache.Close(); close_err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile D: %v\n", close_err)
				return close_err
			}
			read_err = nil
			break // cache full => switch to gfarm
		}

		wrote_bytes, write_err := myWriteWithHash(w_cache, hash, buf[:len])
		if wrote_bytes != len {
			if write_err := w_cache.Close(); write_err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile E: %v\n", write_err)
				return write_err
			}
			read_err = nil
			break // partial write to cache => switch to gfarm
		}

		if write_err != nil {
			if close_err := w_cache.Close(); close_err != nil {
				c.updateCacheUsage(-total)
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile F: %v\n", close_err)
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
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile B: %v\n", close_err)
				return close_err
			}
			break
		}
	}

	n.registerFileSize(gfarm_cache_partName, total)

	if read_err == nil {	// <=> read_err != io.EOF
		w_gfarm, err := n.createGfarmFile(gfarm_url_partName)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile G: %v\n", err)
			return err
		}
		//defer w_gfarm.Close()

		if len != 0 {
			wrote_bytes, write_err := myWriteWithHash(w_gfarm, hash, buf[:len])
			total += int64(wrote_bytes)
			if write_err != nil {
				w_gfarm.Close()
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile H: %v\n", write_err)
				return write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
		}

		total_gfarm, err := myCopyWithHash(w_gfarm, r, hash, buf)
		total += total_gfarm
		if err != nil {
			// fatal error!
			w_gfarm.Close()
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile I: %v\n", err)
			return err
		}

		if close_err := w_gfarm.Close(); close_err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyToCachedFile K: %v\n", close_err)
			return close_err
		}
	}

	if hash != nil {
		n.registerHashValue(gfarm_url_partName, hash)
	}

	return nil
}

func (n *gfarmObjects) copyFromCachedFile(w *gf.File, gfarm_url_partName, gfarm_cache_partName string) error {
//fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile: %q %q\n", gfarm_url_partName, gfarm_cache_partName)
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v copyFromCachedFile start %q\n", myFormatTime(start), gfarm_url_partName)
	c := n.cachectl

	buf := make([]byte, myCopyBufsize)

	r_cache, err := n.openOsFile(gfarm_cache_partName)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile A: %v\n", err)
		return err
	}
	defer r_cache.Close()

	var hash hash.Hash = nil
	if c.enable_partfile_digest {
		hash = md5.New()
	}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q AAA\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

	_, err = gf.Stat(gfarm_url_partName)
	if err == nil {
		r_gfarm, err := n.openGfarmFile(gfarm_url_partName)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile B: %v\n", err)
			return err
		}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q BBB\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

		defer r_gfarm.Close()

		cacheFileSize := n.retrieveFileSize(gfarm_cache_partName)

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q CCC\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

		total, err := myCopyWithHashLimit(w, r_cache, hash, cacheFileSize, buf)

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q DDD\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

		if total != cacheFileSize {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile C total != cacheFileSize: %v\n", io.ErrUnexpectedEOF)
			return io.ErrUnexpectedEOF
		}

		_, err = myCopyWithHash(w, r_gfarm, hash, buf)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile D: %v\n", err)
			return err
		}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q EEE\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

	} else {
		_, err = myCopyWithHash(w, r_cache, hash, buf)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile E: %v\n", err)
			return err
		}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q FFF\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)
	}

	if hash != nil {
		hb := hash.Sum(nil)
		ha := n.retrieveHashValue(gfarm_url_partName)

		if bytes.Compare(hb, ha) != 0 {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile F: hash check failed: %v %v %v\n", hb, ha, io.ErrNoProgress)
			//return io.ErrNoProgress
		} else {
fmt.Fprintf(os.Stderr, "@@@ copyFromCachedFile F: hash check OK\n")
		}
	}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q GGG\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) copyFromCachedFile  %q end\n", myFormatTime(now), now.Sub(start), gfarm_url_partName)

gf.ShowStat()

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
//fmt.Fprintf(os.Stderr, "@@@ copyFromGfarmFile: %q\n", gfarm_url_partName)
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
//fmt.Fprintf(os.Stderr, "@@@ createGfarmFile: %q\n", gfarm_url_path)
	w, err := gf.OpenFile(gfarm_url_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "createGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return w, err
}

func (n *gfarmObjects) openGfarmFile(gfarm_url_path string) (*gf.File, error) {
//fmt.Fprintf(os.Stderr, "@@@ openGfarmFile: %q\n", gfarm_url_path)
	r, err := gf.OpenFile(gfarm_url_path, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		gf.LogError(GFARM_MSG_UNFIXED, "openGfarmFile", "OpenFile", gfarm_url_path, err)
		return nil, err
	}
	return r, err
}

func (n *gfarmObjects) removeGfarmFileIfExists(gfarm_url_path string) error {
//fmt.Fprintf(os.Stderr, "@@@ removeGfarmFileIfExists: %q\n", gfarm_url_path)
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
//fmt.Fprintf(os.Stderr, "@@@ createOsFile: %q\n", gfarm_cache_path)
	return os.OpenFile(gfarm_cache_path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
}

func (n *gfarmObjects) openOsFile(gfarm_cache_path string) (*os.File, error) {
//fmt.Fprintf(os.Stderr, "@@@ openOsFile: %q\n", gfarm_cache_path)
	return os.OpenFile(gfarm_cache_path, os.O_RDONLY, os.FileMode(0644))
}

func (n *gfarmObjects) removeOsFileIfExists(gfarm_cache_path string) error {
//fmt.Fprintf(os.Stderr, "@@@ removeOsFileIfExists: %q\n", gfarm_cache_path)
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
//fmt.Fprintf(os.Stderr, "@@@ registerFileSize: %q %v\n", gfarm_cache_partName, value)
	c := n.cachectl
	c.mutex.Lock()
	c.sizes[gfarm_cache_partName] = value
	c.mutex.Unlock()
}

func (n *gfarmObjects) registerHashValue(gfarm_url_path string, hash hash.Hash) () {
//fmt.Fprintf(os.Stderr, "@@@ registerHashValue: %q %v\n", gfarm_url_path, hash)
	ha := hash.Sum(nil)
	c := n.cachectl
	c.mutex.Lock()
	c.hashes[gfarm_url_path] = ha
	c.mutex.Unlock()
}

func (n *gfarmObjects) retrieveFileSize(gfarm_cache_partName string) int64 {
//fmt.Fprintf(os.Stderr, "@@@ retrieveFileSize: %q\n", gfarm_cache_partName )
	return n.cachectl.sizes[gfarm_cache_partName]
}

func (n *gfarmObjects) retrieveHashValue(gfarm_url_partName string) ([]byte) {
	return n.cachectl.hashes[gfarm_url_partName]
}

func (n *gfarmObjects) deleteFileSize(gfarm_cache_partName string) () {
//fmt.Fprintf(os.Stderr, "@@@ deleteFileSize: %q\n", gfarm_cache_partName)
	delete(n.cachectl.sizes, gfarm_cache_partName)
}

func (n *gfarmObjects) deleteHashValue(gfarm_url_partName string) () {
//fmt.Fprintf(os.Stderr, "@@@ deleteHashValue: %q\n", gfarm_url_partName)
	delete(n.cachectl.hashes, gfarm_url_partName)
}

func myWriteWithHash(w io.Writer, hash hash.Hash, buf []byte) (int, error) {
//fmt.Fprintf(os.Stderr, "@@@ myWriteWithHash\n")
	if hash != nil {
		_, _ = io.Copy(hash, bytes.NewReader(buf))
	}
	return w.Write(buf)
}

func myCopyWithHash(w io.Writer, r io.Reader, hash hash.Hash, buf []byte) (int64, error) {
//fmt.Fprintf(os.Stderr, "@@@ myCopyWithHash\n")
	var total int64 = 0
var read_time, write_time time.Duration = 0, 0
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v myCopyWithHash start\n", myFormatTime(start))
lap := now
	for {
		len, read_err := r.Read(buf)
		if read_err != nil && read_err != io.EOF {
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHash A: %v\n", read_err)
			return total, read_err
		}

now = time.Now()
read_time += now.Sub(lap)
lap = now
		if len != 0 {
			wrote_bytes, write_err := myWriteWithHash(w, hash, buf[:len])

now = time.Now()
write_time += now.Sub(lap)
lap = now
			total += int64(wrote_bytes)
			if write_err != nil {
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHash B: %v\n", write_err)
				return total, write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
		}
		if read_err == io.EOF {
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHash read_time = %v write_time = %v\n", read_time, write_time)
			return total, nil
		}
	}
}

func myCopyWithHashLimit(w io.Writer, r io.Reader, hash hash.Hash, limit int64, buf []byte) (int64, error) {
//fmt.Fprintf(os.Stderr, "@@@ myCopyWithHashLimit\n")
	var total int64 = 0
var read_time, write_time time.Duration = 0, 0
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v myCopyWithHashLimit start\n", myFormatTime(start))
lap := now

	for {
		len, read_err := r.Read(buf)
		if read_err != nil && read_err != io.EOF {
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHashLimit A: %v\n", read_err)
			return total, read_err
		}

now = time.Now()
read_time += now.Sub(lap)
lap = now
		if len != 0 {
			if limit < total + int64(len) {
				len = int(limit - total)
			}
			wrote_bytes, write_err := myWriteWithHash(w, hash, buf[:len])

now = time.Now()
write_time += now.Sub(lap)
lap = now
			total += int64(wrote_bytes)
			if write_err != nil {
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHashLimit B: %v\n", write_err)
				return total, write_err
			}
			myAssert(wrote_bytes == len, "wrote_bytes == len")
			if limit <= total {
				myAssert(limit == total, "limit == total")
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHashLimit read_time = %v write_time = %v\n", read_time, write_time)
now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) myCopyWithHashLimit  end\n", myFormatTime(now), now.Sub(start))

				return total, nil
			}
		}
		if read_err == io.EOF {
			myAssert(limit == total, "limit == total")
fmt.Fprintf(os.Stderr, "@@@ myCopyWithHashLimit read_time = %v write_time = %v\n", read_time, write_time)
now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) myCopyWithHashLimit  end\n", myFormatTime(now), now.Sub(start))
			return total, nil
		}
	}
}

func (c *cacheController) updateCacheUsage(len int64) () {
//fmt.Fprintf(os.Stderr, "@@@ updateCacheUsage\n")
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

/*
 * functions that require following gf.CheckError

low level func(s)	gfarm func		caller(s)

-			Gfarm_initializea	NewGatewayLayer
-			Gfarm_terminate		Shutdown

gfs_stat		Stat			GetBucketInfo, ListObjects, GetObject, GetObjectInfo
						PutObject, NewMultipartUpload, ListMultipartUploads
						checkUploadIDExists, ListObjectParts, PutObjectPart
						CompleteMultipartUpload, AbortMultipartUpload

gfs_pio_open, gfs_pio_create
			OpenFile		GetObject, PutObject, copyToCachedFile, 
						CompleteMultipartUpload, copyFromCachedFile

gfs_pio_close		(f *File) Close		-
gfs_pio_pread		(f *File) ReadAt	-
gfs_pio_read		(f *File) Read		-
gfs_pio_write		(f *File) Write		-

gfs_rename		Rename			PutObject, CompleteMultipartUpload

gfs_stat, gfs_unlink, gfs_rmdir
			Remove			DeleteBucket, deleteObject, cleanupMultipartUploadDir

gfs_mkdir		Mkdir			MakeBucketWithLocation, NewMultipartUpload
			MkdirAll		NewGatewayLayer, PutObject, CompleteMultipartUpload

gfs_opendir_caching, gfs_readdir, gfs_closedir
			ReadDir			ListBuckets, listDirFactory, 
						isObjectDir, cleanupMultipartUploadDir
gfs_statfs		StatFs			StorageInfo
gfs_lsetxattr		LSetXattr		copyToCachedFile
gfs_lgetxattr_cached	LGetXattrCached		copyFromCachedFile, cleanupMultipartUploadDir
 */

/*
 *
 * Hangarian rules for gfarm_url_ and gfarm_cache_
 *
 *
 * gf.Capital <=> first argument shall be a variable that name begins with `gfarm_url_'
/gf\.[A-Z]
/(gfarm_url_
 * &&
 * such variables shall be set by using n.gfarm_url_PathJoin()
/gfarm_url_[a-zA-Z :]*=
 *
 *
 * (os|ioutil).Capital <=> first argument shall be a variable that name begins with `gfarm_cache_'
/os\.[A-Z][a-zA-Z_]*(
/os\.[A-Z][a-zA-Z_]*(gfarm_cache
/os\.[A-Z][a-zA-Z_]*(gfarm_url
/(gfarm_cache
 * &&
 * such variables shall be set by using n.gfarm_cache_PathJoin()
/gfarm_cache_[a-zA-Z :]*=
 *
 *
 * exported methods (functions that have Capial name) of gfarmObjects
/\<gfarmObjects) [A-Z].*\<error\>
 * shall return gfarmToObjectErr(ctx, err, bucket, object)
 *    except for DeleteObjects
 * Note that functions that call another exported methods of gfarmObjects
 * shall not call gfarmToObjectErr again.
 *
 * un-exported methods (functions that have lowercase name) of gfarmObjects
/\<gfarmObjects) [a-z].*\<error\>
 * shall not wrap erros with gfarmToObjectErr().
 * exception: checkUploadIDExists wraps its result by gfarmToObjectErr()
 */

//       gfarmSharedDir          "gfarm:///shared/hpci005858"
//       gfarmSharedVirtualName  "sss"
//
//    Gfarm                       -- S3 API
//    /shared                     -- inaccesible
//    |-- hpci005858              -- bucket pool "s3://"
//    |   |-- .minio.sys          -- invisible
//    |   |-- mybucket            -- bucket      "s3://mybucket"
//    |   `-- sss                 -- virtual bucket
//    |       `-- hpci001971      -- PRE         "s3://sss/hpci001971"
//    |       .   |-- .minio.sys  -- invisible
//    |       .   `-- bucket1     -- PRE         "s3://sss/hpci001971/bucket1"
//    |       .   .   `-- object1 -- object
//    |       .    .. (bucket2)   -- non-shared bucket is invisible (DECIDED NOT TO IMPLMENT)
//    |        .. (hpci001970)    -- total private user is invisible (DECIDED NOT TO IMPLMENT)
//    |
//    |-- hpci001971
//    |   |-- .minio.sys
//    |   |-- bucket1              -- allow hpci005858 to access bucket1
//    |   |   `-- object1
//    |   `-- bucket2              -- private bucket
//    `-- hpci001970               -- this user exports nothing
//        |-- .minio.sys
//        `-- bucket3              -- private bucket

/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gfarm

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"sort"
	"sync"
	"strings"
	"syscall"
	"time"
	"unsafe"
	"github.com/minio/minio/pkg/env"

	gf "github.com/minio/minio/pkg/gfarm"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	humanize "github.com/dustin/go-humanize"
)

const (
	gfarmBackend = "gfarm"

	constGfarmScheme = "gfarm://"

	gfarmS3OffsetKey = "user.gfarms3.offset"
	gfarmS3DigestKey = "user.gfarms3.part_digest"
	gfarmSeparator = minio.SlashSeparator

	gfarmCachePathEnvVar = "MINIO_GFARMS3_CACHEDIR"
	gfarmCacheSizeEnvVar = "MINIO_GFARMS3_CACHEDIR_SIZE_MB"

	gfarmPartfileDigestEnvVar = "GFARMS3_PARTFILE_DIGEST"

	gfarmDefaultCacheSize = 16 * humanize.MiByte
)

func init() {
	const gfarmGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} GFARM-USERNAME GFARM-ROOTDIR GFARM-SHAREDDIR
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
GFARM-USERNAME:
  GFARM username

GFARM-ROOTDIR:
  GFARM rootdir URI

GFARM-SHAREDDIR:
  GFARM shareddir (sss)

EXAMPLES:
  1. Start minio gateway server for GFARM backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_GFARM_CACHE_ROOTDIR{{.AssignmentOperator}}/mnt/cache1
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_GFARM_CACHE_SIZE_MB{{.AssignmentOperator}}16
     {{.Prompt}} {{.HelpName}} gfarm-username gfarm-rootdir gfarm-shareddir

  2. Start minio gateway server for GFARM backend with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_GFARM_CACHE_ROOTDIR{{.AssignmentOperator}}/mnt/cache1
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_GFARM_CACHE_SIZE_MB{{.AssignmentOperator}}16
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}} gfarm-username gfarm-rootdir gfarm-shareddir
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               gfarmBackend,
		Usage:              "Gfarm File System (GFARM)",
		Action:             gfarmGatewayMain,
		CustomHelpTemplate: gfarmGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway gfarm' command line.
func gfarmGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, gfarmBackend, 1)
	}

	minio.StartGateway(ctx, &GFARM{args: ctx.Args()})
}

// GFARM implements Gateway.
type GFARM struct {
	args []string
}

// Name implements Gateway interface.
func (g *GFARM) Name() string {
	return gfarmBackend
}

func (n *gfarmObjects) gfarm_url_PathJoin(pathComponents ...string) string {
	var gfarmPath string
	s3path := minio.PathJoin(pathComponents...)
	if strings.HasPrefix(s3path, n.gfarmctl.gfarmSharedVirtualNamePath) {
		gfarmPath = minio.PathJoin(n.gfarmctl.gfarmSharedDir, "..", s3path[len(n.gfarmctl.gfarmSharedVirtualNamePath):])
	} else {
		gfarmPath = minio.PathJoin(n.gfarmctl.gfarmSharedDir, s3path)
	}
	result := n.gfarmctl.gfarmScheme + gfarmPath
	return result
}

func (n *gfarmObjects) gfarm_cache_PathJoin(pathComponents ...string) string {
	return minio.PathJoin(n.cachectl.cacheRootdir, minio.PathJoin(pathComponents...))
}

// NewGatewayLayer returns gfarm gatewaylayer.
func (g *GFARM) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {

	gfarmScheme := ""
	gfarmSharedDir := g.args[0]
	gfarmSharedVirtualName := g.args[1]

	cacheRootdir := env.Get(gfarmCachePathEnvVar, "")
	cacheCapacity := getCacheSizeFromEnv(gfarmCacheSizeEnvVar, strconv.Itoa(gfarmDefaultCacheSize / humanize.MiByte))

	gfarmSharedDir = strings.TrimSuffix(gfarmSharedDir, gfarmSeparator)
	if strings.HasPrefix(gfarmSharedDir, constGfarmScheme + "/") {
		gfarmScheme = constGfarmScheme
		gfarmSharedDir = gfarmSharedDir[len(constGfarmScheme):]
	}

	cacheRootdir = strings.TrimSuffix(cacheRootdir, gfarmSeparator)

	gfarmSharedVirtualNamePath := minio.PathJoin("/", gfarmSharedVirtualName)

	err := gf.Gfarm_initialize()
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "NewGatewayLayer", "Gfarm_initialize", "", err)
		return nil, err
	}

	gfarmctl := &gfarmController{gfarmScheme, gfarmSharedDir, gfarmSharedVirtualNamePath}
	var cachectl *cacheController = nil
	if cacheRootdir != "" {
		partfile_digest := env.Get(gfarmPartfileDigestEnvVar, "")
		enable_partfile_digest := partfile_digest != "no"
		cachectl = &cacheController{cacheRootdir, 0, int64(cacheCapacity), 0, &sync.Mutex{}, enable_partfile_digest}
	}
	n := &gfarmObjects{gfarmctl: gfarmctl, cachectl: cachectl, listPool: minio.NewTreeWalkPool(time.Minute * 30)}

	gfarm_url_minioMetaTmpBucket := n.gfarm_url_PathJoin(gfarmSeparator, minioMetaTmpBucket)
	if err = gf.MkdirAll(gfarm_url_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "NewGatewayLayer", "MkdirAll", gfarm_url_minioMetaTmpBucket, err)
		return nil, err
	}

	if cachectl != nil {
		gfarm_cache_minioMetaTmpBucket := n.gfarm_cache_PathJoin(gfarmSeparator, minioMetaTmpBucket)
		if err = os.MkdirAll(gfarm_cache_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
			return nil, err
		}
	}

	return n, nil
}

func getCacheSizeFromEnv(envvar string, defaultValue string) int {
	envCacheSize := env.Get(envvar, defaultValue)

	i, err := strconv.ParseFloat(envCacheSize, 64)
	if err != nil {
		logger.LogIf(context.Background(), err)
		return gfarmDefaultCacheSize
	}

/*
@	if i <= 0 || i > 100 {
@		logger.LogIf(context.Background(), fmt.Errorf("ENV '%v' should be a floating point value between 0 and 100.\n"+
@			"The upload chunk size is set to its default: %s\n", gfarmCacheSizeEnvVar, defaultValue))
@		return gfarmDefaultCacheSize
@	}
*/

	return int(i * humanize.MiByte)
}

// Production - gfarm gateway is production ready.
func (g *GFARM) Production() bool {
	return true
}

func (n *gfarmObjects) Shutdown(ctx context.Context) error {
	err := gf.Gfarm_terminate()
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "Shutdown", "Gfarm_terminate", "", err)
	}
	return err
}

func (n *gfarmObjects) StorageInfo(ctx context.Context, _ bool) minio.StorageInfo {
	fsInfo, err := gf.StatFs()
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "StorageInfo", "StatFs", "", err)
		return minio.StorageInfo{}
	}

	sinfo := minio.StorageInfo{}

	sinfo.Used = []uint64{fsInfo.Used}  // Used total used per disk.
	sinfo.Total = []uint64{fsInfo.Total} // Total disk space per disk.
	sinfo.Available = []uint64{fsInfo.Available} // Total disk space available per disk.

	sinfo.Backend.Type = minio.BackendGateway
	sinfo.Backend.GatewayOnline = true
	return sinfo
}

type gfarmController struct {
	gfarmScheme, gfarmSharedDir, gfarmSharedVirtualNamePath string
}

type cacheController struct {
	cacheRootdir string
	fasterTotal, fasterLimit, fasterMax int64
	mutex *sync.Mutex
	enable_partfile_digest bool
}

// gfarmObjects implements gateway for Minio and S3 compatible object storage servers.
type gfarmObjects struct {
	minio.GatewayUnsupported
	gfarmctl *gfarmController
	cachectl *cacheController
	listPool *minio.TreeWalkPool
}

func gfarmToObjectErr(ctx context.Context, err error, params ...string) error {
	if err == nil {
		return nil
	}
	bucket := ""
	object := ""
	uploadID := ""
	switch len(params) {
	case 3:
		uploadID = params[2]
		fallthrough
	case 2:
		object = params[1]
		fallthrough
	case 1:
		bucket = params[0]
	}

	switch {
	case os.IsNotExist(err) || gf.IsNotExist(err):
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case os.IsExist(err) || gf.IsExist(err):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case errors.Is(err, syscall.ENOTEMPTY) || gf.IsENOTEMPTY(err):
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketNotEmpty{Bucket: bucket}
	default:
		logger.LogIf(ctx, err)
		return err
	}
}

// gfarmIsValidBucketName verifies whether a bucket name is valid.
func gfarmIsValidBucketName(bucket string) bool {
	return s3utils.CheckValidBucketNameStrict(bucket) == nil
}

func (n *gfarmObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	err := gfarmToObjectErr(ctx, gf.Remove(gfarm_url_bucket), bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "DeleteBucket", "Remove", gfarm_url_bucket, err)
	}
	return err
}

func (n *gfarmObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	err := gfarmToObjectErr(ctx, gf.Mkdir(gfarm_url_bucket, os.FileMode(0755)), bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "MakeBucketWithLocation", "Mkdir", gfarm_url_bucket, err)
	}
	return err
}

func (n *gfarmObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	fi, err := gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "GetBucketInfo", "Stat", gfarm_url_bucket, err)
		return bi, gfarmToObjectErr(ctx, err, bucket)
	}
	// As gfarm.Stat() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	gfarm_url_root := n.gfarm_url_PathJoin(gfarmSeparator)
	entries, err := gf.ReadDir(gfarm_url_root)
	if err != nil {
		logger.LogIf(ctx, err)
		gf.CheckError(GFARM_MSG_UNFIXED, "ListBuckets", "ReadDir", gfarm_url_root, err)
		return nil, gfarmToObjectErr(ctx, err)
	}

	for _, entry := range entries {
		// Ignore all reserved bucket names and invalid bucket names.
		if isReservedOrInvalidBucket(entry.Name(), false) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name: entry.Name(),
			// As gfarm.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: entry.ModTime(),
		})
	}

	// Sort bucket infos by bucket name.
	sort.Sort(byBucketName(buckets))
	return buckets, nil
}

func (n *gfarmObjects) listDirFactory() minio.ListDirFunc {
	// listDir - lists all the entries at a given prefix and given entry in the prefix.
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string) {

		gfarm_url_bucket_prefixDir := n.gfarm_url_PathJoin(gfarmSeparator, bucket, prefixDir)
		fis, err := gf.ReadDir(gfarm_url_bucket_prefixDir)
		if err != nil {
			if os.IsNotExist(err) || gf.IsNotExist(err) {
				err = nil
			}
			if err != nil {
				gf.CheckError(GFARM_MSG_UNFIXED, "listDirFactory", "ReadDir", gfarm_url_bucket_prefixDir, err)
			}
			logger.LogIf(minio.GlobalContext, err)
			return
		}

		if len(fis) == 0 {
			return true, nil
		}
		for _, fi := range fis {
			if isReservedOrInvalidBucket(fi.Name(), false) {
				continue
			}
			if fi.IsDir() {
				entries = append(entries, fi.Name() + gfarmSeparator)
			} else {
				entries = append(entries, fi.Name())
			}
		}
		return false, minio.FilterMatchingPrefix(entries, prefixEntry)
	}

	// Return list factory instance.
	return listDir
}

// ListObjects lists all blobs in GFARM bucket filtered by prefix.
func (n *gfarmObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	if _, err := gf.Stat(gfarm_url_bucket); err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "ListObjects", "Stat", gfarm_url_bucket, err)
		return loi, gfarmToObjectErr(ctx, err, bucket)
	}

	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		gfarm_url_bucket_entry := n.gfarm_url_PathJoin(gfarmSeparator, bucket, entry)
		fi, err := gf.Stat(gfarm_url_bucket_entry)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "ListObjects", "Stat", gfarm_url_bucket_entry, err)
			return minio.ObjectInfo{}, gfarmToObjectErr(ctx, err, bucket, entry)
		}
		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    entry,
			ModTime: fi.ModTime(),
			Size:    fi.Size(),
			IsDir:   fi.IsDir(),
			AccTime: fi.ModTime(),
		}, nil
	}

	return minio.ListObjects(ctx, n, bucket, prefix, marker, delimiter, maxKeys, n.listPool, n.listDirFactory(), getObjectInfo, getObjectInfo)
}

// deleteObject deletes a file path if its empty. If it's successfully deleted,
// it will recursively move up the tree, deleting empty parent directories
// until it finds one with files in it. Returns nil for a non-empty directory.
func (n *gfarmObjects) deleteObject(basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}

	// Attempt to remove path.
	gfarm_url_deletePath := n.gfarm_url_PathJoin(deletePath)
	if err := gf.Remove(gfarm_url_deletePath); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) || gf.IsENOTEMPTY(err) {
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		}
		gf.CheckError(GFARM_MSG_UNFIXED, "deleteObject", "Remove", gfarm_url_deletePath, err)
		return err
	}

	// Trailing slash is removed when found to ensure
	// slashpath.ir() to work as intended.
	deletePath = strings.TrimSuffix(deletePath, gfarmSeparator)
	deletePath = path.Dir(deletePath)

	// Delete parent directory. Errors for parent directories shouldn't trickle down.
	n.deleteObject(basePath, deletePath)

	return nil
}

// ListObjectsV2 lists all blobs in GFARM bucket filtered by prefix
func (n *gfarmObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := n.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}
	return minio.ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

func (n *gfarmObjects) DeleteObject(ctx context.Context, bucket, object string) error {
	return gfarmToObjectErr(ctx, n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), minio.PathJoin(gfarmSeparator, bucket, object)), bucket, object)
}

func (n *gfarmObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = n.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

func (n *gfarmObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := n.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	startOffset, length, err := rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		nerr := n.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)

}

func (n *gfarmObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(minio.PathJoin(gfarmSeparator, srcBucket, srcObject), minio.PathJoin(gfarmSeparator, dstBucket, dstObject))
	if cpSrcDstSame {
		return n.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	return n.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (n *gfarmObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	if _, err := gf.Stat(gfarm_url_bucket); err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "GetObject", "Stat", gfarm_url_bucket, err)
		return gfarmToObjectErr(ctx, err, bucket)
	}
	gfarm_url_bucket_key := n.gfarm_url_PathJoin(gfarmSeparator, bucket, key)
	rd, err := gf.OpenFile(gfarm_url_bucket_key, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "GetObject", "OpenFile", gfarm_url_bucket_key, err)
		return gfarmToObjectErr(ctx, err, bucket, key)
	}
	defer rd.Close()
	_, err = io.Copy(writer, io.NewSectionReader(rd, startOffset, length))
	if err == io.ErrClosedPipe {
		// gfarm library doesn't send EOF correctly, so io.Copy attempts
		// to write which returns io.ErrClosedPipe - just ignore
		// this for now.
		err = nil
	}
	return gfarmToObjectErr(ctx, err, bucket, key)
}

func (n *gfarmObjects) isObjectDir(ctx context.Context, bucket, object string) bool {
	gfarm_url_bucket_object := n.gfarm_url_PathJoin(gfarmSeparator, bucket, object)
	fis, err := gf.ReadDir(gfarm_url_bucket_object)
	if err != nil {
		if os.IsNotExist(err) || gf.IsNotExist(err) {
			return false
		}
		gf.CheckError(GFARM_MSG_UNFIXED, "isObjectDir", "ReadDir", gfarm_url_bucket_object, err)
		logger.LogIf(ctx, err)
		return false
	}
	return len(fis) == 0
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *gfarmObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "GetObjectInfo", "Stat", gfarm_url_bucket, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}
	if strings.HasSuffix(object, gfarmSeparator) && !n.isObjectDir(ctx, bucket, object) {
		return objInfo, gfarmToObjectErr(ctx, os.ErrNotExist, bucket, object)
	}

	gfarm_url_bucket_object := n.gfarm_url_PathJoin(gfarmSeparator, bucket, object)
	fi, err := gf.Stat(gfarm_url_bucket_object)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "GetObjectInfo", "Stat", gfarm_url_bucket_object, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v PutObject %q start\n", myformat(start), object)

	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "Stat", gfarm_url_bucket, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}

	name := minio.PathJoin(gfarmSeparator, bucket, object)

	// If its a directory create a prefix ??<
	gfarm_url_name := n.gfarm_url_PathJoin(name)
	if strings.HasSuffix(object, gfarmSeparator) && r.Size() == 0 {
		//gfarm_url_name := n.gfarm_url_PathJoin(name)
		if err = gf.MkdirAll(gfarm_url_name, os.FileMode(0755)); err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "MkdirAll", gfarm_url_name, err)
			n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), name)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	} else {
		tmpname := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, minio.MustGetUUID())
		gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
		w, err := gf.OpenFile(gfarm_url_tmpname, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "OpenFile", gfarm_url_tmpname, err)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		defer n.deleteObject(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket), tmpname)
		if _, err = myCopy(w, r); err != nil {
			w.Close()
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		dir := path.Dir(name)
		if dir != "" {
			gfarm_url_dir := n.gfarm_url_PathJoin(dir)
			if err = gf.MkdirAll(gfarm_url_dir, os.FileMode(0755)); err != nil {
				gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "MkdirAll", gfarm_url_dir, err)
				w.Close()
				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
				return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
			}
		}
		w.Close()
		if err = gf.Rename(gfarm_url_tmpname, gfarm_url_name); err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "Rename", gfarm_url_name, err)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := gf.Stat(gfarm_url_name)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "PutObject", "Stat", gfarm_url_name, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}
	info := minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) PutObject %q end\n", myformat(now), now.Sub(start), object)
	return info, nil
}

func (n *gfarmObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "NewMultipartUpload", "Stat", gfarm_url_bucket, err)
		return uploadID, gfarmToObjectErr(ctx, err, bucket)
	}

	uploadID = minio.MustGetUUID()
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	if err = gf.Mkdir(gfarm_url_dirName, os.FileMode(0755)); err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "NewMultipartUpload", "Mkdir", gfarm_url_dirName, err)
		return uploadID, gfarmToObjectErr(ctx, err, bucket)
	}
	if n.cachectl != nil {
		gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
		if err = os.Mkdir(gfarm_cache_dirName, os.FileMode(0755)); err != nil {
			return uploadID, gfarmToObjectErr(ctx, err, bucket)
		}
	}

	return uploadID, nil
}

func (n *gfarmObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "ListMultipartUploads", "Stat", gfarm_url_bucket, err)
		return lmi, gfarmToObjectErr(ctx, err, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *gfarmObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	_, err = gf.Stat(gfarm_url_dirName)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "checkUploadIDExists", "Stat", gfarm_url_dirName, err)
		return gfarmToObjectErr(ctx, err, bucket, object, uploadID)
	}
	if n.cachectl != nil {
		gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
		_, err = os.Stat(gfarm_cache_dirName)
		if err != nil {
			return gfarmToObjectErr(ctx, err, bucket, object, uploadID)
		}
	}
	return nil
}

func (n *gfarmObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "ListObjectParts", "Stat", gfarm_url_bucket, err)
		return result, gfarmToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	// It's decided not to support List parts, hence returning empty result.
	return result, nil
}

func (n *gfarmObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *gfarmObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v PutObjectPart %q start\n", myformat(start), object)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "PutObjectPart", "Stat", gfarm_url_bucket, err)
		return info, gfarmToObjectErr(ctx, err, bucket)
	}

	partName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, fmt.Sprintf("%05d", partID))
	err = n.copyToCachedFile(ctx, bucket, object, r, partName)
	if err != nil {
		return info, gfarmToObjectErr(ctx, err, bucket, object, uploadID)
	}

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) PutObjectPart %q end\n", myformat(now), now.Sub(start), object)

	return info, nil
}

func (n *gfarmObjects) copyToCachedFile(ctx context.Context, bucket, object string, r *minio.PutObjReader, partName string) error {
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	w, err := gf.OpenFile(gfarm_url_partName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "copyToCachedFile", "OpenFile", gfarm_url_partName, err)
		return err
	}
	defer func() { if w != nil { w.Close() } }()

	if n.cachectl != nil {
		var value int64 = 0
		err = gf.LSetXattr(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), unsafe.Sizeof(value), gf.GFS_XATTR_CREATE)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "copyToCachedFile", "LSetXattr", gfarm_url_partName, err)
			return err
		}

		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		w_cache, err := os.OpenFile(gfarm_cache_partName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			return err
		}
		ww, err := n.cachectl.NewCacheReadWriter(w, w_cache)
		defer ww.Close()
		w = nil

		_, err = myCopy(ww, r.Reader)
		if err != nil {
			return err
		}

		value = ww.GetWrittenSize()
		err = gf.LSetXattr(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), unsafe.Sizeof(value), gf.GFS_XATTR_REPLACE)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "copyToCachedFile", "LSetXattr", gfarm_url_partName, err)
			return err
		}

		hash := ww.(*cachedFile).hash
		if hash != nil {
			var hash_size uintptr = uintptr(hash.Size())
			ha := hash.Sum(nil)
			err = gf.LSetXattr(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), hash_size, gf.GFS_XATTR_CREATE)
			if err != nil {
				gf.CheckError(GFARM_MSG_UNFIXED, "copyToCachedFile", "LSetXattr", gfarm_url_partName, err)
				return err
			}
		}

	} else {
		_, err = io.Copy(w, r.Reader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *gfarmObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "CompleteMultipartUpload", "Stat", gfarm_url_bucket, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := minio.PathJoin(gfarmSeparator, bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		gfarm_url_dir := n.gfarm_url_PathJoin(dir)
		if err = gf.MkdirAll(gfarm_url_dir, os.FileMode(0755)); err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "CompleteMultipartUpload", "MkdirAll", gfarm_url_dir, err)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}

	tmpname := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, "00000")
	gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
	w, err := gf.OpenFile(gfarm_url_tmpname, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "CompleteMultipartUpload", "OpenFile", gfarm_url_tmpname, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}

	for _, part := range parts {
		partName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, fmt.Sprintf("%05d", part.PartNumber))
		err = n.copyFromCachedFile(ctx, bucket, object, w, partName)
		if err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}

	err = w.Close()
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}
	gfarm_url_name := n.gfarm_url_PathJoin(name)
	err = gf.Rename(gfarm_url_tmpname, gfarm_url_name)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "CompleteMultipartUpload", "Rename", gfarm_url_name, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}

	fi, err := gf.Stat(gfarm_url_name)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "CompleteMultipartUpload", "Stat", gfarm_url_name, err)
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}

	err = n.cleanupMultipartUploadDir(uploadID)
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5 := minio.ComputeCompleteMultipartMD5(parts)

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    s3MD5,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) copyFromCachedFile(ctx context.Context, bucket, object string, w *gf.File, partName string) error {
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	r, err := gf.OpenFile(gfarm_url_partName, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "copyFromCachedFile", "OpenFile", gfarm_url_partName, err)
		return err
	}
	defer func() { if r != nil { r.Close() } }()
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		r_cache, err := os.OpenFile(gfarm_cache_partName, os.O_RDONLY, os.FileMode(0644))
		if err != nil {
			return err
		}
		var value int64
		size := unsafe.Sizeof(value)
		err = gf.LGetXattrCached(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), &size)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "copyFromCachedFile", "LGetXattrCached", gfarm_url_partName, err)
			return err
		}
		rr, err := n.cachectl.NewCacheReadWriter(r, r_cache)
		rr.SetWrittenSize(value)
		defer rr.Close()
		r = nil
		_, err = myCopy(w, rr)
		if err != nil {
			return err
		}
		hash := rr.(*cachedFile).hash
		if hash != nil {
			var hash_size uintptr = uintptr(hash.Size())
			ha := make([]byte, hash_size)
			hb := hash.Sum(nil)

			err = gf.LGetXattrCached(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), &hash_size)
			if err != nil {
				gf.CheckError(GFARM_MSG_UNFIXED, "copyFromCachedFile", "LGetXattrCached", gfarm_url_partName, err)
				return err
			}

			if bytes.Compare(hb, ha) != 0 {
				return io.ErrNoProgress
			} else {
			}
		}

	} else {
		_, err = io.Copy(w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *gfarmObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "AbortMultipartUpload", "Stat", gfarm_url_bucket, err)
		return gfarmToObjectErr(ctx, err, bucket)
	}
	return gfarmToObjectErr(ctx, n.cleanupMultipartUploadDir(uploadID), bucket, object, uploadID)
}

func (n *gfarmObjects) cleanupMultipartUploadDir(uploadID string) error {
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)

	if n.cachectl != nil {
		gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
		oentries, err := ioutil.ReadDir(gfarm_cache_dirName)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "ReadDir", gfarm_cache_dirName, err)
			return err
		}
		for _, entry := range oentries {
			gfarm_cache_dirName_EntryName := n.gfarm_cache_PathJoin(dirName, entry.Name())
			err = os.Remove(gfarm_cache_dirName_EntryName)
			if err != nil {
				return err
			}
		}
		if os.Remove(gfarm_cache_dirName) != nil {
			return err
		}
	}

	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	entries, err := gf.ReadDir(gfarm_url_dirName)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "ReadDir", gfarm_url_dirName, err)
		return err
	}
	for _, entry := range entries {
		if n.cachectl != nil {
			var value int64
			size := unsafe.Sizeof(value)
			gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
			err = gf.LGetXattrCached(gfarm_url_dirName_entryName, gfarmS3OffsetKey, unsafe.Pointer(&value), &size)
			if err != nil {
				gf.CheckError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "LGetXattrCached", gfarm_url_dirName_entryName, err)
				return err
			}
			n.cachectl.mutex.Lock()
			n.cachectl.fasterTotal -= value
			n.cachectl.mutex.Unlock()
		}
		gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
		err = gf.Remove(gfarm_url_dirName_entryName)
		if err != nil {
			gf.CheckError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "Remove", gfarm_url_dirName_entryName, err)
			return err
		}
	}

	err = gf.Remove(gfarm_url_dirName)
	if err != nil {
		gf.CheckError(GFARM_MSG_UNFIXED, "cleanupMultipartUploadDir", "Remove", gfarm_url_dirName, err)
	}
	return err
}

// IsReady returns whether the layer is ready to take requests.
func (n *gfarmObjects) IsReady(_ context.Context) bool {
	return true
}

type FileReadWriter interface {
	Close() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
}

type CachedFileReadWriter interface {
	Close() error
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
	GetWrittenSize() int64
	SetWrittenSize(int64) ()
	GetReadSize() int64
}

type cachedFile struct {
	slower, faster FileReadWriter
	c *cacheController
	fasterWrittenSize, fasterReadSizeSoFar int64
	hash hash.Hash
}

func (c *cacheController) NewCacheReadWriter(slower, faster FileReadWriter) (CachedFileReadWriter, error) {
	var hash hash.Hash = nil
	if c.enable_partfile_digest {
		hash = md5.New()
	}
	return &cachedFile{slower, faster, c, 0, 0, hash}, nil
}

func (f *cachedFile) GetWrittenSize() int64 {
	return f.fasterWrittenSize
}

func (f *cachedFile) SetWrittenSize(n int64) () {
	f.fasterWrittenSize = n
	if n == 0 {
		f.faster.Close()
		f.faster = nil
	}
}

func (f *cachedFile) GetReadSize() int64 {
	return f.fasterReadSizeSoFar
}

func (f *cachedFile) Close() error {
	if f.faster != nil {
		f.faster.Close()
	}
	return f.slower.Close()
}

func (f *cachedFile) Read(b []byte) (int, error) {
	n, e := f.read(b)
	if f.hash != nil {
		io.Copy(f.hash, bytes.NewReader(b[:n]))		// ignore error
	}
	return n, e
}

func (f *cachedFile) read(b []byte) (int, error) {
	if f.faster != nil {
		n, e := f.faster.Read(b)

		if e != nil && e != io.EOF {
			return n, e
		}

		newReadSize := f.fasterReadSizeSoFar + int64(n)

		if newReadSize >= f.fasterWrittenSize {
			/* e == nil || e == io.EOF */
			if newReadSize == f.fasterWrittenSize {
				// do nothing
			} else {
				// do nothing
			}
			n = int(f.fasterWrittenSize - f.fasterReadSizeSoFar)
			f.faster.Close()
			f.faster = nil
			if n == 0 {
				return 0, io.ErrUnexpectedEOF
			}
			return n, nil
		}

		/* newReadSize < f.fasterWrittenSize */

		if e == io.EOF {
			f.faster.Close()
			f.faster = nil
			return 0, io.ErrUnexpectedEOF
		}

		/* e == nil */

		f.fasterReadSizeSoFar = newReadSize

		return n, nil
	}
	return f.slower.Read(b)
}

func (f *cachedFile) Write(b []byte) (int, error) {
	if f.hash != nil {
		io.Copy(f.hash, bytes.NewReader(b))	// ignore error
	}
	if f.faster != nil {
		n := len(b)
		if f.c.fasterTotal + int64(n) < f.c.fasterLimit {
			n, e := f.faster.Write(b)
			f.c.mutex.Lock()
			f.c.fasterTotal += int64(n)
			if f.c.fasterMax < f.c.fasterTotal {
				f.c.fasterMax = f.c.fasterTotal
			}
			f.c.mutex.Unlock()
			f.fasterWrittenSize += int64(n)
			return n, e
		}
		f.faster.Close()
		f.faster = nil
		// FALLTHRU
	}
	return f.slower.Write(b)
}

func myformat(now time.Time) string {
	return now.UTC().Format("20060102T030405.000000Z")
}

func myCopy(w io.Writer, r io.Reader) (int64, error) {
return io.Copy(w, r)
	var total int64
	total = 0
	buf := make([]byte, 1024 * 1024 * 32)
now := time.Now()
start := now
fmt.Fprintf(os.Stderr, "@@@ %v myCopy Start\n", myformat(start))
	for {
		len, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) myCopy Read %d bytes\n", myformat(now), now.Sub(start), len)
		w.Write(buf[:len])
now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) myCopy Wrote %d bytes\n", myformat(now), now.Sub(start), len)
		total += int64(len)
	}
now = time.Now()
fmt.Fprintf(os.Stderr, "@@@ %v (%v) myCopy End total %d bytes\n", myformat(now), now.Sub(start), total)
	return total, nil
}

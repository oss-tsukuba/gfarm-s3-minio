/*
 *
 * gfarmSeparator を concatenate するタイミング (確認済み)
 *
 *
 * gf.大文字 <=> 第一引数 は、名前 がgfarm_url_で始まる 変数
/gf\.[A-Z]
/(gfarm_url_
 * &&
 * gfarm_url_で始まる 変数 には、n.gfarm_url_PathJoin を使用する
/gfarm_url_[a-zA-Z :]*=
 *
 *
 * os.大文字 <=> 第一引数 は、名前 が gfarm_cache_で始まる 変数
 * ioutil.大文字 <=> 第一引数 は、名前 がgfarm_cache_で始まる 変数
/os\.[A-Z][a-zA-Z_]*(
/os\.[A-Z][a-zA-Z_]*(gfarm_cache
/os\.[A-Z][a-zA-Z_]*(gfarm_url
/(gfarm_cache
 * &&
 * gfarm_cache_で始まる 変数 には、n.gfarm_cache_PathJoin を使用する
/gfarm_cache_[a-zA-Z :]*=
 *
 *
 * gfarmObjectsのメソッドのうち
 * 大文字 で 始まる メソッド
/\<gfarmObjects) [A-Z].*\<error\>
 * は、エラーを gfarmToObjectErr(ctx, err, bucket, object) して かえす
 * 例外 DeleteObjects
 * 再帰 の さきが 大文字 のメソッド なら、
 * 本ルールによりそのままかえせば良いことに注意
 *
 * 逆に、gfarmObjectsのメソッドのうち
 * 小文字 で 始まる もの
/\<gfarmObjects) [a-z].*\<error\>
 * は、生のエラーをかえす (gfarmToObjectErrしない)
 * 例外: checkUploadIDExistsのかえりちは、gfarmToObjectErrされている
 */

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
//	"net"
	"net/http"
	"os"
//	"os/user"
	"path"
	"strconv"
	"sort"
	"sync"
	"strings"
	"syscall"
	"time"
	"unsafe"
	"github.com/minio/minio/pkg/env"

	//gf "github.com/minio/minio/cmd/gateway/gfarm/gfarmClient"
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
		gfarmPath = minio.PathJoin(n.gfarmctl.gfarmSharedDir, s3path[len(n.gfarmctl.gfarmSharedVirtualNamePath):])
	} else {
		gfarmPath = minio.PathJoin(n.gfarmctl.gfarmHomedir, s3path)
	}
	result := n.gfarmctl.gfarmScheme + gfarmPath
	for _, v := range pathComponents {
		fmt.Fprintf(os.Stderr, "@@@ gfarm_url_PathJoin    %q\n", v)
	}
	fmt.Fprintf(os.Stderr, "@@@ gfarm_url_PathJoin => %q\n", result)
	return result
}

func (n *gfarmObjects) gfarm_cache_PathJoin(pathComponents ...string) string {
	return minio.PathJoin(n.cachectl.cacheRootdir, minio.PathJoin(pathComponents...))
}

// NewGatewayLayer returns gfarm gatewaylayer.
func (g *GFARM) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {

fmt.Fprintf(os.Stderr, "@@@ creds = %v\n", creds)
	for i, s := range g.args {
fmt.Fprintf(os.Stderr, "@@@ args[%d] = %q\n", i, s)
	}

	gfarmScheme := ""
	gfarmHomedirName := g.args[0]
	gfarmSharedDir := g.args[1]
	gfarmSharedVirtualName := g.args[2]

//       gfarmHomedirName        "hpci005858"
//       gfarmSharedDir          "gfarm:///shared"
//       gfarmSharedVirtualName  "sss"
//
//    Gfarm                       -- S3 API
//    /shared                     -- アクセス不可
//    |-- hpci005858              -- バケット置き場 "s3://"
//    |   |-- .minio.sys          -- 不可視
//    |   |-- mybucket            -- バケット       "s3://mybucket"
//    |   `-- sss                 -- 仮想バケット
//    |       `-- hpci001971      -- プレフィックス "s3://sss/hpci001971"
//    |       .   |-- .minio.sys  -- 不可視
//    |       .   `-- bucket1     -- プレフィックス "s3://sss/hpci001971/bucket1"
//    |       .   .   `-- object1 -- オブジェクト
//    |       .    .. (bucket2)   -- bucket2はhpci001971直下のリストには現れない
//    |        .. (hpci001970)    -- hpci001970はsss直下のリストには現れない
//    |
//    |-- hpci001971
//    |   |-- .minio.sys
//    |   |-- bucket1              -- ACLを用いてhpci005858にアクセスを許可
//    |   |   `-- object1
//    |   `-- bucket2              -- bucket2は公開しない
//    `-- hpci001970
//        |-- .minio.sys
//        `-- bucket3              -- bucket3は公開しない


	cacheRootdir := env.Get(gfarmCachePathEnvVar, "")
	cacheCapacity := getCacheSizeFromEnv(gfarmCacheSizeEnvVar, strconv.Itoa(gfarmDefaultCacheSize / humanize.MiByte))

	gfarmSharedDir = strings.TrimSuffix(gfarmSharedDir, gfarmSeparator)
	if strings.HasPrefix(gfarmSharedDir, constGfarmScheme + "/") {
		gfarmScheme = constGfarmScheme
		gfarmSharedDir = gfarmSharedDir[len(constGfarmScheme):]
	}

	gfarmHomedir := minio.PathJoin(gfarmSharedDir, gfarmHomedirName)

	cacheRootdir = strings.TrimSuffix(cacheRootdir, gfarmSeparator)

	gfarmSharedVirtualNamePath := minio.PathJoin("/", gfarmSharedVirtualName)

fmt.Fprintf(os.Stderr, "@@@ gfarmScheme = %q\n", gfarmScheme)
fmt.Fprintf(os.Stderr, "@@@ gfarmSharedDir = %q\n", gfarmSharedDir)
fmt.Fprintf(os.Stderr, "@@@ gfarmHomedir = %q\n", gfarmHomedir)
fmt.Fprintf(os.Stderr, "@@@ gfarmSharedVirtualName = %q\n", gfarmSharedVirtualName)
fmt.Fprintf(os.Stderr, "@@@ gfarmSharedVirtualNamePath = %q\n", gfarmSharedVirtualNamePath)
fmt.Fprintf(os.Stderr, "@@@ gfarmHomedirName = %q\n", gfarmHomedirName)
fmt.Fprintf(os.Stderr, "@@@ cacheRootdir = %q\n", cacheRootdir)
fmt.Fprintf(os.Stderr, "@@@ cacheCapacity = %d\n", cacheCapacity)

	err := gf.Gfarm_initialize()
	if err != nil {
		return nil, err
	}

	gfarmctl := &gfarmController{gfarmScheme, gfarmSharedDir, gfarmHomedir, gfarmSharedVirtualName, gfarmSharedVirtualNamePath, gfarmHomedirName}
	var cachectl *cacheController = nil
	if cacheRootdir != "" {
		partfile_digest := env.Get(gfarmPartfileDigestEnvVar, "")
		enable_partfile_digest := partfile_digest != "no"
		cachectl = &cacheController{cacheRootdir, 0, int64(cacheCapacity), 0, &sync.Mutex{}, enable_partfile_digest}
fmt.Fprintf(os.Stderr, "@@@ GFARMS3_PARTFILE_DIGEST = %q, enable_partfile_digest = %v\n", partfile_digest, enable_partfile_digest)
	}
	n := &gfarmObjects{gfarmctl: gfarmctl, cachectl: cachectl, listPool: minio.NewTreeWalkPool(time.Minute * 30)}

	gfarm_url_minioMetaTmpBucket := n.gfarm_url_PathJoin(gfarmSeparator, minioMetaTmpBucket)
	if err = gf.MkdirAll(gfarm_url_minioMetaTmpBucket, os.FileMode(0755)); err != nil {
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

	if i <= 0 || i > 100 {
		logger.LogIf(context.Background(), fmt.Errorf("ENV '%v' should be a floating point value between 0 and 100.\n"+
			"The upload chunk size is set to its default: %s\n", gfarmCacheSizeEnvVar, defaultValue))
		return gfarmDefaultCacheSize
	}

	return int(i * humanize.MiByte)
}

// Production - gfarm gateway is production ready.
func (g *GFARM) Production() bool {
	return true
}

func (n *gfarmObjects) Shutdown(ctx context.Context) error {
	return gf.Gfarm_terminate()
}

//type FsInfo struct {
//	Used, Total, Available uint64
// }

func (n *gfarmObjects) StorageInfo(ctx context.Context, _ bool) minio.StorageInfo {
	fsInfo, err := gf.StatFs()
	if err != nil {
		return minio.StorageInfo{}
	}

	sinfo := minio.StorageInfo{}

	sinfo.Used = []uint64{fsInfo.Used}  // Used total used per disk.
	sinfo.Total = []uint64{fsInfo.Total} // Total disk space per disk.
	sinfo.Available = []uint64{fsInfo.Available} // Total disk space available per disk.

fmt.Fprintf(os.Stderr, "@@@ Total:%d  Used:%d  Available:%d\n", sinfo.Total[0]/1024, sinfo.Used[0]/1024, sinfo.Available[0]/1024)

	sinfo.Backend.Type = minio.BackendGateway
	sinfo.Backend.GatewayOnline = true
	return sinfo
}

type gfarmController struct {
	gfarmScheme, gfarmSharedDir, gfarmHomedir,
	gfarmSharedVirtualName, gfarmSharedVirtualNamePath, gfarmHomedirName string
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
	case errors.Is(err, syscall.ENOTEMPTY):
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
fmt.Fprintf(os.Stderr, "@@@ DeleteBucket %q\n", bucket)
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	return gfarmToObjectErr(ctx, gf.Remove(gfarm_url_bucket), bucket)
}

func (n *gfarmObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
fmt.Fprintf(os.Stderr, "@@@ MakeBucketWithLocation %q\n", bucket)
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	return gfarmToObjectErr(ctx, gf.Mkdir(gfarm_url_bucket, os.FileMode(0755)), bucket)
}

func (n *gfarmObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ GetBucketInfo %q\n", bucket)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	fi, err := gf.Stat(gfarm_url_bucket)
	if err != nil {
		return bi, gfarmToObjectErr(ctx, err, bucket)
	}
	// As gfarm.Stat() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
//fmt.Fprintf(os.Stderr, "@@@ ListBuckets %q\n", buckets)
	gfarm_url_root := n.gfarm_url_PathJoin(gfarmSeparator)
	entries, err := gf.ReadDir(gfarm_url_root)
	if err != nil {
		logger.LogIf(ctx, err)
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

fmt.Fprintf(os.Stderr, "@@@ listDirFactory bucket = %q  prefixDir = %q\n", bucket, prefixDir)
		gfarm_url_bucket_prefixDir := n.gfarm_url_PathJoin(gfarmSeparator, bucket, prefixDir)
		fis, err := gf.ReadDir(gfarm_url_bucket_prefixDir)
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			logger.LogIf(minio.GlobalContext, err)
			return
		}

		if len(fis) == 0 {
			return true, nil
		}
		for _, fi := range fis {
fmt.Fprintf(os.Stderr, "@@@ listDirFactory fi.Name() = %q\n", fi.Name())
			if isReservedOrInvalidBucket(fi.Name(), false) {
fmt.Fprintf(os.Stderr, "@@@ listDirFactory RESERVED %q\n", fi.Name())
				continue
			}
			if bucket == n.gfarmctl.gfarmSharedVirtualName && fi.Name() == n.gfarmctl.gfarmHomedirName {
fmt.Fprintf(os.Stderr, "@@@ listDirFactory EXCLUDE %q\n", fi.Name())
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
fmt.Fprintf(os.Stderr, "@@@ ListObjects %q\n", bucket)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	if _, err := gf.Stat(gfarm_url_bucket); err != nil {
		return loi, gfarmToObjectErr(ctx, err, bucket)
	}

	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		gfarm_url_bucket_entry := n.gfarm_url_PathJoin(gfarmSeparator, bucket, entry)
		fi, err := gf.Stat(gfarm_url_bucket_entry)
		if err != nil {
			return minio.ObjectInfo{}, gfarmToObjectErr(ctx, err, bucket, entry)
		}
		return minio.ObjectInfo{
			Bucket:  bucket,
			Name:    entry,
			ModTime: fi.ModTime(),
			Size:    fi.Size(),
			IsDir:   fi.IsDir(),
			//AccTime: fi.AccessTime(),
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
		if errors.Is(err, syscall.ENOTEMPTY) {
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		}
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
fmt.Fprintf(os.Stderr, "@@@ ListObjectsV2 %q\n", bucket)
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
fmt.Fprintf(os.Stderr, "@@@ DeleteObject %q %q\n", bucket, object)
	return gfarmToObjectErr(ctx, n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), minio.PathJoin(gfarmSeparator, bucket, object)), bucket, object)
}

func (n *gfarmObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
fmt.Fprintf(os.Stderr, "@@@ DeleteObjects %q %v\n", bucket, objects)
	errs := make([]error, len(objects))
	for idx, object := range objects {
		errs[idx] = n.DeleteObject(ctx, bucket, object)
	}
	return errs, nil
}

func (n *gfarmObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
fmt.Fprintf(os.Stderr, "@@@ GetObjectNInfo %q %q\n", bucket, object)
	objInfo, err := n.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	//var startOffset, length int64
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
fmt.Fprintf(os.Stderr, "@@@ CopyObject %q %q %q %q\n", srcBucket, srcObject, dstBucket, dstObject)
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
fmt.Fprintf(os.Stderr, "@@@ GetObject %q %q\n", bucket, key)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	if _, err := gf.Stat(gfarm_url_bucket); err != nil {
		return gfarmToObjectErr(ctx, err, bucket)
	}
	gfarm_url_bucket_key := n.gfarm_url_PathJoin(gfarmSeparator, bucket, key)
	rd, err := gf.OpenFile(gfarm_url_bucket_key, os.O_RDONLY, os.FileMode(0644))
	if err != nil {
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
		if os.IsNotExist(err) {
			return false
		}
		logger.LogIf(ctx, err)
		return false
	}
	return len(fis) == 0
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *gfarmObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ GetObjectInfo %q %q\n", bucket, object)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}
	if strings.HasSuffix(object, gfarmSeparator) && !n.isObjectDir(ctx, bucket, object) {
		return objInfo, gfarmToObjectErr(ctx, os.ErrNotExist, bucket, object)
	}

	gfarm_url_bucket_object := n.gfarm_url_PathJoin(gfarmSeparator, bucket, object)
	fi, err := gf.Stat(gfarm_url_bucket_object)
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		//AccTime: fi.AccessTime(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ PutObject %q %q\n", bucket, object)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}

	name := minio.PathJoin(gfarmSeparator, bucket, object)

	// If its a directory create a prefix {
	gfarm_url_name := n.gfarm_url_PathJoin(name)
	if strings.HasSuffix(object, gfarmSeparator) && r.Size() == 0 {
		//gfarm_url_name := n.gfarm_url_PathJoin(name)
		if err = gf.MkdirAll(gfarm_url_name, os.FileMode(0755)); err != nil {
			n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), name)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	} else {
		tmpname := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, minio.MustGetUUID())
		gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
		w, err := gf.OpenFile(gfarm_url_tmpname, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		defer n.deleteObject(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket), tmpname)
		if _, err = io.Copy(w, r); err != nil {
			w.Close()
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		dir := path.Dir(name)
		if dir != "" {
			gfarm_url_dir := n.gfarm_url_PathJoin(dir)
			if err = gf.MkdirAll(gfarm_url_dir, os.FileMode(0755)); err != nil {
				w.Close()
				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
				return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
			}
		}
		w.Close()
		//gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
		//gfarm_url_name := n.gfarm_url_PathJoin(name)
		if err = gf.Rename(gfarm_url_tmpname, gfarm_url_name); err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}
	//gfarm_url_name := n.gfarm_url_PathJoin(name)
	fi, err := gf.Stat(gfarm_url_name)
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime(),
		Size:    fi.Size(),
		IsDir:   fi.IsDir(),
		//AccTime: fi.AccessTime(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
fmt.Fprintf(os.Stderr, "@@@ NewMultipartUpload %q %q\n", bucket, object)
defer fmt.Fprintf(os.Stderr, "@@@ NewMultipartUpload EXIT %q %q\n", bucket, object)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		return uploadID, gfarmToObjectErr(ctx, err, bucket)
	}

	uploadID = minio.MustGetUUID()
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	if err = gf.Mkdir(gfarm_url_dirName, os.FileMode(0755)); err != nil {
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
fmt.Fprintf(os.Stderr, "@@@ ListMultipartUploads %q %q\n", bucket, prefix)
defer fmt.Fprintf(os.Stderr, "@@@ ListMultipartUploads EXIT %q %q\n", bucket, prefix)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		return lmi, gfarmToObjectErr(ctx, err, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *gfarmObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExists %q %q %q\n", bucket, object, uploadID)
defer fmt.Fprintf(os.Stderr, "@@@ checkUploadIDExists EXIT %q %q %q\n", bucket, object, uploadID)
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)
	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	_, err = gf.Stat(gfarm_url_dirName)
	if err != nil {
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
fmt.Fprintf(os.Stderr, "@@@ ListObjectParts %q %q %q\n", bucket, object, uploadID)
defer fmt.Fprintf(os.Stderr, "@@@ ListObjectParts EXIT %q %q %q\n", bucket, object, uploadID)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
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
fmt.Fprintf(os.Stderr, "@@@ CopyObjectPart srcBucket:%q srcObject:%q dstBucket:%q dstObject:%q uploadID:%q partID:%d startOffset:%d length:%d\n", srcBucket, srcObject, dstBucket, dstObject, uploadID, partID, startOffset, length)
defer fmt.Fprintf(os.Stderr, "@@@ CopyObjectPart EXIT srcBucket:%q srcObject:%q dstBucket:%q dstObject:%q uploadID:%q partID:%d startOffset:%d length:%d\n", srcBucket, srcObject, dstBucket, dstObject, uploadID, partID, startOffset, length)
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *gfarmObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ PutObjectPart bucket:%q object:%q uploadID:%q partID:%d\n", bucket, object, uploadID, partID)
defer fmt.Fprintf(os.Stderr, "@@@ PutObjectPart EXIT bucket:%q object:%q uploadID:%q partID:%d\n", bucket, object, uploadID, partID)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
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

fmt.Fprintf(os.Stderr, "@@@ INFO_HASH = %q\n", info.ETag)

	return info, nil
}

func (n *gfarmObjects) copyToCachedFile(ctx context.Context, bucket, object string, r *minio.PutObjReader, partName string) error {
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	w, err := gf.OpenFile(gfarm_url_partName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return err
	}
	defer func() { if w != nil { w.Close() } }()
	//defer w.Close()

	if n.cachectl != nil {
		var value int64 = 0
//fmt.Fprintf(os.Stderr, "@@@ LSetXattr %s = %x (%d)\n", gfarmS3OffsetKey, value, unsafe.Sizeof(value))
//XXX
		//gfarm_url_partName := n.gfarm_url_PathJoin(partName)
		err = gf.LSetXattr(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), unsafe.Sizeof(value), gf.GFS_XATTR_CREATE)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@@@ LSetXattr ERROR: %q %q\n", gfarmS3OffsetKey, err)
		}

		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		w_cache, err := os.OpenFile(gfarm_cache_partName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))
		if err != nil {
			//w.Close()	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			return err
		}
		//defer w_cache.Close()
		ww, err := n.cachectl.NewCacheReadWriter(w, w_cache)
		defer ww.Close()
		w = nil

		_, err = io.Copy(ww, r.Reader)
		if err != nil {
			return err
		}

		value = ww.GetWrittenSize()
//fmt.Fprintf(os.Stderr, "@@@ LSetXattr %s = %d (%d)\n", gfarmS3OffsetKey, value, unsafe.Sizeof(value))
		//gfarm_url_partName := n.gfarm_url_PathJoin(partName)
		err = gf.LSetXattr(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), unsafe.Sizeof(value), gf.GFS_XATTR_REPLACE)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@@@ LSetXattr ERROR: %q %q\n", gfarmS3OffsetKey, err)
		}

		hash := ww.(*cachedFile).hash
		if hash != nil {
			var hash_size uintptr = uintptr(hash.Size())
			ha := hash.Sum(nil)
fmt.Fprintf(os.Stderr, "@@@ WRITTERN_HASH = %x\n", ha)
			//gfarm_url_partName := n.gfarm_url_PathJoin(partName)
			err = gf.LSetXattr(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), hash_size, gf.GFS_XATTR_CREATE)
			if err != nil {
				fmt.Fprintf(os.Stderr, "@@@ LSetXattr ERROR: %q %q\n", gfarmS3DigestKey, err)
			}
		}

	} else {
		//defer w.Close()
		_, err = io.Copy(w, r.Reader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *gfarmObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ CompleteMultipartUpload bucket:%q object:%q  parts:%v\n", bucket, object, parts)
defer fmt.Fprintf(os.Stderr, "@@@ CompleteMultipartUpload EXIT bucket:%q object:%q  parts:%v\n", bucket, object, parts)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
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
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}

	//var w *gf.File
	tmpname := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, "00000")
	gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
	w, err := gf.OpenFile(gfarm_url_tmpname, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, os.FileMode(0644))

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
	//gfarm_url_tmpname := n.gfarm_url_PathJoin(tmpname)
	gfarm_url_name := n.gfarm_url_PathJoin(name)
	err = gf.Rename(gfarm_url_tmpname, gfarm_url_name)

	//gfarm_url_name := n.gfarm_url_PathJoin(name)
	fi, err := gf.Stat(gfarm_url_name)
	if err != nil {
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
		//AccTime: fi.AccessTime(),
		AccTime: fi.ModTime(),
	}, nil
}

func (n *gfarmObjects) copyFromCachedFile(ctx context.Context, bucket, object string, w *gf.File, partName string) error {
	gfarm_url_partName := n.gfarm_url_PathJoin(partName)
	r, err := gf.OpenFile(gfarm_url_partName, os.O_RDONLY, os.FileMode(0644))
fmt.Fprintf(os.Stderr, "@@@ Copy %q => `w`\n", partName)
	if err != nil {
		return err
	}
	defer func() { if r != nil { r.Close() } }()
	//defer r.Close()
	if n.cachectl != nil {
		gfarm_cache_partName := n.gfarm_cache_PathJoin(partName)
		r_cache, err := os.OpenFile(gfarm_cache_partName, os.O_RDONLY, os.FileMode(0644))
		if err != nil {
			//r.Close()	//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			return err
		}
		//defer r_cache.Close()
		var value int64
		//var size uintptr
		size := unsafe.Sizeof(value)
		//gfarm_url_partName := n.gfarm_url_PathJoin(partName)
		err = gf.LGetXattrCached(gfarm_url_partName, gfarmS3OffsetKey, unsafe.Pointer(&value), &size)
		if err != nil {
			fmt.Fprintf(os.Stderr, "@@@ LGetXattrCached ERROR: %q\n", err)
		}
//fmt.Fprintf(os.Stderr, "@@@ LGetXattrCached %s = %d (%d)\n", gfarmS3OffsetKey, value, unsafe.Sizeof(value))
		rr, err := n.cachectl.NewCacheReadWriter(r, r_cache)
		rr.SetWrittenSize(value)
		defer rr.Close()
		r = nil
		_, err = io.Copy(w, rr)
		if err != nil {
			return err
		}
fmt.Fprintf(os.Stderr, "@@@ WRITTEN_SIZE = %d, READ_SIZE = %d\n", rr.GetWrittenSize(), rr.GetReadSize())
		hash := rr.(*cachedFile).hash
		if hash != nil {
			var hash_size uintptr = uintptr(hash.Size())
			ha := make([]byte, hash_size)
			hb := hash.Sum(nil)

			//gfarm_url_partName := n.gfarm_url_PathJoin(partName)
			err = gf.LGetXattrCached(gfarm_url_partName, gfarmS3DigestKey, unsafe.Pointer(&ha[0]), &hash_size)
			if err != nil {
				fmt.Fprintf(os.Stderr, "@@@ LGetXattrCached ERROR: %q\n", err)
			}

fmt.Fprintf(os.Stderr, "@@@ READBACK_HASH = %x %x\n", hb, ha)

			if bytes.Compare(hb, ha) != 0 {
fmt.Fprintf(os.Stderr, "@@@ READBACK_HASH MISMATCH\n")
				return io.ErrNoProgress
			} else {
fmt.Fprintf(os.Stderr, "@@@ READBACK_HASH OK\n")
			}
		}

	} else {
		//defer r.Close()
		_, err = io.Copy(w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *gfarmObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
fmt.Fprintf(os.Stderr, "@@@ AbortMultipartUpload %q %q\n", bucket, object)
defer fmt.Fprintf(os.Stderr, "@@@ AbortMultipartUpload EXIT %q %q\n", bucket, object)
	gfarm_url_bucket := n.gfarm_url_PathJoin(gfarmSeparator, bucket)
	_, err = gf.Stat(gfarm_url_bucket)
	if err != nil {
		return gfarmToObjectErr(ctx, err, bucket)
	}
	return gfarmToObjectErr(ctx, n.cleanupMultipartUploadDir(uploadID), bucket, object, uploadID)
}

func (n *gfarmObjects) cleanupMultipartUploadDir(uploadID string) error {
fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir %q\n", uploadID)
defer fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir EXIT %q\n", uploadID)
	dirName := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)

	if n.cachectl != nil {
		gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
		oentries, err := ioutil.ReadDir(gfarm_cache_dirName)
		if err != nil {
fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir ioutil.ReadDir ERROR: %q\n", gfarm_cache_dirName)
			return err
		}
		for _, entry := range oentries {
			gfarm_cache_dirName_EntryName := n.gfarm_cache_PathJoin(dirName, entry.Name())
			err = os.Remove(gfarm_cache_dirName_EntryName)
			if err != nil {
fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir os.Remove ERROR: %q\n", gfarm_cache_dirName_EntryName)
				return err
			}
		}
		//gfarm_cache_dirName := n.gfarm_cache_PathJoin(dirName)
		if os.Remove(gfarm_cache_dirName) != nil {
fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir os.Remove ERROR: %q\n", gfarm_cache_dirName)
			return err
		}
	}

	gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	entries, err := gf.ReadDir(gfarm_url_dirName)
	if err != nil {
fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir gf.ReadDir ERROR: %q\n", gfarm_url_dirName)
		return err
	}
	for _, entry := range entries {
		if n.cachectl != nil {
			var value int64
			//var size uintptr
			size := unsafe.Sizeof(value)
			gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
			err = gf.LGetXattrCached(gfarm_url_dirName_entryName, gfarmS3OffsetKey, unsafe.Pointer(&value), &size)
			if err != nil {
				fmt.Fprintf(os.Stderr, "@@@ LGetXattrCached ERROR: %q\n", err)
			}
//fmt.Fprintf(os.Stderr, "@@@ LGetXattrCached %s = %d (%d)\n", gfarmS3OffsetKey, value, unsafe.Sizeof(value))
			n.cachectl.mutex.Lock()
			n.cachectl.fasterTotal -= value
			n.cachectl.mutex.Unlock()
//fmt.Fprintf(os.Stderr, "@@@ n.cachectl.fasterTotal = %d, fasterLimit = %d, fasterMax = %d\n", n.cachectl.fasterTotal, n.cachectl.fasterLimit, n.cachectl.fasterMax)
		}
		gfarm_url_dirName_entryName := n.gfarm_url_PathJoin(dirName, entry.Name())
		err = gf.Remove(gfarm_url_dirName_entryName)
		if err != nil {
//fmt.Fprintf(os.Stderr, "@@@ cleanupMultipartUploadDir gf.Remove ERROR: %q\n", gfarm_url_dirName_entryName)
			return err
		}
	}

	//gfarm_url_dirName := n.gfarm_url_PathJoin(dirName)
	return gf.Remove(gfarm_url_dirName)
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
				fmt.Fprintf(os.Stderr, "@@@ JUST READ DONE\n")
				// do nothing
			} else {
				fmt.Fprintf(os.Stderr, "@@@ READING BEYOND WRITTEN SIZE\n")
				// do nothing
			}
			n = int(f.fasterWrittenSize - f.fasterReadSizeSoFar)
			f.faster.Close()
			f.faster = nil
			if n == 0 {
				fmt.Fprintf(os.Stderr, "@@@ SHOULD NOT HAPPEN\n")
				return 0, io.ErrUnexpectedEOF
			}
			return n, nil
		}

		/* newReadSize < f.fasterWrittenSize */

		if e == io.EOF {
			fmt.Fprintf(os.Stderr, "@@@ UNEXPECTED EOF\n")
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
	//var n int
	//var e error
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
//fmt.Fprintf(os.Stderr, "@@@ f.c.fasterTotal = %d\n", f.c.fasterTotal)
			f.fasterWrittenSize += int64(n)
			return n, e
		}
		// else switch to slower
		f.faster.Close()
		f.faster = nil
		// FALLTHRU
	}
	return f.slower.Write(b)
}

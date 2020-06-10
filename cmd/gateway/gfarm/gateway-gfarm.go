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
	"context"
	"errors"
	"fmt"
	"io"
//	"net"
	"net/http"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/minio/cli"
//	krb "github.com/minio/gokrb5/v7/client"
//	"github.com/minio/gokrb5/v7/config"
//	"github.com/minio/gokrb5/v7/credentials"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/env"
//	xnet "github.com/minio/minio/pkg/net"
)

const (
	gfarmBackend = "gfarm"

	gfarmSeparator = minio.SlashSeparator
)

func init() {
	const gfarmGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} GFARM-NAMENODE [GFARM-NAMENODE...]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
GFARM-NAMENODE:
  GFARM namenode URI

EXAMPLES:
  1. Start minio gateway server for GFARM backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} gfarm-rootdir
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

//func getKerberosClient() (*krb.Client, error) {
//	cfg, err := config.Load(env.Get("KRB5_CONFIG", "/etc/krb5.conf"))
//	if err != nil {
//		return nil, err
//	}
//
//	u, err := user.Current()
//	if err != nil {
//		return nil, err
//	}
//
//	// Determine the ccache location from the environment, falling back to the default location.
//	ccachePath := env.Get("KRB5CCNAME", fmt.Sprintf("/tmp/krb5cc_%s", u.Uid))
//	if strings.Contains(ccachePath, ":") {
//		if strings.HasPrefix(ccachePath, "FILE:") {
//			ccachePath = strings.TrimPrefix(ccachePath, "FILE:")
//		} else {
//			return nil, fmt.Errorf("unable to use kerberos ccache: %s", ccachePath)
//		}
//	}
//
//	ccache, err := credentials.LoadCCache(ccachePath)
//	if err != nil {
//		return nil, err
//	}
//
//	return krb.NewClientFromCCache(ccache, cfg)
//}

// NewGatewayLayer returns gfarm gatewaylayer.
func (g *GFARM) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
//	dialFunc := (&net.Dialer{
//		Timeout:   30 * time.Second,
//		KeepAlive: 30 * time.Second,
//		DualStack: true,
//	}).DialContext

	//hconfig, err := hadoopconf.LoadFromEnvironment()
	//if err != nil {
		//return nil, err
	//}

	//opts := gfarm.ClientOptionsFromConf(hconfig)
	opts := ClientOptionsFromConf()
//	opts.NamenodeDialFunc = dialFunc
//	opts.DatanodeDialFunc = dialFunc

	// Not addresses found, load it from command line.
//	if len(opts.Addresses) == 0 {
//		var addresses []string
//		for _, s := range g.args {
//			u, err := xnet.ParseURL(s)
//			if err != nil {
//				return nil, err
//			}
//			addresses = append(addresses, u.Host)
//		}
//		opts.Addresses = addresses
//	}

	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("Unable to lookup local user: %s", err)
	}

//	if opts.KerberosClient != nil {
//		opts.KerberosClient, err = getKerberosClient()
//		if err != nil {
//			return nil, fmt.Errorf("Unable to initialize kerberos client: %s", err)
//		}
//	} else {
		opts.User = env.Get("HADOOP_USER_NAME", u.Username)
//	}

	clnt, err := NewClient(opts)
	if err != nil {
		return nil, err
	}

	if err = clnt.MkdirAll(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket), os.FileMode(0755)); err != nil {
		return nil, err
	}

	return &gfarmObjects{clnt: clnt, listPool: minio.NewTreeWalkPool(time.Minute * 30)}, nil
}

// Production - gfarm gateway is production ready.
func (g *GFARM) Production() bool {
	return true
}

func (n *gfarmObjects) Shutdown(ctx context.Context) error {
	return n.clnt.Close()
}

func (n *gfarmObjects) StorageInfo(ctx context.Context, _ bool) minio.StorageInfo {
	fsInfo, err := n.clnt.StatFs()
	if err != nil {
		return minio.StorageInfo{}
	}
	sinfo := minio.StorageInfo{}
	sinfo.Used = []uint64{fsInfo.Used}
	sinfo.Backend.Type = minio.BackendGateway
	sinfo.Backend.GatewayOnline = true
	return sinfo
}

// gfarmObjects implements gateway for Minio and S3 compatible object storage servers.
type gfarmObjects struct {
	minio.GatewayUnsupported
	clnt     *Client
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
	case os.IsNotExist(err):
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case os.IsExist(err):
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
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	return gfarmToObjectErr(ctx, n.clnt.Remove(minio.PathJoin(gfarmSeparator, bucket)), bucket)
}

func (n *gfarmObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	if !gfarmIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	return gfarmToObjectErr(ctx, n.clnt.Mkdir(minio.PathJoin(gfarmSeparator, bucket), os.FileMode(0755)), bucket)
}

func (n *gfarmObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fi, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
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
	entries, err := n.clnt.ReadDir(gfarmSeparator)
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

		fis, err := n.clnt.ReadDir(minio.PathJoin(gfarmSeparator, bucket, prefixDir))
		if err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			logger.LogIf(minio.GlobalContext, err)
			return
		}

//		f, err := n.clnt.Open(minio.PathJoin(gfarmSeparator, bucket, prefixDir))
//		if err != nil {
//			if os.IsNotExist(err) {
//				err = nil
//			}
//			logger.LogIf(minio.GlobalContext, err)
//			return
//		}
//		defer f.Close()
//		fis, err := f.Readdir(0)
//		if err != nil {
//			logger.LogIf(minio.GlobalContext, err)
//			return
//		}

		if len(fis) == 0 {
			return true, nil
		}
		for _, fi := range fis {
			if fi.IsDir() {
				entries = append(entries, fi.Name()+gfarmSeparator)
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
	if _, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket)); err != nil {
		return loi, gfarmToObjectErr(ctx, err, bucket)
	}

	getObjectInfo := func(ctx context.Context, bucket, entry string) (minio.ObjectInfo, error) {
		fi, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket, entry))
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
	if err := n.clnt.Remove(deletePath); err != nil {
		if errors.Is(err, syscall.ENOTEMPTY) {
			// Ignore errors if the directory is not empty. The server relies on
			// this functionality, and sometimes uses recursion that should not
			// error on parent directories.
			return nil
		}
		return err
	}

	// Trailing slash is removed when found to ensure
	// slashpath.Dir() to work as intended.
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

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
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
	if _, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket)); err != nil {
		return gfarmToObjectErr(ctx, err, bucket)
	}
	rd, err := n.clnt.Open(minio.PathJoin(gfarmSeparator, bucket, key))
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
//	sb, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket, object))
//	if err != nil {
//		if os.IsNotExist(err) {
//			return false
//		}
//		logger.LogIf(ctx, err)
//		return false
//	}

fmt.Fprintf(os.Stderr, "@@@ isObjectDir %q %q\n", bucket, object)

	f, err := n.clnt.Open(minio.PathJoin(gfarmSeparator, bucket, object))
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		logger.LogIf(ctx, err)
		return false
	}
	defer f.Close()
	fis, err := f.Readdir(1)
	if err != nil && err != io.EOF {
		logger.LogIf(ctx, err)
		return false
	}
	// Readdir returns an io.EOF when len(fis) == 0.
	return len(fis) == 0
}

// GetObjectInfo reads object info and replies back ObjectInfo.
func (n *gfarmObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}
	if strings.HasSuffix(object, gfarmSeparator) && !n.isObjectDir(ctx, bucket, object) {
		return objInfo, gfarmToObjectErr(ctx, os.ErrNotExist, bucket, object)
	}

	fi, err := n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket, object))
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
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}

	name := minio.PathJoin(gfarmSeparator, bucket, object)

	// If its a directory create a prefix {
	if strings.HasSuffix(object, gfarmSeparator) && r.Size() == 0 {
		if err = n.clnt.MkdirAll(name, os.FileMode(0755)); err != nil {
			n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), name)
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	} else {
		tmpname := minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, minio.MustGetUUID())
		var w *FileReadWriter
		w, err = n.clnt.Create(tmpname)
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
			if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
				w.Close()
				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
				return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
			}
		}
		w.Close()
		if err = n.clnt.Rename(tmpname, name); err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}
	fi, err := n.clnt.Stat(name)
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
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return uploadID, gfarmToObjectErr(ctx, err, bucket)
	}

	uploadID = minio.MustGetUUID()
	//if err = n.clnt.CreateEmptyFile(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)); err != nil {
	if err = n.clnt.Mkdir(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID), os.FileMode(0755)); err != nil {
		return uploadID, gfarmToObjectErr(ctx, err, bucket)
	}

	return uploadID, nil
}

func (n *gfarmObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ ListMultipartUploads %q %q\n", bucket, prefix)
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return lmi, gfarmToObjectErr(ctx, err, bucket)
	}

	// It's decided not to support List Multipart Uploads, hence returning empty result.
	return lmi, nil
}

func (n *gfarmObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID))
	if err != nil {
		return gfarmToObjectErr(ctx, err, bucket, object, uploadID)
	}
	return nil
}

func (n *gfarmObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
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
	return n.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (n *gfarmObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ PutObjectPart bucket:%q object:%q uploadID:%q partID:%d\n", bucket, object, uploadID, partID)
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return info, gfarmToObjectErr(ctx, err, bucket)
	}

	var w *FileReadWriter
fmt.Fprintf(os.Stderr, "@@@ Append object:%q uploadID:%q partID:%d\n", object, uploadID, partID)
	//w, err = n.clnt.Append(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID))
	partName := fmt.Sprintf("%05d", partID)
	w, err = n.clnt.Append(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, partName))
	if err != nil {
		return info, gfarmToObjectErr(ctx, err, bucket, object, uploadID)
	}
	defer w.Close()
	_, err = io.Copy(w, r.Reader)
	if err != nil {
		return info, gfarmToObjectErr(ctx, err, bucket, object, uploadID)
	}

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (n *gfarmObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
fmt.Fprintf(os.Stderr, "@@@ CompleteMultipartUpload bucket:%q object:%q  parts:%v\n", bucket, object, parts)
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return objInfo, gfarmToObjectErr(ctx, err, bucket)
	}

	if err = n.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := minio.PathJoin(gfarmSeparator, bucket, object)
	dir := path.Dir(name)
	if dir != "" {
		if err = n.clnt.MkdirAll(dir, os.FileMode(0755)); err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}

	//w, err = n.clnt.Append(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID))
	//w, err = n.clnt.Append(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID))
	var w *FileReadWriter
	w, err = n.clnt.Append(name)
	if os.IsExist(err) {
		if err = n.clnt.Remove(name); err != nil {
			if dir != "" {
				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
			}
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		w, err = n.clnt.Append(name)
		if err != nil {
			if dir != "" {
				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
			}
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
	}

	for _, part := range parts {
		var r *FileReadWriter
		partName := fmt.Sprintf("%05d", part.PartNumber)
		r, err = n.clnt.Open(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, partName))
fmt.Fprintf(os.Stderr, "@@@ Copy %q => %q\n", minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID, partName), name)
		if err != nil {
			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
		}
		defer r.Close()
		_, err = io.Copy(w, r)
	}

	err = n.clnt.RemoveAll(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID))

	//err = n.clnt.Rename(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID), name)
	// Object already exists is an error on GFARM
	// remove it and then create it again.
//	if os.IsExist(err) {
//		if err = n.clnt.Remove(name); err != nil {
//			if dir != "" {
//				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
//			}
//			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
//		}
//		if err = n.clnt.Rename(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID), name); err != nil {
//			if dir != "" {
//				n.deleteObject(minio.PathJoin(gfarmSeparator, bucket), dir)
//			}
//			return objInfo, gfarmToObjectErr(ctx, err, bucket, object)
//		}
//	}
	fi, err := n.clnt.Stat(name)
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

func (n *gfarmObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) (err error) {
fmt.Fprintf(os.Stderr, "@@@ AbortMultipartUpload %q %q\n", bucket, object)
	_, err = n.clnt.Stat(minio.PathJoin(gfarmSeparator, bucket))
	if err != nil {
		return gfarmToObjectErr(ctx, err, bucket)
	}
	return gfarmToObjectErr(ctx, n.clnt.RemoveAll(minio.PathJoin(gfarmSeparator, minioMetaTmpBucket, uploadID)), bucket, object, uploadID)
}

// IsReady returns whether the layer is ready to take requests.
func (n *gfarmObjects) IsReady(_ context.Context) bool {
	return true
}

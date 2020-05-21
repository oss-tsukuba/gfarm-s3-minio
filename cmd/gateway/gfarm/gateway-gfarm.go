/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
//	"path"
//	"sync/atomic"

	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
//	"github.com/minio/minio/cmd/logger"

	"net/http"
	"fmt"
	"os"
)

const (
	gfarmBackend = "gfarm"
)

func init() {
	const gfarmGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} PATH
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
PATH:
  path to Gfarm mount point

EXAMPLES:
  1. Start minio gateway server for Gfarm backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}} /shared/nasvol

  2. Start minio gateway server for Gfarm with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ACCESS_KEY{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_SECRET_KEY{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85

     {{.Prompt}} {{.HelpName}} /shared/nasvol
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               gfarmBackend,
		Usage:              "gfarm (Gfarm)",
		Action:             gfarmGatewayMain,
		CustomHelpTemplate: gfarmGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway gfarm' command line.
func gfarmGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if !ctx.Args().Present() || ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, gfarmBackend, 1)
	}

	minio.StartGateway(ctx, &Gfarm{ctx.Args().First()})
}

// Gfarm implements Gateway.
type Gfarm struct {
	path string
}

// Name implements Gateway interface.
func (g *Gfarm) Name() string {
	return gfarmBackend
}

// NewGatewayLayer returns gfarm gatewaylayer.
func (g *Gfarm) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	var err error
	newObject, err := minio.NewFSObjectLayer(g.path)
fmt.Fprintf(os.Stderr, "@@@ GatewayLayer: newObject = %T %v\n", newObject, newObject)
	newObject = &GfarmFSObjects{newObject}
	if err != nil {
		return nil, err
	}
	return &gfarmObjects{newObject}, nil
}

// Production - gfarm gateway is production ready.
func (g *Gfarm) Production() bool {
	return true
}

// IsListenBucketSupported returns whether listen bucket notification is applicable for this gateway.
func (n *gfarmObjects) IsListenBucketSupported() bool {
	return false
}

func (n *gfarmObjects) StorageInfo(ctx context.Context, _ bool) minio.StorageInfo {
	sinfo := n.ObjectLayer.StorageInfo(ctx, false)
	sinfo.Backend.GatewayOnline = sinfo.Backend.Type == minio.BackendFS
	sinfo.Backend.Type = minio.BackendGateway
	return sinfo
}

// gfarmObjects implements gateway for MinIO and S3 compatible object storage servers.
type gfarmObjects struct {
	minio.ObjectLayer
}

// IsReady returns whether the layer is ready to take requests.
func (n *gfarmObjects) IsReady(ctx context.Context) bool {
	sinfo := n.ObjectLayer.StorageInfo(ctx, false)
	return sinfo.Backend.Type == minio.BackendFS
}

type GfarmFSObjects struct {
	minio.ObjectLayer
}

func (fs *GfarmFSObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
fmt.Fprintf(os.Stderr, "@@@ GetObjectNInfo: ctx:%p bucket:%q object:%q rs:%v h:%v lockType:%v opts:%v\n", ctx, bucket, object, rs, h, lockType, opts)
	return fs.ObjectLayer.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
}

func (fs *GfarmFSObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, retErr error) {
fmt.Fprintf(os.Stderr, "@@@ 111 @@@ GfarmFSObjects.PutObject: ctx:%p bucket:%q object:%q r:%p opts:%v\n", ctx, bucket, object, r, opts)
defer fmt.Fprintf(os.Stderr, "@@@ 111 @@@ EXIT\n")
	//return fs.PutObjec2(ctx, bucket, object, r, opts)
	//return minio.PutObjec2(fs.ObjectLayer, ctx, bucket, object, r, opts)
	return fs.ObjectLayer.PutObject(ctx, bucket, object, r, opts)
}

func (fs *GfarmFSObjects) putObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, retErr error) {
fmt.Fprintf(os.Stderr, "@@@ 333 @@@@ GfarmFSObjects.putObject: ctx:%p bucket:%q object:%q r:%p opts:%v\n", ctx, bucket, object, r, opts)
defer fmt.Fprintf(os.Stderr, "@@@ 333 @@@ EXIT\n")
	return fs.putObject(ctx, bucket, object, r, opts)
}

//func (fs *GfarmFSObjects) PutObjec2(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, retErr error) {
//fmt.Fprintf(os.Stderr, "@@@ 999 @@@ FSObjects.PutObjec2: ctx:%p bucket:%q object:%q r:%p opts:%v\n", ctx, bucket, object, r, opts)
//defer fmt.Fprintf(os.Stderr, "@@@ 999 @@@ EXIT\n");
//	if err := checkPutObjectArgs(ctx, bucket, object, fs, r.Size()); err != nil {
//		return minio.ObjectInfo{}, err
//	}
//
//	// Lock the object.
//	objectLock := (minio.FSObjects)(fs).NewNSLock(ctx, bucket, object)
//	if err := objectLock.GetLock(globalObjectTimeout); err != nil {
//		logger.LogIf(ctx, err)
//		return objInfo, err
//	}
//	defer objectLock.Unlock()
//	defer minio.ObjectPathUpdated(path.Join(bucket, object))
//
//	atomic.AddInt64(&(*minio.FSObjects)(fs.ObjectLayer).activeIOCount, 1)
//	defer func() {
//		atomic.AddInt64(&(*minio.FSObjects)(fs.ObjectLayer).activeIOCount, -1)
//	}()
//
//	return fs.putObject(ctx, bucket, object, r, opts)
//}

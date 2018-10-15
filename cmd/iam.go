/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package cmd

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"sync"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/madmin"
)

const (
	// IAM configuration directory.
	iamConfigPrefix = minioConfigPrefix + "/iam"

	// IAM users directory.
	iamConfigUsersPrefix = iamConfigPrefix + "/users/"

	// IAM sts directory.
	iamConfigSTSPrefix = iamConfigPrefix + "/sts/"

	// IAM identity file which captures identity credentials.
	iamIdentityFile = "identity.json"

	// IAM policy file which provides policies for each users.
	iamPolicyFile = "policy.json"
)

// IAMSys - config system.
type IAMSys struct {
	sync.RWMutex
	iamUsersMap  map[string]auth.Credentials
	iamPolicyMap map[string]iampolicy.Policy
}

// Load - load iam.json
func (sys *IAMSys) Load(objAPI ObjectLayer) error {
	return sys.Init(objAPI)
}

// Init - initializes config system from iam.json
func (sys *IAMSys) Init(objAPI ObjectLayer) error {
	if objAPI == nil {
		return errInvalidArgument
	}

	defer func() {
		// Refresh IAMSys in background.
		go func() {
			ticker := time.NewTicker(globalRefreshIAMInterval)
			defer ticker.Stop()
			for {
				select {
				case <-globalServiceDoneCh:
					return
				case <-ticker.C:
					sys.refresh(objAPI)
				}
			}
		}()
	}()

	doneCh := make(chan struct{})
	defer close(doneCh)

	// Initializing IAM needs a retry mechanism for
	// the following reasons:
	//  - Read quorum is lost just after the initialization
	//    of the object layer.
	retryTimerCh := newRetryTimerSimple(doneCh)
	for {
		select {
		case _ = <-retryTimerCh:
			// Load IAMSys once during boot.
			if err := sys.refresh(objAPI); err != nil {
				if err == errDiskNotFound ||
					strings.Contains(err.Error(), InsufficientReadQuorum{}.Error()) ||
					strings.Contains(err.Error(), InsufficientWriteQuorum{}.Error()) {
					logger.Info("Waiting for IAM subsystem to be initialized..")
					continue
				}
				return err
			}
			return nil
		}
	}
}

// SetPolicy - sets policy to given user name.  If policy is empty,
// existing policy is removed.
func (sys *IAMSys) SetPolicy(accessKey string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		if err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data); err != nil {
			return err
		}
	} else {
		if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
			return err
		}
	}

	sys.Lock()
	defer sys.Unlock()

	if p.IsEmpty() {
		delete(sys.iamPolicyMap, accessKey)
	} else {
		sys.iamPolicyMap[accessKey] = p
	}

	return nil
}

// SaveTempPolicy - this is used for temporary credentials only.
func (sys *IAMSys) SaveTempPolicy(accessKey string, p iampolicy.Policy) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamPolicyFile)
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		if err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data); err != nil {
			return err
		}
	} else {
		if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
			return err
		}
	}

	sys.Lock()
	defer sys.Unlock()

	if p.IsEmpty() {
		delete(sys.iamPolicyMap, accessKey)
	} else {
		sys.iamPolicyMap[accessKey] = p
	}

	return nil
}

// DeletePolicy - sets policy to given user name.  If policy is empty,
// existing policy is removed.
func (sys *IAMSys) DeletePolicy(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	var err error
	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	if globalEtcdClient != nil {
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, configFile)
	} else {
		err = deleteConfig(context.Background(), objectAPI, configFile)
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamPolicyMap, accessKey)

	return err
}

// DeleteUser - set user credentials.
func (sys *IAMSys) DeleteUser(accessKey string) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	var err error
	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamPolicyFile)
	if globalEtcdClient != nil {
		err = deleteConfigEtcd(context.Background(), globalEtcdClient, configFile)
	} else {
		err = deleteConfig(context.Background(), objectAPI, configFile)
	}

	sys.Lock()
	defer sys.Unlock()

	delete(sys.iamUsersMap, accessKey)
	return err
}

// SetTempUser - set temporary user credentials, these credentials have an expiry.
func (sys *IAMSys) SetTempUser(accessKey string, cred auth.Credentials) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigSTSPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(cred)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		if err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data); err != nil {
			return err
		}
	} else {
		if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
			return err
		}
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap[accessKey] = cred
	return nil
}

// ListUsers - list all users.
func (sys *IAMSys) ListUsers() (map[string]madmin.UserInfo, error) {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return nil, errServerNotInitialized
	}

	var users = make(map[string]madmin.UserInfo)

	sys.RLock()
	defer sys.RUnlock()

	for k, v := range sys.iamUsersMap {
		users[k] = madmin.UserInfo{
			Status: madmin.AccountStatus(v.Status),
		}
	}

	return users, nil
}

// SetUser - set user credentials.
func (sys *IAMSys) SetUser(accessKey string, uinfo madmin.UserInfo) error {
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		return errServerNotInitialized
	}

	configFile := pathJoin(iamConfigUsersPrefix, accessKey, iamIdentityFile)
	data, err := json.Marshal(uinfo)
	if err != nil {
		return err
	}

	if globalEtcdClient != nil {
		if err = saveConfigEtcd(context.Background(), globalEtcdClient, configFile, data); err != nil {
			return err
		}
	} else {
		if err = saveConfig(context.Background(), objectAPI, configFile, data); err != nil {
			return err
		}
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap[accessKey] = auth.Credentials{
		AccessKey: accessKey,
		SecretKey: uinfo.SecretKey,
		Status:    string(uinfo.Status),
	}

	return nil
}

// GetUser - get user credentials
func (sys *IAMSys) GetUser(accessKey string) (cred auth.Credentials, ok bool) {
	sys.RLock()
	defer sys.RUnlock()

	cred, ok = sys.iamUsersMap[accessKey]
	return cred, ok && cred.IsValid()
}

// IsAllowed - checks given policy args is allowed to continue the Rest API.
func (sys *IAMSys) IsAllowed(args iampolicy.Args) bool {
	sys.RLock()
	defer sys.RUnlock()

	// If opa is configured, use OPA always.
	if globalPolicyOPA != nil {
		return globalPolicyOPA.IsAllowed(args)
	}

	// If policy is available for given user, check the policy.
	if p, found := sys.iamPolicyMap[args.AccountName]; found {
		return p.IsAllowed(args)
	}

	// As policy is not available and OPA is not configured, return the owner value.
	return args.IsOwner
}

var defaultContextTimeout = 5 * time.Minute

// Similar to reloadUsers but updates users, policies maps from etcd server,
func reloadEtcdUsers(prefix string, usersMap map[string]auth.Credentials, policyMap map[string]iampolicy.Policy) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultContextTimeout)
	r, err := globalEtcdClient.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithKeysOnly())
	defer cancel()
	if err != nil {
		return err
	}
	// No users are created yet.
	if r.Count == 0 {
		return nil
	}

	users := set.NewStringSet()
	for _, kv := range r.Kvs {
		// Extract user by stripping off the `prefix` value as suffix,
		// then strip off the remaining basename to obtain the prefix
		// value, usually in the following form.
		//
		//  key := "config/iam/users/newuser/identity.json"
		//  prefix := "config/iam/users/"
		//  v := trim(trim(key, prefix), base(key)) == "newuser"
		//
		user := strings.TrimSuffix(strings.TrimSuffix(string(kv.Key), prefix), path.Base(string(kv.Key)))
		if !users.Contains(user) {
			users.Add(user)
		}
	}

	// Reload config and policies for all users.
	for _, user := range users.ToSlice() {
		idFile := pathJoin(prefix, user, iamIdentityFile)
		pFile := pathJoin(prefix, user, iamPolicyFile)
		cdata, cerr := readConfigEtcd(ctx, globalEtcdClient, idFile)
		pdata, perr := readConfigEtcd(ctx, globalEtcdClient, pFile)
		if cerr != nil && cerr != errConfigNotFound {
			return cerr
		}
		if perr != nil && perr != errConfigNotFound {
			return perr
		}
		if cerr == errConfigNotFound && perr == errConfigNotFound {
			continue
		}
		if cerr == nil {
			var cred auth.Credentials
			if err = json.Unmarshal(cdata, &cred); err != nil {
				return err
			}
			cred.AccessKey = user
			if cred.IsExpired() {
				deleteConfigEtcd(ctx, globalEtcdClient, idFile)
				deleteConfigEtcd(ctx, globalEtcdClient, pFile)
				continue
			}
			usersMap[cred.AccessKey] = cred
		}
		if perr == nil {
			var p iampolicy.Policy
			if err = json.Unmarshal(pdata, &p); err != nil {
				return err
			}
			policyMap[path.Base(prefix)] = p
		}
	}
	return nil
}

// reloadUsers reads an updates users, policies from object layer into user and policy maps.
func reloadUsers(objectAPI ObjectLayer, prefix string, usersMap map[string]auth.Credentials, policyMap map[string]iampolicy.Policy) error {
	marker := ""
	for {
		var lo ListObjectsInfo
		var err error
		lo, err = objectAPI.ListObjects(context.Background(), minioMetaBucket, prefix, marker, "/", 1000)
		if err != nil {
			return err
		}
		marker = lo.NextMarker
		for _, prefix := range lo.Prefixes {
			idFile := pathJoin(prefix, iamIdentityFile)
			pFile := pathJoin(prefix, iamPolicyFile)
			cdata, cerr := readConfig(context.Background(), objectAPI, idFile)
			pdata, perr := readConfig(context.Background(), objectAPI, pFile)
			if cerr != nil && cerr != errConfigNotFound {
				return cerr
			}
			if perr != nil && perr != errConfigNotFound {
				return perr
			}
			if cerr == errConfigNotFound && perr == errConfigNotFound {
				continue
			}
			if cerr == nil {
				var cred auth.Credentials
				if err = json.Unmarshal(cdata, &cred); err != nil {
					return err
				}
				cred.AccessKey = path.Base(prefix)
				if cred.IsExpired() {
					// Delete expired identity.
					objectAPI.DeleteObject(context.Background(), minioMetaBucket, idFile)
					// Delete expired identity policy.
					objectAPI.DeleteObject(context.Background(), minioMetaBucket, pFile)
					continue
				}
				usersMap[cred.AccessKey] = cred
			}
			if perr == nil {
				var p iampolicy.Policy
				if err = json.Unmarshal(pdata, &p); err != nil {
					return err
				}
				policyMap[path.Base(prefix)] = p
			}
		}
		if !lo.IsTruncated {
			break
		}
	}
	return nil
}

// Refresh IAMSys.
func (sys *IAMSys) refresh(objAPI ObjectLayer) error {
	iamUsersMap := make(map[string]auth.Credentials)
	iamPolicyMap := make(map[string]iampolicy.Policy)

	if globalEtcdClient != nil {
		if err := reloadEtcdUsers(iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
		if err := reloadEtcdUsers(iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
	} else {
		if err := reloadUsers(objAPI, iamConfigUsersPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
		if err := reloadUsers(objAPI, iamConfigSTSPrefix, iamUsersMap, iamPolicyMap); err != nil {
			return err
		}
	}

	sys.Lock()
	defer sys.Unlock()

	sys.iamUsersMap = iamUsersMap
	sys.iamPolicyMap = iamPolicyMap

	return nil
}

// NewIAMSys - creates new config system object.
func NewIAMSys() *IAMSys {
	return &IAMSys{
		iamUsersMap:  make(map[string]auth.Credentials),
		iamPolicyMap: make(map[string]iampolicy.Policy),
	}
}

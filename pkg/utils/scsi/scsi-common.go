// +build linux,amd64

/*
 * Mini Object Storage, (C) 2014 Minio, Inc.
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

package scsi

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
)

func getattrs(scsiAttrPath string, scsiAttrList []string) map[string]string {
	attrMap := make(map[string]string)
	for _, attr := range scsiAttrList {
		value, _ := ioutil.ReadFile(path.Join(scsiAttrPath, attr))
		attrMap[attr] = strings.Trim(string(value[:]), "\n") // remove odd newlines
	}
	return attrMap
}

func filterdisks(files []os.FileInfo) (scsidisks []string) {
	for _, fi := range files {
		if strings.Contains(fi.Name(), "host") {
			continue
		}
		if strings.Contains(fi.Name(), "target") {
			continue
		}
		scsidisks = append(scsidisks, fi.Name())
	}
	return scsidisks
}

// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package backends

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
)

type (
	ProviderStorage interface {
		CreateProvider(*Provider) CaliopenError
		RetrieveProvider(name, instance string) (*Provider, CaliopenError)
		UpdateProvider(*Provider, map[string]interface{}) CaliopenError
		DeleteProvider(*Provider) CaliopenError
	}
)

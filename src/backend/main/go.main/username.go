// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package caliopen

func (f *facility) UsernameIsAvailable(username string) (bool, error) {
	return f.store.UsernameIsAvailable(username)
}
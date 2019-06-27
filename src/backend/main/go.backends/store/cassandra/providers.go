// Copyleft (ɔ) 2019 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package store

import (
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	log "github.com/Sirupsen/logrus"
	"github.com/gocassa/gocassa"
	"gopkg.in/oleiade/reflections.v1"
)

func (cb *CassandraBackend) CreateProvider(provider *Provider) CaliopenError {
	table := cb.IKeyspace.Table("provider", &Provider{},
		gocassa.Keys{
			PartitionKeys: []string{"name", "instance"},
		}).WithOptions(gocassa.Options{TableName: "provider"})

	err := table.Set(provider).Run()
	if err != nil {
		log.WithError(err).Errorf("[CassandraBackend] set fails for %+v", provider)
		return WrapCaliopenErrf(err, DbCaliopenErr, "[CassandraBackend] CreateProvider fails : %s", err.Error())
	}
	return nil
}

func (cb *CassandraBackend) RetrieveProvider(name, instance string) (provider *Provider, err CaliopenError) {
	provider = new(Provider)
	m := map[string]interface{}{}
	q := cb.SessionQuery(`SELECT * FROM provider WHERE name = ? AND instance = ?`, name, instance)
	e := q.MapScan(m)
	if e != nil {
		return nil, WrapCaliopenErr(e, DbCaliopenErr, "")
	}
	provider.UnmarshalCQLmap(m)
	return
}

func (cb *CassandraBackend) UpdateProvider(provider *Provider, fields map[string]interface{}) (err CaliopenError) {
	// check if provider exists before executing UPDATE because `IF EXISTST` statement not supported by scylladb as of june 2019
	if cb.SessionQuery(`SELECT name FROM providers WHERE name = ? AND instance = ?`, provider.Name, provider.Instance).Iter().NumRows() == 0 {
		return NewCaliopenErr(NotFoundCaliopenErr, "not found")
	}
	if len(fields) > 0 {
		//get cassandra's field name for each field to modify
		cassaFields := map[string]interface{}{}
		for field, value := range fields {
			cassaField, err := reflections.GetFieldTag(provider, field, "cql")
			if err != nil {
				return NewCaliopenErrf(UnprocessableCaliopenErr, "[CassandraBackend] UpdateProvider failed to find a cql field for object field %s", field)
			}
			if cassaField != "-" {
				cassaFields[cassaField] = value
			}
		}
		table := cb.IKeyspace.Table("provider", &Provider{},
			gocassa.Keys{
				PartitionKeys: []string{"name", "instance"},
			}).WithOptions(gocassa.Options{TableName: "provider"})
		e := table.Where(gocassa.Eq("name", provider.Name), gocassa.Eq("instance", provider.Instance)).Update(cassaFields).Run()
		if e != nil {
			log.WithError(e).Errorf("[CassandraBackend] UpdateProvider failed to UPDATE with fields %+v", cassaFields)
			return WrapCaliopenErr(e, DbCaliopenErr, "")
		}
	}
	return nil
}

func (cb *CassandraBackend) DeleteProvider(provider *Provider) CaliopenError {
	err := cb.SessionQuery(`DELETE FROM provider WHERE name = ? AND instance = ?`, provider.Name, provider.Instance).Exec()
	if err != nil {
		log.WithError(err).Errorf("[CassandraBackend] UpdateProvider failed to DELETE provider %+v", provider)
		return WrapCaliopenErr(err, DbCaliopenErr, "")

	}
	return nil
}

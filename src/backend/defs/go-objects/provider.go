package objects

import (
	"github.com/emersion/go-imap"
	"time"
)

type Provider struct {
	DateInsert       time.Time         `json:"date_insert"                      cql:"date_insert"`
	Instance         string            `json:"instance"                         cql:"instance"`
	Infos            map[string]string `json:"-"                                cql:"infos"         mapstructure:"infos"                frontend:"omit"`
	Name             string            `json:"name"                             cql:"name"          mapstructure:"name"`
	OauthCallbackUri string            `json:"oauth_callback_uri,omitempty"     cql:"-"             mapstructure:"oauth_callback_uri"`
	OauthRequestUrl  string            `json:"oauth_request_url,omitempty"      cql:"-"             mapstructure:"oauth_request_url"`
	Protocol         string            `json:"protocol"                         cql:"-"             mapstructure:"protocol"`
	Capabilities     map[string]bool   `json:"omitempty"                        cql:"-"` // capabilities sent back by provider at connection time
	FetchItems       []imap.FetchItem  `json:"omitempty"                        cql:"-"` // provider specific items that we want to fetch
}

type OauthSession struct {
	RequestSecret string
	RequestToken  string
	UserId        string
	Params        map[string]string
}

// return a JSON representation of Provider suitable for frontend client
// for now, Infos is not returned. If client need it in future, we shall cleanup Infos for sensitive data.
func (p *Provider) MarshalFrontEnd() ([]byte, error) {
	return JSONMarshaller("frontend", p)
}

func (p *Provider) UnmarshalCQLmap(input map[string]interface{}) error {
	if name, ok := input["name"]; ok {
		p.Name = name.(string)
	}
	if instance, ok := input["instance"]; ok {
		p.Instance = instance.(string)
	}
	if infos, ok := input["infos"].(map[string]string); ok {
		p.Infos = make(map[string]string)
		for k, v := range infos {
			p.Infos[k] = v
		}
	}
	if dateInsert, ok := input["date_insert"].(time.Time); ok {
		p.DateInsert = dateInsert
	}
	return nil
}

// Copyleft (ɔ) 2017 The Caliopen contributors.
// Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
// license (AGPL) that can be found in the LICENSE file.

package objects

// constants declares global scope, general usage vars

const (
	// protocols' strings
	EmailProtocol     = "email"
	ImapProtocol      = "imap"
	SmtpProtocol      = "smtp"
	IrcProtocol       = "irc"
	SmsProtocol       = "sms"
	XmppProtocol      = "xmpp"
	FacebookProtocl   = "facebook"
	TwitterProtocol   = "twitter"
	GnuSocialProtocol = "GNUsocial"
	MastodonProtocol  = "mastodon"

	TimeISO8601      = "2006-01-02T15:04:05-07:00"
	TimeUTCmicro     = "2006-01-02T15:04:05.999999"
	RFC3339Milli     = "2006-01-02T15:04:05.000Z07:00"
	MessageType      = "message"
	ContactType      = "contact"
	MessageIndexType = "indexed_message"
	ContactIndexType = "indexed_contact"

	// authentication methods
	LoginPassword = "login-password"
	Oauth1        = "Oauth1"
	Oauth2        = "Oauth2"

	//nats related constants
	Nats_contact_tmpl         = "{\"order\":\"%s\", \"contact_id\":\"%s\", \"user_id\":\"%s\"}"
	Nats_outSMTP_topicKey     = "outSMTP_topic"
	Nats_inSMTP_topicKey      = "inSMTP_topic"
	Nats_Contacts_topicKey    = "contacts_topic"
	Nats_outIMAP_topicKey     = "outIMAP_topic"
	Nats_outTwitter_topicKey  = "outTWITTER_topic"
	Nats_outMastodon_topicKey = "outMASTODON_topic"
	Nats_Keys_topicKey        = "keys_topic"
	Nats_IdPoller_topicKey    = "idpoller_topic"

	//participant types
	ParticipantBcc     = "Bcc"
	ParticipantCC      = "Cc"
	ParticipantFrom    = "From"
	ParticipantReplyTo = "Reply-To"
	ParticipantSender  = "Sender"
	ParticipantTo      = "To"

	//notifications types
	NotifAdminMail        = "adminMail"
	NotifPasswordReset    = "passwordReset"
	NotifDeviceValidation = "deviceValidation"
	OnboardingMails       = "onboardingMails"

	//identity types
	LocalIdentity  = "local"
	RemoteIdentity = "remote"

	//praticipants lookup
	UrisKind         = "uris"
	ParticipantsKind = "participants"

	//device status
	DeviceVerifiedStatus   = "verified"
	DeviceUnverifiedStatus = "unverified"
	DeviceDeletedStatus    = "deleted"

	//device types
	DeviceOtherType      = "other"
	DeviceDesktopType    = "desktop"
	DeviceLaptopType     = "laptop"
	DeviceSmartphoneType = "smartphone"
	DeviceTabletType     = "tablet"

	/** JWA strings from RFC7518 **/
	// "alg" param from RFC7581#3.1
	RSA256   = "RS256"
	RSA384   = "RS384"
	RSA512   = "RS512"
	ECDSA256 = "ES256"
	ECDSA384 = "ES384"
	// supplementary algorithms to handle GPG keys
	DSA256     = "DSA256"
	DSA384     = "DSA384"
	DSA512     = "DSA512"
	ELGAMAL256 = "ELGAMAL256"
	ELGAMAL384 = "ELGAMAL384"
	ELGAMAL512 = "ELGAMAL512"
	ECDH256    = "ECDH256"
	ECDH384    = "ECDH384"
	ECDH512    = "ECDH512"
	// "crv" param from RFC7518#7.6.2
	CURVE256 = "P-256"
	CURVE384 = "P-384"
	CURVE521 = "P-521"
	// "kty" param from RFC7518#6.1
	RSA_KEY_TYPE   = "RSA"
	EC_KEY_TYPE    = "EC"
	OCTET_KEY_TYPE = "oct" // (yes, lower case !)
	//supplementary types to handle GPG keys
	PGP_KEY_TYPE     = "PGP"
	DSA_KEY_TYPE     = "DSA"
	ELGAMAL_KEY_TYPE = "ELGAMAL"
	// "use" param (from RFC ??)
	SIGNATURE_KEY  = "sig"
	ENCRYPTION_KEY = "enc"
	/** end of JWA strings **/
)

// A Initiator specifies what kind of actor is triggering a PATCH method on any object
type Initiator int

const (
	Unknown Initiator = iota
	SystemActor
	UserActor
)

// map internal constants to json tags used within struct definition
var Initiators = map[string]Initiator{
	"system": SystemActor,
	"user":   UserActor,
}

var DeviceTypes = [5]string{"other", "laptop", "desktop", "smartphone", "tablet"} // always put the default string at first, it will be found by device's func to fill default type.

var EmptyUUID = UUID{} // allocate an emptyUUID that should only be used for testing zero value.

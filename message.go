package carrierController

import (
	"strings"

	programs "github.com/0nramp/carrier/pkg/program"
	"github.com/0nramp/carrier/pkg/referralPartner"
	c "github.com/0nramp/protos/common"
	"github.com/shopspring/decimal"
)

type Carrier struct {
	Id                     uint
	Uuid                   string
	ExternalId             string
	PublicId               string
	DotNumber              string
	Name                   string
	Street1                string
	Street2                string
	City                   string
	Region                 string
	PostalCode             string
	PrimaryContact         string
	Phone                  string
	StripeAccountId        *string
	ReferralPartnerUuid    *string
	RewardStatus           CarrierRewardStatus
	DriverAttributeHeaders []string

	// programs
	Program          *programs.Program
	ShowDiscountFlag ShowDiscountFlag

	ReferralPartner *referralPartner.ReferralPartner
	Status          CarrierStatus
}

type Prompts struct {
	Id               uint
	CarrierUuid      string
	CarrierId        uint
	HasOdometer      bool
	HasTrailerNumber bool
	HasTripNumber    bool
	HasTruckNumber   bool
	AsOfDate         string
}

type PromptsFilters struct {
	DriverUuid  string
	CarrierUuid string
}

func (carrier Carrier) GetAddress() *c.Address {
	return &c.Address{
		Street1:    carrier.Street1,
		Street2:    carrier.Street2,
		City:       carrier.City,
		Region:     carrier.Region,
		PostalCode: carrier.PostalCode,
	}
}

type CarrierRewardStatus struct {
	RewardsActive          bool
	RewardThresholdAmount  decimal.Decimal
	RewardRedeemableAmount decimal.Decimal
}

type ShowDiscountFlag uint32

const (
	// keep order same as carrier.proto ShowDiscountToDrivers
	ShowDiscounFlagUnspecified ShowDiscountFlag = iota
	ShowDiscountFlagAlways
	ShowDiscountFlagNever
)

func ParseShowDiscountFlag(df string) ShowDiscountFlag {
	switch strings.ToLower(df) {
	case "always":
		return ShowDiscountFlagAlways
	case "never":
		return ShowDiscountFlagNever
	case "unspecified":
		return ShowDiscounFlagUnspecified
	default:
		return ShowDiscounFlagUnspecified
	}
}

func (l ShowDiscountFlag) String() string {
	switch l {
	case ShowDiscountFlagAlways:
		return "always"
	case ShowDiscountFlagNever:
		return "never"
	default:
		return "unspecified"
	}
}

type CarrierStatus uint32

const (
	// keep order same as carrier.proto CarrierStatus
	CarrierStatusUnspecified CarrierStatus = iota
	CarrierStatusActive
	CarrierStatusInactive
	CarrierStatusSuspended
	CarrierStatusDeleted
)

func ParseCarrierStatus(df string) CarrierStatus {
	switch strings.ToLower(df) {
	case "active":
		return CarrierStatusActive
	case "inactive":
		return CarrierStatusInactive
	case "suspended":
		return CarrierStatusSuspended
	case "deleted":
		return CarrierStatusDeleted
	case "unspecified":
		return CarrierStatusUnspecified
	default:
		return CarrierStatusUnspecified
	}
}

func (l CarrierStatus) String() string {
	switch l {
	case CarrierStatusActive:
		return "active"
	case CarrierStatusInactive:
		return "inactive"
	case CarrierStatusSuspended:
		return "suspended"
	case CarrierStatusDeleted:
		return "deleted"
	default:
		return "unspecified"
	}
}

type SftpConfig struct {
	// DO NOT USE PLAINTEXT HERE.
	// The value of this field should point to the name of a secret
	// that exists in secret manager with the actual SFTP host.
	// The secret name should use the following all upper-case format for
	// the secret name:
	//
	// <CARRIER_NAME>_FTP_HOST
	Host string

	// DO NOT USE PLAINTEXT HERE.
	// The value of this field should point to the name of a secret
	// that exists in secret manager with the actual SFTP port.
	// The secret name should use the following all upper-case format for
	// the secret name:
	//
	// <CARRIER_NAME>_FTP_PORT
	Port string

	// DO NOT USE PLAINTEXT HERE.
	// The value of this field should point to the name of a secret
	// that exists in secret manager with the actual SFTP username.
	// The secret name should use the following all upper-case format for
	// the secret name:
	//
	// <CARRIER_NAME>_FTP_USERNAME
	Username string

	// DO NOT USE PLAINTEXT HERE.
	// The value of this field should point to the name of a secret
	// that exists in secret manager with the actual SFTP password.
	// The secret name should use the following all upper-case format for
	// the secret name:
	//
	// <CARRIER_NAME>_FTP_PASSWORD
	Password string

	// DO NOT USE PLAINTEXT HERE.
	// The value of this field should point to the name of a secret
	// that exists in secret manager with the actual SFTP ssh-key.
	// The secret manager secret value should be in ~/.ssh/known_hosts format.
	// e.g. roadranger.hostedftp.com ssh-rsa AAAAB3NzaC1yc2EAA...
	// The secret should use the following all lowercase format for
	// the secret name:
	//
	// <CARRIER_NAME>_FTP_SSH_KEY
	//
	// If not provided when the delivery method is for an ftp push,
	// then the push will use an insecure connection.
	SshKey string
}

type EmailConfig struct {
	Recipients   []string
	CcRecipients []string
}

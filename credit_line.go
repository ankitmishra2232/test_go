package credit

import (
	"github.com/shopspring/decimal"
)

type CreditLine struct {
	CarrierUuid string
	Uuid        string
	AsOfDate    string
	Amount      decimal.Decimal
	ChangedBy   string
}

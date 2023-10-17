package credit

import (
	"html"

	pb "github.com/0nramp/protos/carrier"
	"github.com/microcosm-cc/bluemonday"
)

type CreditApplication struct {
	Uuid         string
	Status       string
	UpdatedBy    string
	ApprovedDate string
	DeclinedDate string
	CarrierUuid  string

	CarrierType         string
	BusinessName        string
	BusinessEmail       string
	BusinessAddress1    string
	BusinessAddress2    string
	BusinessCity        string
	BusinessRegion      string
	BusinessZipCode     string
	BusinessCountry     string
	BusinessPhoneNumber string
	BusinessEin         string
	PromotionCode       string

	PrimaryContactName        string
	PrimaryContactEmail       string
	PrimaryContactPhoneNumber string

	DriverId                         string
	NumberOfDrivers                  string
	DepartmentOfTransportationNumber string
	MotorCarrierNumber               string
	EstimatedWeeklySpent             string

	OwnerLegalName     string
	OwnerSsn           string
	OwnerDateOfBirth   string
	OwnerDriverLicense string
	OwnerAddress1      string
	OwnerAddress2      string
	OwnerCity          string
	OwnerRegion        string
	OwnerZipCode       string
	OwnerCountry       string
}

// Sanitize the values of a CreditApplication. This information will be coming
// from a unauthenticated form and we want to do an extra check
// to avoid any malicious value
func (ca CreditApplication) Sanitize() CreditApplication {
	p := bluemonday.UGCPolicy()

	return CreditApplication{
		Uuid:                ca.Uuid,
		PromotionCode:       p.Sanitize(ca.PromotionCode),
		CarrierType:         p.Sanitize(ca.CarrierType),
		BusinessName:        p.Sanitize(ca.BusinessName),
		BusinessEmail:       p.Sanitize(ca.BusinessEmail),
		BusinessAddress1:    p.Sanitize(ca.BusinessAddress1),
		BusinessAddress2:    p.Sanitize(ca.BusinessAddress2),
		BusinessCity:        p.Sanitize(ca.BusinessCity),
		BusinessRegion:      p.Sanitize(ca.BusinessRegion),
		BusinessZipCode:     p.Sanitize(ca.BusinessZipCode),
		BusinessCountry:     p.Sanitize(ca.BusinessCountry),
		BusinessPhoneNumber: p.Sanitize(ca.BusinessPhoneNumber),
		BusinessEin:         p.Sanitize(ca.BusinessEin),

		PrimaryContactName:        p.Sanitize(ca.PrimaryContactName),
		PrimaryContactEmail:       p.Sanitize(ca.PrimaryContactEmail),
		PrimaryContactPhoneNumber: p.Sanitize(ca.PrimaryContactPhoneNumber),

		DriverId:                         p.Sanitize(ca.DriverId),
		NumberOfDrivers:                  p.Sanitize(ca.NumberOfDrivers),
		DepartmentOfTransportationNumber: p.Sanitize(ca.DepartmentOfTransportationNumber),
		MotorCarrierNumber:               p.Sanitize(ca.MotorCarrierNumber),
		EstimatedWeeklySpent:             ca.EstimatedWeeklySpent,

		OwnerLegalName:     p.Sanitize(ca.OwnerLegalName),
		OwnerSsn:           p.Sanitize(ca.OwnerSsn),
		OwnerDateOfBirth:   p.Sanitize(ca.OwnerDateOfBirth),
		OwnerDriverLicense: p.Sanitize(ca.OwnerDriverLicense),
		OwnerAddress1:      p.Sanitize(ca.OwnerAddress1),
		OwnerAddress2:      p.Sanitize(ca.OwnerAddress2),
		OwnerCity:          p.Sanitize(ca.OwnerCity),
		OwnerRegion:        p.Sanitize(ca.OwnerRegion),
		OwnerZipCode:       p.Sanitize(ca.OwnerZipCode),
		OwnerCountry:       p.Sanitize(ca.OwnerCountry),
	}
}

func (ca CreditApplication) ToProto() *pb.CreditApplication {
	return &pb.CreditApplication{
		Uuid:                ca.Uuid,
		PromotionCode:       ca.PromotionCode,
		Status:              ca.Status,
		CarrierType:         ca.CarrierType,
		BusinessName:        html.UnescapeString(ca.BusinessName),
		BusinessEmail:       ca.BusinessEmail,
		BusinessAddress1:    ca.BusinessAddress1,
		BusinessAddress2:    ca.BusinessAddress2,
		BusinessCity:        ca.BusinessCity,
		BusinessRegion:      ca.BusinessRegion,
		BusinessZipCode:     ca.BusinessZipCode,
		BusinessCountry:     ca.BusinessCountry,
		BusinessPhoneNumber: ca.BusinessPhoneNumber,
		BusinessEin:         ca.BusinessEin,

		PrimaryContactName:        ca.PrimaryContactName,
		PrimaryContactEmail:       ca.PrimaryContactEmail,
		PrimaryContactPhoneNumber: ca.PrimaryContactPhoneNumber,

		DriverId:                         ca.DriverId,
		NumberOfDrivers:                  ca.NumberOfDrivers,
		DepartmentOfTransportationNumber: ca.DepartmentOfTransportationNumber,
		MotorCarrierNumber:               ca.MotorCarrierNumber,
		EstimatedWeeklySpent:             ca.EstimatedWeeklySpent,

		OwnerLegalName:     ca.OwnerLegalName,
		OwnerSsn:           ca.OwnerSsn,
		OwnerDateOfBirth:   ca.OwnerDateOfBirth,
		OwnerDriverLicense: ca.OwnerDriverLicense,
		OwnerAddress1:      ca.OwnerAddress1,
		OwnerAddress2:      ca.OwnerAddress2,
		OwnerCity:          ca.OwnerCity,
		OwnerRegion:        ca.OwnerRegion,
		OwnerZipCode:       ca.OwnerZipCode,
		OwnerCountry:       ca.OwnerCountry,
	}
}

func (ca CreditApplication) FromProto(in *pb.CreateCreditApplicationRequest) CreditApplication {
	return CreditApplication{
		Uuid:                ca.Uuid,
		PromotionCode:       in.PromotionCode,
		CarrierType:         in.CarrierType,
		BusinessName:        in.BusinessName,
		BusinessEmail:       in.BusinessEmail,
		BusinessAddress1:    in.BusinessAddress1,
		BusinessAddress2:    in.BusinessAddress2,
		BusinessCity:        in.BusinessCity,
		BusinessRegion:      in.BusinessRegion,
		BusinessZipCode:     in.BusinessZipCode,
		BusinessCountry:     in.BusinessCountry,
		BusinessPhoneNumber: in.BusinessPhoneNumber,
		BusinessEin:         in.BusinessEin,

		PrimaryContactName:        in.PrimaryContactName,
		PrimaryContactEmail:       in.PrimaryContactEmail,
		PrimaryContactPhoneNumber: in.PrimaryContactPhoneNumber,

		DriverId:                         in.DriverId,
		NumberOfDrivers:                  in.NumberOfDrivers,
		DepartmentOfTransportationNumber: in.DepartmentOfTransportationNumber,
		MotorCarrierNumber:               in.MotorCarrierNumber,
		EstimatedWeeklySpent:             in.EstimatedWeeklySpent,

		OwnerLegalName:     in.OwnerLegalName,
		OwnerSsn:           in.OwnerSsn,
		OwnerDateOfBirth:   in.OwnerDateOfBirth,
		OwnerDriverLicense: in.OwnerDriverLicense,
		OwnerAddress1:      in.OwnerAddress1,
		OwnerAddress2:      in.OwnerAddress2,
		OwnerCity:          in.OwnerCity,
		OwnerRegion:        in.OwnerRegion,
		OwnerZipCode:       in.OwnerZipCode,
		OwnerCountry:       in.OwnerCountry,
	}
}

package credit

import (
	"context"

	"github.com/0nramp/utils/trace"
	"github.com/shopspring/decimal"
)

type Service interface {
	// Credit Lines
	CreateCreditLine(ctx context.Context, carrierId string, amount decimal.Decimal, date string) error
	GetCreditLine(ctx context.Context, carrierId string) (CreditLine, error)

	// Credit Application
	CreateCreditApplication(ctx context.Context, ca CreditApplication) (CreditApplication, error)
	GetCreditApplications(ctx context.Context) ([]CreditApplication, error)
	ApproveApplication(ctx context.Context, applicationUuid string) (CreditApplication, error)
	DeclineApplication(ctx context.Context, applicationUuid string) error
}

type Repository interface {
	// Credit lines
	InsertCreditLine(ctx context.Context, carrierId string, amount decimal.Decimal, date string) error
	GetCreditLineByCarrier(ctx context.Context, carrierId string) (CreditLine, error)

	// Credit Application
	GetCreditApplications(ctx context.Context) ([]CreditApplication, error)
	SetCreditApplication(context.Context, CreditApplication) (CreditApplication, error)
	ApproveApplication(ctx context.Context, applicationUuid string) (CreditApplication, error)
	DeclineApplication(ctx context.Context, applicationUuid string) error
}

type service struct {
	repo Repository
}

func NewService(r Repository) Service {
	return &service{repo: r}
}

func (s *service) CreateCreditLine(ctx context.Context, carrierId string, amount decimal.Decimal, date string) error {
	ctx, span := trace.NewSpan(ctx, "service.CreateCreditLine")
	defer span.End()
	return s.repo.InsertCreditLine(ctx, carrierId, amount, date)
}

func (s *service) GetCreditLine(ctx context.Context, carrierId string) (CreditLine, error) {
	ctx, span := trace.NewSpan(ctx, "service.GetCreditLine")
	defer span.End()
	return s.repo.GetCreditLineByCarrier(ctx, carrierId)
}

func (s *service) CreateCreditApplication(ctx context.Context, ca CreditApplication) (CreditApplication, error) {
	ctx, span := trace.NewSpan(ctx, "service.CreateCreditApplication")
	defer span.End()
	return s.repo.SetCreditApplication(ctx, ca.Sanitize())
}

func (s *service) GetCreditApplications(ctx context.Context) ([]CreditApplication, error) {
	ctx, span := trace.NewSpan(ctx, "service.GetCreditApplications")
	defer span.End()
	return s.repo.GetCreditApplications(ctx)
}

func (s *service) ApproveApplication(ctx context.Context, applicationUuid string) (CreditApplication, error) {
	ctx, span := trace.NewSpan(ctx, "service.ApproveApplication")
	defer span.End()
	return s.repo.ApproveApplication(ctx, applicationUuid)
}

func (s *service) DeclineApplication(ctx context.Context, applicationUuid string) error {
	ctx, span := trace.NewSpan(ctx, "service.DeclineApplication")
	defer span.End()
	return s.repo.DeclineApplication(ctx, applicationUuid)

}

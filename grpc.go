package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/slack-go/slack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	b64 "encoding/base64"
	"encoding/json"

	pb "github.com/0nramp/protos/carrier"
	"github.com/0nramp/protos/common"
	c "github.com/0nramp/protos/common"
	"github.com/0nramp/protos/identity"
	"github.com/0nramp/protos/payment"
	"github.com/0nramp/protos/paymentgateway"

	"github.com/0nramp/utils"
	"github.com/0nramp/utils/authz"
	"github.com/0nramp/utils/constants"
	"github.com/0nramp/utils/convert"
	"github.com/0nramp/utils/email"
	"github.com/0nramp/utils/email/layouts"
	"github.com/0nramp/utils/ff"

	"github.com/0nramp/utils/validate"
	"github.com/0nramp/utils/xgrpc"
	"github.com/0nramp/utils/xlog"

	"github.com/0nramp/carrier/pkg/carrierController"
	"github.com/0nramp/carrier/pkg/credit"
	d "github.com/0nramp/carrier/pkg/driver"
	"github.com/0nramp/carrier/pkg/feeController"
	"github.com/0nramp/carrier/pkg/limiter"
	program "github.com/0nramp/carrier/pkg/program"
	"github.com/0nramp/carrier/pkg/referralPartner"
	"github.com/0nramp/carrier/pkg/rewarder"
	"github.com/0nramp/carrier/pkg/storage"
	"github.com/0nramp/carrier/pkg/subsidy"
	"github.com/0nramp/carrier/pkg/tractorController"
	"github.com/0nramp/utils/trace"
)

type server struct {
	pb.UnimplementedCarrierServiceServer
	idtClient        identity.IdentityServiceClient
	pmtClient        payment.PaymentServiceClient
	pmtGatewayClient paymentgateway.PaymentGatewayServiceClient
	emailClient      email.Client

	limiter                limiter.Service
	carrierController      carrierController.Service
	tractorController      tractorController.Service
	driver                 d.Service
	creditor               credit.Service
	feeController          feeController.Service
	subsidyService         subsidy.Service
	rewardService          rewarder.Service
	programService         program.Service
	referralPartnerService referralPartner.Service
	featureFlags           *ff.FeatureFlags
}

type Repository interface {
	d.Repository
	carrierController.Repository
	tractorController.Repository
	limiter.Repository
	credit.Repository
	feeController.Repository
	subsidy.Repository
	rewarder.Repository
	program.Repository
	referralPartner.Repository
}

const (
	originAddress  = "mail-admin@onrampcard.com"
	supportAddress = "support@onrampcard.com"
)

const ReportTZ = "EST"

func NewCarrierServer(
	r Repository,
	ic identity.IdentityServiceClient,
	pc payment.PaymentServiceClient,
	pg paymentgateway.PaymentGatewayServiceClient,
	ec email.Client,
	ff *ff.FeatureFlags,

) pb.CarrierServiceServer {
	return &server{
		idtClient:        ic,
		pmtClient:        pc,
		pmtGatewayClient: pg,
		emailClient:      ec,
		featureFlags:     ff,

		limiter:                limiter.NewService(r),
		carrierController:      carrierController.NewService(r),
		driver:                 d.NewService(r, ic),
		tractorController:      tractorController.NewService(r),
		creditor:               credit.NewService(r),
		feeController:          feeController.NewService(r),
		subsidyService:         subsidy.NewService(r),
		rewardService:          rewarder.NewService(r, pg, pc, ic),
		programService:         program.NewService(r, ic),
		referralPartnerService: referralPartner.NewService(r),
	}
}

func (s *server) CreateCarrier(ctx context.Context, in *pb.CreateCarrierRequest) (*pb.CreateCarrierResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCarrier")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	pgm, err := s.getProgramFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	var externalId string
	if len(in.ExternalId) > 0 {
		externalId = in.ExternalId
	} else {
		externalId = strconv.Itoa(utils.GenerateRandomNumber(8))
	}

	newCarrier, err := s.carrierController.CreateCarrier(ctx, carrierController.Carrier{
		Uuid:                   uuid.NewString(),
		ExternalId:             externalId,
		PublicId:               strconv.Itoa(utils.GenerateRandomNumber(8)),
		DotNumber:              in.DotNumber,
		PrimaryContact:         in.PrimaryContactName,
		Street1:                in.Address.Street1,
		Street2:                in.Address.Street2,
		City:                   in.Address.City,
		Region:                 in.Address.Region,
		PostalCode:             in.Address.PostalCode,
		Phone:                  in.Phone,
		Name:                   in.Name,
		Program:                pgm,
		DriverAttributeHeaders: in.DriverAttributeHeaders,
		ShowDiscountFlag:       carrierController.ShowDiscountFlagAlways,
	})

	if err != nil {
		return nil, xgrpc.Errorf(xgrpc.CodeFromError(err), "failed to insert carrier: %v", err)
	}

	if pgm != nil {
		if err = s.setCarrierDefaults(ctx, newCarrier.Uuid); err != nil {
			return nil, xgrpc.Errorf(xgrpc.CodeFromError(err), "failed to set carrier defaults: %w", err)
		}

		// TODO: once referral_partners (a.k.a brokers) can create carriers we need to patch this
		// to support that behavior, right now assuming only programs are parents of carriers
		if _, err = s.idtClient.SetPermissions(ctx, &identity.SetPermissionsRequest{
			Permissions: []*identity.Permission{{
				Actor:      convert.ToActorPb(constants.AUTHZ_PROGRAM_STRING, pgm.Uuid),
				Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, newCarrier.Uuid),
				Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_EDIT),
			}},
		}); err != nil {
			return nil, xgrpc.Errorf(codes.Internal, "failed to set permissions: %w", err)
		}
	}

	err = s.createLedgerAccount(ctx, newCarrier.Uuid, newCarrier.ExternalId, newCarrier.Name)
	if err != nil {
		ctxLog.Errorf("Unable to create ledger account for carrier %v, err %v", newCarrier.Name, err)
		return nil, xgrpc.Error(codes.Internal, "Internal Error creating carrier")
	}
	ctxLog.Infof("Successfully created ledger account for: %v", newCarrier.Name)

	return &pb.CreateCarrierResponse{
		Carrier: toCarrierPb(newCarrier),
	}, nil
}

func (s *server) setCarrierDefaults(ctx context.Context, carrierUuid string) error {
	err := s.creditor.CreateCreditLine(ctx, carrierUuid, decimal.NewFromFloat(10000), convert.FormatDateToString(time.Now()))
	if err != nil {
		return fmt.Errorf("failed setting carrier credit line: %w", err)
	}

	dieselLimit := limiter.Limit{
		CarrierUUID:     carrierUuid,
		LimitUUID:       uuid.NewString(),
		ProductCategory: convert.DIESEL_CATEGORY,
		AsOfDate:        convert.FormatDateToString(time.Now()),
		Amount:          decimal.NewFromFloat(1000),
	}
	reeferLimit := limiter.Limit{
		CarrierUUID:     carrierUuid,
		LimitUUID:       uuid.NewString(),
		ProductCategory: convert.REEFER_CATEGORY,
		AsOfDate:        convert.FormatDateToString(time.Now()),
		Amount:          decimal.NewFromFloat(300),
	}
	defLimit := limiter.Limit{
		CarrierUUID:     carrierUuid,
		LimitUUID:       uuid.NewString(),
		ProductCategory: convert.DEF_CATEGORY,
		AsOfDate:        convert.FormatDateToString(time.Now()),
		Amount:          decimal.NewFromFloat(150),
	}
	additiveLimit := limiter.Limit{
		CarrierUUID:     carrierUuid,
		LimitUUID:       uuid.NewString(),
		ProductCategory: convert.ADDITIVE_CATEGORY,
		AsOfDate:        convert.FormatDateToString(time.Now()),
		Amount:          decimal.NewFromFloat(80),
	}
	limits := []*limiter.Limit{&dieselLimit, &reeferLimit, &defLimit, &additiveLimit}

	err = s.limiter.CreateLimits(ctx, limits)
	if err != nil {
		return fmt.Errorf("failed setting carrier driver limits: %w", err)
	}

	c, err := s.carrierController.RetrieveCarrier(ctx, carrierUuid)
	if err != nil {
		return fmt.Errorf("failed getting carrier : %w", err)
	}
	err = s.carrierController.CreatePrompts(ctx,
		carrierController.Prompts{
			CarrierId:        c.Id,
			HasTruckNumber:   true,
			HasTrailerNumber: true,
			AsOfDate:         convert.FormatDateToString(time.Now()),
		},
	)
	if err != nil {
		return fmt.Errorf("failed setting carrier driver prompts: %w", err)
	}
	return nil

}

func (s *server) UpdateCarrier(ctx context.Context, in *pb.UpdateCarrierRequest) (*pb.UpdateCarrierResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.UpdateCarrier")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Update Carrier: %v", in.String())
	if len(in.Carrier.Uuid) > 0 {
		if err := validate.Uuid(in.Carrier.Uuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid carrier uuid: %w", in.Carrier.Uuid, err)
		}
	} else if len(in.Carrier.ExternalId) == 0 {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "externalId and carrier uuid both unset; expected one or the other")
	}

	carrier, err := s.getCarrierFromUuidOrExtId(ctx, in.Carrier.Uuid, in.Carrier.ExternalId)
	if err != nil {
		code := xgrpc.CodeFromError(err)
		if errors.Is(err, utils.ErrNoData) {
			return nil, xgrpc.Errorf(code, "carrier not found: %w", err)
		}
		return nil, xgrpc.Errorf(code, "unexpected error getting carrier: %w", err)
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	if len(in.Carrier.ExternalId) > 0 && in.Carrier.ExternalId != carrier.ExternalId {
		return nil, xgrpc.Error(codes.InvalidArgument, "can not update carrier externalId")
	}

	if len(in.Carrier.Uuid) > 0 && in.Carrier.Uuid != carrier.Uuid {
		return nil, xgrpc.Error(codes.InvalidArgument, "can not update carrier uuid")
	}

	if len(in.Carrier.DotNumber) > 0 {
		carrier.DotNumber = in.Carrier.DotNumber
	}
	if len(in.Carrier.PrimaryContactName) > 0 {
		carrier.PrimaryContact = in.Carrier.PrimaryContactName
	}
	if in.Carrier.Address != nil && len(in.Carrier.Address.Street1) > 0 {
		carrier.Street1 = in.Carrier.Address.Street1
	}

	if in.Carrier.Address != nil && len(in.Carrier.Address.Street2) > 0 {
		carrier.Street2 = in.Carrier.Address.Street2
	}
	if in.Carrier.Address != nil && len(in.Carrier.Address.City) > 0 {
		carrier.City = in.Carrier.Address.City
	}

	if in.Carrier.Address != nil && len(in.Carrier.Address.Region) > 0 {
		carrier.Region = in.Carrier.Address.Region
	}

	if in.Carrier.Address != nil && len(in.Carrier.Address.PostalCode) > 0 {
		carrier.PostalCode = in.Carrier.Address.PostalCode
	}
	if len(in.Carrier.Phone) > 0 {
		carrier.Phone = in.Carrier.Phone
	}
	if len(in.Carrier.Name) > 0 {
		carrier.Name = in.Carrier.Name
	}
	if len(in.Carrier.DriverAttributeHeaders) > 0 {
		carrier.DriverAttributeHeaders = in.Carrier.DriverAttributeHeaders
	}
	if in.Carrier.ShowDiscountFlag != pb.ShowDiscountToDrivers_SHOW_DISCOUNT_TO_DRIVERS_UNSPECIFIED {
		carrier.ShowDiscountFlag = carrierController.ShowDiscountFlag(in.Carrier.ShowDiscountFlag.Number())
	}
	if in.Carrier.Status != pb.CarrierStatus_CARRIER_STATUS_UNSPECIFIED {
		carrier.Status = carrierController.CarrierStatus(in.Carrier.Status.Number())
	}

	newCarrier, err := s.carrierController.UpdateCarrier(ctx, *carrier)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "failed to update carrier: %w", err)
	}
	return &pb.UpdateCarrierResponse{
		Carrier: toCarrierPb(newCarrier),
	}, nil
}

func (s *server) SetProgram(ctx context.Context, in *pb.SetProgramRequest) (*pb.SetProgramResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetProgram")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("received Set Program: %v", in.String())

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	var progRevSharePercent decimal.Decimal
	var err error
	if len(in.RevenueSharePercentage) > 0 {
		progRevSharePercent, err = decimal.NewFromString(in.RevenueSharePercentage)
		if err != nil {
			return nil, status.Error(
				codes.InvalidArgument,
				fmt.Sprintf("invalid value for revenue_share_percentage [%s]", in.RevenueSharePercentage),
			)
		}
	}

	program, err := s.programService.SetProgram(
		ctx,
		in.Uuid,
		in.ExternalId,
		in.Name,
		in.ApiHandle,
		convert.ToStringPaymentTypes(in.AcceptedPaymentTypes),
		in.CarrierUuids,
		progRevSharePercent,
	)
	if err != nil {
		ctxLog.Errorf("error inserting program %v", err)
		return nil, status.Error(codes.Internal, fmt.Errorf("%w", err).Error())
	}

	if len(in.CarrierUuids) > 0 {
		perms := []*identity.Permission{}
		for _, cUid := range in.CarrierUuids {
			perms = append(perms,
				&identity.Permission{
					Actor:      convert.ToActorPb(constants.AUTHZ_PROGRAM_STRING, program.Uuid),
					Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, cUid),
					Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_EDIT),
				},
			)
		}

		if _, err := s.idtClient.SetPermissions(ctx, &identity.SetPermissionsRequest{
			Permissions: perms,
		}); err != nil {
			return nil, xgrpc.Errorf(codes.Internal, "failed to set permissions: %w", err)
		}
	}

	if len(in.RemovedCarrierUuids) > 0 {
		perms := []*identity.Permission{}
		for _, cUid := range in.RemovedCarrierUuids {
			perms = append(perms,
				&identity.Permission{
					Actor:      convert.ToActorPb(constants.AUTHZ_PROGRAM_STRING, program.Uuid),
					Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, cUid),
					Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_EDIT),
				},
			)
		}

		if _, err := s.idtClient.DeletePermissions(ctx, &identity.DeletePermissionsRequest{
			Permissions: perms,
		}); err != nil && err != utils.ErrNoData {
			return nil, xgrpc.Errorf(codes.Internal, "failed to delete permissions: %w", err)
		}
	}

	return &pb.SetProgramResponse{
		Program: toProgramPb(&program),
	}, nil
}

func (s *server) ListPrograms(ctx context.Context, in *pb.ListProgramsRequest) (*pb.ListProgramsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListPrograms")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("received ListPrograms: %v", in.String())

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	programs, err := s.programService.ListPrograms(ctx)
	if err != nil {
		return &pb.ListProgramsResponse{}, err
	}

	programPbs := make([]*pb.Program, len(programs))
	for i, v := range programs {
		programPbs[i] = toProgramPb(&v)
	}
	return &pb.ListProgramsResponse{Programs: programPbs}, err
}

func (s *server) RetrieveProgram(ctx context.Context, in *pb.RetrieveProgramRequest) (*pb.RetrieveProgramResponse, error) {

	var p *program.Program
	var err error
	if len(in.Uuid) == 0 && len(in.ApiHandle) == 0 {
		return nil, status.Error(codes.InvalidArgument, "program uuid or api handle required")
	}
	if len(in.Uuid) > 0 {
		p, err = s.programService.GetProgramByUuid(ctx, in.Uuid)
		if err != nil {
			return nil, status.Error(codes.Internal, "unable to get program by uuid")
		}
	}
	if len(in.ApiHandle) > 0 {
		p, err = s.programService.GetProgramByApiHandle(ctx, in.ApiHandle)
		if err != nil {
			return nil, status.Error(codes.Internal, "unable to get program by apiHandle")
		}
	}

	return &pb.RetrieveProgramResponse{
		Program: toProgramPb(p),
	}, nil

}

func (s *server) CreateMultipleCarriers(ctx context.Context, in *pb.CreateMultipleCarriersRequest) (*pb.CreateMultipleCarriersResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateMultipleCarriers")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("CreateMultipleCarriers with: %v", in.String())

	// NOTE1: if we open this endpoint to programs and referral_partner we should
	// change this logic to check for different set of permissions
	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	var inserted int32
	for _, v := range in.Carriers {
		var externalId string
		if len(v.ExternalId) > 0 {
			externalId = v.ExternalId
		} else {
			externalId = strconv.Itoa(utils.GenerateRandomNumber(8))
		}
		carrier := carrierController.Carrier{
			Uuid:             uuid.NewString(),
			ExternalId:       externalId,
			PublicId:         externalId,
			DotNumber:        v.DotNumber,
			PrimaryContact:   v.PrimaryContactName,
			Street1:          v.Address.GetStreet1(),
			Street2:          v.Address.GetStreet2(),
			City:             v.Address.GetCity(),
			Region:           v.Address.GetRegion(),
			PostalCode:       v.Address.GetPostalCode(),
			Phone:            v.Phone,
			Name:             v.Name,
			ShowDiscountFlag: carrierController.ShowDiscountFlagAlways,
		}
		_, err := s.carrierController.CreateCarrier(ctx, carrier)
		if err != nil {
			ctxLog.Errorf("Error inserting carrier %v", err)
		} else {
			inserted++
			err = s.setCarrierDefaults(ctx, carrier.Uuid)
			if err != nil {
				return nil, xgrpc.Errorf(xgrpc.CodeFromError(err), "failed setting carrier defaults: %w", err)
			}
			err = s.createLedgerAccount(ctx, carrier.Uuid, carrier.ExternalId, carrier.Name)
			if err != nil {
				return nil, xgrpc.Errorf(codes.Internal, "Unable to create ledger account for carrier %v, err %w", carrier.Name, err)
			}
			xlog.Infof("Successfully created ledger account for: %v", carrier.Name)
		}
	}

	ctxLog.Infof("Success creating %v carriers", inserted)

	return &pb.CreateMultipleCarriersResponse{
		Inserted: inserted,
	}, nil
}

func (s *server) ListCarriers(ctx context.Context, in *pb.ListCarriersRequest) (*pb.ListCarriersResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListCarriers")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()
	ctxLog.Infof("ListCarriers")

	carriers, err := s.carrierController.ListCarriers(ctx, in.ProgramUuid)
	if errors.Is(err, utils.ErrPermissionDenied) {
		return nil, xgrpc.Error(codes.PermissionDenied, err)
	}
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "error listing carriers %v", err)
	}

	rv := []*pb.Carrier{}
	for _, v := range carriers {
		rv = append(rv, toCarrierPb(v))
	}
	sort.Slice(rv, func(i, j int) bool {
		return rv[i].Name < rv[j].Name
	})
	return &pb.ListCarriersResponse{
		Carriers: rv,
	}, nil
}

func (s *server) RetrieveCarrier(ctx context.Context, in *pb.RetrieveCarrierRequest) (*pb.RetrieveCarrierResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.RetrieveCarrier")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("RetrieveCarrier with uuid:%s externalId: %s", in.Uuid, in.ExternalId)
	var carrier carrierController.Carrier
	var err error
	if len(in.Uuid) > 0 {
		if err := validate.Uuid(in.Uuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid carrier uuid: %v", in.Uuid, err)
		}
		carrier, err = s.carrierController.RetrieveCarrier(ctx, in.Uuid)
		if err != nil {
			code := xgrpc.CodeFromError(err)
			if errors.Is(err, utils.ErrNoData) {
				return nil, xgrpc.Errorf(code, "carrier for uuid[%s] not found: %w", in.Uuid, err)
			}
			return nil, xgrpc.Errorf(code, "unexpected error retreiving carrier by uuid[%s]: %w", in.Uuid, err)
		}
	} else if len(in.ExternalId) > 0 {
		carrier, err = s.carrierController.GetCarrierByExternalId(ctx, in.ExternalId)
		if err != nil {
			code := xgrpc.CodeFromError(err)
			if errors.Is(err, utils.ErrNoData) {
				return nil, xgrpc.Errorf(code, "carrier for external-id[%s] not found: %w", in.ExternalId, err)
			}
			return nil, xgrpc.Errorf(code, "unexpected error retrieving carrier for external-id[%s]: %w", in.ExternalId, err)
		}
	} else {
		return nil, xgrpc.Error(codes.InvalidArgument, "uuid or externalId required to retrieve a carrier")
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_VIEW); !hasPerm {
		return &pb.RetrieveCarrierResponse{}, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	return &pb.RetrieveCarrierResponse{
		Carrier: toCarrierPb(carrier),
	}, nil

}

func (s *server) GetCarrierReadiness(ctx context.Context, in *pb.GetCarrierReadinessRequest) (*pb.GetCarrierReadinessResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierReadiness")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("GetCarrierReadiness with carrierUuids:%v", in.CarrierUuids)

	p, err := s.getProgramFromCtx(ctx)

	if err != nil {
		return nil, xgrpc.Errorf(codes.Unauthenticated, "error authorizing request %v", err)
	}

	carrierUuids := []string{}
	if p != nil {
		pCarriers, err := s.carrierController.ListCarriersByProgram(ctx, p.Uuid)
		if err != nil {
			return nil, xgrpc.Errorf(codes.Unauthenticated, "error authorizing request %v", err)
		}
		for _, v := range in.CarrierUuids {
			for _, pc := range pCarriers {
				if v == pc.Uuid {
					carrierUuids = append(carrierUuids, v)
					break
				}
			}
		}
	} else {
		carrierUuids = in.CarrierUuids
	}

	statuses, err := s.carrierController.GetReadiness(ctx, carrierUuids)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "Unable to get carrier readiness for carriers %v, err: %v", carrierUuids, err)
	}

	listUsers, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{
		ParentUuids: carrierUuids,
		Role:        identity.Role_FLEET_MANAGER,
	})

	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "Unable to get fleet app readiness for carriers %v, err: %v", carrierUuids, err)
	}

	usersWithRole := listUsers.Users
	fleetAppRoleMap := make(map[string]bool)
	for _, user := range usersWithRole {
		if user.Role == identity.Role_FLEET_MANAGER {
			fleetAppRoleMap[user.ParentUuid] = true
		}
	}
	for _, s := range statuses {
		_, ok := fleetAppRoleMap[s.CarrierUuid]
		if ok {
			s.FleetAppReady = &pb.ReadinessDetail{
				Status: pb.ReadyStatus_READY_STATUS_READY,
			}
		}
	}

	rv := &pb.GetCarrierReadinessResponse{
		Status: statuses,
	}
	return rv, nil
}

func (s *server) CreateCarrierTractorUnit(ctx context.Context, in *pb.CreateCarrierTractorUnitRequest) (*pb.CreateCarrierTractorUnitResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCarrierTractorUnit")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Create Tractor: %v", in.String())
	carrier, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	if err != nil {
		ctxLog.Errorf("Unable to retrieve carrier with info %+v err:  %v", in, err)
		return &pb.CreateCarrierTractorUnitResponse{}, err
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	tractor := tractorController.Tractor{
		Uuid:        uuid.NewString(),
		Vin:         in.Vin,
		UnitId:      in.UnitId,
		CarrierUuid: in.CarrierUuid,
		StartDate:   in.StartDate,
	}
	err = s.tractorController.CreateTractor(ctx, tractor)
	if err != nil {
		return &pb.CreateCarrierTractorUnitResponse{}, err
	}

	tractorResp := &pb.CarrierTractorUnit{
		Uuid:        tractor.Uuid,
		Vin:         tractor.Vin,
		UnitId:      tractor.UnitId,
		CarrierUuid: tractor.CarrierUuid,
	}

	return &pb.CreateCarrierTractorUnitResponse{
		CarrierTractorUnit: tractorResp,
	}, nil
}

func (s *server) CreateCarrierTractorUnits(
	ctx context.Context, r *pb.CreateCarrierTractorUnitsRequest,
) (*pb.CreateCarrierTractorUnitsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCarrierTractorUnits")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	err := validateCreateCarrierTractorUnitsRequest(r)
	if err != nil {
		return &pb.CreateCarrierTractorUnitsResponse{}, err
	}

	for _, tr := range r.TractorUnits {
		err := s.tractorController.CreateTractor(
			ctx,
			tractorController.Tractor{
				Uuid:        uuid.New().String(),
				Vin:         tr.Vin,
				UnitId:      tr.UnitId,
				CarrierUuid: tr.CarrierUuid,
				StartDate:   tr.StartDate,
			},
		)
		if err != nil {
			ctxLog.Errorf("There was an issue adding Tractor Unit: %v", err)
		}

	}

	return &pb.CreateCarrierTractorUnitsResponse{
		//Tractors: rv,
	}, nil
}

func (s *server) ListCarrierTractorUnits(
	ctx context.Context, in *pb.ListCarrierTractorUnitsRequest,
) (*pb.ListCarrierTractorUnitsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCarrierTractorUnits")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received ListCarrierTractorUnits %v", in.String())

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	tractors, err := s.tractorController.ListCarrierTractorUnits(ctx)
	if err != nil {
		ctxLog.Infof("Error listing tractors is %v", err)
		return &pb.ListCarrierTractorUnitsResponse{}, err
	}

	rv := []*pb.CarrierTractorUnit{}
	for _, v := range tractors {

		t := pb.CarrierTractorUnit{
			Uuid:        v.Uuid,
			Vin:         v.Vin,
			UnitId:      v.UnitId,
			CarrierUuid: v.CarrierUuid,
			StartDate:   v.StartDate,
		}
		rv = append(rv, &t)
	}
	ctxLog.Info("Success listing tractors")

	return &pb.ListCarrierTractorUnitsResponse{
		TractorUnits: rv,
	}, nil
}

func (s *server) SetLimits(
	ctx context.Context, in *pb.SetLimitsRequest,
) (*pb.SetLimitsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetLimits")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("received Set Limits: %v", in.String())
	carrier, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	if err != nil {
		ctxLog.Errorf("Unable to retrieve carrier with info %+v err:  %v", in, err)
		return nil, err
	}
	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	for i, limit := range in.Limits {
		if err := validateLimit(limit); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "failed to validate limit at index [%v] err: %v", i, err)
		}
	}

	limits := []*limiter.Limit{}
	for _, limit := range in.Limits {
		amt, _ := decimal.NewFromString(limit.Amount)
		qty, _ := decimal.NewFromString(limit.Quantity)

		limits = append(limits, &limiter.Limit{
			CarrierUUID:     in.CarrierUuid,
			LimitUUID:       uuid.NewString(),
			Amount:          amt,
			AsOfDate:        limit.AsOfDate,
			ProductCategory: convert.ToProductCategory(limit.ProductCategory),
			Quantity:        qty,
		})
	}

	err = s.limiter.CreateLimits(ctx, limits)
	if err != nil {
		return nil, err
	}

	return &pb.SetLimitsResponse{
		Limits: toLimitsPb(limits),
	}, nil

}

func (s *server) getCarrierFromUuidOrExtId(ctx context.Context, uuid string, extId string) (*carrierController.Carrier, error) {
	if len(uuid) > 0 {
		carrier, err := s.carrierController.RetrieveCarrier(ctx, uuid)
		if err != nil {
			return nil, fmt.Errorf("failed to get carrier by uuid [%v]: %w", uuid, err)
		}
		return &carrier, err
	}
	carrier, err := s.carrierController.GetCarrierByExternalId(ctx, extId)
	if err != nil {
		return nil, fmt.Errorf("failed to get carrier by external id [%v]: %w", extId, err)
	}
	return &carrier, err
}

func (s *server) SetLimit(
	ctx context.Context, in *pb.SetLimitRequest,
) (*pb.SetLimitResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetLimit")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()
	ctxLog.Infof("received Set Single Limit: %v", in.String())
	if len(in.CarrierUuid) > 0 {
		if err := validate.Uuid(in.CarrierUuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid carrier uuid: %w", in.CarrierUuid, err)
		}
	} else if len(in.CarrierExternalId) == 0 {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "externalId and carrier uuid both unset; expected one or the other")
	}

	carrier, err := s.getCarrierFromUuidOrExtId(ctx, in.CarrierUuid, in.CarrierExternalId)
	if err != nil {
		code := xgrpc.CodeFromError(err)
		if errors.Is(err, utils.ErrNoData) {
			return nil, xgrpc.Errorf(code, "carrier not found: %w", err)
		}
		return nil, xgrpc.Errorf(code, "unexpected error getting carrier: %w", err)
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}
	if err := validateLimit(in.Limit); err != nil {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "failed to validate single limit %w", err)
	}
	amt, _ := decimal.NewFromString(in.Limit.Amount)
	qty, _ := decimal.NewFromString(in.Limit.Quantity)

	limit := limiter.Limit{
		CarrierUUID:     carrier.Uuid,
		LimitUUID:       uuid.NewString(),
		Amount:          amt,
		AsOfDate:        in.Limit.AsOfDate,
		ProductCategory: convert.ToProductCategory(in.Limit.ProductCategory),
		Quantity:        qty,
	}

	err = s.limiter.CreateLimit(ctx, limit)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "Unable to create limit %w", err)
	}

	return &pb.SetLimitResponse{
		Limit: toLimitPb(&limit),
	}, nil

}

func (s *server) ListSubsidies(ctx context.Context, in *pb.ListSubsidiesRequest) (*pb.ListSubsidiesResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListSubsidies")
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	ss, err := s.subsidyService.ListSubsidies(ctx, subsidy.SubsidyFilters{
		MerchantUuid:   in.MerchantUuid,
		CarrierUuid:    in.CarrierUuid,
		Date:           in.AsOfDate,
		PaymentNetwork: pbToPaymentNetwork(in.PaymentNetwork),
	})

	if err != nil && errors.Unwrap(err) != storage.ErrNoData {
		return &pb.ListSubsidiesResponse{}, status.Error(codes.Internal, fmt.Errorf("%w", err).Error())
	}

	out := []*pb.Subsidy{}
	for _, sub := range ss {
		out = append(out, toSubsidyPb(sub))
	}

	return &pb.ListSubsidiesResponse{Subsidies: out}, nil
}

func (s *server) SetSubsidy(ctx context.Context, in *pb.SetSubsidyRequest) (*pb.SetSubsidyResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetSubsidy")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	if err := validateSetSubsidyRequest(in); err != nil {
		return nil, err
	}

	car, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	if err != nil {
		ctxLog.Errorf("Carrier not found for uuid: %v err: %v", in.CarrierUuid, err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	sub := subsidy.Subsidy{
		Uuid:           uuid.New().String(),
		Carrier:        car,
		MerchantUuid:   in.MerchantUuid,
		AsOfDate:       in.AsOfDate,
		Amount:         decimal.RequireFromString(in.Amount),
		PaymentNetwork: pbToPaymentNetwork(in.PaymentNetwork),
	}

	if len(in.LocationUuids) == 0 {
		err = s.subsidyService.CreateSubsidy(ctx, sub)
	} else {
		subs := []subsidy.Subsidy{}
		for _, lUuid := range in.LocationUuids {
			sub.LocationUuid = lUuid
			subs = append(subs, sub)
		}
		err = s.subsidyService.CreateSubsidies(ctx, subs)
	}

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.SetSubsidyResponse{}, nil
}

func (s *server) GetSubsidy(ctx context.Context, in *pb.GetSubsidyRequest) (*pb.GetSubsidyResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetSubsidy")
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, status.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	sf := subsidy.SubsidyFilters{
		MerchantUuid:   in.MerchantUuid,
		CarrierUuid:    in.CarrierUuid,
		Date:           in.AsOfDate,
		LocationUuid:   in.LocationUuid,
		PaymentNetwork: pbToPaymentNetwork(in.PaymentNetwork),
	}
	var sub subsidy.Subsidy
	sub, err := s.subsidyService.GetSubsidy(ctx, sf)

	if err != nil {
		var code codes.Code
		if errors.Unwrap(err) == storage.ErrNoData {
			code = codes.NotFound
		} else {
			code = codes.Internal
		}
		return &pb.GetSubsidyResponse{}, status.Error(code, fmt.Errorf("%w", err).Error())
	}
	return &pb.GetSubsidyResponse{Subsidy: toSubsidyPb(sub)}, nil
}

func (s *server) GetLimits(ctx context.Context, in *pb.GetLimitsRequest) (*pb.GetLimitsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetLimits")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	p, err := s.getProgramFromCtx(ctx)
	if err != nil {
		return &pb.GetLimitsResponse{}, err
	}

	carrier, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)

	if err != nil {
		ctxLog.Errorf("Unable to retrieve carrier with info %+v err:  %v", in, err)
		return &pb.GetLimitsResponse{}, err
	}

	if p != nil {
		// TODO: replace this by HasPermission check
		if carrier.Program == nil || carrier.Program.Uuid != (*p).Uuid {
			return &pb.GetLimitsResponse{}, status.Error(codes.PermissionDenied, "insufficient perimissions")
		}
	}

	limits, err := s.limiter.GetLimits(ctx, limiter.LimitFilters{
		CarrierUuid: in.CarrierUuid,
		AsOfDate:    in.RequestDate,
	})

	if err != nil {
		ctxLog.Error(err)
		return &pb.GetLimitsResponse{}, err
	}
	rv := toLimitsPb(limits)
	return &pb.GetLimitsResponse{
		Limits: rv,
	}, nil
}

func (s *server) SetTransactionFees(
	ctx context.Context, in *pb.SetTransactionFeesRequest,
) (*pb.SetTransactionFeesResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetTransactionFees")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Set Fees: %v", in.String())
	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, xgrpc.Error(codes.PermissionDenied, "insufficient perimissions")
	}

	rv := []*pb.FeeOrError{}
	for _, fee := range in.Fees {
		feeOrError := &pb.FeeOrError{}
		var applicationError c.ApplicationError
		var rvFee pb.TransactionFee
		err := validateCreateTransactionFeeRequest(fee)

		if err != nil {
			applicationError = c.ApplicationError{
				ErrorCode:    int32(codes.InvalidArgument),
				ErrorMessage: err.Error(),
			}
			feeOrError.Error = &applicationError
		} else {
			fa, _ := decimal.NewFromString(fee.FlatAmount)
			pa, _ := decimal.NewFromString(fee.PercentAmount)
			pg, _ := decimal.NewFromString(fee.PerGallonAmount)

			feeStruct := feeController.Fee{
				CarrierUuid:     in.CarrierUuid,
				ProgramUuid:     in.ProgramUuid,
				Uuid:            uuid.NewString(),
				FlatAmount:      fa,
				PercentAmount:   pa,
				AsOfDate:        fee.AsOfDate,
				Type:            convert.ToFeeType(fee.Type),
				ProductCategory: convert.ToProductCategory(fee.ProductCategory),
				BillingType:     convert.NewBillingTypeFromProto(fee.BillingType),
				FeeCategory:     fee.FeeCategory.String(),
				MerchantUuid:    fee.MerchantUuid,
				PerGallonAmount: pg,
			}
			err = s.feeController.CreateTransactionFee(ctx, feeStruct)

			if err != nil {
				applicationError = c.ApplicationError{
					ErrorCode:    int32(codes.Internal),
					ErrorMessage: err.Error(),
				}
				feeOrError.Error = &applicationError
			} else {
				rvFee = pb.TransactionFee{
					CarrierUuid:     in.CarrierUuid,
					AsOfDate:        fee.AsOfDate,
					PercentAmount:   fee.PercentAmount,
					FlatAmount:      fee.FlatAmount,
					ProductCategory: fee.ProductCategory,
					Type:            fee.Type,
					ProgramUuid:     in.ProgramUuid,
					BillingType:     fee.BillingType,
					MerchantUuid:    fee.MerchantUuid,
					PerGallonAmount: fee.PerGallonAmount,
					FeeCategory:     fee.FeeCategory,
				}
				feeOrError.Fee = &rvFee
			}
		}
		rv = append(rv, feeOrError)
	}

	return &pb.SetTransactionFeesResponse{
		Responses: rv,
	}, nil

}

func (s *server) GetTransactionFees(ctx context.Context, in *pb.GetTransactionFeesRequest) (*pb.GetTransactionFeesResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetTransactionFees")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	fees, err := s.feeController.GetTransactionFees(ctx, feeController.FeeFilters{
		CarrierUuid: in.CarrierUuid,
		AsOfDate:    in.RequestDate,
		ProgramUuid: in.ProgramUuid,
	})
	if err != nil {
		ctxLog.Error(err)
		return &pb.GetTransactionFeesResponse{}, err
	}

	rv := toTransactionFeesPb(fees)
	return &pb.GetTransactionFeesResponse{
		Fees: rv,
	}, nil

}

func (s *server) GetFullCarrierData(ctx context.Context, in *pb.GetFullCarrierDataRequest) (*pb.GetFullCarrierDataResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetFullCarrierData")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if in.BillingType == c.BillingType_BILLING_TYPE_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "BillingType required")
	}

	carrier, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	if err != nil {
		ctxLog.Errorf("Unable to retrieve carrier with info %+v err:  %v", in, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	var programUuid string
	if carrier.Program != nil {
		programUuid = carrier.Program.Uuid
	} else {
		programUuid = ""
	}

	// Limits
	limits, err := s.limiter.GetLimits(ctx, limiter.LimitFilters{
		CarrierUuid: carrier.Uuid,
		AsOfDate:    in.AsOfDate,
	})
	if err != nil {
		ctxLog.Errorf("Unable to get limits for carrier %s, [%v]", carrier.Uuid, err)
		return &pb.GetFullCarrierDataResponse{}, status.Error(codes.Internal, err.Error())
	}

	// Fees
	fees, err := s.feeController.GetTransactionFees(ctx, feeController.FeeFilters{
		CarrierUuid:  carrier.Uuid,
		AsOfDate:     in.AsOfDate,
		ProgramUuid:  programUuid,
		BillingType:  convert.NewBillingTypeFromProto(in.BillingType),
		MerchantUuid: in.MerchantUuid,
	})

	if err != nil {
		ctxLog.Errorf("Unable to get fees for carrier %s, [%v]", carrier.Uuid, err)
		return &pb.GetFullCarrierDataResponse{}, status.Error(codes.Internal, err.Error())
	}

	// Fee triage logic:
	// assumption: carrier transaction fees are tied to product categories
	//     case #1: cash advance fee is a flat fee for product type 955
	//     case #2: transaction fee is a flat fee for dispensable products (diesel, reefer, def)
	// note: if multiple line items qualify for a fee in a transaction (e.g diesel. reefer).
	// fee should only apply once

	// Example #1: flat transaction fee of $.95, driver buys diesel and reefer outside
	//    2 line items for prod codes 19 and 32, fee of $.95 on diesel line item
	// Example #2: flat transaction fee of $.95, cash advance fee $1, driver buys fuel and takes cash
	//    2 line items for prod codes 19 (diesel) and 955 (cash), fee of $.95 on diesel, $1 on cash
	// Example #3: driver takes cash advance
	//    1 line item, cash advance code 955, fee $1
	// Example #4: driver buys nonfuel item inside
	//    1 line item, no fees
	for _, f := range fees {
		if f.ProductCategory != convert.CASHADVANCE_CATEGORY && f.ProductCategory != convert.DIESEL_CATEGORY {
			// TODO: consider maybe adding other diepsensable here (DEF_CATEGORY and REEFER_CATEGORY)
			// but this is nice and simple for now and covers 90% of transactions people do
			return &pb.GetFullCarrierDataResponse{}, status.Error(
				codes.FailedPrecondition,
				fmt.Sprintf("Carrier transaction fee should apply to cash advance or fuel, received %v", f),
			)
		}
	}

	// CreditLine
	cLine, err := s.creditor.GetCreditLine(ctx, carrier.Uuid)
	if err != nil {
		ctxLog.Errorf("Unable to get credit line for carrier %s, [%v]", carrier.Uuid, err)
		return &pb.GetFullCarrierDataResponse{}, status.Error(codes.Internal, fmt.Sprintf("Unable to get credit line for carrier %v", carrier.Uuid))
	}

	// Subsidy
	sf := subsidy.SubsidyFilters{
		MerchantUuid:   in.MerchantUuid,
		CarrierUuid:    carrier.Uuid,
		Date:           in.AsOfDate,
		LocationUuid:   in.LocationUuid,
		PaymentNetwork: pbToPaymentNetwork(in.PaymentNetwork),
	}
	sub, err := s.subsidyService.GetSubsidy(ctx, sf)
	if err != nil {
		ctxLog.Errorf("Unable to get subsidy for carrier %s, [%v]", carrier.Uuid, err)
		var code codes.Code
		if errors.Unwrap(err) == storage.ErrNoData {
			code = codes.NotFound
		} else {
			code = codes.Internal
		}
		return &pb.GetFullCarrierDataResponse{}, status.Error(code, err.Error())
	}

	var pmtTypes []c.PaymentInstrumentType
	if carrier.Program != nil {
		pmtTypes = convert.ToPbPaymentTypes(carrier.Program.AcceptedPaymentTypes)
	} else {
		pmtTypes = []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN}
	}
	xlog.Infof("carrier svc returning fees %+v", fees)
	return &pb.GetFullCarrierDataResponse{
		Limits:               toLimitsPb(limits),
		Fees:                 toTransactionFeesPb(fees),
		CreditLine:           toCreditLinePb(cLine),
		Subsidy:              toSubsidyPb(sub),
		AcceptedPaymentTypes: pmtTypes,
	}, nil

}

func (s *server) SetCarrierUser(
	ctx context.Context, in *pb.SetCarrierUserRequest,
) (*pb.SetCarrierUserResponse, error) {
	resp, err := s.idtClient.CreateUser(ctx, &identity.CreateUserRequest{
		User: &identity.IdentityUser{
			ParentUuid:  in.CarrierUuid, //todo remove
			ParentUuids: []string{in.CarrierUuid},
			FirstName:   in.User.FirstName,
			LastName:    in.User.LastName,
			Email:       in.User.Email,
			PhoneNumber: in.User.PhoneNumber,
			Role:        in.User.Role,
			EntityUuid:  uuid.NewString(),
		},
	})
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "There was an issue setting user: %w", err)
	}
	return &pb.SetCarrierUserResponse{
		User: &identity.IdentityUser{
			EntityUuid:   resp.User.EntityUuid,
			IdentityUuid: resp.User.IdentityUuid,
			FirstName:    resp.User.FirstName,
			LastName:     resp.User.LastName,
			Email:        resp.User.Email,
			PhoneNumber:  resp.User.PhoneNumber,
			Role:         resp.User.Role,
		},
	}, err
}

func (s *server) CreateDrivers(
	ctx context.Context, r *pb.CreateDriversRequest,
) (*pb.CreateDriversResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateDrivers")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	err := validateCreateDriversRequest(r)
	if err != nil {
		return &pb.CreateDriversResponse{}, err
	}

	carrier, err := s.carrierController.RetrieveCarrier(ctx, r.Drivers[0].CarrierUuid)
	if err != nil {
		return nil, fmt.Errorf("failed to get carrier by uuid [%v]: %w", r.Drivers[0].CarrierUuid, err)
	}

	users := []*identity.IdentityUser{}
	for _, d := range r.Drivers {
		d.Uuid = uuid.NewString()
		u := identity.IdentityUser{
			EntityUuid:  d.Uuid,
			FirstName:   d.FirstName,
			LastName:    d.LastName,
			PhoneNumber: d.PhoneNumber,
			Email:       d.Email,
			Role:        identity.Role_DRIVER,
			ParentUuid:  carrier.Uuid,
			ParentUuids: []string{carrier.Uuid},
		}
		users = append(users, &u)
	}

	_, err = s.idtClient.CreateUsers(ctx, &identity.CreateUsersRequest{Users: users})
	if err != nil {
		ctxLog.Error(err)
		return &pb.CreateDriversResponse{}, status.Error(codes.DataLoss, "couldn't connect with Keycloak to create users")
	}
	rv := []*pb.Driver{}
	for _, dd := range r.Drivers {
		// TODO: move this to a db txn
		_, err := s.driver.CreateDriver(ctx,
			d.Driver{
				Uuid:             dd.Uuid,
				CarrierUuid:      dd.CarrierUuid,
				DriverExternalId: dd.DriverExternalId,
				Type:             dd.Type,
				StartDate:        dd.StartDate,
				Attributes:       dd.Attributes,
			},
		)
		if err != nil {
			return &pb.CreateDriversResponse{}, xgrpc.Errorf(codes.Internal, "There was an issue adding drivers: %w", err)
		} else {
			rv = append(rv, &pb.Driver{
				Uuid:             dd.Uuid,
				CarrierUuid:      dd.CarrierUuid,
				DriverExternalId: dd.DriverExternalId,
				Type:             dd.Type,
				StartDate:        dd.StartDate,
			})

			ctxLog = ctxLog.WithFields(xlog.Fields{
				"alert":       "DRIVER_CREATED",
				"carrierUuid": dd.CarrierUuid,
				"uuid":        dd.Uuid,
				"externalId":  dd.DriverExternalId,
				"first":       dd.FirstName,
				"last":        dd.LastName,
				"phone":       dd.PhoneNumber,
				"mail":        dd.Email,
				"startDate":   dd.StartDate,
			})
			ctxLog.Infof("DRIVER CREATED")
		}
	}

	return &pb.CreateDriversResponse{
		Drivers: rv,
	}, nil
}

func (s *server) CreateDriver(
	ctx context.Context, in *pb.CreateDriverRequest,
) (*pb.CreateDriverResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateDriver")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if len(in.CarrierUuid) > 0 {
		if err := validate.Uuid(in.CarrierUuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid carrier uuid: %w", in.CarrierUuid, err)
		}
	} else if len(in.CarrierExternalId) == 0 {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "externalId and carrier uuid both unset; expected one or the other")
	}

	carrier, err := s.getCarrierFromUuidOrExtId(ctx, in.CarrierUuid, in.CarrierExternalId)
	if err != nil {
		code := xgrpc.CodeFromError(err)
		if errors.Is(err, utils.ErrNoData) {
			return nil, xgrpc.Errorf(code, "carrier not found: %w", err)
		}
		return nil, xgrpc.Errorf(code, "unexpected error getting carrier: %w", err)
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	if err := validateCreateDriverRequest(in, *carrier); err != nil {
		return nil, err
	}
	uuid := uuid.New().String()

	identityResp, err := s.idtClient.CreateUser(ctx,
		&identity.CreateUserRequest{
			User: &identity.IdentityUser{
				EntityUuid:  uuid,
				FirstName:   in.FirstName,
				LastName:    in.LastName,
				PhoneNumber: in.PhoneNumber,
				Email:       in.Email,
				Role:        identity.Role_DRIVER,
				ParentUuid:  carrier.Uuid,
				ParentUuids: []string{carrier.Uuid},
			},
		},
	)
	if err != nil {
		code := codes.Internal
		if errors.Is(err, utils.ErrEmailAlreadyInUse) {
			code = codes.FailedPrecondition
		}
		return nil, xgrpc.Errorf(code, "failed to create user: %w", err)
	}

	dd, err := s.driver.CreateDriver(ctx,
		d.Driver{
			Uuid:             uuid,
			CarrierUuid:      carrier.Uuid,
			DriverExternalId: in.DriverExternalId,
			Type:             in.Type,
			StartDate:        in.StartDate,
			Attributes:       in.Attributes,
		},
	)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "failed to add driver: %w", err)
	}
	layout, _ := layouts.NewDriverEmailLayout()
	mail, err := email.NewEmail(
		&email.Draft{
			Subject:          "ONRAMP App ready to install!",
			OriginAddress:    email.Address(originAddress),
			OriginName:       "ONRAMP",
			RecipientAddress: email.Address(in.Email),
			RecipientName:    in.FirstName + " " + in.LastName,
			Layout:           layout,
			ReplyTo:          email.Address(supportAddress),
			BCC:              []email.Address{"outgoing@onrampcard.com"},
		},
	)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "failed to initialize driver email %w", err)
	}

	if err := s.emailClient.Provider().SendEmail(ctx, mail); err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "failed to send driver email %w", err)
	} else {
		xlog.Infof("email sent! %v", mail)
	}

	ctxLog = ctxLog.WithFields(xlog.Fields{
		"alert":       "DRIVER_CREATED",
		"carrierUuid": carrier.Uuid,
		"uuid":        uuid,
		"externalId":  in.DriverExternalId,
		"first":       in.FirstName,
		"last":        in.LastName,
		"phone":       in.PhoneNumber,
		"mail":        in.Email,
		"startDate":   in.StartDate,
	})
	ctxLog.Infof("DRIVER CREATED")

	return &pb.CreateDriverResponse{
		Driver: toDriverPb(dd, *identityResp.User, carrier),
	}, nil
}

func (s *server) UpdateDriver(
	ctx context.Context, in *pb.UpdateDriverRequest,
) (*pb.UpdateDriverResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.UpdateDriver")
	defer span.End()

	err := validateUpdateDriverRequest(in)
	if err != nil {
		return nil, err
	}

	var carrier carrierController.Carrier
	if len(in.Driver.CarrierUuid) > 0 {
		if carrier, err = s.carrierController.RetrieveCarrier(ctx, in.Driver.CarrierUuid); err != nil {
			return &pb.UpdateDriverResponse{}, status.Errorf(codes.Internal, "issue to update driver, err: %v", err)
		}
	} else if len(in.Driver.CarrierExternalId) > 0 {
		if carrier, err = s.carrierController.GetCarrierByExternalId(ctx, in.Driver.CarrierExternalId); err != nil {
			return &pb.UpdateDriverResponse{}, status.Errorf(codes.Internal, "issue to update driver, err: %v", err)
		}
	} else {
		return &pb.UpdateDriverResponse{}, status.Error(codes.FailedPrecondition, "missing carrier_uuid or carrier_external_id")
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, carrier.Uuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	pDriver := &d.Driver{
		StartDate:  in.Driver.StartDate,
		EndDate:    in.Driver.EndDate,
		Attributes: in.Driver.Attributes,
	}

	if len(in.Driver.CarrierUuid) > 0 && len(in.Driver.Uuid) > 0 {
		pDriver.Uuid = in.Driver.Uuid
		pDriver.CarrierUuid = in.Driver.CarrierUuid
		err = s.driver.UpdateDriverByUuid(ctx, pDriver)
	} else if len(in.Driver.CarrierExternalId) > 0 && len(in.Driver.DriverExternalId) > 0 {
		pDriver.CarrierExternalId = in.Driver.CarrierExternalId
		pDriver.DriverExternalId = in.Driver.DriverExternalId
		err = s.driver.UpdateDriverByExternalId(ctx, pDriver)
	}
	if err != nil {
		return nil, status.Errorf(xgrpc.CodeFromError(err), "failed to update driver: %v", err)
	}

	// TODO: this can be replaced by a separate API call to Disable user
	// if we decide to slim this API
	if len(in.Driver.EndDate) > 0 {
		if disRes, err := s.idtClient.DisableUser(ctx, &identity.DisableUserRequest{
			EntityUuid: pDriver.Uuid,
		}); err == nil {
			return &pb.UpdateDriverResponse{
				Driver: toDriverPb(*pDriver, *disRes.User, &carrier),
			}, nil
		} else {
			return &pb.UpdateDriverResponse{}, status.Errorf(
				codes.Internal,
				"failed to update driver, issue trying to disable user: %v",
				err,
			)
		}
	}

	// to return the domain object
	r, err := s.idtClient.GetUser(ctx, &identity.GetUserRequest{EntityUuid: pDriver.Uuid})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update driver: %v", err)
	}

	return &pb.UpdateDriverResponse{
		Driver: toDriverPb(*pDriver, *r.User, &carrier),
	}, nil
}

func (s *server) DeactivateDriver(
	ctx context.Context, in *pb.DeactivateDriverRequest,
) (*pb.DeactivateDriverResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.DeactivateDriver")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if hasPerm := s.hasPermissionToCarrier(ctx, in.CarrierUuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	_, err := s.idtClient.DisableUser(ctx, &identity.DisableUserRequest{
		IdentityUuid: in.IdentityUuid,
		EntityUuid:   in.DriverUuid,
	})

	if err != nil {
		ctxLog.Error(err)
		return &pb.DeactivateDriverResponse{}, err
	}

	err = s.driver.DeactivateDriver(ctx, d.DriversFilters{
		CarrierUuid: in.CarrierUuid,
		DriverUuid:  in.DriverUuid,
	})
	if err != nil {
		ctxLog.Error(err)
	}

	return &pb.DeactivateDriverResponse{}, err
}

func (s *server) ActivateDriver(
	ctx context.Context, in *pb.ActivateDriverRequest,
) (*pb.ActivateDriverResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ActivateDriver")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if hasPerm := s.hasPermissionToCarrier(ctx, in.CarrierUuid, constants.AUTHZ_PERMISSION_EDIT); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	_, err := s.idtClient.EnableUser(ctx, &identity.EnableUserRequest{
		IdentityUuid: in.IdentityUuid,
		EntityUuid:   in.DriverUuid,
	})
	if err != nil {
		ctxLog.Error(err)
		return &pb.ActivateDriverResponse{}, err
	}

	err = s.driver.ActivateDriver(ctx, d.DriversFilters{
		CarrierUuid: in.CarrierUuid,
		DriverUuid:  in.DriverUuid,
	})
	if err != nil {
		ctxLog.Error(err)
	}

	return &pb.ActivateDriverResponse{}, err
}

func (s *server) ListCarrierUsers(
	ctx context.Context, in *pb.ListCarrierUsersRequest,
) (*pb.ListCarrierUsersResponse, error) {
	if hasPerm := s.hasPermissionToCarrier(ctx, in.CarrierUuid, constants.AUTHZ_PERMISSION_VIEW); !hasPerm {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{ParentUuid: in.CarrierUuid, Role: in.Role})
	if err != nil {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "Unable to list carrier users for carrier %v: %v", in.CarrierUuid, err)
	}

	return &pb.ListCarrierUsersResponse{
		Users: r.Users,
	}, nil
}

func (s *server) ListDrivers(
	ctx context.Context, in *pb.ListDriversRequest,
) (*pb.ListDriversResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListDrivers")
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); !isStaff {
		return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	var ds, uuids = []d.Driver{}, []string{}
	var err error
	if len(in.DriverUuids) > 0 {
		ds, err = s.driver.ListDriversByUuid(ctx, in.DriverUuids)
	} else {
		ds, err = s.driver.ListDrivers(ctx)
	}

	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("failed to list drivers %w", err).Error())
	}

	for _, v := range ds {
		uuids = append(uuids, v.Uuid)
	}

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: uuids})

	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("failed to list drivers %w", err).Error())
	}

	rv := []*pb.Driver{}
	for _, v := range r.Users {
		dd := filterDriver(v.EntityUuid, ds)
		if dd == nil {
			return &pb.ListDriversResponse{}, status.Error(
				codes.Internal,
				fmt.Sprintf("Invalid driver uuid [%s]", v.EntityUuid))
		}

		d := toDriverPb(*dd, *v, nil)
		rv = append(rv, d)
	}

	sort.Slice(rv, func(i, j int) bool {
		return rv[i].CarrierName < rv[j].CarrierName
	})

	return &pb.ListDriversResponse{
		Drivers: rv,
	}, nil
}

func (s *server) SetCarrierStripeAccount(
	ctx context.Context, in *pb.SetCarrierStripeAccountRequest,
) (*pb.SetCarrierStripeAccountResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetCarrierStripeAccount")
	defer span.End()

	err := s.carrierController.SetCarrierStripeAccount(ctx, in.CarrierUuid, in.StripeAccountId)

	return &pb.SetCarrierStripeAccountResponse{}, err
}

func (s *server) GetDriverByCard(
	ctx context.Context, in *pb.GetDriverByCardRequest,
) (*pb.GetDriverByCardResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetDriverByCard")
	defer span.End()

	driver, driverErr := s.driver.GetDriverByCard(ctx, in.Card)

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: []string{driver.Uuid}})

	if driverErr == nil && err == nil && len(r.Users) > 0 {
		v := r.Users[0]
		return &pb.GetDriverByCardResponse{
			Driver: toDriverPb(driver, *v, nil),
			Carrier: &pb.Carrier{
				Uuid:       driver.CarrierUuid,
				ExternalId: driver.CarrierExternalId,
			}}, err
	}

	return nil, err
}

func (s *server) GetDriverByExternalId(
	ctx context.Context, in *pb.GetDriverByExternalIdRequest,
) (*pb.GetDriverByExternalIdResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetDriverByExternalId")
	defer span.End()

	carrierUuid, err := uuid.Parse(in.CarrierUuid)
	if err != nil {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "invalid carrier uuid: %v", err)
	}

	if in.ExternalId == "" {
		return nil, errors.New("external id is required")
	}

	driver, driverErr := s.driver.GetDriverByExternalId(ctx, carrierUuid, in.ExternalId)

	if driverErr != nil {
		return nil, driverErr
	}

	if driver == nil {
		return nil, xgrpc.Errorf(codes.NotFound, "driver not found: %s", in.ExternalId)
	}

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: []string{driver.Uuid}})

	if driverErr == nil && err == nil && len(r.Users) > 0 {
		v := r.Users[0]
		return &pb.GetDriverByExternalIdResponse{
			Driver: toDriverPb(*driver, *v, nil)}, err
	}

	return nil, err
}

func (s *server) AssignDriverCard(
	ctx context.Context, in *pb.AssignDriverCardRequest,
) (*pb.AssignDriverCardResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.AssignDriverCard")
	defer span.End()

	err := s.driver.AssignDriverCard(ctx, in.DriverUuid, in.Card)

	return &pb.AssignDriverCardResponse{}, err
}

func (s *server) RemoveDriverCard(
	ctx context.Context, in *pb.RemoveDriverCardRequest,
) (*pb.RemoveDriverCardResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.RemoveDriverCard")
	defer span.End()

	err := s.driver.RemoveDriverCard(ctx, in.Card)

	return &pb.RemoveDriverCardResponse{}, err
}

func (s *server) GetCarrierTransactionsReport(
	ctx context.Context,
	in *pb.GetCarrierTransactionsReportRequest,
) (*pb.GetCarrierTransactionsReportResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierTransactionsReport")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	p, err := s.getProgramFromCtx(ctx)
	if err != nil {
		return &pb.GetCarrierTransactionsReportResponse{}, err
	}

	var car carrierController.Carrier
	if len(in.CarrierUuid) > 0 {
		if err := validate.Uuid(in.CarrierUuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid uuid", in.CarrierUuid)
		}
		car, err = s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	} else {
		car, err = s.carrierController.GetCarrierByExternalId(ctx, in.CarrierExternalId)
	}
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "unable to retrieve carrier with info %+v err: %v", in, err)
	}
	if p != nil {
		// TODO: replace this by HasPermission check
		if car.Program == nil || car.Program.Uuid != (*p).Uuid {
			return nil, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
		}
	}

	txns, err := s.pmtClient.ListAllReceiptsByCarrier(ctx, &payment.ListAllReceiptsByCarrierRequest{
		CarrierId: car.Uuid,
		StartDate: in.StartDate,
		EndDate:   in.EndDate,
		TimeZone:  in.TimeZone,
	})
	if err != nil {
		return &pb.GetCarrierTransactionsReportResponse{}, err
	} else if len(txns.Receipts) == 0 {
		return &pb.GetCarrierTransactionsReportResponse{TransactionRecords: []*pb.TransactionRecord{}}, nil
	}

	uds := []string{}
	for _, r := range txns.Receipts {
		if _, err := uuid.Parse(r.Txn.DriverContext.Uuid); err != nil {
			m := fmt.Sprintf("DriverUuid is not valid: %+v", r.Txn.DriverContext.Uuid)
			return &pb.GetCarrierTransactionsReportResponse{}, status.Error(codes.FailedPrecondition, m)
		}
		uds = append(uds, r.Txn.DriverContext.Uuid)
	}

	drivers, err := s.driver.ListDriversByUuid(ctx, uds)
	if err != nil {
		return &pb.GetCarrierTransactionsReportResponse{}, err
	}

	ru, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: uds})
	if err != nil {
		ctxLog.Error(err)
	}

	ts := []*pb.TransactionRecord{}
	loc, _ := time.LoadLocation(ReportTZ)

	for _, r := range txns.Receipts {
		for _, p := range r.Txn.Products {

			t := convert.DtToTime(r.CompletionTime)
			u := filterUser(r.Txn.DriverContext.Uuid, ru.Users)
			d := filterDriver(r.Txn.DriverContext.Uuid, drivers)

			if d == nil {
				return &pb.GetCarrierTransactionsReportResponse{}, status.Error(
					codes.Internal,
					fmt.Sprintf("Invalid driver uuid [%s]", r.Txn.DriverContext.Uuid),
				)
			}

			ts = append(ts, &pb.TransactionRecord{
				TransactionId:       r.Txn.Id,
				TransactionNumber:   r.GetReceiptNumber(),
				Date:                fmt.Sprint(t.In(loc).Format("2006/01/02")),
				Time:                fmt.Sprint(t.In(loc).Format("15:04:05 MST")),
				DriverName:          fmt.Sprintf("%s %s", u.FirstName, u.LastName),
				DriverNumber:        d.DriverExternalId,
				AuthorizationNumber: r.Auth,

				City:                   r.Txn.Location.LocationCity,
				Region:                 r.Txn.Location.LocationRegion,
				MerchantId:             r.Txn.Location.MerchantName,
				LocationId:             r.Txn.Location.LocationExternalId,
				PumpNumber:             r.Txn.Location.PumpNumber,
				TruckStopName:          r.Txn.Location.LocationName,
				StoreNumber:            r.Txn.Location.StoreNumber,
				ProductCode:            p.Code,
				ProductName:            p.Name,
				Quantity:               p.Quantity,
				UnitRetailCost:         p.Price,
				ProductRetailTotal:     p.Amount,
				UnitDiscountedCost:     p.DiscountedPrice,
				ProductDiscountedTotal: p.DiscountedAmount,
				DiscountTotal:          p.TotalDiscount,
				Fee:                    p.TransactionFee.Value,
				GrandTotal:             p.GrandTotal.Value,

				TrailerNumber:        r.Txn.TransactionData.Prompts.GetTrailerNumber(),
				UnitNumber:           r.Txn.TransactionData.Prompts.TruckNumber,
				TripNumber:           r.Txn.TransactionData.Prompts.WorkOrderNumber,
				Odometer:             r.Txn.TransactionData.Prompts.Odometer,
				BillingTypeIndicator: convert.NewBillingTypeFromProto(r.Txn.Location.MerchantBillingType).Indicator(),

				CarrierId:   car.PublicId,
				CarrierName: car.Name,

				DriverAttributes: d.Attributes,
				LineItemId:       fmt.Sprintf("%s-%03s", r.Txn.Id, p.Code),
				LineItemUuid:     p.Uuid,
				PricingSource:    convert.NewPricingSourceFromProto(r.Txn.TransactionData.Discount.Metadata.PricingSource).String(),
			})
		}
	}

	if err != nil {
		return &pb.GetCarrierTransactionsReportResponse{}, err
	}

	return &pb.GetCarrierTransactionsReportResponse{TransactionRecords: ts, Carrier: toCarrierPb(car)}, nil
}

func (s *server) GetCarrierInvoiceReport(
	ctx context.Context,
	in *pb.GetCarrierInvoiceReportRequest,
) (*pb.GetCarrierInvoiceReportResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierInvoiceReport")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	p, err := s.getProgramFromCtx(ctx)
	if err != nil {
		return nil, xgrpc.Error(codes.PermissionDenied, err)
	}

	var car carrierController.Carrier
	if len(in.CarrierUuid) > 0 {
		car, err = s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	} else if len(in.CarrierExternalId) > 0 {
		car, err = s.carrierController.GetCarrierByExternalId(ctx, in.CarrierExternalId)
	} else {
		return &pb.GetCarrierInvoiceReportResponse{}, status.Error(codes.InvalidArgument, "uuid or externalId required to get invoice report")
	}

	if err != nil {
		ctxLog.Errorf("Unable to retrieve carrier with info %+v err:  %v", in, err)
		return &pb.GetCarrierInvoiceReportResponse{}, err
	}
	if p != nil {
		// TODO: replace this by HasPermission check
		if car.Program == nil || car.Program.Uuid != (*p).Uuid {
			return &pb.GetCarrierInvoiceReportResponse{}, status.Error(codes.PermissionDenied, "insufficient perimissions")
		}
	}

	txns, err := s.pmtClient.ListAllReceiptsByCarrier(ctx, &payment.ListAllReceiptsByCarrierRequest{
		CarrierId: car.Uuid,
		StartDate: in.StartDate,
		EndDate:   in.EndDate,
		TimeZone:  in.TimeZone,
	})
	if err != nil {
		return &pb.GetCarrierInvoiceReportResponse{}, err
	} else if len(txns.Receipts) == 0 {
		return &pb.GetCarrierInvoiceReportResponse{TransactionRecords: []*pb.TransactionRecord{}}, nil
	}

	ts := []*pb.TransactionRecord{}
	loc, _ := time.LoadLocation(ReportTZ)

	for _, r := range txns.Receipts {
		for _, p := range r.Txn.Products {
			dueDate := time.Now().Add(time.Hour * 24 * 7)
			ts = append(ts, &pb.TransactionRecord{
				InvoiceNumber:          fmt.Sprint(time.Now().In(loc).Format("20060102")),
				Date:                   fmt.Sprint(time.Now().In(loc).Format("2006/01/02")),
				DueDate:                fmt.Sprint(dueDate.In(loc).Format("2006/01/02")),
				ProductName:            p.Name,
				Quantity:               p.Quantity,
				UnitDiscountedCost:     p.DiscountedPrice,
				ProductDiscountedTotal: p.DiscountedAmount,
				CarrierName:            car.Name,
				CarrierId:              car.PublicId,
			})
		}
	}

	return &pb.GetCarrierInvoiceReportResponse{TransactionRecords: ts}, nil
}

func (s *server) GetProgramTransactionsReport(ctx context.Context, in *pb.GetProgramTransactionsReportRequest) (*pb.GetProgramTransactionsReportResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetProgramTransactionsReport")
	defer span.End()

	carriers, err := s.carrierController.ListCarriers(ctx, in.ProgramUuid)
	if err != nil {
		return &pb.GetProgramTransactionsReportResponse{}, err
	} else if len(carriers) == 0 {
		return &pb.GetProgramTransactionsReportResponse{
			TransactionRecords: []*pb.TransactionRecord{},
		}, nil
	}

	ts := []*pb.TransactionRecord{}
	for _, c := range carriers {
		mtr, err := s.GetCarrierTransactionsReport(ctx, &pb.GetCarrierTransactionsReportRequest{
			CarrierUuid: c.Uuid,
			StartDate:   in.StartDate,
			EndDate:     in.EndDate,
			TimeZone:    in.TimeZone,
		})

		if err != nil {
			return &pb.GetProgramTransactionsReportResponse{}, err
		}

		ts = append(ts, mtr.TransactionRecords...)
	}

	return &pb.GetProgramTransactionsReportResponse{
		TransactionRecords: ts,
	}, nil
}

func (s *server) GetCarrierByDriverUuid(
	ctx context.Context,
	in *pb.GetCarrierByDriverUuidRequest,
) (*pb.GetCarrierByDriverUuidResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierByDriverUuid")
	defer span.End()

	if in.Uuid == "" {
		return &pb.GetCarrierByDriverUuidResponse{}, status.Error(codes.FailedPrecondition, "missing uuid")
	}

	cr, err := s.carrierController.GetCarrierByDriverUuid(ctx, in.Uuid)
	if err != nil {
		return &pb.GetCarrierByDriverUuidResponse{}, err
	}

	return &pb.GetCarrierByDriverUuidResponse{Carrier: &pb.Carrier{
		Uuid:               cr.Uuid,
		ExternalId:         cr.ExternalId,
		DotNumber:          cr.DotNumber,
		PrimaryContactName: cr.PrimaryContact,
		Name:               cr.Name,
		Phone:              cr.Phone,
		Address: &c.Address{
			Street1:    cr.Street1,
			Street2:    cr.Street2,
			City:       cr.City,
			Region:     cr.Region,
			PostalCode: cr.PostalCode,
		},
		Program:          toProgramPb(cr.Program),
		ShowDiscountFlag: pb.ShowDiscountToDrivers(cr.ShowDiscountFlag),
		Status:           pb.CarrierStatus(cr.Status),
	}}, nil
}

func (s *server) GetCarrierPrompts(
	ctx context.Context,
	in *pb.GetCarrierPromptsRequest,
) (*pb.GetCarrierPromptsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierPrompts")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	p, err := s.carrierController.GetPrompts(ctx, carrierController.PromptsFilters{
		CarrierUuid: in.CarrierUuid,
		DriverUuid:  in.DriverUuid,
	})

	if err == storage.ErrNoData {
		return &pb.GetCarrierPromptsResponse{Prompt: &pb.CarrierPrompts{}}, nil
	}

	if err != nil {
		ctxLog.Error(err)
		return &pb.GetCarrierPromptsResponse{Prompt: &pb.CarrierPrompts{}}, err
	}

	return &pb.GetCarrierPromptsResponse{
		Prompt: &pb.CarrierPrompts{
			CarrierUuid:      p.CarrierUuid,
			HasTruckNumber:   p.HasTruckNumber,
			HasOdometer:      p.HasOdometer,
			HasTrailerNumber: p.HasTrailerNumber,
			HasTripNumber:    p.HasTripNumber,
			AsOfDate:         p.AsOfDate,
		},
	}, nil
}

func (s *server) CreateCarrierPrompts(
	ctx context.Context,
	in *pb.CreateCarrierPromptsRequest,
) (*pb.CreateCarrierPromptsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCarrierPrompts")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	c, err := s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
	if err != nil {
		ctxLog.Error(err)
		return &pb.CreateCarrierPromptsResponse{}, err
	}

	s.carrierController.CreatePrompts(ctx,
		carrierController.Prompts{
			CarrierId:        c.Id,
			HasOdometer:      in.HasOdometer,
			HasTruckNumber:   in.HasTruckNumber,
			HasTrailerNumber: in.HasTrailerNumber,
			HasTripNumber:    in.HasTripNumber,
			AsOfDate:         in.AsOfDate,
		},
	)

	return &pb.CreateCarrierPromptsResponse{}, nil
}

func (s *server) CreateDriverFeedback(
	ctx context.Context,
	in *pb.CreateDriverFeedbackRequest,
) (*pb.CreateDriverFeedbackResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateDriverFeedback")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: []string{in.DriverUuid}})

	if err != nil {
		ctxLog.Error(err)
		return &pb.CreateDriverFeedbackResponse{}, err
	}

	u := r.Users[0]

	f := d.Feedback{
		Uuid:               uuid.New().String(),
		DriverUuid:         u.EntityUuid,
		DriverName:         fmt.Sprintf("%s %s", u.FirstName, u.LastName),
		DriverEmail:        u.Email,
		AppVersion:         in.Feedback.AppVersion,
		MessageHelpful:     in.Feedback.MessageHelpful,
		MessageImprovement: in.Feedback.MessageImprovement,
		Device:             in.Feedback.Device,
		Score:              feedbackScorePbToString(in.Feedback.Score),
	}

	s.driver.CreateFeedback(ctx, f)

	err = utils.SlackClient.SendMessage(
		"Driver app feedback",
		slack.Attachment{
			Fields: []slack.AttachmentField{
				{
					Title: "How are we doing?",
					Value: f.Score,
					Short: true,
				},
				{
					Title: "Device",
					Value: f.Device,
					Short: true,
				},
				{
					Title: "App version",
					Value: f.AppVersion,
					Short: true,
				},
				{
					Title: "Name",
					Value: f.DriverName,
					Short: true,
				},
				{
					Title: "Email",
					Value: f.DriverEmail,
					Short: true,
				},
				{
					Title: "What can we improve?",
					Value: f.MessageImprovement,
				},
				{
					Title: "What was helpful?",
					Value: f.MessageHelpful,
				},
			},
		},
		os.Getenv("SLACK_FEEDBACK_CHANNEL"),
	)

	if err != nil {
		ctxLog.Error(err)
	}

	return &pb.CreateDriverFeedbackResponse{}, err
}

func (s *server) CreateCreditLine(ctx context.Context, in *pb.CreateCreditLineRequest) (*pb.CreateCreditLineResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCreditLine")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received CreateCreditLine: %+v", in)

	err := validateAddCreditLineRequest(ctx, in)

	if err != nil {
		ctxLog.Error(err)
		return nil, err
	}
	amount, err := convert.FromPbDecimalString(in.Amount)

	if err != nil {
		ctxLog.Error(err)
		return nil, err
	}

	err = s.creditor.CreateCreditLine(ctx, in.CarrierUuid, amount, in.AsOfDate)

	if err != nil {
		errStr := fmt.Sprintf("Unable to insert credit line for %v, err: %v", in.CarrierUuid, err)
		ctxLog.Errorf("ADD CREDIT LINE FAIL %s", errStr)
		return &pb.CreateCreditLineResponse{}, err
	}

	ctxLog.Infof("Add credit line success for  %v", in.CarrierUuid)
	// TODO: the return value should have uuid populated
	rv := &pb.CreateCreditLineResponse{
		CreditLine: &pb.CreditLine{
			CarrierUuid: in.CarrierUuid,
			Amount:      in.Amount,
			AsOfDate:    in.AsOfDate,
		},
	}
	return rv, nil

}

func (s *server) GetRewards(ctx context.Context, in *pb.GetRewardsRequest) (*pb.GetRewardsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetRewards")
	defer span.End()

	rewardDetails, err := s.rewardService.GetRewards(ctx, in.DriverUuid)
	if err != nil {
		return &pb.GetRewardsResponse{}, err
	}

	return &pb.GetRewardsResponse{
		Reward: &pb.Reward{
			Threshold:        rewardDetails.ThresholdAmount.StringFixed(2),
			RewardsAvailable: rewardDetails.RewardsAvailableAmount.StringFixed(0),
			LifetimeRewards:  rewardDetails.LifetimeRewardsAmount.StringFixed(2),
			AmountTillNext:   rewardDetails.TillNextRewardAmount.StringFixed(2),
		},
	}, nil

}

func (s *server) GetBalance(ctx context.Context, in *pb.GetBalanceRequest) (*pb.GetBalanceResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetBalance")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if in.CarrierUuid == "" && in.DriverUuid == "" {
		errStr := fmt.Sprintf("missing Carrier Uuid and Driver Uuid, received: [%+v]", in)
		ctxLog.Infof("GET BALANCE FAIL %s", errStr)
		return &pb.GetBalanceResponse{}, status.Error(codes.InvalidArgument, "missing Carrier and driver uuid")
	}

	if in.CarrierUuid == "" && in.DriverUuid != "" {
		balanceResp, err := s.pmtClient.GetBalanceV2(ctx,
			&payment.GetBalanceV2Request{
				DriverUuid: in.DriverUuid,
			})
		return &pb.GetBalanceResponse{
			BalanceAmount: convert.Total(*balanceResp.Balances).StringFixed(2),
		}, err

	} else {
		cl, err := s.creditor.GetCreditLine(ctx, in.CarrierUuid)
		if err != nil {
			errStr := fmt.Errorf("unable to get credit line err: [%w]", err)
			ctxLog.Errorf("GET BALANCE FAIL %s", errStr)
			return &pb.GetBalanceResponse{}, status.Error(codes.InvalidArgument, errStr.Error())
		}
		balanceResp, err := s.pmtClient.GetBalanceV2(ctx,
			&payment.GetBalanceV2Request{
				CarrierUuid: in.CarrierUuid,
			})
		if err != nil {
			errStr := fmt.Errorf("unable to get balance from payments service err: [%w]", err)
			ctxLog.Errorf("GET BALANCE FAIL %s", errStr)
			return &pb.GetBalanceResponse{}, status.Error(codes.InvalidArgument, errStr.Error())
		}

		charges := convert.Total(*balanceResp.Balances)
		payments, err := decimal.NewFromString(balanceResp.PaymentsTotal)
		if err != nil {
			errStr := fmt.Errorf("unable to convert payments to decimal err: [%w]", err)
			ctxLog.Errorf("GET BALANCE FAIL %s", errStr)
			return &pb.GetBalanceResponse{}, status.Error(codes.InvalidArgument, errStr.Error())
		}
		totalBalance := charges.Sub(payments)

		availFunds := cl.Amount.Sub(totalBalance)

		return &pb.GetBalanceResponse{
			BalanceAmount:  totalBalance.StringFixed(2),
			AvailableFunds: availFunds.StringFixed(2),
		}, err

	}

}

func (s *server) SetRewards(ctx context.Context, in *pb.SetRewardsRequest) (*pb.SetRewardsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetRewards")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	var err error
	if in.ThresholdAmount == "" {
		err = s.rewardService.SetRewards(ctx, in.CarrierUuid, false, decimal.Zero, decimal.Zero)
		return &pb.SetRewardsResponse{}, err

	}
	thresholdAmount, err := decimal.NewFromString(in.ThresholdAmount)
	if err != nil {
		ctxLog.Errorf("Failed to parse threshold amount")
		return &pb.SetRewardsResponse{}, err
	}
	rewardAmount, err := decimal.NewFromString(in.RewardRedeemableAmount)
	if err != nil {
		ctxLog.Errorf("Failed to parse reward amount")
		return &pb.SetRewardsResponse{}, err
	}
	err = s.rewardService.SetRewards(ctx, in.CarrierUuid, in.RewardsActive, thresholdAmount, rewardAmount)

	if !in.RewardsActive {
		thresholdAmount = decimal.Zero
	}

	return &pb.SetRewardsResponse{
		CarrierRewardStatus: &pb.CarrierRewardStatus{
			ThresholdAmount:        thresholdAmount.StringFixed(2),
			RewardsActive:          in.RewardsActive,
			RewardRedeemableAmount: rewardAmount.StringFixed(2),
		},
	}, err

}

func (s *server) GetCarrierRewardStatus(ctx context.Context, in *pb.GetCarrierRewardStatusRequest) (*pb.GetCarrierRewardStatusResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCarrierRewardStatus")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	var err error
	if in.CarrierUuid == "" {
		return &pb.GetCarrierRewardStatusResponse{}, status.Error(codes.InvalidArgument, "missing Carrier uuid")

	}
	carrierRewardStatus, err := s.rewardService.GetCarrierRewardInformation(ctx, in.CarrierUuid)

	if err != nil {
		ctxLog.Errorf("Unable to get carrier reward info %s", err)
		return &pb.GetCarrierRewardStatusResponse{}, status.Error(codes.Internal, err.Error())
	}

	return &pb.GetCarrierRewardStatusResponse{
		CarrierRewardStatus: &pb.CarrierRewardStatus{
			ThresholdAmount:        carrierRewardStatus.RewardThresholdAmount.StringFixed(2),
			RewardsActive:          carrierRewardStatus.RewardsActive,
			RewardRedeemableAmount: carrierRewardStatus.RewardRedeemableAmount.StringFixed(2),
		},
	}, nil

}

func (s *server) ListRewards(ctx context.Context, in *pb.ListRewardsRequest) (*pb.ListRewardsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListRewards")
	defer span.End()

	rewards, err := s.rewardService.ListRewards(ctx, rewarder.RewardsFilters{DriverUuid: in.DriverUuid})
	if err != nil && errors.Unwrap(err) != storage.ErrNoData {
		return nil, xgrpc.Errorf(codes.Internal, "failed to list rewards %w", err)
	}

	return &pb.ListRewardsResponse{
		Rewards: toPbRewards(rewards),
	}, nil

}

func (s *server) GetCreditLine(ctx context.Context, in *pb.GetCreditLineRequest) (*pb.GetCreditLineResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetCreditLine")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if in.CarrierUuid == "" {
		errStr := fmt.Sprintf("missing Carrier Uuid, received: [%+v]", in)
		ctxLog.Errorf("GET CREDIT LIMIT FAIL %s", errStr)
		return &pb.GetCreditLineResponse{}, status.Error(codes.InvalidArgument, "missing Carrier uuid")
	}

	p, err := s.creditor.GetCreditLine(ctx, in.CarrierUuid)

	if err == storage.ErrNoData {

		errStr := fmt.Sprintf("no record found for credit line Carrier Uuid, received: [%+v]", in)
		ctxLog.Infof("GET CREDIT LINE FAIL %s", errStr)
		return &pb.GetCreditLineResponse{}, nil
	}

	if err != nil {
		ctxLog.Error(err)
		return &pb.GetCreditLineResponse{}, err
	}

	return &pb.GetCreditLineResponse{
		CreditLine: toCreditLinePb(p),
	}, nil
}

func (s *server) ApproveCreditApplication(ctx context.Context,
	in *pb.ApproveCreditApplicationRequest) (*pb.ApproveCreditApplicationResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ApproveCreditApplication")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	xlog.Debug("received approve credit application")
	if in.ApplicationUuid == "" {
		errStr := fmt.Sprintf("missing application Uuid, received: [%+v]", in)
		ctxLog.Errorf("Approve Credit Application FAIL %s", errStr)
		return &pb.ApproveCreditApplicationResponse{}, status.Error(codes.InvalidArgument, "missing application uuid")
	}

	app, err := s.creditor.ApproveApplication(ctx, in.ApplicationUuid)

	if err != nil {
		errStr := fmt.Sprintf("error approving application with uuid: [%+v], err: %v", in.ApplicationUuid, err)
		ctxLog.Errorf("Approve Credit Application FAIL %s", errStr)

		return &pb.ApproveCreditApplicationResponse{}, status.Error(codes.Internal, errStr)
	}

	carrierResp, err := s.createCarrierFromApplication(ctx, app)
	if err != nil {
		errStr := fmt.Sprintf("error creating carrier from application with uuid: [%+v], err: %v", in.ApplicationUuid, err)

		ctxLog.Errorf("Error creating carrier Approve Credit Application Fail %v", err)
		return &pb.ApproveCreditApplicationResponse{}, status.Error(codes.Internal, errStr)
	}

	return &pb.ApproveCreditApplicationResponse{
		Carrier: carrierResp,
	}, nil

}

func (s *server) DeclineCreditApplication(ctx context.Context,
	in *pb.DeclineCreditApplicationRequest) (*pb.DeclineCreditApplicationResponse, error) {

	ctx, span := trace.NewSpan(ctx, "grpc.DeclineCreditApplication")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	xlog.Debug("received decline credit application")
	if in.ApplicationUuid == "" {
		errStr := fmt.Sprintf("missing application Uuid, received: [%+v]", in)
		ctxLog.Errorf("Decline Credit Application FAIL %s", errStr)
		return &pb.DeclineCreditApplicationResponse{}, status.Error(codes.InvalidArgument, "missing application uuid")
	}

	err := s.creditor.DeclineApplication(ctx, in.ApplicationUuid)

	if err != nil {
		errStr := fmt.Sprintf("error declining application with uuid: [%+v], err: %v", in.ApplicationUuid, err)
		ctxLog.Errorf("Decline Credit Application FAIL %s", errStr)

		return &pb.DeclineCreditApplicationResponse{}, status.Error(codes.Internal, errStr)
	}

	return &pb.DeclineCreditApplicationResponse{
		Reason: "declined",
	}, nil

}

func (s *server) ListCarrierDrivers(
	ctx context.Context, in *pb.ListCarrierDriversRequest,
) (*pb.ListCarrierDriversResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListCarrierDrivers")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	var err error
	var car carrierController.Carrier
	var df d.DriversFilters
	if len(in.CarrierUuid) > 0 {
		if err := validate.Uuid(in.CarrierUuid); err != nil {
			return nil, xgrpc.Errorf(codes.InvalidArgument, "%q is an invalid carrier uuid", in.CarrierUuid)
		}

		car, err = s.carrierController.RetrieveCarrier(ctx, in.CarrierUuid)
		if err != nil {
			code := xgrpc.CodeFromError(err)
			if errors.Is(err, utils.ErrNoData) {
				return nil, xgrpc.Errorf(code, "carrier for uuid[%s] not found: %w", in.CarrierUuid, err)
			}
			return nil, xgrpc.Errorf(code, "unexpected error retrieving carrier by uuid[%s]: %w", in.CarrierUuid, err)
		}

		df = d.DriversFilters{
			CarrierUuid: in.CarrierUuid,
		}
	} else {
		car, err = s.carrierController.GetCarrierByExternalId(ctx, in.CarrierExternalId)
		if err != nil {
			code := xgrpc.CodeFromError(err)
			if errors.Is(err, utils.ErrNoData) {
				return nil, xgrpc.Errorf(code, "carrier for external-id[%s] not found: %w", in.CarrierExternalId, err)
			}
			return nil, xgrpc.Errorf(code, "carrier for external-id[%s] not found: %w", in.CarrierExternalId, err)
		}

		df = d.DriversFilters{
			CarrierExternalId: in.CarrierExternalId,
		}
	}

	if hasPerm := s.hasPermissionToCarrier(ctx, car.Uuid, constants.AUTHZ_PERMISSION_VIEW); !hasPerm {
		return &pb.ListCarrierDriversResponse{}, xgrpc.Error(codes.PermissionDenied, utils.ErrPermissionDenied)
	}

	var ds, uuids = []d.Driver{}, []string{}
	ds, err = s.driver.ListDriversByCarrier(ctx, df)

	if err != nil {
		ctxLog.Errorf("Error listing drivers %v", err)
		return &pb.ListCarrierDriversResponse{}, err
	}

	for _, v := range ds {
		uuids = append(uuids, v.Uuid)
	}

	r, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: uuids})
	if status.Code(err) == codes.Canceled {
		ctxLog.Warn("request canceled while listing drivers")
		return &pb.ListCarrierDriversResponse{}, err
	}

	if status.Code(err) == codes.Unavailable {
		ctxLog.Errorf("there was an issue connecting to Keycloak to ListUsers. error: %v", err)
		return nil, err
	}

	if err != nil {
		ctxLog.Errorf("Error listing drivers %v", err)
		return nil, err
	}

	rv := []*pb.Driver{}
	for _, v := range r.Users {
		dd := filterDriver(v.EntityUuid, ds)
		if dd == nil {
			return &pb.ListCarrierDriversResponse{}, status.Error(
				codes.Internal,
				fmt.Sprintf("invalid driver uuid [%s]", v.EntityUuid))
		}
		rv = append(rv, toDriverPb(*dd, *v, &car))
	}

	return &pb.ListCarrierDriversResponse{
		Drivers: rv,
	}, nil
}

func (s *server) CreateCreditApplication(
	ctx context.Context,
	in *pb.CreateCreditApplicationRequest,
) (*pb.CreateCreditApplicationResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateCreditApplication")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ca := credit.CreditApplication{
		Uuid: uuid.New().String(),
	}

	out, err := s.creditor.CreateCreditApplication(ctx,
		ca.FromProto(in),
	)

	if err != nil {
		ctxLog.Errorf("Error creating credit application %v", err)
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("Error creating credit application %v", err))
	}

	err = utils.SlackClient.SendMessage(
		"New Credit Application Received",
		slack.Attachment{
			Fields: []slack.AttachmentField{
				{
					Title: "Carrier Name",
					Value: in.BusinessName,
					Short: true,
				},
				{
					Title: "Contact Name",
					Value: out.PrimaryContactName,
					Short: true,
				},
				{
					Title: "Owner Name",
					Value: out.OwnerLegalName,
					Short: true,
				},
				{
					Title: "Phone Number",
					Value: out.BusinessPhoneNumber,
					Short: true,
				},
				{
					Title: "Application Link",
					Value: os.Getenv("STAFF_TOOLS_URL") + "/applications/" + out.Uuid + "/credit",
				},
			},
		},
		os.Getenv("SLACK_CREDIT_APPLICATION_CHANNEL"),
	)

	if err != nil {
		ctxLog.Error(err)
	}

	return &pb.CreateCreditApplicationResponse{Application: out.ToProto()}, nil
}

func (s *server) GetDriverProfile(
	ctx context.Context,
	in *pb.GetDriverProfileRequest,
) (*pb.GetDriverProfileResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.GetDriverProfile")
	// ctxLog := xlog.WithContext(ctx)
	defer span.End()

	dd, err := s.driver.GetDriver(ctx, d.DriversFilters{
		DriverUuid: in.DriverUuid,
	})
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "issue return driver profile: %w", err)
	}

	userResp, err := s.idtClient.ListUsers(ctx, &identity.ListUsersRequest{Uuids: []string{in.DriverUuid}})
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "issue return driver profile: %w", err)
	}

	enableNewTokenScreen := s.featureFlags.EvaluateRule(
		"enableNewTokenScreen",
		map[string]interface{}{
			"carrierUuid": dd.CarrierUuid,
		},
	)

	return &pb.GetDriverProfileResponse{
		Driver: toDriverPb(*dd, *userResp.Users[0], nil),
		FeatureFlags: map[string]string{
			"enableNewTokenScreen": strconv.FormatBool(enableNewTokenScreen),
		},
	}, nil
}

func (s *server) ListCreditApplications(
	ctx context.Context,
	in *pb.ListCreditApplicationsRequest,
) (*pb.ListCreditApplicationsResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListCreditApplications")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	cas, err := s.creditor.GetCreditApplications(ctx)
	if err != nil {
		ctxLog.Errorf("Error creating credit application %v", err)
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("Error creating credit application %v", err))
	}

	out := []*pb.CreditApplication{}
	for _, ca := range cas {
		out = append(out, ca.ToProto())
	}

	return &pb.ListCreditApplicationsResponse{Applications: out}, nil
}

func (s *server) InitiateDriverReward(
	ctx context.Context,
	in *pb.InitiateDriverRewardRequest,
) (*pb.InitiateDriverRewardResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.InitiateDriverReward")
	defer span.End()

	if err := s.rewardService.InitiateDriverReward(ctx, in); err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("failed to initiate driver reward: %w", err).Error())
	}

	return &pb.InitiateDriverRewardResponse{}, nil
}

func (s *server) SetPassword(ctx context.Context, in *pb.SetPasswordRequest) (*pb.SetPasswordResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.SetPassword")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Set Password Request for driver %+v", in.EntityUuid)

	_, err := s.driver.ListDriversByCarrier(ctx, d.DriversFilters{
		CarrierUuid: in.CarrierUuid,
		DriverUuid:  in.EntityUuid,
	})

	if err != nil {
		ctxLog.Errorf("Unable to retrieve drivers with driver uuid %v and carrier uuid %v", in.EntityUuid, in.CarrierUuid)
		return &pb.SetPasswordResponse{}, err
	}

	_, err = s.idtClient.SetPassword(ctx, &identity.SetPasswordRequest{
		IdentityUuid: in.IdentityUuid,
		EntityUuid:   in.EntityUuid,
		Password:     in.Password,
		Temporary:    in.Temporary,
	})

	if err != nil {
		errStr := fmt.Sprintf("unable to set password for driver %v err: : [%+v]", in.EntityUuid, err)
		ctxLog.Errorf("SET PASSWORD FAIL %s", errStr)
		return &pb.SetPasswordResponse{}, status.Error(codes.Internal, errStr)
	}

	return &pb.SetPasswordResponse{
		EntityUuid: in.EntityUuid,
	}, nil

}

func (s *server) CreateReferralPartner(ctx context.Context, in *pb.CreateReferralPartnerRequest) (*pb.CreateReferralPartnerResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.CreateReferralPartner")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Create Referral Partner: %v", in.String())

	if err := validateCreateReferralPartnerRequest(ctx, in); err != nil {
		return nil, xgrpc.Errorf(codes.InvalidArgument, "invalid request: %w", err)
	}
	refParUid := uuid.NewString()

	amount, _ := decimal.NewFromString(in.Amount)

	referralPartner := referralPartner.ReferralPartner{
		Uuid:         uuid.New().String(),
		Name:         in.Name,
		Amount:       amount,
		CarrierUuids: in.CarrierUuids,
		ContractType: in.ContractType.String(),
	}

	newReferralPartner, err := s.referralPartnerService.CreateReferralPartner(ctx, referralPartner)
	if err != nil {
		return nil, xgrpc.Errorf(codes.Internal, "issue inserting referral partner: %w", err)
	}
	perms := []*identity.Permission{}
	for _, cUuid := range in.CarrierUuids {
		perms = append(perms, &identity.Permission{
			Actor:      convert.ToActorPb(constants.AUTHZ_REFERRAL_PARTNER_STRING, refParUid),
			Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, cUuid),
			Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_VIEW),
		})
	}

	if _, err := s.idtClient.SetPermissions(ctx, &identity.SetPermissionsRequest{
		Permissions: perms,
	}); err != nil {
		return nil, status.Errorf(xgrpc.CodeFromError(err), "failed to set permissions: %v", err)
	}

	return &pb.CreateReferralPartnerResponse{
		ReferralPartner: toReferralPartnerPb(&newReferralPartner),
	}, nil
}

func (s *server) UpdateReferralPartner(ctx context.Context, in *pb.UpdateReferralPartnerRequest) (*pb.UpdateReferralPartnerResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.UpdateReferralPartner")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("Received Update Referral Partner: %v", in.String())

	var amount decimal.Decimal
	var err error
	if len(in.Amount) > 0 {
		amount, err = decimal.NewFromString(in.Amount)
		if err != nil {
			return nil, status.Error(
				codes.InvalidArgument,
				fmt.Sprintf("invalid value for amount [%s]", in.Amount),
			)
		}
	}

	referralPartner := referralPartner.ReferralPartner{
		Uuid:         in.ReferralPartnerUuid,
		Name:         in.Name,
		Amount:       amount,
		CarrierUuids: in.CarrierUuids,
		ContractType: in.ContractType.String(),
	}

	err = s.referralPartnerService.UpdateReferralPartner(ctx, referralPartner)
	if err != nil {
		return &pb.UpdateReferralPartnerResponse{}, status.Errorf(xgrpc.CodeFromError(err), "issue updating referral partner: %v", err)
	}

	if len(in.CarrierUuids) > 0 {
		perms := []*identity.Permission{}
		for _, cUuid := range in.CarrierUuids {
			perms = append(perms, &identity.Permission{
				Actor:      convert.ToActorPb(constants.AUTHZ_REFERRAL_PARTNER_STRING, in.ReferralPartnerUuid),
				Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, cUuid),
				Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_VIEW),
			})
		}
		if _, err := s.idtClient.SetPermissions(ctx, &identity.SetPermissionsRequest{
			Permissions: perms,
		}); err != nil {
			return nil, status.Errorf(xgrpc.CodeFromError(err), "failed to set permissions: %v", err)
		}
	}

	if len(in.RemovedCarrierUuids) > 0 {
		perms := []*identity.Permission{}
		for _, cUid := range in.RemovedCarrierUuids {
			perms = append(perms,
				&identity.Permission{
					Actor:      convert.ToActorPb(constants.AUTHZ_REFERRAL_PARTNER_STRING, in.ReferralPartnerUuid),
					Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, cUid),
					Permission: convert.ToPermissionLevelPb(constants.AUTHZ_PERMISSION_VIEW),
				},
			)
		}
		if _, err := s.idtClient.DeletePermissions(ctx, &identity.DeletePermissionsRequest{
			Permissions: perms,
		}); err != nil && err != utils.ErrNoData {
			return nil, xgrpc.Errorf(codes.Internal, "failed to delete permissions: %w", err)
		}
	}

	return &pb.UpdateReferralPartnerResponse{}, nil
}

func (s *server) ListReferralPartners(ctx context.Context, in *pb.ListReferralPartnersRequest) (*pb.ListReferralPartnersResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.ListReferralPartners")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("List Referral Partners: %v", in.String())

	referralPartners, err := s.referralPartnerService.ListReferralPartners(ctx)

	if err != nil {
		return &pb.ListReferralPartnersResponse{}, status.Errorf(xgrpc.CodeFromError(err), "issue getting referral partners: %v", err)
	}

	referralPartnerPbs := make([]*pb.ReferralPartner, 0)
	for _, rp := range referralPartners {
		referralPartnerPbs = append(referralPartnerPbs, toReferralPartnerPb(&rp))
	}

	return &pb.ListReferralPartnersResponse{ReferralPartners: referralPartnerPbs}, nil
}

func (s *server) RetrieveReferralPartner(ctx context.Context, in *pb.RetrieveReferralPartnerRequest) (*pb.RetrieveReferralPartnerResponse, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.RetrieveReferralPartner")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	ctxLog.Infof("RetrieveReferralPartner: %v", in.String())

	referralPartner, err := s.referralPartnerService.RetrieveReferralPartner(ctx, in.ReferralPartnerUuid)

	if err != nil {
		return &pb.RetrieveReferralPartnerResponse{}, status.Errorf(xgrpc.CodeFromError(err), "issue getting referral partners: %v", err)
	}

	return &pb.RetrieveReferralPartnerResponse{ReferralPartner: toReferralPartnerPb(&referralPartner)}, nil
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Client Unexported methods
//_______________________________________________________________________

func (s *server) hasPermissionToCarrier(ctx context.Context, carrierUuid string, permLevel string) bool {
	ctx, span := trace.NewSpan(ctx, "grpc.hasPermissionToCarrier")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if isStaff := authz.UserIsStaff(ctx); isStaff {
		return true
	}

	// TODO: we need to support granular permission by fleetManager, once that is available
	// we can remove this and let the HasPermission check do its work
	if isFleetManager := authz.UserIsFleetManager(ctx); isFleetManager {
		return true
	}

	role, _ := authz.GetRoleFromCtx(ctx)
	entityUuid, _ := authz.GetEntityUuidFromCtx(ctx)
	perm := &identity.Permission{
		Actor:      convert.ToActorPb(role, entityUuid),
		Asset:      convert.ToAssetPb(constants.AUTHZ_CARRIER_STRING, carrierUuid),
		Permission: convert.ToPermissionLevelPb(permLevel),
	}

	r, err := s.idtClient.HasPermission(ctx, &identity.HasPermissionRequest{Permission: perm})
	if err != nil {
		ctxLog.WithFields(xlog.Fields{
			"permission": fmt.Sprintf("%+v", &perm),
		}).Errorf("issue checking for permission %v", err)
		return false
	}

	return r.HasPermission
}

func toSubsidyPb(sub subsidy.Subsidy) *pb.Subsidy {
	return &pb.Subsidy{
		CarrierUuid:    sub.Carrier.Uuid,
		MerchantUuid:   sub.MerchantUuid,
		AsOfDate:       sub.AsOfDate,
		Amount:         sub.Amount.StringFixed(2),
		LocationUuid:   sub.LocationUuid,
		PaymentNetwork: stringPaymentNetworkToPb(sub.PaymentNetwork),
	}
}

func pbToPaymentNetwork(n c.PaymentNetwork) string {
	switch n {
	case c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP:
		return subsidy.CLOSED_LOOP
	case c.PaymentNetwork_PAYMENT_NETWORK_VISA:
		return subsidy.VISA
	case c.PaymentNetwork_PAYMENT_NETWORK_UNSPECIFIED:
		return subsidy.UNKNOWN_PAYMENT_NETWORK
	}
	return subsidy.UNKNOWN_PAYMENT_NETWORK
}

func stringPaymentNetworkToPb(s string) c.PaymentNetwork {
	switch s {
	case subsidy.CLOSED_LOOP:
		return c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP
	case subsidy.VISA:
		return c.PaymentNetwork_PAYMENT_NETWORK_VISA
	case subsidy.UNKNOWN_PAYMENT_NETWORK:
		return c.PaymentNetwork_PAYMENT_NETWORK_UNSPECIFIED
	}
	return c.PaymentNetwork_PAYMENT_NETWORK_UNSPECIFIED
}

func validateSetSubsidyRequest(r *pb.SetSubsidyRequest) error {
	if r.CarrierUuid == "" {
		return status.Error(codes.InvalidArgument, "Subsidy Carrier Uuid can not be empty")
	}
	if r.Amount == "" {
		return status.Error(codes.InvalidArgument, "Subsidy amount can not be empty")
	}
	if len(r.GetAsOfDate()) == 0 {
		return status.Error(codes.InvalidArgument, "Subsidy as of date can not be empty")
	}
	if r.PaymentNetwork == 0 {
		return status.Error(codes.InvalidArgument, "Subsidy payment network can not be empty")
	}

	return nil
}

func validateUpdateDriverRequest(in *pb.UpdateDriverRequest) error {
	var errStr []string
	d := in.Driver
	if len(d.Email) > 0 {
		errStr = append(errStr, "email")
	}
	if len(d.CarrierName) > 0 {
		errStr = append(errStr, "name")
	}
	if len(d.PhoneNumber) > 0 {
		errStr = append(errStr, "phoneNumber")
	}
	if len(d.FirstName) > 0 {
		errStr = append(errStr, "firstName")
	}
	if len(d.LastName) > 0 {
		errStr = append(errStr, "lastName")
	}

	if len(errStr) > 0 {
		return status.Errorf(codes.FailedPrecondition, fmt.Sprintf("cannot update fields: %v", strings.Join(errStr, ", ")))
	}

	if err := validateDriverEndDate(d.EndDate); err != nil {
		return err
	}

	return nil
}

func validateDriverEndDate(endDateStr string) error {
	if len(endDateStr) == 0 {
		return nil
	}
	eDate, err := convert.ParseStringToDate(endDateStr)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, fmt.Sprintf("cannot parse %s as a valid date", endDateStr))
	}
	nowDay := convert.TruncateToDayStart(time.Now().In(convert.EST_TZ()))
	if !eDate.Before(nowDay) {
		return status.Error(
			codes.FailedPrecondition,
			"endDate cannot be later than today (EST)",
		)
	}
	return nil
}

func validateCreateDriversRequest(r *pb.CreateDriversRequest) error {
	if len(r.Drivers) == 0 {
		return status.Error(codes.FailedPrecondition, "drivers can not be empty")
	}
	var carrierUuid string
	var driverExternalId string
	for idx, d := range r.Drivers {
		if len(d.GetUuid()) > 0 || len(d.GetIdentityUuid()) > 0 {
			return status.Errorf(
				codes.InvalidArgument,
				"failed at index %d: updates are not supported", idx,
			)
		}
		if len(d.GetEmail()) == 0 {
			return status.Errorf(
				codes.InvalidArgument, "failed at index %d: email is empty", idx,
			)
		}
		if len(d.GetCarrierUuid()) == 0 && len(d.CarrierExternalId) == 0 {
			return status.Errorf(
				codes.InvalidArgument, "failed at index %d: carrier_uuid and carrier_external_id are empty", idx,
			)
		}
		if len(d.GetPhoneNumber()) == 0 {
			return status.Errorf(
				codes.InvalidArgument, "failed at index %d: phone_number is empty", idx,
			)
		}
		if err := validateDriverEndDate(d.EndDate); err != nil {
			return err
		}
		if len(driverExternalId) > 0 && d.DriverExternalId == driverExternalId {
			return status.Errorf(
				codes.InvalidArgument, "failed at index %d: driverExternalId should be unique", idx,
			)
		} else if len(driverExternalId) == 0 {
			driverExternalId = d.DriverExternalId
		}
		if len(carrierUuid) > 0 && d.CarrierUuid != carrierUuid {
			return status.Errorf(
				codes.InvalidArgument, "failed at index %d: carrierUuid mismatch", idx,
			)
		} else if len(carrierUuid) == 0 {
			carrierUuid = d.CarrierUuid
		}
	}

	return nil
}

func (s *server) createCarrierFromApplication(ctx context.Context, app credit.CreditApplication) (*pb.Carrier, error) {
	rdnId := strconv.Itoa(utils.GenerateRandomNumber(8))
	carrier := carrierController.Carrier{
		Uuid:           app.CarrierUuid,
		DotNumber:      app.DepartmentOfTransportationNumber,
		ExternalId:     rdnId,
		PublicId:       rdnId,
		PrimaryContact: app.PrimaryContactName,
		Street1:        app.BusinessAddress1,
		Street2:        app.BusinessAddress2,
		City:           app.BusinessCity,
		Region:         app.BusinessRegion,
		PostalCode:     app.BusinessZipCode,
		Phone:          app.BusinessPhoneNumber,
		Name:           app.BusinessName,
	}

	newCarrier, err := s.carrierController.CreateCarrier(ctx, carrier)
	if err != nil {
		return &pb.Carrier{}, err
	}

	return toCarrierPb(newCarrier), nil
}
func validateCreateDriverRequest(r *pb.CreateDriverRequest, carrier carrierController.Carrier) error {

	if len(r.GetUuid()) > 0 || len(r.GetIdentityUuid()) > 0 {
		m := fmt.Sprintf(`
				payload: %v has identityUuid or uuid, updates are not supported yet. 
				Remove row, and try again!`,
			r,
		)
		return status.Error(codes.FailedPrecondition, m)
	}
	if len(r.GetEmail()) == 0 {
		m := fmt.Sprintf("email is empty for driver: %v on payload", r)
		return status.Error(codes.FailedPrecondition, m)
	}
	if len(r.GetCarrierUuid()) == 0 && len(r.CarrierExternalId) == 0 {
		m := fmt.Sprintf("both carrier_uuid and carrier_external_id are empty for %v", r.GetEmail())
		return status.Error(codes.FailedPrecondition, m)
	}
	if len(r.GetPhoneNumber()) == 0 {
		m := fmt.Sprintf("phone_number is empty for %v", r.GetPhoneNumber())
		return status.Error(codes.FailedPrecondition, m)
	}

	for k := range r.Attributes {
		good := false
		for _, h := range carrier.DriverAttributeHeaders {
			if h == k {
				good = true
			}
		}
		if !good {
			return status.Error(codes.FailedPrecondition, fmt.Sprintf("Unknown driver attribute %s", k))
		}
	}

	return validateDriverEndDate(r.EndDate)
}

func validateCreateCarrierTractorUnitsRequest(
	r *pb.CreateCarrierTractorUnitsRequest,
) error {
	for _, r := range r.TractorUnits {
		if len(r.GetCarrierUuid()) == 0 {
			m := fmt.Sprintf("CarrierUUID is empty: %+v on payload", r)
			return status.Error(codes.FailedPrecondition, m)
		}
		if len(r.GetStartDate()) == 0 {
			m := fmt.Sprintf("StartDate is empty: %+v on payload", r)
			return status.Error(codes.FailedPrecondition, m)
		}
		if len(r.GetUnitId()) == 0 {
			m := fmt.Sprintf("Unit ID is empty: %+v on payload", r)
			return status.Error(codes.FailedPrecondition, m)
		}
		if len(r.GetVin()) == 0 {
			m := fmt.Sprintf("Vin is empty: %+v on payload", r)
			return status.Error(codes.FailedPrecondition, m)
		}
	}
	return nil
}

func validateLimit(r *pb.CarrierLimit) error {
	if len(r.GetAsOfDate()) == 0 {
		return errors.New("as of date unset")
	}
	if len(r.Amount) == 0 && len(r.Quantity) == 0 {
		return limiter.ErrMissingQuantityAndAmount
	}
	var err error
	var amt, qty decimal.Decimal

	if r.Amount != "" {
		amt, err = decimal.NewFromString(r.Amount)
		if err != nil {
			return fmt.Errorf("amount is not valid %s: %w", r.Amount, err)
		}
	}

	if r.Quantity != "" {
		qty, err = decimal.NewFromString(r.Quantity)
		if err != nil {
			return fmt.Errorf("quantity is not valid %s: %w", r.Quantity, err)
		}
	}

	if amt.LessThan(decimal.Zero) {
		return errors.New("amount cannot be less than zero")
	}

	if qty.LessThan(decimal.Zero) {
		return errors.New("quantity cannot be less than zero")
	}

	if qty.GreaterThan(decimal.Zero) && amt.GreaterThan(decimal.Zero) {
		return fmt.Errorf("cannot have both amount and quantity on limit %s, %s", amt, qty)
	}

	if r.ProductCategory == pb.ProductCategory_PRODUCT_CATEGORY_UNSPECIFIED {
		return errors.New("invalid product category")
	}

	return nil
}

func validateCreateTransactionFeeRequest(r *pb.TransactionFee) error {
	if r == nil {
		m := fmt.Sprintln("Nil create carrier fee request")
		return status.Error(codes.InvalidArgument, m)
	}
	if len(r.GetAsOfDate()) == 0 {
		m := fmt.Sprintf("AsOfDate is empty: %+v on payload", r)
		return status.Error(codes.InvalidArgument, m)
	}
	if r.ProgramUuid == "" { //product category is only required of program uuid is empty
		if r.GetProductCategory() == 0 {
			m := fmt.Sprintf("ProductCategory is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		}
	} else { //billing type is required only if program uuid is not empty
		if r.GetBillingType() == 0 {
			m := fmt.Sprintf("BillingType is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		}
	}

	if r.GetType() == 0 {
		m := fmt.Sprintf("FeeType is empty: %+v on payload", r)
		return status.Error(codes.InvalidArgument, m)
	}
	if convert.ToFeeType(r.GetType()) == convert.FLAT_AMOUNT_FEE {
		if r.GetFlatAmount() == "" {
			m := fmt.Sprintf("Flat Amount is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		} else {
			_, err := decimal.NewFromString(r.FlatAmount)
			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Flat Amount is not a decimal %s", r.FlatAmount))
			}
		}
	} else if convert.ToFeeType(r.GetType()) == convert.PERCENT_AMOUNT_FEE {
		if r.GetPercentAmount() == "" {
			m := fmt.Sprintf("Percent Amount is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		} else {
			_, err := decimal.NewFromString(r.PercentAmount)
			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Percent Amount is not a decimal %s", r.PercentAmount))
			}
		}
	} else if convert.ToFeeType(r.GetType()) == convert.PER_GALLON_FEE {
		if r.GetPerGallonAmount() == "" {
			m := fmt.Sprintf("per gallon Amount is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		} else {
			_, err := decimal.NewFromString(r.PerGallonAmount)
			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Per gallon Amount is not a decimal %s", r.PerGallonAmount))
			}
		}
	} else {
		if r.GetFlatAmount() == "" || r.GetPercentAmount() == "" {
			m := fmt.Sprintf("Percent Amount or Flat Amount is empty: %+v on payload", r)
			return status.Error(codes.InvalidArgument, m)
		} else {
			_, err := decimal.NewFromString(r.PercentAmount)

			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Percent Amount %s or Flat Amount %s is not a decimal", r.PercentAmount, r.FlatAmount))
			}
			_, err = decimal.NewFromString(r.FlatAmount)

			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Percent Amount %s or Flat Amount %s is not a decimal", r.PercentAmount, r.FlatAmount))
			}
		}
	}
	if (r.FeeCategory == common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING) || (r.FeeCategory == common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING) {
		if r.MerchantUuid == "" {
			return errors.New("missing merchant uuid on marketing fee")
		}
	}

	return nil
}

func toAddressPb(car carrierController.Carrier) *c.Address {
	return &c.Address{
		Street1:    car.Street1,
		Street2:    car.Street2,
		City:       car.City,
		Region:     car.Region,
		PostalCode: car.PostalCode,
	}
}

func toCarrierPb(c carrierController.Carrier) *pb.Carrier {
	pbCarrier := &pb.Carrier{
		Uuid:                   c.Uuid,
		ExternalId:             c.ExternalId,
		PublicId:               c.PublicId,
		Name:                   c.Name,
		PrimaryContactName:     c.PrimaryContact,
		Phone:                  c.Phone,
		DotNumber:              c.DotNumber,
		Program:                toProgramPb(c.Program),
		Address:                toAddressPb(c),
		DriverAttributeHeaders: c.DriverAttributeHeaders,
		ShowDiscountFlag:       pb.ShowDiscountToDrivers(c.ShowDiscountFlag),
		ReferralPartner:        toReferralPartnerPb(c.ReferralPartner),
		Status:                 pb.CarrierStatus(c.Status),
	}
	if c.StripeAccountId != nil {
		pbCarrier.StripeAccountId = *c.StripeAccountId
	}
	return pbCarrier
}

func toProgramPb(p *program.Program) *pb.Program {
	if p == nil {
		return nil
	}
	c := make([]*pb.ProgramCarrier, 0)
	for _, v := range p.Carriers {
		c = append(c, &pb.ProgramCarrier{
			Name:       v.Name,
			Uuid:       v.Uuid,
			ExternalId: v.ExternalId,
		})
	}
	fees := make([]*pb.TransactionFee, 0)
	for _, f := range p.Fees {
		fees = append(fees, &pb.TransactionFee{
			Uuid:            f.Uuid,
			AsOfDate:        f.AsOfDate,
			FlatAmount:      f.FlatAmount.StringFixed(2),
			PercentAmount:   f.PercentAmount.StringFixed(4),
			PerGallonAmount: f.PerGallonAmount.StringFixed(2),
			Type:            convert.ToPbFeeType(f.Type),
			BillingType:     f.BillingType.Pb(),
			ProductCategory: convert.ToProductCategoryPb(f.ProductCategory),
			FeeCategory:     common.FeeCategory(common.FeeCategory_value[f.FeeCategory]),
			MerchantUuid:    f.MerchantUuid,
		})
	}

	return &pb.Program{
		Name:                   p.Name,
		Uuid:                   p.Uuid,
		ExternalId:             p.ExternalId,
		ApiHandle:              p.ApiHandle,
		AcceptedPaymentTypes:   convert.ToPbPaymentTypes(p.AcceptedPaymentTypes),
		Carriers:               c,
		Fees:                   fees,
		RevenueSharePercentage: p.RevenueSharePercentage.StringFixed(4),
	}
}

func validateAddCreditLineRequest(ctx context.Context, in *pb.CreateCreditLineRequest) error {
	ctx, span := trace.NewSpan(ctx, "grpc.validateAddCreditLineRequest")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if in == nil {
		m := fmt.Sprintln("Nil create carrier line request")
		return status.Error(codes.InvalidArgument, m)
	}
	if in.CarrierUuid == "" {
		errStr := fmt.Sprintf("missing Creditee Uuid, received: [%+v]", in)
		ctxLog.Errorf("ADD CREDIT LIMIT FAIL %s", errStr)
		return status.Error(codes.InvalidArgument, "missing Creditee uuid")
	}
	amount, err := convert.FromPbDecimalString(in.Amount)
	if amount == decimal.Zero || err != nil {
		errStr := fmt.Sprintf("missing amount, received: [%+v]", in)
		ctxLog.Errorf("ADD CREDIT LIMIT FAIL %s", errStr)

		return status.Error(codes.InvalidArgument, "missing credit line amount")
	}
	if len(in.GetAsOfDate()) == 0 {
		m := fmt.Sprintf("AsOfDate is empty: %+v on payload", in)
		return status.Error(codes.InvalidArgument, m)
	}

	return nil
}

func filterDriver(u string, ds []d.Driver) *d.Driver {
	for _, v := range ds {
		if v.Uuid == u {
			return &v
		}
	}
	return nil
}

func filterUser(u string, us []*identity.IdentityUser) identity.IdentityUser {
	for i := range us {
		if us[i].EntityUuid == u {
			return *us[i]
		}
	}
	return identity.IdentityUser{}
}

func feedbackScorePbToString(in pb.FeedbackScore) string {
	switch in {
	case pb.FeedbackScore_FEEDBACK_SCORE_HAPPY:
		return "Happy"
	case pb.FeedbackScore_FEEDBACK_SCORE_NEUTRAL:
		return "Neutral"
	case pb.FeedbackScore_FEEDBACK_SCORE_UNHAPPY:
		return "Unhappy"
	default:
		return "Undefined"
	}
}

func toLimitPb(l *limiter.Limit) *pb.CarrierLimit {
	return &pb.CarrierLimit{
		CarrierUuid:     l.CarrierUUID,
		AsOfDate:        l.AsOfDate,
		Amount:          l.Amount.String(),
		ProductCategory: convert.ToProductCategoryPb(l.ProductCategory),
		Quantity:        l.Quantity.String(),
	}
}

func toLimitsPb(limits []*limiter.Limit) []*pb.CarrierLimit {
	rv := []*pb.CarrierLimit{}
	for _, l := range limits {
		rv = append(rv, &pb.CarrierLimit{
			CarrierUuid:     l.CarrierUUID,
			AsOfDate:        l.AsOfDate,
			Amount:          l.Amount.String(),
			ProductCategory: convert.ToProductCategoryPb(l.ProductCategory),
			Quantity:        l.Quantity.String(),
		})

	}
	return rv
}

func toTransactionFeesPb(fees []feeController.Fee) []*pb.TransactionFee {
	rv := []*pb.TransactionFee{}
	for _, f := range fees {
		rv = append(rv, toTransactionFeePb(f))
	}
	return rv
}

func toTransactionFeePb(fee feeController.Fee) *pb.TransactionFee {

	return &pb.TransactionFee{
		CarrierUuid:     fee.CarrierUuid,
		AsOfDate:        fee.AsOfDate,
		FlatAmount:      fee.FlatAmount.StringFixed(2),
		PercentAmount:   fee.PercentAmount.StringFixed(4),
		ProductCategory: convert.ToProductCategoryPb(fee.ProductCategory),
		Type:            convert.ToPbFeeType(fee.Type),
		BillingType:     fee.BillingType.Pb(),
		ProgramUuid:     fee.ProgramUuid,
		PerGallonAmount: fee.PerGallonAmount.StringFixed(2),
		MerchantUuid:    fee.MerchantUuid,
		FeeCategory:     common.FeeCategory(common.FeeCategory_value[fee.FeeCategory]),
	}
}

func toCreditLinePb(cl credit.CreditLine) *pb.CreditLine {
	return &pb.CreditLine{
		Uuid:        cl.Uuid,
		Amount:      cl.Amount.StringFixed(2),
		CarrierUuid: cl.CarrierUuid,
		AsOfDate:    cl.AsOfDate,
	}
}

func toDriverPb(dd d.Driver, iu identity.IdentityUser, car *carrierController.Carrier) *pb.Driver {
	pd := &pb.Driver{
		Uuid:                     dd.Uuid,
		CarrierUuid:              dd.CarrierUuid,
		CarrierPublicId:          dd.CarrierPublicId,
		DriverExternalId:         dd.DriverExternalId,
		CarrierName:              dd.CarrierName,
		CarrierExternalId:        dd.CarrierExternalId,
		Type:                     dd.Type,
		StartDate:                dd.StartDate,
		EndDate:                  dd.EndDate,
		IdentityUuid:             iu.IdentityUuid,
		FirstName:                iu.FirstName,
		LastName:                 iu.LastName,
		Email:                    iu.Email,
		PhoneNumber:              iu.PhoneNumber,
		Enabled:                  iu.Enabled,
		Attributes:               dd.Attributes,
		CarrierProgramExternalId: dd.CarrierExternalProgramlId,
	}
	if car != nil {
		pd.CarrierExternalId = car.ExternalId
		pd.CarrierName = car.Name
	}
	return pd
}

func tokenFromHeader(ctx context.Context) (*map[string]interface{}, error) {
	ctx, span := trace.NewSpan(ctx, "grpc.tokenFromHeader")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	md, ok := metadata.FromIncomingContext(ctx)
	var tokens []string
	if ok {
		tokens = md.Get("grpc-metadata-x-jwt")
		ctxLog.Infof("Tokens %T %d %+v", tokens, len(tokens), tokens)
	} else {
		ctxLog.Errorf("Could not construct metadata from ctx %v", ctx)
		return nil, status.Error(codes.InvalidArgument, "bad request metadata")
	}

	if len(tokens) > 0 {
		encToken := tokens[0]
		token, err := b64.RawStdEncoding.DecodeString(encToken)

		if err != nil {
			ctxLog.Errorf("Could not decode token %s %v", encToken, err)
			return nil, status.Error(codes.InvalidArgument, "malformed header")
		}

		var dat map[string]interface{}
		if err := json.Unmarshal([]byte(token), &dat); err != nil {
			ctxLog.Errorf("Could not parse token %s %v", token, err)
			return nil, status.Error(codes.InvalidArgument, "malformed header")
		}

		xlog.Debugf("Token parsed: %+v", dat)
		return &dat, nil
	}
	return nil, nil
}

// Deprecated
func programHandleFromApiToken(ctx context.Context, dat map[string]interface{}) *string {
	ctx, span := trace.NewSpan(ctx, "grpc.programHandleFromApiToken")
	ctxLog := xlog.WithContext(ctx)
	defer span.End()

	if dat["aud"] == "api" {
		if len(dat["role"].(string)) > 0 && hasProgramRole(dat["role"].(string)) {
			pString, ok := dat["azp"].(string)
			if ok {
				ctxLog.Infof("Program handle from token is %s", pString)
				return &pString
			}
			// TOOD this is an error actually
			return nil
		}
		// TOOD this is an error actually
		return nil
	}
	return nil
}

// Deprecated
func roleFromApiToken(dat map[string]interface{}) *string {

	if dat["aud"] == "api" {
		if len(dat["role"].(string)) > 0 {
			rString, ok := dat["role"].(string)
			if ok {
				return &rString
			}
			// TOOD this is an error actually
			return nil
		}
		// TOOD this is an error actually
		return nil
	}
	return nil
}

func (s *server) getProgramFromCtx(ctx context.Context) (*program.Program, error) {
	role, err := authz.GetRoleFromCtx(ctx)
	if err != nil {
		return nil, utils.ErrCtxRoleUnset
	}

	entityUuid, err := authz.GetEntityUuidFromCtx(ctx)
	if err == nil && len(entityUuid) > 0 && role == "program" {
		program, err := s.programService.GetProgramByUuid(ctx, entityUuid)
		if err != nil {
			return nil, fmt.Errorf("could not find program by uuid:%v", entityUuid)
		}
		return program, nil
	}

	xlog.Infof("skip GetProgramByUuid for role:%v entityUuid:%v", role, entityUuid)
	return nil, nil
}

// Deprecated
func (s *server) programFromHeaders(ctx context.Context) (*program.Program, error) {

	token, err := tokenFromHeader(ctx)
	if err != nil {
		return nil, err
	}

	if token == nil {
		return nil, nil
	}

	pHandle := programHandleFromApiToken(ctx, *token)

	if pHandle == nil {
		return nil, nil
	}

	if pHandle != nil {
		p, err := s.programService.GetProgramByApiHandle(ctx, *pHandle)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, status.Error(
				codes.Unauthenticated,
				fmt.Sprintf("unknown program: %s", *pHandle),
			)
		}
		return p, nil
	}
	return nil, nil
}

// Deprecated
func apiRoleFromHeaders(ctx context.Context) (*string, error) {

	token, err := tokenFromHeader(ctx)
	if err != nil {
		return nil, err
	}

	if token == nil {
		return nil, nil
	}

	return roleFromApiToken(*token), nil
}

// Deprecated
func hasProgramRole(roleStr string) bool {
	return strings.Contains(strings.ToLower(roleStr), "program")
}

func toReferralPartnerPb(rp *referralPartner.ReferralPartner) *pb.ReferralPartner {

	if rp == nil {
		return nil
	}
	return &pb.ReferralPartner{
		Uuid:         rp.Uuid,
		Name:         rp.Name,
		Amount:       rp.Amount.StringFixed(2),
		CarrierUuids: rp.CarrierUuids,
		ContractType: pb.ContractType(pb.ContractType_value[rp.ContractType]),
	}
}

func toPbRewards(rewards []rewarder.Reward) []*pb.RewardDetails {
	rv := []*pb.RewardDetails{}

	for _, v := range rewards {
		rv = append(rv, &pb.RewardDetails{
			Uuid:                  v.Uuid,
			DriverUuid:            v.DriverUuid,
			DateGiven:             v.DateGiven,
			RewardRedeemedAmount:  v.RewardRedeemedAmount.String(),
			BalanceConsumedAmount: v.BalanceConsumedAmount.String(),
			RewardExternalId:      v.RewardExternalId,
			Status:                v.Status,
		})
	}
	return rv
}

func (s *server) createLedgerAccount(ctx context.Context, uuid string, externalId string, name string) error {
	_, err := s.pmtClient.CreateLedgerAccount(ctx, &payment.CreateLedgerAccountRequest{
		EntityUuid:           uuid,
		Name:                 name + "-" + "Account Receivable",
		ExternalId:           externalId + "-" + "Account Receivable",
		DefaultDebitOrCredit: payment.DebitOrCredit_DEBIT_OR_CREDIT_DEBIT,
	})
	return err
}

var (
	errReferralPartnerNameUnset  = errors.New("referral name unset")
	errReferralAmountUnset       = errors.New("referral amount unset")
	errReferralTypeUnset         = errors.New("referral contract type unset")
	errReferralCarrierUuidsUnset = errors.New("referral carriers unset")
)

func validateCreateReferralPartnerRequest(ctx context.Context, req *pb.CreateReferralPartnerRequest) error {
	switch {
	case req.Name == "":
		return errReferralPartnerNameUnset
	case req.Amount == "":
		return errReferralAmountUnset
	case req.ContractType == pb.ContractType_CONTRACT_TYPE_UNSPECIFIED:
		return errReferralTypeUnset
	case len(req.CarrierUuids) < 1:
		return errReferralCarrierUuidsUnset
	default:
		_, err := decimal.NewFromString(req.Amount)
		if err != nil {
			return fmt.Errorf("invalid referral amount: %w", err)
		}

	}
	return nil
}

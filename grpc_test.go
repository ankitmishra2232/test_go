package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/0nramp/carrier/pkg/limiter"
	"github.com/0nramp/carrier/pkg/rewarder"
	"github.com/0nramp/carrier/pkg/storage"
	"github.com/0nramp/carrier/pkg/storage/psql"
	carrier "github.com/0nramp/protos/carrier"
	"github.com/0nramp/protos/common"
	c "github.com/0nramp/protos/common"
	identity "github.com/0nramp/protos/identity"
	mock_identity "github.com/0nramp/protos/mocks/identity"
	mock_payment "github.com/0nramp/protos/mocks/payment"
	mock_payment_gateway "github.com/0nramp/protos/mocks/paymentgateway"
	"github.com/0nramp/utils/constants"
	"github.com/0nramp/utils/convert"
	"github.com/0nramp/utils/email"
	"github.com/0nramp/utils/email/providers/local"
	"github.com/0nramp/utils/ff"
	"github.com/0nramp/utils/interceptors"
	"github.com/go-errors/errors"

	"github.com/0nramp/protos/payment"
	"github.com/0nramp/protos/paymentgateway"

	"github.com/0nramp/utils"
	"github.com/shopspring/decimal"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	goose "github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/type/datetime"
	pdecimal "google.golang.org/genproto/googleapis/type/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	gormPostgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestCarrierService(t *testing.T) {
	t.Run("ListCarriers", func(t *testing.T) {
		t.Parallel()
		withSuite(t, "should pass", func(t *testing.T, ctx context.Context, suite testSuite) {
			for _, test := range []struct {
				name                 string
				token                string
				listCarrierRequest   carrier.ListCarriersRequest
				expectedCarrierCount int
				expectedErr          error
				expectedErrCode      codes.Code
			}{
				{
					name:                 "when using staff token then returns three carriers and no errors",
					token:                staffToken,
					listCarrierRequest:   carrier.ListCarriersRequest{},
					expectedCarrierCount: 3,
					expectedErr:          nil,
					expectedErrCode:      codes.OK,
				},
				{
					name:                 "when using staff token and programUuid then returns one carrier and no errors",
					token:                staffToken,
					listCarrierRequest:   carrier.ListCarriersRequest{ProgramUuid: testProgramUuid},
					expectedCarrierCount: 1,
					expectedErr:          nil,
					expectedErrCode:      codes.OK,
				},
				{
					name:                 "when using program have no carriers then valid token returns zero carriers",
					token:                validProgramToken,
					listCarrierRequest:   carrier.ListCarriersRequest{},
					expectedCarrierCount: 0,
					expectedErr:          nil,
					expectedErrCode:      codes.OK,
				},
				{
					name:                 "when using referral_partner token then returns zero carriers",
					token:                testReferralPartnerToken,
					listCarrierRequest:   carrier.ListCarriersRequest{},
					expectedCarrierCount: 1,
					expectedErr:          nil,
					expectedErrCode:      codes.OK,
				},
				{
					name:                 "when using invalid token then get invalid argument error",
					token:                "invalid_token_string",
					listCarrierRequest:   carrier.ListCarriersRequest{},
					expectedCarrierCount: 0,
					expectedErr:          errors.New("failed to parse token"),
					expectedErrCode:      codes.InvalidArgument,
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					// Arrange
					ctx := metadata.AppendToOutgoingContext(ctx, "authorization", test.token)

					// Act
					resp, err := suite.carrierClient.ListCarriers(ctx, &test.listCarrierRequest)

					// Assert
					if err == nil {
						require.Len(t, resp.Carriers, test.expectedCarrierCount)
					} else if err != nil && test.expectedErr == nil {
						// make sure no unexpected error happens, this will only happen due to some unexpected changes on
						// the service
						require.NoError(t, err, "this is a real error, ListCarriers or the demo data might have changed, fix it and try again")
					} else {
						require.ErrorContains(t, err, test.expectedErr.Error())
						require.ErrorContains(t, err, test.expectedErrCode.String())
					}

				})
			}
		})
	})
	t.Run("ShouldPass", func(t *testing.T) {
		t.Parallel()
		withSuite(t, "when generating invoice report by carrier uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			dUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsers(t, []string{dUuid})
			suite.mockListAllReceiptsByCarrierResponse(t, dUuid)
			resp, err := suite.carrierClient.GetCarrierInvoiceReport(
				ctxWithStaffToken(ctx), &carrier.GetCarrierInvoiceReportRequest{
					CarrierUuid: oasisCarrierUuid,
				})

			//Assert
			require.NoError(t, err)
			require.Len(t, resp.TransactionRecords, 3)
			require.Equal(t, "Regular Diesel #2", resp.TransactionRecords[0].ProductName)
			require.Equal(t, "1.0", resp.TransactionRecords[0].Quantity.Value)

			require.Equal(t, "5.110", resp.TransactionRecords[0].UnitDiscountedCost.Value)
			require.Equal(t, "5.11", resp.TransactionRecords[0].ProductDiscountedTotal.Value)
			require.Equal(t, "Oasis Trucking Inc", resp.TransactionRecords[0].CarrierName)
			loc, err := time.LoadLocation(reportTz)
			require.Equal(t, time.Now().In(loc).Format("20060102"), resp.TransactionRecords[0].InvoiceNumber)
			require.NoError(t, err)
			require.Equal(t, time.Now().In(loc).Format("20060102"), resp.TransactionRecords[0].InvoiceNumber)
			dueDate := time.Now().In(loc).Add(time.Hour * 24 * 7).Format("2006/01/02")
			require.Equal(t, dueDate, resp.TransactionRecords[0].DueDate)

		})
		withSuite(t, "when generating transactions report carrierId should match publicId", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			dUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsers(t, []string{dUuid})
			suite.mockListAllReceiptsByCarrierResponse(t, dUuid)

			// Act
			resp, err := suite.getGetCarrierTransactionsReport(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierTransactionsReportRequest{
					CarrierUuid: oasisCarrierUuid,
				},
			)

			// Assert
			require.NoError(t, err)
			require.Equal(t, resp.Carrier.PublicId, resp.TransactionRecords[0].CarrierId)

		})

		withSuite(t, "when generating transactions report by carrier uuid", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange

			// demo driver, has attribute s {"DriverPin": "1234"}
			dUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsers(t, []string{dUuid})
			suite.mockListAllReceiptsByCarrierResponse(t, dUuid)

			// Act
			resp, err := suite.getGetCarrierTransactionsReport(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierTransactionsReportRequest{
					CarrierUuid: oasisCarrierUuid,
				},
			)

			//Assert
			require.NoError(t, err)

			require.Equal(t, resp.Carrier.PublicId, resp.TransactionRecords[0].CarrierId)
			require.Len(t, resp.TransactionRecords, 3)
			require.Equal(t, "5.160", resp.TransactionRecords[0].UnitRetailCost.Value)
			require.Equal(t, "5.110", resp.TransactionRecords[0].UnitDiscountedCost.Value)
			require.Equal(t, "5.16", resp.TransactionRecords[0].ProductRetailTotal.Value)
			require.Equal(t, "5.11", resp.TransactionRecords[0].ProductDiscountedTotal.Value)
			require.Equal(t, "Regular Diesel #2", resp.TransactionRecords[0].ProductName)
			require.Equal(t, "F", resp.TransactionRecords[0].BillingTypeIndicator)

			require.Equal(t, int32(5), resp.TransactionRecords[0].PumpNumber)
			require.Equal(t, int32(3600000), resp.TransactionRecords[0].Odometer)
			require.Equal(t, "Redwood City", resp.TransactionRecords[0].City)
			require.Equal(t, "Tree Inc", resp.TransactionRecords[0].MerchantId)
			require.Equal(t, "12", resp.TransactionRecords[0].StoreNumber)

			require.Equal(t, "12.90", resp.TransactionRecords[1].UnitRetailCost.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].UnitDiscountedCost.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].ProductRetailTotal.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].ProductDiscountedTotal.Value)
			require.Equal(t, "General Merchandise", resp.TransactionRecords[1].ProductName)

			// Refunded Merchandise
			require.Equal(t, "-12.90", resp.TransactionRecords[2].UnitRetailCost.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].UnitDiscountedCost.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].ProductRetailTotal.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].ProductDiscountedTotal.Value)
			require.Equal(t, "General Merchandise", resp.TransactionRecords[1].ProductName)

			// Driver attributes
			for _, txn := range resp.TransactionRecords {
				require.Equal(t, map[string]string{"DriverPin": "1234"}, txn.DriverAttributes)
			}
		})

		withSuite(t, "when generating transactions report by external carrier id", func(t *testing.T, ctx context.Context, suite testSuite) {
			dUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsers(t, []string{dUuid})
			suite.mockListAllReceiptsByCarrierResponse(t, dUuid)
			resp, err := suite.getGetCarrierTransactionsReport(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierTransactionsReportRequest{
					CarrierExternalId: "EXT123",
				},
			)
			require.NoError(t, err)

			//Assert
			require.Len(t, resp.TransactionRecords, 3)
		})

		withSuite(t, "when generating transactions report by program", func(t *testing.T, ctx context.Context, suite testSuite) {
			pResp, err := suite.setProgram(
				ctx,
				&carrier.SetProgramRequest{
					Uuid:                   "4c1e3606-208e-4441-967b-997eeafd6221",
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{oasisCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)
			require.NoError(t, err)

			ctx = metadata.AppendToOutgoingContext(ctx, "grpc-metadata-x-jwt", staffToken)
			// demo driver, has attribute s {"DriverPin": "1234"}
			dUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsers(t, []string{dUuid})
			suite.mockListAllReceiptsByCarrierResponse(t, dUuid)
			resp, err := suite.getProgramTransactionsReport(ctx, pResp.Program.Uuid)
			require.NoError(t, err)

			//Assert
			fmt.Print(resp.TransactionRecords[0])
			require.Len(t, resp.TransactionRecords, 3)
			require.Equal(t, "5.160", resp.TransactionRecords[0].UnitRetailCost.Value)
			require.Equal(t, "5.110", resp.TransactionRecords[0].UnitDiscountedCost.Value)
			require.Equal(t, "5.16", resp.TransactionRecords[0].ProductRetailTotal.Value)
			require.Equal(t, "5.11", resp.TransactionRecords[0].ProductDiscountedTotal.Value)
			require.Equal(t, "Regular Diesel #2", resp.TransactionRecords[0].ProductName)

			require.Equal(t, int32(5), resp.TransactionRecords[0].PumpNumber)
			require.Equal(t, int32(3600000), resp.TransactionRecords[0].Odometer)
			require.Equal(t, "Redwood City", resp.TransactionRecords[0].City)
			require.Equal(t, "Tree Inc", resp.TransactionRecords[0].MerchantId)
			require.Equal(t, "12", resp.TransactionRecords[0].StoreNumber)

			require.Equal(t, "12.90", resp.TransactionRecords[1].UnitRetailCost.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].UnitDiscountedCost.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].ProductRetailTotal.Value)
			require.Equal(t, "12.90", resp.TransactionRecords[1].ProductDiscountedTotal.Value)
			require.Equal(t, "General Merchandise", resp.TransactionRecords[1].ProductName)

			// Refunded Merchandise
			require.Equal(t, "-12.90", resp.TransactionRecords[2].UnitRetailCost.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].UnitDiscountedCost.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].ProductRetailTotal.Value)
			require.Equal(t, "-12.90", resp.TransactionRecords[2].ProductDiscountedTotal.Value)
			require.Equal(t, "General Merchandise", resp.TransactionRecords[1].ProductName)

			// Driver attributes
			for _, txn := range resp.TransactionRecords {
				require.Equal(t, map[string]string{"DriverPin": "1234"}, txn.DriverAttributes)
			}

			require.Equal(t, "baadb017-bdbd-4188-80b6-708266cde70a-019", resp.TransactionRecords[0].LineItemId)
			require.Equal(t, "baadb017-bdbd-4188-80b6-708266cde70a-400", resp.TransactionRecords[1].LineItemId)
			require.Equal(t, "baadb017-bdbd-4188-80b6-708266cde70a-430", resp.TransactionRecords[2].LineItemId)
		})

		// @TODO: move this test to a place where we test psql/store stuff
		withSuite(t, "when seed test driver exists", func(t *testing.T, ctx context.Context, suite testSuite) {
			id := suite.store.GetDriverIdByUuid(context.TODO(), testDriverUuid)
			require.Equal(t, uint(2), id)
		})

		withSuite(t, "when creating a carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.mockCreateLedgerAccount(t, carrierName)
			newCarrierName := randAlphaString(10)

			// Act
			resp, err := suite.carrierClient.CreateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.CreateCarrierRequest{
					ExternalId:             carrierExternalID,
					Address:                &carrierAddress,
					PrimaryContactName:     carrierContact,
					Name:                   newCarrierName,
					Phone:                  carrierPhone,
					DriverAttributeHeaders: []string{"CostCenter"},
				},
			)

			// Assert
			require.NoError(t, err)
			require.Equal(t, newCarrierName, resp.Carrier.Name)
			require.Equal(t, carrierExternalID, resp.Carrier.ExternalId)
			require.Nil(t, resp.Carrier.Program)
			require.Equal(t, "CostCenter", resp.Carrier.DriverAttributeHeaders[0])
			require.Equal(t, carrier.ShowDiscountToDrivers_SHOW_DISCOUNT_TO_DRIVERS_ALWAYS, resp.Carrier.ShowDiscountFlag)

		})

		withSuite(t, "when updating a carrier by uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			car := listCarriersResp.Carriers[0]
			name := car.Name
			newName := name + "_foo"

			resp, err := suite.updateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.Carrier{
					Uuid: car.Uuid,
					Name: newName,
				},
			)
			require.NoError(t, err)

			require.Equal(t, newName, resp.Carrier.Name)
			require.Equal(t, car.Uuid, resp.Carrier.Uuid)
			require.Equal(t, car.Phone, resp.Carrier.Phone)
		})
		withSuite(t, "when deactivating a carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			ctx = ctxWithStaffToken(ctx)
			listCarriersResp, err := suite.listCarriers(ctx)
			require.NoError(t, err)

			car := listCarriersResp.Carriers[0]
			require.Equal(t, carrier.CarrierStatus_CARRIER_STATUS_ACTIVE, car.Status)

			require.NoError(t, err)

			resp, err := suite.updateCarrier(
				ctx,
				&carrier.Carrier{
					Uuid:   car.Uuid,
					Status: carrier.CarrierStatus_CARRIER_STATUS_SUSPENDED,
				},
			)
			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Carrier.Uuid)

			listCarriersResp, err = suite.listCarriers(ctx)
			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Carrier.Uuid)
			car = listCarriersResp.Carriers[0]
			require.Equal(t, carrier.CarrierStatus_CARRIER_STATUS_SUSPENDED, car.Status)

		})
		withSuite(t, "when deleting a carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			ctx = ctxWithStaffToken(ctx)
			listCarriersResp, err := suite.listCarriers(ctx)
			require.NoError(t, err)

			car := listCarriersResp.Carriers[0]
			require.Equal(t, carrier.CarrierStatus_CARRIER_STATUS_ACTIVE, car.Status)
			createDriverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: "dExtId",
				CarrierUuid:      car.Uuid,
				FirstName:        "Test1",
				LastName:         "Driver1",
				PhoneNumber:      "1234567892",
				Email:            "testdriver2@email.com",
				StartDate:        "20231002",
				Attributes:       map[string]string{"DriverPin": "5255"},
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{})

			resp, err := suite.updateCarrier(
				ctx,
				&carrier.Carrier{
					Uuid:   car.Uuid,
					Status: carrier.CarrierStatus_CARRIER_STATUS_DELETED,
				},
			)
			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Carrier.Uuid)

			listCarriersResp, err = suite.listCarriers(ctx)
			require.NoError(t, err)

			for _, c := range listCarriersResp.Carriers {
				require.NotEqual(t, car.Uuid, c.Uuid)
			}

			driverResp, err := suite.carrierClient.ListDrivers(ctx, &carrier.ListDriversRequest{})
			require.NoError(t, err)
			for _, d := range driverResp.Drivers {
				require.NotEqual(t, createDriverResp.Driver.Uuid, d.Uuid)
			}
		})
		withSuite(t, "when updating a carrier by external id", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			car := listCarriersResp.Carriers[0]
			require.True(t, len(car.ExternalId) > 0)
			name := car.Name
			newName := name + "_foo"

			resp, err := suite.updateCarrier(ctxWithStaffToken(ctx), &carrier.Carrier{
				ExternalId: car.ExternalId,
				Name:       newName,
			})

			require.NoError(t, err)

			require.Equal(t, newName, resp.Carrier.Name)
			require.Equal(t, car.Uuid, resp.Carrier.Uuid)
			require.Equal(t, car.ExternalId, resp.Carrier.ExternalId)
		})
		withSuite(t, "when creating carrier under a program then should set default values", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetPermissions(t)
			resp, err := suite.createCarrier(ctxWithProgramToken(ctx))
			require.NoError(t, err)
			require.Equal(t, resp.Carrier.Program.Name, "EDS")

			carrierUuid := resp.Carrier.Uuid
			getLimitResp, err := suite.getLimits(ctxWithProgramToken(ctx), carrierUuid, convert.FormatDateToString(time.Now()))
			require.NoError(t, err)
			require.Equal(t, 4, len(getLimitResp.Limits))

			require.Equal(t, "1000", getLimitResp.Limits[0].Amount)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getLimitResp.Limits[0].ProductCategory)
			require.Equal(t, "300", getLimitResp.Limits[1].Amount)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_REEFER, getLimitResp.Limits[1].ProductCategory)
			require.Equal(t, "150", getLimitResp.Limits[2].Amount)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DEF, getLimitResp.Limits[2].ProductCategory)
			require.Equal(t, "80", getLimitResp.Limits[3].Amount)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_ADDITIVES, getLimitResp.Limits[3].ProductCategory)

			getCreditLineResp, err := suite.getCreditLine(ctxWithProgramToken(ctx), carrierUuid)
			require.NoError(t, err)
			require.Equal(t, "10000.00", getCreditLineResp.CreditLine.Amount)

			p, err := suite.carrierClient.GetCarrierPrompts(ctxWithStaffToken(ctx), &carrier.GetCarrierPromptsRequest{
				CarrierUuid: carrierUuid,
			})

			require.NoError(t, err)
			require.Equal(t, true, p.Prompt.HasTrailerNumber)
			require.Equal(t, true, p.Prompt.HasTruckNumber)
			require.Equal(t, false, p.Prompt.HasTripNumber)
			require.Equal(t, false, p.Prompt.HasOdometer)

		})

		withSuite(t, "when listing programs with carrierUuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			acceptedPaymentTypes := []c.PaymentInstrumentType{
				c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT,
				c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_OUT_OF_NETWORK_FUEL_CARD,
			}

			suite.setProgram(ctx,
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           []string{jesseCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)

			// Act
			r, err := suite.carrierClient.ListPrograms(ctxWithStaffToken(ctx), &carrier.ListProgramsRequest{})
			program := r.Programs[2]

			// Assert
			require.NoError(t, err)
			require.Equal(t, programName, program.Name)
			require.Equal(t, apiHandle, program.ApiHandle)
			require.Equal(t, acceptedPaymentTypes, program.AcceptedPaymentTypes)
			require.Equal(t, jesseCarrierUuid, program.Carriers[0].Uuid)
		})

		withSuite(t, "when creating a program", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			acceptedPaymentTypes := []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT}

			// Act
			resp, err := suite.setProgram(ctx,
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           []string{jesseCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)

			// Assert
			require.NoError(t, err)
			require.Equal(t, programName, resp.Program.Name)
			require.Equal(t, apiHandle, resp.Program.ApiHandle)
			require.Equal(t, acceptedPaymentTypes, resp.Program.AcceptedPaymentTypes)
			require.Equal(t, jesseCarrierUuid, resp.Program.Carriers[0].Uuid)
			require.Equal(t, "50.0000", resp.Program.RevenueSharePercentage)
		})

		withSuite(t, "when updating a program", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			carrierUuids := []string{}
			acceptedPaymentTypes := []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT}

			// Act
			resp, err := suite.setProgram(ctx,
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           carrierUuids,
					RevenueSharePercentage: revenueSharePercentage,
				},
			)

			// Arrange
			require.Equal(t, programName, resp.Program.Name)
			pUid := resp.Program.Uuid
			carrierResp, err := suite.createCarrier(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			carrierUuids = []string{carrierResp.Carrier.Uuid}

			// Act
			resp2, err := suite.setProgram(ctx,
				&carrier.SetProgramRequest{
					Uuid:                   pUid,
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           carrierUuids,
					RevenueSharePercentage: "55.00",
				},
			)

			// Assert
			require.NoError(t, err)
			require.Equal(t, programName, resp2.Program.Name)
			require.Len(t, resp2.Program.Carriers, 1)
			require.Equal(t, "55.0000", resp2.Program.RevenueSharePercentage)

			require.Equal(t, carrierUuids[0], resp2.Program.Carriers[0].Uuid)

		})

		withSuite(t, "when updating a program name only", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			carrierUuids := []string{}
			acceptedPaymentTypes := []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT}

			cResp, err := suite.setProgram(ctx,
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           carrierUuids,
					RevenueSharePercentage: revenueSharePercentage,
				},
			)
			require.NoError(t, err)

			// Act
			uResp, err := suite.setProgram(
				ctx,
				&carrier.SetProgramRequest{
					Uuid: cResp.Program.Uuid,
					Name: cResp.Program.Name + "foobar",
				})

			// Assert
			require.NoError(t, err)
			require.Equal(t, cResp.Program.Name+"foobar", uResp.Program.Name)
			require.Equal(t, len(cResp.Program.Carriers), len(uResp.Program.Carriers))
			require.Equal(t, cResp.Program.ApiHandle, uResp.Program.ApiHandle)
			require.Equal(t, cResp.Program.RevenueSharePercentage, uResp.Program.RevenueSharePercentage)
		})

		withSuite(t, "when listing carriers as program then return only carriers that belongs to program", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.setProgram(
				ctx,
				&carrier.SetProgramRequest{
					Uuid:                   "4c1e3606-208e-4441-967b-997eeafd6221",
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{jesseCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)

			programCarriersResp, err := suite.listCarriers(ctxWithProgramToken(ctx))
			require.NoError(t, err)
			require.Len(t, programCarriersResp.Carriers, 1)
			require.Equal(t, jesseCarrierUuid, programCarriersResp.Carriers[0].Uuid)

		})

		withSuite(t, "when retrieving carrier with token from program then it should return carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			c, _ := suite.createCarrier(ctxWithProgramToken(ctx))

			// Act
			suite.mockIdentityHasPermission(t, true)
			resp, err := suite.retrieveCarrier(ctxWithProgramToken(ctx), c.Carrier.Uuid)

			// Assert
			require.NoError(t, err)
			require.NotNil(t, resp.Carrier.Program)

		})

		withSuite(t, "when retrieve carrier using token from program and carrier has no program then return permission denied", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			c, _ := suite.createCarrier(ctxWithStaffToken(ctx))

			// Act
			suite.mockIdentityHasPermission(t, false)
			resp, err := suite.retrieveCarrier(ctxWithProgramToken(ctx), c.Carrier.Uuid)

			// Assert
			require.Nil(t, resp)
			require.ErrorContains(t, err, codes.PermissionDenied.String())

		})

		withSuite(t, "when creating a carrier tractor unit", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))

			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrieruuid := listCarriersResp.Carriers[0].Uuid

			//create tractor
			resp, err := suite.createCarrierTractorUnit(ctx, carrieruuid)
			require.NoError(t, err)
			require.Equal(t, resp.CarrierTractorUnit.Vin, tractorVin)
		})
		withSuite(t, "when creating single driver", func(t *testing.T, ctx context.Context, suite testSuite) {
			// demo carrier - has known set of driver attribute headers, {DriverPin}
			carrierUuid := oasisCarrierUuid

			dExtId := "123-abc"
			startDate := "20221115"

			// create driver
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: dExtId,
				CarrierUuid:      carrierUuid,
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        startDate,
				Attributes:       map[string]string{"DriverPin": "5555"},
			})
			require.NoError(t, err)

			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			carrierResp, err := suite.retrieveCarrier(ctxWithStaffToken(ctx), carrierUuid)
			require.NoError(t, err)

			require.Equal(t, carrierUuid, driverResp.Driver.CarrierUuid)
			require.Equal(t, dExtId, driverResp.Driver.DriverExternalId)
			require.Equal(t, carrierResp.Carrier.ExternalId, driverResp.Driver.CarrierExternalId)
			require.Equal(t, carrierResp.Carrier.Name, driverResp.Driver.CarrierName)
			require.Equal(t, startDate, driverResp.Driver.StartDate)
			require.Equal(t, "", driverResp.Driver.EndDate)
			// these are coming from mocked up identity svc
			require.Equal(t, "Test User 0 First", driverResp.Driver.FirstName)
			require.Equal(t, "Test User 0 Last", driverResp.Driver.LastName)
			require.Equal(t, "9876543210", driverResp.Driver.PhoneNumber)
			require.Equal(t, "User0@onramp.com", driverResp.Driver.Email)
			require.True(t, driverResp.Driver.Enabled)
			require.NotNil(t, driverResp.Driver.Attributes)
			require.Equal(t, 1, len(driverResp.Driver.Attributes))
			require.Equal(t, "5555", driverResp.Driver.Attributes["DriverPin"])

		})
		withSuite(t, "when updating driver with endDate startDate and attributes", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			suite.mockIdentityDisableUser(t)
			suite.mockIdentityGetUser(t, uuid.NewString())

			// Act
			driverResp, err := suite.updateDriver(ctxWithStaffToken(ctx), &carrier.Driver{
				CarrierUuid: fiveStarsCarrierUuid,
				Uuid:        "1bab3eb7-9ef0-498e-910b-65dd0a7ec3f0",
				Attributes:  map[string]string{"DriverDOB": "19991010"},
				StartDate:   "20220101",
				EndDate:     "20221010",
			})
			// Assert
			require.NoError(t, err)
			require.Equal(t, "20220101", driverResp.Driver.StartDate)
			require.Equal(t, "20221010", driverResp.Driver.EndDate)
			require.Equal(t, "19991010", driverResp.Driver.Attributes["DriverDOB"])

		})

		withSuite(t, "when updating driver with only start date by uuid", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			suite.mockIdentityDisableUser(t)
			suite.mockIdentityGetUser(t, uuid.NewString())

			// Act
			driverResp, err := suite.updateDriver(ctxWithStaffToken(ctx), &carrier.Driver{
				CarrierUuid: oasisCarrierUuid,
				Uuid:        "b1a31129-bf4b-4b4c-8343-fa261c906443",
				StartDate:   "20220101",
			})

			// Assert
			require.NoError(t, err)
			require.Equal(t, "20220101", driverResp.Driver.StartDate)
			require.Equal(t, "", driverResp.Driver.EndDate)
			require.Equal(t, "1234", driverResp.Driver.Attributes["DriverPin"])
			require.Equal(t, "EXT123", driverResp.Driver.CarrierExternalId)
			require.Equal(t, "JBW-433", driverResp.Driver.DriverExternalId)
		})

		withSuite(t, "when updating driver with only start date by external id", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			suite.mockIdentityDisableUser(t)
			suite.mockIdentityGetUser(t, uuid.NewString())

			// Act
			driverResp, err := suite.updateDriver(ctxWithStaffToken(ctx), &carrier.Driver{
				CarrierExternalId: "EXT123",
				DriverExternalId:  "JBW-433",
				StartDate:         "20220101",
			})

			// Assert
			require.NoError(t, err)
			require.Equal(t, "20220101", driverResp.Driver.StartDate)
			require.Equal(t, "", driverResp.Driver.EndDate)
			require.Equal(t, "1234", driverResp.Driver.Attributes["DriverPin"])
			require.Equal(t, "b1a31129-bf4b-4b4c-8343-fa261c906443", driverResp.Driver.Uuid)
		})

		withSuite(t, "when creating single driver with carrier external id", func(t *testing.T, ctx context.Context, suite testSuite) {
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			carrierExternalId := listCarriersResp.Carriers[0].ExternalId
			require.True(t, len(carrierExternalId) > 0)
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			startDate := "20221115"
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierExternalId: carrierExternalId,
				FirstName:         "Test",
				LastName:          "Driver",
				PhoneNumber:       "1234567890",
				Email:             "testdriver@email.com",
				StartDate:         startDate,
			})
			require.NoError(t, err)

			require.Equal(t, carrierUuid, driverResp.Driver.CarrierUuid)
			require.Equal(t, listCarriersResp.Carriers[0].ExternalId, driverResp.Driver.CarrierExternalId)
			require.Equal(t, listCarriersResp.Carriers[0].Name, driverResp.Driver.CarrierName)
			require.Equal(t, startDate, driverResp.Driver.StartDate)
			require.Equal(t, "", driverResp.Driver.EndDate)
			// these are coming from mocked up identity svc
			require.Equal(t, "Test User 0 First", driverResp.Driver.FirstName)
			require.Equal(t, "Test User 0 Last", driverResp.Driver.LastName)
			require.Equal(t, "9876543210", driverResp.Driver.PhoneNumber)
			require.Equal(t, "User0@onramp.com", driverResp.Driver.Email)

		})
		withSuite(t, "when creating carrier user", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityCreateUser(t, uuid.NewString())
			resp, err := suite.carrierClient.SetCarrierUser(ctxWithStaffToken(ctx), &carrier.SetCarrierUserRequest{
				User: &identity.IdentityUser{
					FirstName:   "test user first",
					LastName:    "test user last",
					PhoneNumber: "9876543210",
					Email:       "testUser@email.com",
					Role:        identity.Role_FLEET_MANAGER,
				},
			})
			require.NoError(t, err)
			// these are coming from mocked up identity svc
			require.Equal(t, "Test User 0 First", resp.User.FirstName)
			require.Equal(t, "Test User 0 Last", resp.User.LastName)
			require.Equal(t, "9876543210", resp.User.PhoneNumber)
			require.Equal(t, "User0@onramp.com", resp.User.Email)

		})
		withSuite(t, "when creating single driver as program", func(t *testing.T, ctx context.Context, suite testSuite) {
			// mock needed because createCarrier for program
			suite.mockIdentitySetPermissions(t)
			r, _ := suite.createCarrier(ctxWithProgramToken(ctx))
			carrierUuid := r.Carrier.Uuid
			dExtId := "123-abc"
			startDate := "20221115"

			// create driver
			suite.mockIdentityHasPermission(t, true)
			driverResp, err := suite.createDriver(t, ctxWithProgramToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: dExtId,
				CarrierUuid:      carrierUuid,
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        startDate,
			})
			require.NoError(t, err)
			require.Equal(t, carrierUuid, driverResp.Driver.CarrierUuid)
			require.Equal(t, dExtId, driverResp.Driver.DriverExternalId)
			require.Equal(t, startDate, driverResp.Driver.StartDate)
			require.Equal(t, "", driverResp.Driver.EndDate)
			require.True(t, driverResp.Driver.Enabled)
			// these are coming from mocked up identity svc
			require.Equal(t, "Test User 0 First", driverResp.Driver.FirstName)
			require.Equal(t, "Test User 0 Last", driverResp.Driver.LastName)
			require.Equal(t, "9876543210", driverResp.Driver.PhoneNumber)
			require.Equal(t, "User0@onramp.com", driverResp.Driver.Email)

		})

		withSuite(t, "when getting driver profile", func(t *testing.T, ctx context.Context, suite testSuite) {
			driverUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsersOnce(t, []string{driverUuid})
			dResp, err := suite.carrierClient.GetDriverProfile(ctxWithStaffToken(ctx), &carrier.GetDriverProfileRequest{
				DriverUuid: driverUuid,
			})
			require.NoError(t, err)
			require.Equal(t, "Oasis Trucking Inc", dResp.Driver.CarrierName)
		})

		withSuite(t, "when getting driver profile and the enableNewTokenScreen flag is on for the carrier then return true", func(t *testing.T, ctx context.Context, suite testSuite) {
			driverUuid := "1bab3eb7-9ef0-498e-910b-65dd0a7ec3f0"
			suite.mockIdentityListUsersOnce(t, []string{driverUuid})
			dResp, err := suite.carrierClient.GetDriverProfile(ctxWithStaffToken(ctx), &carrier.GetDriverProfileRequest{
				DriverUuid: driverUuid,
			})
			require.NoError(t, err)
			require.Equal(t, "Five Stars Trucking", dResp.Driver.CarrierName)
			require.Equal(t, map[string]string{"enableNewTokenScreen": "true"}, dResp.FeatureFlags)
		})
		withSuite(t, "when getting driver profile and the enableNewTokenScreen flag is off for the carrier then return false", func(t *testing.T, ctx context.Context, suite testSuite) {
			driverUuid := "b1a31129-bf4b-4b4c-8343-fa261c906443"
			suite.mockIdentityListUsersOnce(t, []string{driverUuid})
			dResp, err := suite.carrierClient.GetDriverProfile(ctxWithStaffToken(ctx), &carrier.GetDriverProfileRequest{
				DriverUuid: driverUuid,
			})
			require.NoError(t, err)
			require.Equal(t, map[string]string{"enableNewTokenScreen": "false"}, dResp.FeatureFlags)
			require.Equal(t, "Oasis Trucking Inc", dResp.Driver.CarrierName)
		})

		withSuite(t, "when listing all drivers", func(t *testing.T, ctx context.Context, suite testSuite) {
			// demo driver uuids
			driverUuids := []string{"b1a31129-bf4b-4b4c-8343-fa261c906443", "595a1753-26d4-4412-8dbf-9566b98b5d8e"}

			suite.mockIdentityListUsersOnce(t, driverUuids)
			dResp, err := suite.listDrivers(ctx, staffToken, driverUuids)
			require.NoError(t, err)
			require.Len(t, dResp.Drivers, 2)

			// create driver
			startDate := "20221117"
			cdResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: "abcd",
				CarrierUuid:      "14111301-2b7e-40da-9849-53ca863e8061",
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        startDate,
			})
			require.NoError(t, err)

			driverUuids = append(driverUuids, cdResp.Driver.Uuid)
			suite.mockIdentityListUsersOnce(t, driverUuids)
			dResp, err = suite.listDrivers(ctx, staffToken, driverUuids)
			require.NoError(t, err)
			require.Len(t, dResp.Drivers, 3)

			count := 0
			for _, d := range dResp.Drivers {
				if d.Uuid == driverUuids[0] {
					//demo driver uuid b1a31129-bf4b-4b4c-8343-fa261c906443
					require.Equal(t, "JBW-433", d.DriverExternalId)
					require.Equal(t, "20210131", d.StartDate)
					require.NotNil(t, d.Attributes)
					require.Equal(t, "1234", d.Attributes["DriverPin"])
					count++
				}
				if d.Uuid == driverUuids[1] {
					//demo driver uuid 595a1753-26d4-4412-8dbf-9566b98b5d8e
					require.Equal(t, "JBW#234", d.DriverExternalId)
					require.Equal(t, "20210404", d.StartDate)
					require.NotNil(t, d.Attributes)
					require.Equal(t, "9876", d.Attributes["DriverPin"])
					count++
				}
				if d.Uuid == cdResp.Driver.Uuid {
					//new driver
					require.Equal(t, "abcd", d.DriverExternalId)
					require.Equal(t, "20221117", d.StartDate)
					require.Nil(t, d.Attributes)
					count++
				}
			}
			require.Equal(t, 3, count)
		})
		withSuite(t, "when listing drivers by uuids", func(t *testing.T, ctx context.Context, suite testSuite) {
			// demo driver uuids
			driverUuids := []string{"b1a31129-bf4b-4b4c-8343-fa261c906443", "595a1753-26d4-4412-8dbf-9566b98b5d8e"}

			suite.mockIdentityListUsersOnce(t, []string{driverUuids[0]})
			dResp, err := suite.listDrivers(ctx, staffToken, []string{driverUuids[0]})
			require.NoError(t, err)
			require.Len(t, dResp.Drivers, 1)

			suite.mockIdentityListUsersOnce(t, driverUuids)
			dResp, err = suite.listDrivers(ctx, staffToken, driverUuids)
			require.NoError(t, err)
			require.Len(t, dResp.Drivers, 2)

			count := 0
			for _, d := range dResp.Drivers {
				if d.Uuid == driverUuids[0] {
					//demo driver uuid b1a31129-bf4b-4b4c-8343-fa261c906443
					require.Equal(t, "JBW-433", d.DriverExternalId)
					require.Equal(t, "20210131", d.StartDate)
					require.NotNil(t, d.Attributes)
					require.Equal(t, "1234", d.Attributes["DriverPin"])
					count++
				}
				if d.Uuid == driverUuids[1] {
					//demo driver uuid 595a1753-26d4-4412-8dbf-9566b98b5d8e
					require.Equal(t, "JBW#234", d.DriverExternalId)
					require.Equal(t, "20210404", d.StartDate)
					require.NotNil(t, d.Attributes)
					require.Equal(t, "9876", d.Attributes["DriverPin"])
					count++
				}
			}
			require.Equal(t, 2, count)
		})
		withSuite(t, "when creating single limit", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierId := listCarriersResp.Carriers[0].ExternalId
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			// Act
			limit := &carrier.CarrierLimit{
				AsOfDate:        "20220131",
				CarrierUuid:     carrierId,
				Amount:          "20.00",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				Quantity:        "",
			}

			resp, err := suite.setLimit(ctxWithStaffToken(ctx), &carrier.SetLimitRequest{
				CarrierExternalId: carrierId,
				Limit:             limit,
			})
			require.NoError(t, err)
			require.Equal(t, "20", resp.Limit.Amount)
			require.Equal(t, "0", resp.Limit.Quantity)

			requestDate := "20220131"

			getLimitResp, _ := suite.getLimits(ctxWithStaffToken(ctx), carrierUuid, requestDate)
			require.Equal(t, requestDate, getLimitResp.Limits[0].AsOfDate)
			require.Equal(t, "20", getLimitResp.Limits[0].Amount)
			require.Equal(t, carrierUuid, getLimitResp.Limits[0].CarrierUuid)

		})

		withSuite(t, "when creating multiple limits with same as_of_date gets latest limit", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			// Act
			requestDate := "20220131"
			suite.createLimit(ctx, carrierUuid, requestDate, "10.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			suite.createLimit(ctx, carrierUuid, requestDate, "20.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")

			getLimitResp, _ := suite.getLimits(ctxWithStaffToken(ctx), carrierUuid, requestDate)
			require.Equal(t, requestDate, getLimitResp.Limits[0].AsOfDate)
			require.Equal(t, "20", getLimitResp.Limits[0].Amount)
			require.Equal(t, carrierUuid, getLimitResp.Limits[0].CarrierUuid)

		})
		withSuite(t, "when creating multiple quantity limits with same as_of_date gets latest limit", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			limit1, err := suite.createLimit(ctx, carrierUuid, "20220131", "", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "200.12345")
			require.NoError(t, err)
			require.Equal(t, "200.12345", limit1.Quantity)
			require.Equal(t, "0", limit1.Amount)

			limit2, err := suite.createLimit(ctx, carrierUuid, "20220131", "", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "100.54321")
			require.NoError(t, err)
			require.Equal(t, "100.54321", limit2.Quantity)
			require.Equal(t, "0", limit2.Amount)

			requestDate := "20220131"

			getLimitResp, err := suite.getLimits(ctxWithStaffToken(ctx), carrierUuid, requestDate)
			require.NoError(t, err)
			require.Equal(t, requestDate, getLimitResp.Limits[0].AsOfDate)
			require.Equal(t, "0", getLimitResp.Limits[0].Amount)
			require.Equal(t, "100.54321", getLimitResp.Limits[0].Quantity)

			require.Equal(t, carrierUuid, getLimitResp.Limits[0].CarrierUuid)

		})

		withSuite(t, "when creating bulk limits for carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			limits := []*carrier.CarrierLimit{}

			diesel_limit := &carrier.CarrierLimit{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220624",
				Amount:          "340.00",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
			}
			reefer_limit := &carrier.CarrierLimit{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220624",
				Amount:          "200.00",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_REEFER,
			}
			def_limit := &carrier.CarrierLimit{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220624",
				Amount:          "100.55",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DEF,
			}

			limits = append(limits, diesel_limit, reefer_limit, def_limit)

			createBulkLimit, err := suite.setLimits(ctxWithStaffToken(ctx), carrierUuid, limits)
			require.NoError(t, err)
			require.Equal(t, "340", createBulkLimit.Limits[0].Amount)
			require.Equal(t, "200", createBulkLimit.Limits[1].Amount)
			require.Equal(t, "100.55", createBulkLimit.Limits[2].Amount)

			requestDate := "20220624"

			getLimitResp, err := suite.getLimits(ctxWithStaffToken(ctx), carrierUuid, requestDate)
			require.NoError(t, err)
			require.Equal(t, requestDate, getLimitResp.Limits[0].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getLimitResp.Limits[0].ProductCategory)
			require.Equal(t, carrierUuid, getLimitResp.Limits[0].CarrierUuid)

		})

		withSuite(t, "when calling create feedback for a driver",
			func(t *testing.T, ctx context.Context, suite testSuite) {
				suite.mockIdentityListUsers(t, []string{testDriverUuid})
				ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxGrpcMetadataXJwt.String(), driverToken)
				_, err := suite.carrierClient.CreateDriverFeedback(ctx, &carrier.CreateDriverFeedbackRequest{
					DriverUuid: testDriverUuid,
					Feedback: &carrier.Feedback{
						AppVersion:     "v1.0.0",
						MessageHelpful: "Hi",
					},
				})
				require.Error(t, err)

				f, _ := suite.store.GetFeedbackByDriverUuid(context.TODO(), testDriverUuid)
				require.Equal(t, "v1.0.0", f.AppVersion)
				require.Equal(t, "Undefined", f.Score)
				require.Equal(t, "Hi", f.MessageHelpful)
				require.Equal(t, "", f.MessageImprovement)

			})

		withSuite(t, "calling create feedback for a driver with feedback type",
			func(t *testing.T, ctx context.Context, suite testSuite) {
				suite.mockIdentityListUsers(t, []string{testDriverUuid})
				_, err := suite.carrierClient.CreateDriverFeedback(ctxWithStaffToken(ctx), &carrier.CreateDriverFeedbackRequest{
					DriverUuid: testDriverUuid,
					Feedback: &carrier.Feedback{
						AppVersion:     "v1.0.0",
						Score:          carrier.FeedbackScore_FEEDBACK_SCORE_HAPPY,
						MessageHelpful: "Very helpful, thanks!",
					},
				})
				require.Error(t, err)

				f, _ := suite.store.GetFeedbackByDriverUuid(context.TODO(), testDriverUuid)
				require.Equal(t, "v1.0.0", f.AppVersion)
				require.Equal(t, "Very helpful, thanks!", f.MessageHelpful)
				require.Equal(t, "Happy", f.Score)
			})

		withSuite(t, "when checking insert and get credit line", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err := suite.createCarrier(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			//get carrier uuid
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			carrieruuid := listCarriersResp.Carriers[0].Uuid

			_, err = suite.createCreditLine(ctxWithStaffToken(ctx), "", "0.00", "")
			require.ErrorContains(t, err, codes.InvalidArgument.String())

			amount := "120.00"

			resp, err := suite.createCreditLine(ctxWithStaffToken(ctx), carrieruuid, amount, "20221213")

			require.NoError(t, err)

			require.Equal(t, resp.CreditLine.Amount, amount)

			getResp, err := suite.getCreditLine(ctxWithStaffToken(ctx), carrieruuid)
			require.NoError(t, err)

			require.Equal(t, getResp.CreditLine.Amount, amount)

		})
		withSuite(t, "when getting carrier balance", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.mockIdentitySetPermissions(t)
			resp, _ := suite.createCarrier(ctxWithProgramToken(ctx))
			suite.paymentClient.EXPECT().GetBalanceV2(
				gomock.Any(), gomock.Any(),
			).Return(&payment.GetBalanceV2Response{
				Balances: &c.Balance{
					DieselBalance:      &pdecimal.Decimal{Value: "1200.00"},
					ReeferBalance:      &pdecimal.Decimal{Value: "100.00"},
					DefBalance:         &pdecimal.Decimal{Value: "50.00"},
					OilBalance:         &pdecimal.Decimal{Value: "0.00"},
					AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
					MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
					CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
					TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
					DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
				},
				PaymentsTotal: "100.00",
			}, nil,
			).AnyTimes()

			// Act
			balanceresp, err := suite.getCarrierBalance(ctxWithStaffToken(ctx), resp.Carrier.Uuid)

			// Assert
			require.NoError(t, err)
			require.Equal(t, "1350.00", balanceresp.BalanceAmount)
			require.Equal(t, "8650.00", balanceresp.AvailableFunds)

		})

		withSuite(t, "when deactivating active driver", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			emptyEndDate := ""
			ds, errGetDriversByUuid := suite.store.GetDriversByUuid(context.TODO(), []string{testDriverUuid})
			require.Equal(t, emptyEndDate, ds[0].EndDate)

			// Act
			suite.mockIdentityDisableUser(t)
			_, errDeactivateDriver := suite.carrierClient.DeactivateDriver(ctxWithStaffToken(ctx), &carrier.DeactivateDriverRequest{
				CarrierUuid:  ds[0].CarrierUuid,
				DriverUuid:   ds[0].Uuid,
				IdentityUuid: testDriverIdentityUuid,
			})

			ds, err := suite.store.GetDriversByUuid(context.TODO(), []string{testDriverUuid})

			// Assert
			require.NoError(t, errGetDriversByUuid)
			require.NoError(t, errDeactivateDriver)
			require.NoError(t, err)
			require.NotEqual(t, emptyEndDate, ds[0].EndDate)

		})

		withSuite(t, "when activating inactive driver", func(t *testing.T, ctx context.Context, suite testSuite) {

			ds, err := suite.store.GetDriversByUuid(context.TODO(), []string{testDriverUuid})
			require.NoError(t, err)
			require.Equal(t, "", ds[0].EndDate)

			suite.mockIdentityDisableUser(t)
			suite.carrierClient.DeactivateDriver(ctxWithStaffToken(ctx), &carrier.DeactivateDriverRequest{
				CarrierUuid:  ds[0].CarrierUuid,
				DriverUuid:   ds[0].Uuid,
				IdentityUuid: testDriverIdentityUuid,
			})

			suite.mockIdentityEnableUser(t)
			suite.carrierClient.ActivateDriver(ctxWithStaffToken(ctx), &carrier.ActivateDriverRequest{
				CarrierUuid:  ds[0].CarrierUuid,
				DriverUuid:   ds[0].Uuid,
				IdentityUuid: testDriverIdentityUuid,
			})

			ds, _ = suite.store.GetDriversByUuid(context.TODO(), []string{testDriverUuid})
			require.Equal(t, "", ds[0].EndDate)

		})

		withSuite(t, "when creating credit application with status submitted", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.carrierClient.CreateCreditApplication(ctxWithStaffToken(ctx), &carrier.CreateCreditApplicationRequest{
				BusinessName:         "The New Carrier",
				EstimatedWeeklySpent: "1000",
			})

			ca := storage.CreditApplication{}
			suite.store.Gdb.Table("carrier.credit_applications").Last(&ca)

			require.Equal(t, ca.ApplicationStatus, "submitted")
			require.Equal(t, ca.BusinessName, "The New Carrier")
			require.Equal(t, ca.EstimatedWeeklySpent, "1000.00")

		})
		withSuite(t, "when approving applications", func(t *testing.T, ctx context.Context, suite testSuite) {
			newCarrierName := "Test Trucking"
			newCarrierAddress1 := "123 Main street"

			suite.carrierClient.CreateCreditApplication(ctxWithStaffToken(ctx), &carrier.CreateCreditApplicationRequest{
				BusinessName:         newCarrierName,
				EstimatedWeeklySpent: "1000",
				BusinessAddress1:     newCarrierAddress1,
			})

			ca := storage.CreditApplication{}
			suite.store.Gdb.Table("carrier.credit_applications").Last(&ca)

			resp, err := suite.carrierClient.ApproveCreditApplication(ctxWithStaffToken(ctx), &carrier.ApproveCreditApplicationRequest{
				ApplicationUuid: ca.Uuid,
			})

			require.NoError(t, err)
			carrierUuid := resp.Carrier.Uuid

			ap := storage.CreditApplication{}
			suite.store.Gdb.Table("carrier.credit_applications").Last(&ap)
			require.Equal(t, ap.ApplicationStatus, "approved")

			//check if carrier was created when application approved
			retrieveResp, err := suite.retrieveCarrier(ctxWithStaffToken(ctx), carrierUuid)
			require.NoError(t, err)
			require.Equal(t, retrieveResp.Carrier.Name, newCarrierName)
			require.Equal(t, retrieveResp.Carrier.Address.Street1, newCarrierAddress1)

		})

		withSuite(t, "when declining applications", func(t *testing.T, ctx context.Context, suite testSuite) {
			newCarrierName := "Test Trucking"

			suite.carrierClient.CreateCreditApplication(ctxWithStaffToken(ctx), &carrier.CreateCreditApplicationRequest{
				BusinessName:         newCarrierName,
				EstimatedWeeklySpent: "1000",
			})

			ca := storage.CreditApplication{}
			suite.store.Gdb.Table("carrier.credit_applications").Last(&ca)

			_, err := suite.carrierClient.DeclineCreditApplication(ctxWithStaffToken(ctx), &carrier.DeclineCreditApplicationRequest{
				ApplicationUuid: ca.Uuid,
			})

			require.NoError(t, err)

			ap := storage.CreditApplication{}
			suite.store.Gdb.Table("carrier.credit_applications").Last(&ap)
			require.Equal(t, ap.ApplicationStatus, "declined")

		})

		withSuite(t, "when getting carrier drivers", func(t *testing.T, ctx context.Context, suite testSuite) {
			demoDriverUuids := []string{"b1a31129-bf4b-4b4c-8343-fa261c906443", "595a1753-26d4-4412-8dbf-9566b98b5d8e"}

			suite.mockIdentityListUsers(t, demoDriverUuids)
			r, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierUuid:       oasisCarrierUuid,
					CarrierExternalId: "",
				},
			)
			require.NoError(t, err)
			require.Equal(t, len(r.Drivers), 2)
			count := 0
			for _, cd := range r.Drivers {
				if strings.ToLower(cd.Uuid) == demoDriverUuids[0] {
					require.Equal(t, 1, len(cd.Attributes))
					require.Equal(t, "1234", cd.Attributes["DriverPin"])
					count++
				} else if strings.ToLower(cd.Uuid) == demoDriverUuids[1] {
					require.Equal(t, 1, len(cd.Attributes))
					require.Equal(t, "9876", cd.Attributes["DriverPin"])
					count++
				}
			}
			require.Equal(t, 2, count)
		})

		withSuite(t, "when getting carrier drivers by carrier external id", func(t *testing.T, ctx context.Context, suite testSuite) {
			demoDriverUuids := []string{"b1a31129-bf4b-4b4c-8343-fa261c906443", "595a1753-26d4-4412-8dbf-9566b98b5d8e"}
			suite.mockIdentityListUsers(t, demoDriverUuids)
			r, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierUuid:       "",
					CarrierExternalId: "EXT123",
				},
			)
			require.NoError(t, err)
			require.Equal(t, len(r.Drivers), 2)
			count := 0
			for _, cd := range r.Drivers {
				if strings.ToLower(cd.Uuid) == demoDriverUuids[0] {
					require.Equal(t, 1, len(cd.Attributes))
					require.Equal(t, "1234", cd.Attributes["DriverPin"])
					count++
				} else if strings.ToLower(cd.Uuid) == demoDriverUuids[1] {
					require.Equal(t, 1, len(cd.Attributes))
					require.Equal(t, "9876", cd.Attributes["DriverPin"])
					count++
				}
			}
			require.Equal(t, 2, count)

		})
		withSuite(t, "when getting drivers by carrier and carrier has no drivers should return zero", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityListUsers(t, []string{})
			r, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierUuid: "14111301-2b7e-40da-9849-53ca863e8061",
				},
			)
			require.NoError(t, err)
			require.Equal(t, len(r.Drivers), 0)
		})
		withSuite(t, "when listing carrier drivers by invalid carrier uuid should return 400", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityListUsers(t, []string{})
			_, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierUuid: "foo",
				},
			)
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when updating password for driver", func(t *testing.T, ctx context.Context, suite testSuite) {
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			// create driver
			cd, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			// get driver uuid
			driverUuid := cd.Driver.Uuid
			suite.mockIdentitySetPassword(t, []string{uuid.NewString()})
			_, err = suite.carrierClient.SetPassword(
				ctxWithStaffToken(ctx),
				&carrier.SetPasswordRequest{
					CarrierUuid:  carrierUuid,
					EntityUuid:   driverUuid,
					Temporary:    true,
					Password:     "test123",
					IdentityUuid: "",
				})
			require.NoError(t, err)

		})
		withSuite(t, "when creating bulk fees for carrier", func(t *testing.T, ctx context.Context, suite testSuite) {

			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			fees := []*carrier.TransactionFee{}

			cash_advance_fee := &carrier.TransactionFee{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220816",
				FlatAmount:      "1.50",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
			}

			cash_advance_fee2 := &carrier.TransactionFee{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220817",
				PercentAmount:   "5",
				Type:            c.FeeType_FEE_TYPE_PERCENT_AMOUNT,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
			}
			cash_advance_fee3 := &carrier.TransactionFee{
				CarrierUuid:     carrierUuid,
				AsOfDate:        "20220818",
				PercentAmount:   "3",
				FlatAmount:      "2.50",
				Type:            c.FeeType_FEE_TYPE_HIGHER_OF,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
			}
			cash_advance_fee4 := &carrier.TransactionFee{
				CarrierUuid:   carrierUuid,
				AsOfDate:      "20220818",
				PercentAmount: "3",
				FlatAmount:    "2.50",
				Type:          c.FeeType_FEE_TYPE_HIGHER_OF,
				//missing product category
			}

			fees = append(fees, cash_advance_fee, cash_advance_fee2, cash_advance_fee3, cash_advance_fee4)

			createBulkFee, err := suite.setFees(ctx, carrierUuid, fees)
			require.NoError(t, err)
			require.Equal(t, "1.50", createBulkFee.Responses[0].Fee.FlatAmount)
			require.Equal(t, "5", createBulkFee.Responses[1].Fee.PercentAmount)
			require.Equal(t, "3", createBulkFee.Responses[2].Fee.PercentAmount)
			require.Equal(t, "2.50", createBulkFee.Responses[2].Fee.FlatAmount)
			require.Equal(t, "2.50", createBulkFee.Responses[2].Fee.FlatAmount)
			require.Equal(t, int32(3), createBulkFee.Responses[3].Error.ErrorCode)

			requestDate := "20220818"

			getFeeResp, err := suite.carrierClient.GetTransactionFees(ctxWithStaffToken(ctx), &carrier.GetTransactionFeesRequest{
				CarrierUuid: carrierUuid,
				RequestDate: requestDate,
			})
			require.NoError(t, err)
			require.Len(t, getFeeResp.Fees, 1)
			require.Equal(t, requestDate, getFeeResp.Fees[0].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE, getFeeResp.Fees[0].ProductCategory)
			require.Equal(t, "2.50", getFeeResp.Fees[0].FlatAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_HIGHER_OF, getFeeResp.Fees[0].Type)

		})
		withSuite(t, "when creating fees by program and getting latest", func(t *testing.T, ctx context.Context, suite testSuite) {
			// create carrier
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.mockIdentitySetPermissions(t)
			carrierResp, _ := suite.createCarrier(ctxWithProgramToken(ctx))

			pUid := carrierResp.Carrier.Program.Uuid

			direct_fee := &carrier.TransactionFee{
				AsOfDate:    "20221106",
				FlatAmount:  "1.50",
				Type:        c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProgramUuid: pUid,
				BillingType: c.BillingType_BILLING_TYPE_DIRECT_BILL,
			}

			funded_fee := &carrier.TransactionFee{
				AsOfDate:    "20221106",
				FlatAmount:  "2.50",
				Type:        c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProgramUuid: pUid,
				BillingType: c.BillingType_BILLING_TYPE_FUNDED,
			}

			direct_fee_newer := &carrier.TransactionFee{
				AsOfDate:    "20221107",
				FlatAmount:  "3.50",
				Type:        c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProgramUuid: pUid,
				BillingType: c.BillingType_BILLING_TYPE_DIRECT_BILL,
			}

			funded_fee_newer := &carrier.TransactionFee{
				AsOfDate:    "20221107",
				FlatAmount:  "4.50",
				Type:        c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProgramUuid: pUid,
				BillingType: c.BillingType_BILLING_TYPE_FUNDED,
			}

			fee_missing_billing_type := &carrier.TransactionFee{
				AsOfDate:    "20221107",
				FlatAmount:  "5.50",
				Type:        c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProgramUuid: pUid,
			}
			fees := []*carrier.TransactionFee{
				direct_fee,
				funded_fee,
				direct_fee_newer,
				funded_fee_newer,
				fee_missing_billing_type,
			}

			setFeeResp, err := suite.carrierClient.SetTransactionFees(
				ctxWithStaffToken(ctx),
				&carrier.SetTransactionFeesRequest{
					ProgramUuid: pUid,
					Fees:        fees,
				})

			require.NoError(t, err)
			require.Equal(t, len(fees), len(setFeeResp.Responses))
			require.Equal(t, "1.50", setFeeResp.Responses[0].Fee.FlatAmount)
			require.Equal(t, c.BillingType_BILLING_TYPE_DIRECT_BILL, setFeeResp.Responses[0].Fee.BillingType)

			require.Equal(t, "2.50", setFeeResp.Responses[1].Fee.FlatAmount)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, setFeeResp.Responses[1].Fee.BillingType)

			require.Equal(t, "3.50", setFeeResp.Responses[2].Fee.FlatAmount)
			require.Equal(t, c.BillingType_BILLING_TYPE_DIRECT_BILL, setFeeResp.Responses[2].Fee.BillingType)

			require.Equal(t, "4.50", setFeeResp.Responses[3].Fee.FlatAmount)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, setFeeResp.Responses[3].Fee.BillingType)

			require.Equal(t, int32(3), setFeeResp.Responses[4].Error.ErrorCode)

			requestDate := "20221107"

			getFeeResp, err := suite.carrierClient.GetTransactionFees(ctxWithStaffToken(ctx), &carrier.GetTransactionFeesRequest{
				RequestDate: requestDate,
				ProgramUuid: pUid,
			})

			require.NoError(t, err)
			require.Len(t, getFeeResp.Fees, 2)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, getFeeResp.Fees[0].BillingType)

			require.Equal(t, "4.50", getFeeResp.Fees[0].FlatAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_FLAT_AMOUNT, getFeeResp.Fees[0].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, getFeeResp.Fees[0].BillingType)

			require.Equal(t, "3.50", getFeeResp.Fees[1].FlatAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_FLAT_AMOUNT, getFeeResp.Fees[1].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_DIRECT_BILL, getFeeResp.Fees[1].BillingType)

		})

		withSuite(t, "when fees show up on program object for list programs", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			merchantUuid1 := uuid.NewString()
			merchantUuid2 := uuid.NewString()
			resp, err := suite.carrierClient.SetProgram(ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					ExternalId:             "mp",
					Name:                   "my program",
					ApiHandle:              "my program",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)
			require.NoError(t, err)
			programUuid := resp.Program.Uuid
			// Act
			suite.carrierClient.SetTransactionFees(
				ctxWithStaffToken(ctx),
				&carrier.SetTransactionFeesRequest{
					ProgramUuid: programUuid,
					Fees: []*carrier.TransactionFee{
						{
							AsOfDate:        "20221106",
							FlatAmount:      "1.50",
							Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_DIRECT_BILL,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM,
						},
						{
							AsOfDate:        "20221106",
							FlatAmount:      "2.50",
							Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM,
						},
						{
							AsOfDate:        "20221107",
							FlatAmount:      "3.50",
							Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_DIRECT_BILL,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM,
						},
						{
							AsOfDate:        "20221107",
							FlatAmount:      "4.50",
							Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM,
						},
						{
							AsOfDate:        "20230610",
							PerGallonAmount: "0.50",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
							MerchantUuid:    merchantUuid1,
						},
						{
							AsOfDate:        "20230610",
							PerGallonAmount: "0.24",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
							MerchantUuid:    merchantUuid1,
						},
						{
							AsOfDate:        "20230611",
							PerGallonAmount: "0.40",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
							MerchantUuid:    merchantUuid2,
						},
						{
							AsOfDate:        "20230611",
							PerGallonAmount: "0.14",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
							MerchantUuid:    merchantUuid2,
						},
					},
				})

			var program carrier.Program

			if r, err := suite.carrierClient.ListPrograms(ctxWithStaffToken(ctx), &carrier.ListProgramsRequest{}); err == nil {
				program = *r.Programs[2]
			}

			fees := program.Fees

			sort.Slice(fees, func(i, j int) bool {
				if fees[i].FlatAmount == "0.00" {
					return fees[i].PerGallonAmount < fees[j].PerGallonAmount
				} else {
					return fees[i].FlatAmount < fees[j].FlatAmount
				}
			})
			t.Logf("FEES %+v", fees)
			// Assert
			require.Len(t, fees, 6)
			require.Equal(t, programUuid, program.Uuid)
			require.Equal(t, "4.50", fees[0].FlatAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_FLAT_AMOUNT, fees[0].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, fees[0].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[0].ProductCategory)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM, fees[0].FeeCategory)

			require.Equal(t, "0.14", fees[1].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, fees[1].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, fees[1].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[1].ProductCategory)
			require.Equal(t, merchantUuid2, fees[1].MerchantUuid)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING, fees[1].FeeCategory)

			require.Equal(t, "0.24", fees[2].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, fees[2].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, fees[2].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[2].ProductCategory)
			require.Equal(t, merchantUuid1, fees[2].MerchantUuid)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING, fees[2].FeeCategory)

			require.Equal(t, "0.40", fees[3].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, fees[3].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, fees[3].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[3].ProductCategory)
			require.Equal(t, merchantUuid2, fees[3].MerchantUuid)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING, fees[3].FeeCategory)

			require.Equal(t, "0.50", fees[4].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, fees[4].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_FUNDED, fees[4].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[4].ProductCategory)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING, fees[4].FeeCategory)

			require.Equal(t, "3.50", fees[5].FlatAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_FLAT_AMOUNT, fees[5].Type)
			require.Equal(t, c.BillingType_BILLING_TYPE_DIRECT_BILL, fees[5].BillingType)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, fees[5].ProductCategory)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM, fees[5].FeeCategory)

		})
		withSuite(t, "when creating marketing fees and getting", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})

			// Act
			merchantUuid1 := "2cb911fd-aa2f-4c55-b041-5dec20ed8906"
			merchantUuid2 := "cd970fc8-09aa-4a49-8660-6eaedd3ed320"

			programUuid := uuid.NewString()
			_, err := suite.carrierClient.SetTransactionFees(ctxWithStaffToken(ctx),
				&carrier.SetTransactionFeesRequest{
					ProgramUuid: programUuid,
					Fees: []*carrier.TransactionFee{
						{
							AsOfDate:        "20230610",
							PerGallonAmount: "0.50",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
							MerchantUuid:    merchantUuid1,
						},
						{
							AsOfDate:        "20230611",
							PerGallonAmount: "0.40",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
							MerchantUuid:    merchantUuid1,
						},
						{
							AsOfDate:        "20230610",
							PerGallonAmount: "0.30",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
							MerchantUuid:    merchantUuid2,
						},
						{
							AsOfDate:        "20230611",
							PerGallonAmount: "0.20",
							Type:            c.FeeType_FEE_TYPE_PER_GALLON,
							ProgramUuid:     programUuid,
							BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
							ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
							FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
							MerchantUuid:    merchantUuid2,
						},
					},
				})
			require.NoError(t, err)

			getFeeResp, err := suite.carrierClient.GetTransactionFees(ctxWithStaffToken(ctx), &carrier.GetTransactionFeesRequest{
				RequestDate: "20230612",
				ProgramUuid: programUuid,
			})
			require.NoError(t, err)

			require.Len(t, getFeeResp.Fees, 4)

			require.Equal(t, "20230610", getFeeResp.Fees[0].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getFeeResp.Fees[0].ProductCategory)
			require.Equal(t, "0.50", getFeeResp.Fees[0].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, getFeeResp.Fees[0].Type)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING, getFeeResp.Fees[0].FeeCategory)
			require.Equal(t, merchantUuid1, getFeeResp.Fees[0].MerchantUuid)

			require.Equal(t, "20230610", getFeeResp.Fees[1].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getFeeResp.Fees[1].ProductCategory)
			require.Equal(t, "0.30", getFeeResp.Fees[1].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, getFeeResp.Fees[1].Type)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING, getFeeResp.Fees[1].FeeCategory)
			require.Equal(t, merchantUuid2, getFeeResp.Fees[1].MerchantUuid)

			require.Equal(t, "20230611", getFeeResp.Fees[2].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getFeeResp.Fees[2].ProductCategory)
			require.Equal(t, "0.40", getFeeResp.Fees[2].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, getFeeResp.Fees[2].Type)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING, getFeeResp.Fees[2].FeeCategory)
			require.Equal(t, merchantUuid1, getFeeResp.Fees[2].MerchantUuid)

			require.Equal(t, "20230611", getFeeResp.Fees[3].AsOfDate)
			require.Equal(t, carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, getFeeResp.Fees[3].ProductCategory)
			require.Equal(t, "0.20", getFeeResp.Fees[3].PerGallonAmount)
			require.Equal(t, c.FeeType_FEE_TYPE_PER_GALLON, getFeeResp.Fees[3].Type)
			require.Equal(t, common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING, getFeeResp.Fees[3].FeeCategory)
			require.Equal(t, merchantUuid2, getFeeResp.Fees[3].MerchantUuid)

		})
		withSuite(t, "when checking readiness as carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			//create carrier
			suite.mockCreateLedgerAccount(t, carrierName)
			suite.mockIdentityListUsersOnceWithRole(t, uuid.NewString(), identity.Role_DRIVER, uuid.NewString())
			resp, err := suite.carrierClient.CreateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.CreateCarrierRequest{
					ExternalId:         "abc123",
					Address:            &carrierAddress,
					PrimaryContactName: carrierContact,
					Name:               "My Carrier",
					Phone:              carrierPhone,
				},
			)

			require.NoError(t, err)
			cUuid := resp.Carrier.Uuid

			cuuids := []string{cUuid}

			readyResp, err := suite.carrierClient.GetCarrierReadiness(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierReadinessRequest{
					CarrierUuids: cuuids,
				},
			)
			require.NoError(t, err)

			status := readyResp.Status[0]

			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.LimitsReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.PromptsReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.CreditLineReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.FeesReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.DriverReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_NOT_READY.String(), status.FleetAppReady.Status.String())

			//create limit
			_, err = suite.createLimit(ctx, cUuid, "20220228", "300.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			require.NoError(t, err)

			//create prompts
			_, err = suite.carrierClient.CreateCarrierPrompts(
				ctxWithStaffToken(ctx),
				&carrier.CreateCarrierPromptsRequest{
					CarrierUuid:      cUuid,
					HasOdometer:      true,
					HasTruckNumber:   true,
					HasTrailerNumber: true,
					HasTripNumber:    true,
					AsOfDate:         "20220523",
				},
			)
			require.NoError(t, err)

			// create credit line
			_, err = suite.createCreditLine(ctxWithStaffToken(ctx), cUuid, "120.00", "20220523")
			require.NoError(t, err)

			// create fees
			_, err = suite.setFees(
				ctxWithStaffToken(ctx),
				cUuid,
				[]*carrier.TransactionFee{{
					CarrierUuid:     cUuid,
					AsOfDate:        "20220816",
					FlatAmount:      "1.50",
					Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
					ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
				}},
			)
			require.NoError(t, err)

			requestDate := "20220818"

			getFeeResp, err := suite.carrierClient.GetTransactionFees(ctxWithStaffToken(ctx), &carrier.GetTransactionFeesRequest{
				CarrierUuid: cUuid,
				RequestDate: requestDate,
			})

			require.NoError(t, err)
			require.Len(t, getFeeResp.Fees, 1)
			suite.mockIdentitySetPermissions(t)

			//create drivers
			suite.mockIdentityCreateUserOnce(t, uuid.NewString(), identity.Role_DRIVER)
			_, err = suite.carrierClient.CreateDriver(ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: cUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			//create fleet app user
			suite.mockIdentityCreateUserOnce(t, uuid.NewString(), identity.Role_FLEET_MANAGER)

			userResp, err := suite.carrierClient.SetCarrierUser(ctxWithStaffToken(ctx), &carrier.SetCarrierUserRequest{
				User: &identity.IdentityUser{
					Role:        identity.Role_FLEET_MANAGER,
					FirstName:   "admin",
					LastName:    "admin",
					PhoneNumber: "9877890123",
					Email:       "admin@email.com",
					ParentUuid:  cUuid,
				},
			})
			require.NoError(t, err)
			suite.mockIdentityListUsersOnceWithRole(t, userResp.User.EntityUuid, identity.Role_FLEET_MANAGER, cUuid)

			readyResp2, err := suite.carrierClient.GetCarrierReadiness(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierReadinessRequest{
					CarrierUuids: cuuids,
				},
			)
			require.NoError(t, err)

			status = readyResp2.Status[0]

			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.LimitsReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.PromptsReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.CreditLineReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.FeesReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.DriverReady.Status.String())
			require.Equal(t, carrier.ReadyStatus_READY_STATUS_READY.String(), status.FleetAppReady.Status.String())
		})

		withSuite(t, "when checking readiness as program", func(t *testing.T, ctx context.Context, suite testSuite) {
			// create program
			suite.mockIdentitySetPermissions(t)

			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			suite.mockIdentityListUsers(t, []string{uuid.NewString()})

			suite.mockCreateLedgerAccount(t, carrierName)
			_, err := suite.carrierClient.SetProgram(
				ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					Uuid:                   "",
					ExternalId:             "abc",
					Name:                   programName,
					ApiHandle:              "eds",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{},
					RevenueSharePercentage: revenueSharePercentage,
				},
			)
			require.NoError(t, err)

			// create program carrier
			suite.mockIdentitySetPermissions(t)
			carrierResp, err := suite.carrierClient.CreateCarrier(
				ctxWithProgramToken(ctx),
				&carrier.CreateCarrierRequest{
					ExternalId:         "abc123",
					Address:            &carrierAddress,
					PrimaryContactName: carrierContact,
					Name:               "My Carrier",
					Phone:              carrierPhone,
				},
			)
			require.NoError(t, err)

			readyResp, err := suite.carrierClient.GetCarrierReadiness(
				ctxWithProgramToken(ctx),
				&carrier.GetCarrierReadinessRequest{
					CarrierUuids: []string{
						carrierResp.Carrier.Uuid,
					},
				},
			)
			require.NoError(t, err)

			// only one carrier is visible to the program
			require.Len(t, readyResp.Status, 1)
		})

		withSuite(t, "when getting full carrier data without program", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange: Limits
			limit, err := suite.createLimit(ctx, jesseCarrierUuid, "20220831", "20.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			require.NoError(t, err)

			// Arrange: Fees
			caFee := &carrier.TransactionFee{
				CarrierUuid:     jesseCarrierUuid,
				AsOfDate:        "20220816",
				FlatAmount:      "1.50",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
			}
			fResp, err := suite.setFees(ctx, jesseCarrierUuid, []*carrier.TransactionFee{caFee})
			require.NoError(t, err)
			require.Len(t, fResp.Responses, 1)

			// Arrange: CreditLine
			clResp, err := suite.createCreditLine(ctx, jesseCarrierUuid, "30000.00", "20220823")
			require.NoError(t, err)

			// Act
			resp, err := suite.getFullCarrierData(
				ctx,
				driverToken,
				jesseCarrierUuid,
				"20221213",
				c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				c.BillingType_BILLING_TYPE_FUNDED,
			)

			// Assert
			require.NoError(t, err)
			require.Len(t, resp.Limits, 1)
			require.Equal(t, limit.Amount, resp.Limits[0].Amount)
			require.Len(t, resp.Fees, 1)
			require.Equal(t, "1.50", resp.Fees[0].FlatAmount)
			require.NotNil(t, resp.CreditLine)
			require.Equal(t, clResp.CreditLine.Amount, resp.CreditLine.Amount)
			require.NotNil(t, resp.Subsidy)
			require.Equal(t, jesseCarrierUuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)
			require.Len(t, resp.AcceptedPaymentTypes, 1)
			require.Equal(t, resp.AcceptedPaymentTypes[0], c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN)
		})

		withSuite(t, "when getting full carrier data with program", func(t *testing.T, ctx context.Context, suite testSuite) {
			ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxAuthorization.String(), staffToken)
			// Arrange: program
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			pResp, err := suite.carrierClient.SetProgram(
				ctx,
				&carrier.SetProgramRequest{
					Name:                   "testProgram",
					ApiHandle:              "testtest",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT},
					CarrierUuids:           []string{jesseCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				})
			require.NoError(t, err)

			// Arrange: Limits
			limit, err := suite.createLimit(ctx, jesseCarrierUuid, "20220831", "20.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			require.NoError(t, err)

			// Arrange: Fees
			txnFee := &carrier.TransactionFee{
				ProgramUuid:     pResp.Program.Uuid,
				AsOfDate:        "20220816",
				FlatAmount:      "0.75",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
			}
			cashAdvanceFee := &carrier.TransactionFee{
				CarrierUuid:     jesseCarrierUuid,
				AsOfDate:        "20220816",
				FlatAmount:      "1.50",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
			}
			fResp, err := suite.carrierClient.SetTransactionFees(ctx,
				&carrier.SetTransactionFeesRequest{
					ProgramUuid: pResp.Program.Uuid,
					Fees:        []*carrier.TransactionFee{txnFee},
				})
			require.NoError(t, err)
			require.Len(t, fResp.Responses, 1)

			fResp, err = suite.carrierClient.SetTransactionFees(ctx,
				&carrier.SetTransactionFeesRequest{
					CarrierUuid: jesseCarrierUuid,
					Fees:        []*carrier.TransactionFee{cashAdvanceFee},
				})
			require.NoError(t, err)
			require.Len(t, fResp.Responses, 1)

			// Arrange: CreditLine
			clResp, err := suite.createCreditLine(ctx, jesseCarrierUuid, "30000.00", "20220823")
			require.NoError(t, err)

			// Act
			resp, err := suite.getFullCarrierData(
				ctx,
				driverToken,
				jesseCarrierUuid,
				"20220901",
				c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				c.BillingType_BILLING_TYPE_FUNDED,
			)

			// Assert
			require.NoError(t, err)
			require.Len(t, resp.Limits, 1)
			require.Equal(t, limit.Amount, resp.Limits[0].Amount)
			require.Len(t, resp.Fees, 2)
			count := 0
			for _, f := range resp.Fees {
				if f.ProductCategory == carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL {
					require.Equal(t, "0.75", f.FlatAmount)
					count++
				} else if f.ProductCategory == carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE {
					require.Equal(t, "1.50", f.FlatAmount)
					count++
				}
			}
			require.Equal(t, 2, count)
			require.NotNil(t, resp.CreditLine)
			require.Equal(t, clResp.CreditLine.Amount, resp.CreditLine.Amount)
			require.NotNil(t, resp.Subsidy)
			require.Equal(t, jesseCarrierUuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)
			require.Len(t, resp.AcceptedPaymentTypes, 1)
			require.Equal(t, resp.AcceptedPaymentTypes[0], c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT)
		})
		withSuite(t, "when getting full carrier data with program with funded and marketing fees", func(t *testing.T, ctx context.Context, suite testSuite) {
			ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxAuthorization.String(), staffToken)
			// Arrange: program
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			pResp, err := suite.carrierClient.SetProgram(
				ctx,
				&carrier.SetProgramRequest{
					Name:                   "testProgram",
					ApiHandle:              "testtest",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT},
					CarrierUuids:           []string{jesseCarrierUuid},
					RevenueSharePercentage: revenueSharePercentage,
				})
			require.NoError(t, err)

			// Arrange: Limits
			limit, err := suite.createLimit(ctx, jesseCarrierUuid, "20220831", "20.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			require.NoError(t, err)

			merchantUuid1 := uuid.NewString()
			merchantUuid2 := uuid.NewString()

			// Arrange: Fees
			txnFee := &carrier.TransactionFee{
				ProgramUuid:     pResp.Program.Uuid,
				AsOfDate:        "20220816",
				FlatAmount:      "0.75",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM,
			}
			cashAdvanceFee := &carrier.TransactionFee{
				CarrierUuid:     jesseCarrierUuid,
				AsOfDate:        "20220816",
				FlatAmount:      "1.50",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_CARRIER,
			}
			programMarketingFee := &carrier.TransactionFee{
				AsOfDate:        "20230611",
				PerGallonAmount: "0.40",
				Type:            c.FeeType_FEE_TYPE_PER_GALLON,
				ProgramUuid:     pResp.Program.Uuid,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
				MerchantUuid:    merchantUuid1,
			}
			onrampMarketingFee := &carrier.TransactionFee{
				AsOfDate:        "20230611",
				PerGallonAmount: "0.41",
				Type:            c.FeeType_FEE_TYPE_PER_GALLON,
				ProgramUuid:     pResp.Program.Uuid,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
				MerchantUuid:    merchantUuid1,
			}
			programMarketingFee2 := &carrier.TransactionFee{
				AsOfDate:        "20230611",
				PerGallonAmount: "0.42",
				Type:            c.FeeType_FEE_TYPE_PER_GALLON,
				ProgramUuid:     pResp.Program.Uuid,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING,
				MerchantUuid:    merchantUuid2,
			}
			onrampMarketingFee2 := &carrier.TransactionFee{
				AsOfDate:        "20230611",
				PerGallonAmount: "0.43",
				Type:            c.FeeType_FEE_TYPE_PER_GALLON,
				ProgramUuid:     pResp.Program.Uuid,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
				FeeCategory:     common.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING,
				MerchantUuid:    merchantUuid2,
			}

			fResp, err := suite.carrierClient.SetTransactionFees(ctx,
				&carrier.SetTransactionFeesRequest{
					ProgramUuid: pResp.Program.Uuid,
					Fees:        []*carrier.TransactionFee{txnFee, programMarketingFee, onrampMarketingFee, cashAdvanceFee, programMarketingFee2, onrampMarketingFee2},
				})
			require.NoError(t, err)
			require.Len(t, fResp.Responses, 6)

			// Arrange: CreditLine
			clResp, err := suite.createCreditLine(ctx, jesseCarrierUuid, "30000.00", "20220823")
			require.NoError(t, err)

			// Act
			resp, err := suite.carrierClient.GetFullCarrierData(ctx,
				&carrier.GetFullCarrierDataRequest{
					CarrierUuid:    jesseCarrierUuid,
					AsOfDate:       "20230612",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
					BillingType:    c.BillingType_BILLING_TYPE_FUNDED,
					MerchantUuid:   merchantUuid1,
				},
			)

			// Assert
			require.NoError(t, err)
			require.Len(t, resp.Limits, 1)
			require.Equal(t, limit.Amount, resp.Limits[0].Amount)
			require.Len(t, resp.Fees, 4)
			count := 0
			for _, f := range resp.Fees {
				if f.ProductCategory == carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL && f.FeeCategory == c.FeeCategory_FEE_CATEGORY_PROGRAM {
					require.Equal(t, "0.75", f.FlatAmount)
					count++
				} else if f.ProductCategory == carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE {
					require.Equal(t, "1.50", f.FlatAmount)
					count++
				} else if f.FeeCategory == c.FeeCategory_FEE_CATEGORY_PROGRAM_MARKETING {
					require.Equal(t, "0.40", f.PerGallonAmount)
					count++
				} else if f.FeeCategory == c.FeeCategory_FEE_CATEGORY_ONRAMP_MARKETING {
					require.Equal(t, "0.41", f.PerGallonAmount)
					count++
				}
			}
			require.Equal(t, 4, count)
			require.NotNil(t, resp.CreditLine)
			require.Equal(t, clResp.CreditLine.Amount, resp.CreditLine.Amount)
			require.NotNil(t, resp.Subsidy)
			require.Equal(t, jesseCarrierUuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)
			require.Len(t, resp.AcceptedPaymentTypes, 1)
			require.Equal(t, resp.AcceptedPaymentTypes[0], c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT)
		})

		withSuite(t, "when assigning carrier stripe account id", func(t *testing.T, ctx context.Context, suite testSuite) {

			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid
			stripeAccountId := "TESTSTRIPEACCOUNTID"

			_, err := suite.carrierClient.SetCarrierStripeAccount(
				ctxWithStaffToken(ctx),
				&carrier.SetCarrierStripeAccountRequest{
					CarrierUuid:     carrierUuid,
					StripeAccountId: stripeAccountId,
				},
			)
			require.NoError(t, err)

			c := storage.Carriers{}
			r := suite.store.Gdb.Table("carrier.carriers").Where("uuid=?", carrierUuid).Last(&c)
			newStripedAccountId := *c.StripeAccountId

			require.NoError(t, err)
			require.NoError(t, r.Error)
			require.Equal(t, stripeAccountId, newStripedAccountId)
		})
		withSuite(t, "when setting discount flag to never", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Act
			suite.updateCarrier(ctxWithStaffToken(ctx), &carrier.Carrier{
				Uuid:             fiveStarsCarrierUuid,
				ShowDiscountFlag: carrier.ShowDiscountToDrivers_SHOW_DISCOUNT_TO_DRIVERS_NEVER,
			})

			rCarrierResp, err := suite.retrieveCarrier(ctxWithStaffToken(ctx), fiveStarsCarrierUuid)

			// Assert
			require.NoError(t, err)
			require.Equal(t, carrier.ShowDiscountToDrivers_SHOW_DISCOUNT_TO_DRIVERS_NEVER, rCarrierResp.Carrier.ShowDiscountFlag)
		})

		withSuite(t, "when_initiate_reward_on_vendor_create_reward_on_carrier_and_update_status_to_redeemed_and_set_external_id", func(t *testing.T, ctx context.Context, suite testSuite) {
			externalId := "247JZ6WYORLX"
			suite.mockIdentityListUsers(t, []string{testDriverUuid})
			suite.mockPaymentGatewayInitiateReward(t, externalId)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "1200.00"},
				ReeferBalance:      &pdecimal.Decimal{Value: "100.00"},
				DefBalance:         &pdecimal.Decimal{Value: "50.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			ds, _ := suite.store.GetDriversByUuid(ctx, []string{testDriverUuid})
			driver := ds[0]

			suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            driver.CarrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        "500.00",
					RewardRedeemableAmount: "20.00",
				},
			)

			suite.carrierClient.InitiateDriverReward(
				ctxWithStaffToken(ctx),
				&carrier.InitiateDriverRewardRequest{
					DriverUuid: testDriverUuid,
				},
			)

			var reward storage.DriverRewards
			suite.store.Gdb.Last(&reward)
			require.Equal(t, rewarder.REWARD_STATUS_REDEEMED, reward.Status)
			require.Equal(t, externalId, reward.RewardExternalId)
		})

		withSuite(t, "when rewardAmount is higher than safeThresholdAmount should error out", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityListUsers(t, []string{testDriverUuid})
			suite.mockPaymentGatewayInitiateReward(t, "247JZ6WYORLX")
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				ReeferBalance:      &pdecimal.Decimal{Value: "100.00"},
				DefBalance:         &pdecimal.Decimal{Value: "50.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},

				// set DieselBalance to a high number to trigger the safeThresholdAmount check
				DieselBalance: &pdecimal.Decimal{Value: "10200.00"},
			})

			ds, _ := suite.store.GetDriversByUuid(ctx, []string{testDriverUuid})
			suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            ds[0].CarrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        "500.00",
					RewardRedeemableAmount: "20.00",
				},
			)

			_, err := suite.carrierClient.InitiateDriverReward(
				ctxWithStaffToken(ctx),
				&carrier.InitiateDriverRewardRequest{
					DriverUuid: testDriverUuid,
				},
			)
			require.ErrorContains(t, err, "failed to initiate driver reward: rewardAmount($400) is higher than safeThresholdAmount($250)")
		})

		withSuite(t, "when setting rewards to participating and getting reward participating status ", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			thresholdAmount := "500.00"
			rewardAmount := "20.00"

			_, err := suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)

			c := storage.Carriers{}
			r := suite.store.Gdb.Table("carrier.carriers").Where("uuid=?", carrierUuid).Last(&c)
			threshold := c.RewardThresholdAmount
			reward := c.RewardRedeemableAmount

			decimalAmount, _ := decimal.NewFromString(thresholdAmount)
			decimalRewardAmount, _ := decimal.NewFromString(rewardAmount)

			active := c.RewardsActive

			require.NoError(t, err)
			require.True(t, active)

			require.NoError(t, r.Error)
			require.Equal(t, decimalAmount, threshold)
			require.Equal(t, decimalRewardAmount, reward)

			getResp, err := suite.carrierClient.GetCarrierRewardStatus(
				ctxWithStaffToken(ctx),
				&carrier.GetCarrierRewardStatusRequest{
					CarrierUuid: carrierUuid,
				},
			)
			require.NoError(t, err)
			require.Equal(t, getResp.CarrierRewardStatus.ThresholdAmount, threshold.StringFixed(2))
			require.Equal(t, getResp.CarrierRewardStatus.RewardsActive, true)
			require.Equal(t, getResp.CarrierRewardStatus.RewardRedeemableAmount, reward.StringFixed(2))

		})
		withSuite(t, "when setting rewards status to not participating", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			thresholdAmount := "500.00"
			rewardAmount := "20.00"

			_, err := suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          false,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)

			c := storage.Carriers{}
			r := suite.store.Gdb.Table("carrier.carriers").Where("uuid=?", carrierUuid).Last(&c)
			amount := c.RewardThresholdAmount
			active := c.RewardsActive
			t.Logf("amount %v", amount)
			require.NoError(t, err)
			require.NoError(t, r.Error)
			require.False(t, active)

			require.Equal(t, decimal.Zero.String(), amount.String()) //0 if rewards not active

		})
		withSuite(t, "when getting rewards and not participating", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "1200.00"},
				ReeferBalance:      &pdecimal.Decimal{Value: "100.00"},
				DefBalance:         &pdecimal.Decimal{Value: "50.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)
			require.Equal(t, decimal.Zero.StringFixed(2), rewardsResp.Reward.Threshold)

		})
		withSuite(t, "when setting reward participation and getting rewards with no balance", func(t *testing.T, ctx context.Context, suite testSuite) {
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: oasisCarrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			thresholdAmount := "600.00"
			rewardAmount := "20.00"

			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            oasisCarrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "0.00"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"}, //this balance wont be included in rewards calculation
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})
			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			amountTillNext := decimal.RequireFromString("600.00") //sine 0 balance
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, decimal.Zero.StringFixed(2), rewardsResp.Reward.LifetimeRewards) //no redemptions
			require.Equal(t, "0", rewardsResp.Reward.RewardsAvailable)
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)

		})
		withSuite(t, "when getting rewards with balance less than threshold ", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			thresholdAmount := "600.00"
			rewardAmount := "20.00"

			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "437.19"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			amountTillNext := decimal.RequireFromString("162.81") //sine 437.19 balance and 600 threshold
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, decimal.Zero.StringFixed(2), rewardsResp.Reward.LifetimeRewards) //no redemptions
			require.Equal(t, "0", rewardsResp.Reward.RewardsAvailable)
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})
		withSuite(t, "when getting rewards with balance greater than threshold ", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			thresholdAmount := "600.00"
			rewardAmount := "20.00"

			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "637.19"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})
			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			amountTillNext := decimal.RequireFromString("562.81") //sine 637.19 balance and 600 threshold
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, decimal.Zero.StringFixed(2), rewardsResp.Reward.LifetimeRewards) //no redemptions
			require.Equal(t, "1", rewardsResp.Reward.RewardsAvailable)                        //reward now available
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})
		withSuite(t, "when redeeming reward and checking reward status after ", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			thresholdAmount := "600.00"
			rewardAmount := "20.00"

			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "637.19"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			suite.mockPaymentGatewayInitiateReward(t, "")
			redemptionResp, err := suite.carrierClient.InitiateDriverReward(
				ctxWithStaffToken(ctx),
				&carrier.InitiateDriverRewardRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)

			decimalBalanceAmount, _ := decimal.NewFromString("637.19")

			decimalRewardAmount, _ := decimal.NewFromString(rewardAmount)

			deletedUuid := uuid.NewString()

			deletedReward := &storage.DriverRewards{
				Uuid:                  deletedUuid,
				DriverUuid:            driverResp.Driver.Uuid,
				DateGiven:             sql.NullTime{Time: time.Now(), Valid: true},
				RewardRedeemedAmount:  decimalRewardAmount,
				BalanceConsumedAmount: decimalBalanceAmount,
				RewardExternalId:      "ABCDEF",
				Status:                "redeemed",
			}

			r := suite.store.Gdb.Model(&storage.DriverRewards{}).Create(deletedReward)
			require.NoError(t, r.Error)

			r2 := suite.store.Gdb.Delete(&deletedReward)

			require.NoError(t, r2.Error)

			t.Logf("redemptionResp %v", redemptionResp)
			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)

			require.NoError(t, err)

			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			amountTillNext := decimal.RequireFromString("562.81") //since 637.19 balance and 600 threshold
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, rewardAmount, rewardsResp.Reward.LifetimeRewards) //20 since it not count deleted award
			require.Equal(t, "0", rewardsResp.Reward.RewardsAvailable)         //since we redeemed
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})
		withSuite(t, "when checking reward available after small transaction", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			thresholdAmount := "500.00"
			rewardAmount := "20.00"
			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "3004.16"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			decimalRewardAmount, _ := decimal.NewFromString(rewardAmount)

			decimalPreviousBalanceConsumed, _ := decimal.NewFromString("2654.88")

			//add deleted reward
			deletedUuid := uuid.NewString()

			previousReward := &storage.DriverRewards{
				Uuid:                  deletedUuid,
				DriverUuid:            driverResp.Driver.Uuid,
				DateGiven:             sql.NullTime{Time: time.Now(), Valid: true},
				RewardRedeemedAmount:  decimalRewardAmount,
				BalanceConsumedAmount: decimalPreviousBalanceConsumed,
				RewardExternalId:      "ABCDEF",
				Status:                "redeemed",
			}

			r := suite.store.Gdb.Model(&storage.DriverRewards{}).Create(previousReward)
			require.NoError(t, r.Error)

			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)

			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			//3004.16 balance; 2654.88 redeemed; 3004.16-2654.88=349.28 into the next 500; 349.28/500=0 due; 500-349.28=150.72 left to reward
			amountTillNext := decimal.RequireFromString("150.72")
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, rewardAmount, rewardsResp.Reward.LifetimeRewards)
			require.Equal(t, "0", rewardsResp.Reward.RewardsAvailable)
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})
		withSuite(t, "when checking reward available after large transaction", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid
			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			thresholdAmount := "500.00"
			rewardAmount := "20.00"
			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "3734.16"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			decimalRewardAmount, _ := decimal.NewFromString(rewardAmount)

			decimalPreviousBalanceConsumed, _ := decimal.NewFromString("2654.88")

			//add deleted reward
			deletedUuid := uuid.NewString()

			previousReward := &storage.DriverRewards{
				Uuid:                  deletedUuid,
				DriverUuid:            driverResp.Driver.Uuid,
				DateGiven:             sql.NullTime{Time: time.Now(), Valid: true},
				RewardRedeemedAmount:  decimalRewardAmount,
				BalanceConsumedAmount: decimalPreviousBalanceConsumed,
				RewardExternalId:      "ABCDEF",
				Status:                "redeemed",
			}

			r := suite.store.Gdb.Model(&storage.DriverRewards{}).Create(previousReward)
			require.NoError(t, r.Error)

			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)

			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			//3734.16 balance; 2654.88 redeemed; 3734.16-2654.88=1079.28 toward rewards; 1079.28/500=2 due; 500*3-1079.28=420.72 left to reward
			amountTillNext := decimal.RequireFromString("420.72")
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, rewardAmount, rewardsResp.Reward.LifetimeRewards)
			require.Equal(t, "2", rewardsResp.Reward.RewardsAvailable)
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})
		withSuite(t, "when checking reward available after overredemption", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			thresholdAmount := "500.00"
			rewardAmount := "20.00"
			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "3004.16"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			decimalRewardAmount, _ := decimal.NewFromString(rewardAmount)

			decimalPreviousBalanceConsumed, _ := decimal.NewFromString("3654.88")

			//add deleted reward
			deletedUuid := uuid.NewString()

			previousReward := &storage.DriverRewards{
				Uuid:                  deletedUuid,
				DriverUuid:            driverResp.Driver.Uuid,
				DateGiven:             sql.NullTime{Time: time.Now(), Valid: true},
				RewardRedeemedAmount:  decimalRewardAmount,
				BalanceConsumedAmount: decimalPreviousBalanceConsumed,
				RewardExternalId:      "ABCDEF",
				Status:                "redeemed",
			}

			r := suite.store.Gdb.Model(&storage.DriverRewards{}).Create(previousReward)
			require.NoError(t, r.Error)

			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)

			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			//3004.16 balance; 3654.88 redeemed; 3654.88-3004.16=650.72 to catch up to 0 balance; 650.72+500=1150.72 to earn 1 reward
			amountTillNext := decimal.RequireFromString("1150.72")
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, rewardAmount, rewardsResp.Reward.LifetimeRewards)
			require.Equal(t, "-2", rewardsResp.Reward.RewardsAvailable)
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)
		})

		withSuite(t, "when multiple rewards due and redeeming ", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[2].Uuid

			driverResp, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)
			suite.mockIdentityListUsers(t, []string{driverResp.Driver.Uuid})

			thresholdAmount := "600.00"
			rewardAmount := "20.00"

			_, err = suite.carrierClient.SetRewards(
				ctxWithStaffToken(ctx),
				&carrier.SetRewardsRequest{
					CarrierUuid:            carrierUuid,
					RewardsActive:          true,
					ThresholdAmount:        thresholdAmount,
					RewardRedeemableAmount: rewardAmount,
				},
			)
			require.NoError(t, err)
			suite.mockPaymentGetBalanceV2(t, &c.Balance{
				DieselBalance:      &pdecimal.Decimal{Value: "1637.19"},
				ReeferBalance:      &pdecimal.Decimal{Value: "0.00"},
				DefBalance:         &pdecimal.Decimal{Value: "0.00"},
				OilBalance:         &pdecimal.Decimal{Value: "0.00"},
				AdditiveBalance:    &pdecimal.Decimal{Value: "0.00"},
				MerchandiseBalance: &pdecimal.Decimal{Value: "100.00"},
				CashAdvanceBalance: &pdecimal.Decimal{Value: "0.00"},
				TaxBalance:         &pdecimal.Decimal{Value: "0.00"},
				DiscountBalance:    &pdecimal.Decimal{Value: "0.00"},
			})

			suite.mockPaymentGatewayInitiateReward(t, "")
			redemptionResp, err := suite.carrierClient.InitiateDriverReward(
				ctxWithStaffToken(ctx),
				&carrier.InitiateDriverRewardRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)
			t.Logf("redemptionResp %v", redemptionResp)
			rewardsResp, err := suite.carrierClient.GetRewards(
				ctxWithStaffToken(ctx),
				&carrier.GetRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)

			decimalAmount := decimal.RequireFromString(thresholdAmount)

			lifetime := decimal.RequireFromString("40.00") //since two rewards given and reward amount = 20

			amountTillNext := decimal.RequireFromString("162.81") //since 1637.19 balance and 600 threshold
			require.Equal(t, decimalAmount.StringFixed(2), rewardsResp.Reward.Threshold)
			require.Equal(t, lifetime.StringFixed(2), rewardsResp.Reward.LifetimeRewards)
			require.Equal(t, "0", rewardsResp.Reward.RewardsAvailable) //since we redeemed
			require.Equal(t, amountTillNext.StringFixed(2), rewardsResp.Reward.AmountTillNext)

			listRewardsResp, err := suite.carrierClient.ListRewards(
				ctxWithStaffToken(ctx),
				&carrier.ListRewardsRequest{
					DriverUuid: driverResp.Driver.Uuid,
				},
			)
			require.NoError(t, err)
			require.Equal(t, 1, len(listRewardsResp.Rewards))
			require.Equal(t, driverResp.Driver.Uuid, listRewardsResp.Rewards[0].DriverUuid)
			require.Equal(t, lifetime.String(), listRewardsResp.Rewards[0].RewardRedeemedAmount)
			require.Equal(t, "1200", listRewardsResp.Rewards[0].BalanceConsumedAmount)

		})

		// TODO: remove this since the code is not used?
		withSuite(t, "when assigning and removing driver card", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.createCarrier(ctxWithStaffToken(ctx))
			// get carrier uuid
			listCarriersResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			suite.mockIdentityCreateUsers(t, []string{uuid.NewString()})
			suite.mockIdentitySetPassword(t, []string{uuid.NewString()})
			suite.mockIdentityCreateUser(t, uuid.NewString())

			// create driver
			cd, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				CarrierUuid: carrierUuid,
				FirstName:   "Test",
				LastName:    "Driver",
				PhoneNumber: "1234567890",
				Email:       "testdriver@email.com",
			})
			require.NoError(t, err)

			// get driver uuid
			driverUuid := cd.Driver.Uuid
			suite.mockIdentityListUsers(t, []string{driverUuid})

			// assign a card to driver uuid
			_, err = suite.carrierClient.AssignDriverCard(
				ctxWithStaffToken(ctx),
				&carrier.AssignDriverCardRequest{
					DriverUuid: driverUuid,
					Card:       "TESTCARDID",
				},
			)
			require.NoError(t, err)

			cdr, err := suite.carrierClient.GetDriverByCard(
				ctxWithStaffToken(ctx),
				&carrier.GetDriverByCardRequest{
					Card: "TESTCARDID",
				},
			)
			require.NoError(t, err)
			require.Equal(t, cdr.Driver.Uuid, driverUuid)

			// remove a card assignment
			_, err = suite.carrierClient.RemoveDriverCard(
				ctxWithStaffToken(ctx),
				&carrier.RemoveDriverCardRequest{
					Card: "TESTCARDID",
				},
			)
			require.NoError(t, err)

			_, err = suite.carrierClient.GetDriverByCard(
				ctxWithStaffToken(ctx),
				&carrier.GetDriverByCardRequest{
					Card: "TESTCARDID",
				},
			)
			require.Error(t, err)
		})

		withSuite(t, "when listing subsidies by carrier but none are set", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]

			resp, err := suite.carrierClient.ListSubsidies(
				ctxWithStaffToken(ctx),
				&carrier.ListSubsidiesRequest{
					CarrierUuid:    car.Uuid,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Len(t, resp.Subsidies, 0)
		})

		withSuite(t, "when_listing_subsidies_by_carrier_validate_payment_network_filter", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			carResp, _ := suite.listCarriers(ctxWithStaffToken(ctx))
			carUuid := carResp.Carriers[0].Uuid
			asOfDate := "20220102"
			amount := "0.17"

			tests := []struct {
				name   string
				seed   []carrier.SetSubsidyRequest
				filter carrier.ListSubsidiesRequest
				want   int
			}{
				{
					name: "with no payment network filter then return all subsidies",
					seed: []carrier.SetSubsidyRequest{
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_VISA,
						},
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
						},
					},
					filter: carrier.ListSubsidiesRequest{
						CarrierUuid: carUuid,
					},
					want: 2,
				},
				{
					name: "with closed loop filter theen only return closed loop",
					seed: []carrier.SetSubsidyRequest{
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_VISA,
						},
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
						},
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_VISA,
						},
					},
					filter: carrier.ListSubsidiesRequest{
						CarrierUuid:    carUuid,
						PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
					},
					want: 1,
				},
				{
					name: "with visa filter then only return visa",
					seed: []carrier.SetSubsidyRequest{
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_VISA,
						},
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
						},
						{
							CarrierUuid:    carUuid,
							Amount:         amount,
							AsOfDate:       asOfDate,
							PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
						},
					},
					filter: carrier.ListSubsidiesRequest{
						CarrierUuid:    carUuid,
						PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_VISA,
					},
					want: 1,
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					// Act
					for _, s := range tt.seed {
						_, e := suite.carrierClient.SetSubsidy(ctxWithStaffToken(ctx), &s)
						require.NoError(t, e)
					}
					resp, err := suite.carrierClient.ListSubsidies(ctxWithStaffToken(ctx), &tt.filter)

					// Assert
					require.NoError(t, err)
					require.Len(t, resp.Subsidies, tt.want)
				})
			}
		})

		withSuite(t, "when listing subsidies by carrier and merchant_A then return only merchant_A subsidies", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			anotherMerUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   anotherMerUuid,
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.ListSubsidies(
				ctxWithStaffToken(ctx),
				&carrier.ListSubsidiesRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Len(t, resp.Subsidies, 1)
			require.Equal(t, amount, resp.Subsidies[0].Amount)
			require.Equal(t, car.Uuid, resp.Subsidies[0].CarrierUuid)
			require.Equal(t, merUuid, resp.Subsidies[0].MerchantUuid)
		})

		withSuite(t, "when getting subsidy by carrier but none are set", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					LocationUuid:   locUuid,
					MerchantUuid:   merUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "", resp.Subsidy.LocationUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)

		})

		withSuite(t, "when setting subsidy by carrier and date and getting by carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					LocationUuid:   locUuid,
					MerchantUuid:   merUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "", resp.Subsidy.LocationUuid)
			require.Equal(t, amount, resp.Subsidy.Amount)

		})

		withSuite(t, "when setting subsidy by carrier and date and getting by another carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 1)
			car := carResp.Carriers[0]
			anotherCar := carResp.Carriers[1]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    anotherCar.Uuid,
					MerchantUuid:   merUuid,
					LocationUuid:   locUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, anotherCar.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "", resp.Subsidy.LocationUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)

		})

		withSuite(t, "when setting subsidy by carrier and date and merchant and getting by carrier and merchant", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					LocationUuid:   locUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, merUuid, resp.Subsidy.MerchantUuid)
			require.Equal(t, "", resp.Subsidy.LocationUuid)
			require.Equal(t, amount, resp.Subsidy.Amount)

		})

		withSuite(t, "when setting subsidy by carrier and date and merchant and location and getting by carrier and merchant and loc", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					LocationUuids:  []string{locUuid},
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					LocationUuid:   locUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, merUuid, resp.Subsidy.MerchantUuid)
			require.Equal(t, locUuid, resp.Subsidy.LocationUuid)
			require.Equal(t, amount, resp.Subsidy.Amount)

		})

		withSuite(t, "when setting subsidy by carrier and date and merchant and location and getting by carrier and merchant and another loc", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]
			merUuid := uuid.New().String()
			locUuid := uuid.New().String()
			anotherLocUuid := uuid.New().String()

			asOfDate := "20220102"
			amount := "0.17"

			_, err = suite.carrierClient.SetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.SetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					LocationUuids:  []string{locUuid},
					Amount:         amount,
					AsOfDate:       asOfDate,
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				},
			)
			require.NoError(t, err)

			resp, err := suite.carrierClient.GetSubsidy(
				ctxWithStaffToken(ctx),
				&carrier.GetSubsidyRequest{
					CarrierUuid:    car.Uuid,
					MerchantUuid:   merUuid,
					LocationUuid:   anotherLocUuid,
					AsOfDate:       "20220920",
					PaymentNetwork: c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				})

			require.NoError(t, err)
			require.Equal(t, car.Uuid, resp.Subsidy.CarrierUuid)
			require.Equal(t, "", resp.Subsidy.MerchantUuid)
			require.Equal(t, "", resp.Subsidy.LocationUuid)
			require.Equal(t, "0.00", resp.Subsidy.Amount)
		})
		withSuite(t, "when creating referral partner", func(t *testing.T, ctx context.Context, suite testSuite) {
			ctx = ctxWithStaffToken(ctx)
			resp, err := suite.createCarrier(ctx)
			require.NoError(t, err)

			carrierUUID := resp.Carrier.Uuid

			suite.mockIdentitySetPermissions(t)
			suite.mockIdentityDeletePermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err = suite.carrierClient.CreateReferralPartner(
				ctx,
				&carrier.CreateReferralPartnerRequest{
					Name:         referralPartnerName,
					CarrierUuids: []string{carrierUUID},
					Amount:       revenueSharePercentage,
					ContractType: carrier.ContractType_CONTRACT_TYPE_TRANSACTION_PERCENTAGE,
				})

			require.NoError(t, err)

			listResp, err := suite.carrierClient.ListReferralPartners(ctx, &carrier.ListReferralPartnersRequest{})

			require.NoError(t, err)
			require.Len(t, listResp.ReferralPartners, 2)
			require.Len(t, listResp.ReferralPartners[1].CarrierUuids, 1)
			require.Equal(t, revenueSharePercentage, listResp.ReferralPartners[1].Amount)

			listCarriers, err := suite.listCarriers(ctx)
			require.NoError(t, err)
			for i, c := range listCarriers.Carriers {
				if c.Uuid == carrierUUID {
					require.Equal(t, referralPartnerName, listCarriers.Carriers[i].ReferralPartner.Name)
				}
			}
		})

		withSuite(t, "when creating multiple drivers with different carrier uuid should retturn error",
			func(t *testing.T, ctx context.Context, suite testSuite) {
				_, err := suite.carrierClient.CreateDrivers(
					ctxWithStaffToken(ctx),
					&carrier.CreateDriversRequest{
						Drivers: []*carrier.Driver{
							{
								CarrierUuid:      uuid.NewString(),
								FirstName:        "Jon",
								LastName:         "Snow",
								Email:            "d@onramp.com",
								PhoneNumber:      "123456789",
								StartDate:        "20220330",
								DriverExternalId: "ext_id1",
							},
							{
								CarrierUuid:      uuid.NewString(),
								FirstName:        "Jon",
								LastName:         "Snow",
								Email:            "d@onramp.com",
								PhoneNumber:      "123456789",
								StartDate:        "20220330",
								DriverExternalId: "ext_id",
							},
						},
					},
				)
				require.ErrorContains(t, err, "carrierUuid mismatch")
				require.ErrorContains(t, err, codes.InvalidArgument.String())
			},
		)
	})

	t.Run("ShouldFail", func(t *testing.T) {
		t.Parallel()
		withSuite(t, "when retrieving carrier that does not exist", func(t *testing.T, ctx context.Context, suite testSuite) {
			invalidExtId := "fd6f04f4-cb1e-4d7c-8a52-ed561fa9a0x"
			_, err := suite.retrieveCarrier(ctxWithStaffToken(ctx), invalidExtId)
			require.Error(t, err)
		})

		withSuite(t, "when updating driver and read only fields are passed", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})

			// Act
			_, err := suite.updateDriver(ctxWithStaffToken(ctx), &carrier.Driver{
				CarrierUuid: fiveStarsCarrierUuid,
				Uuid:        testDriverUuid,
				FirstName:   "bob",
				LastName:    "doe",
				Email:       "a@b.com",
			})

			// Assert
			require.EqualError(t, err, "rpc error: code = FailedPrecondition desc = cannot update fields: email, firstName, lastName")

		})

		withSuite(t, "when querying subsidies without carrier", func(t *testing.T, ctx context.Context, suite testSuite) {
			_, err := suite.carrierClient.ListSubsidies(ctxWithStaffToken(ctx), &carrier.ListSubsidiesRequest{
				CarrierUuid: "",
			})
			require.Error(t, err)
		})

		withSuite(t, "when getting subsidies with network not set", func(t *testing.T, ctx context.Context, suite testSuite) {
			carResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			require.True(t, len(carResp.Carriers) > 0)
			car := carResp.Carriers[0]

			_, err = suite.carrierClient.GetSubsidy(ctx, &carrier.GetSubsidyRequest{
				CarrierUuid: car.Uuid,
				AsOfDate:    "20220102",
			})
			require.Error(t, err)
		})
		// Deprecated
		// TODO: remove since apiHandle logic will be removed
		withSuite(t, "when creating a program with duplicate api handle", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			carrierResp, err := suite.createCarrier(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			carrierUuids := []string{carrierResp.Carrier.Uuid}
			acceptedPaymentTypes := []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_BANK_ACCOUNT}

			// Act
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err = suite.carrierClient.SetProgram(
				ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           carrierUuids,
					RevenueSharePercentage: revenueSharePercentage,
				},
			)

			require.NoError(t, err)

			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err = suite.carrierClient.SetProgram(
				ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					ExternalId:             programExternalID + "too",
					Name:                   programName + "too",
					ApiHandle:              apiHandle,
					AcceptedPaymentTypes:   acceptedPaymentTypes,
					CarrierUuids:           carrierUuids,
					RevenueSharePercentage: revenueSharePercentage,
				},
			)
			// Assert
			require.EqualError(t,
				err,
				"rpc error: code = Internal desc = psql.UpsertProgram error creating program pq: duplicate key value violates unique constraint \"unique_program_api_handle\"",
			)
			require.ErrorContains(t, err, codes.Internal.String())
		})

		withSuite(t, "when listing carriers with bad token then should return invalid argument error", func(t *testing.T, ctx context.Context, suite testSuite) {

			ctx = metadata.AppendToOutgoingContext(ctx, "grpc-metadata-x-jwt", "foobar")
			_, err := suite.listCarriers(ctx)
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})

		withSuite(t, "when listing carriers but program has no carriers then should return 0 carriers", func(t *testing.T, ctx context.Context, suite testSuite) {
			resp, err := suite.listCarriers(ctxWithProgramToken(ctx))
			require.NoError(t, err)
			require.Equal(t, 0, len(resp.Carriers), "this program has no carriers and should return 0")
		})

		withSuite(t, "when listing programs with program token then should return permission denied", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err := suite.carrierClient.SetProgram(
				ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					Uuid:                   "",
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              "eds",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{},
					RevenueSharePercentage: revenueSharePercentage,
				})
			require.NoError(t, err)

			// NOTE: this is a staff endpoint
			_, err = suite.carrierClient.ListPrograms(ctxWithProgramToken(ctx), &carrier.ListProgramsRequest{})
			require.ErrorContains(t, err, codes.PermissionDenied.String())

		})

		withSuite(t, "when retrieving carrier using program token but program has no access to carrier then return permission denied", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetPermissions(t)
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			_, err = suite.carrierClient.SetProgram(
				ctxWithStaffToken(ctx),
				&carrier.SetProgramRequest{
					Uuid:                   "",
					ExternalId:             programExternalID,
					Name:                   programName,
					ApiHandle:              "eds",
					AcceptedPaymentTypes:   []c.PaymentInstrumentType{c.PaymentInstrumentType_PAYMENT_INSTRUMENT_TYPE_ONE_TIME_TOKEN},
					CarrierUuids:           []string{},
					RevenueSharePercentage: revenueSharePercentage,
				})
			require.NoError(t, err)

			suite.mockIdentityHasPermission(t, false)
			_, err = suite.retrieveCarrier(ctxWithProgramToken(ctx), listCarriersResp.Carriers[0].Uuid)
			require.ErrorContains(t, err, codes.PermissionDenied.String())
		})
		withSuite(t, "when creating driver as a program with no access to carrier then return permission denied", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			car := listCarriersResp.Carriers[1]
			require.Nil(t, car.Program)

			// Act
			// create driver
			suite.mockIdentityHasPermission(t, false)
			_, err = suite.createDriver(t, ctxWithProgramToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: "123-abc",
				CarrierUuid:      car.Uuid,
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        "20221115",
			})

			// Assert
			require.ErrorContains(t, err, codes.PermissionDenied.String())
		})

		withSuite(t, "when creating single driver with invalid attributes", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			// demo carrier - has known set of driver attribute headers, {DriverPin}
			carrierUuid := oasisCarrierUuid

			// create driver
			_, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: "D123",
				CarrierUuid:      carrierUuid,
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        "20221115",
				Attributes:       map[string]string{"Foo": "bar"},
			})
			require.ErrorContains(t, err, codes.FailedPrecondition.String())
		})

		withSuite(t, "when updating driver with unparseable end date", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			suite.mockIdentityDisableUser(t)
			suite.mockIdentityGetUser(t, uuid.NewString())

			// Act
			_, err := suite.carrierClient.UpdateDriver(
				ctxWithStaffToken(ctx),
				&carrier.UpdateDriverRequest{
					Driver: &carrier.Driver{
						EndDate: "foobar",
					},
				},
			)

			// Assert
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})

		withSuite(t, "when updating driver with end date that is after today est", func(t *testing.T, ctx context.Context, suite testSuite) {

			// Arrange
			suite.mockIdentityDisableUser(t)
			suite.mockIdentityGetUser(t, uuid.NewString())

			nowEst := time.Now().In(convert.EST_TZ())

			// Act
			_, err := suite.updateDriver(
				ctxWithStaffToken(ctx),
				&carrier.Driver{
					EndDate: nowEst.Add(time.Hour * 24).Format(convert.DATE_FORMAT),
				})

			// Assert
			require.ErrorContains(t, err, codes.FailedPrecondition.String())
		})
		withSuite(t, "when getting full carrier data without credit line added", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange: Limits
			_, err := suite.createLimit(ctx, jesseCarrierUuid, "20220831", "20.00", carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL, "")
			require.NoError(t, err)

			// Arrange: Fees
			caFee := &carrier.TransactionFee{
				CarrierUuid:     jesseCarrierUuid,
				AsOfDate:        "20220816",
				FlatAmount:      "1.50",
				Type:            c.FeeType_FEE_TYPE_FLAT_AMOUNT,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_CASH_ADVANCE,
				BillingType:     c.BillingType_BILLING_TYPE_FUNDED,
			}
			fResp, err := suite.setFees(ctx, jesseCarrierUuid, []*carrier.TransactionFee{caFee})
			require.NoError(t, err)
			require.Len(t, fResp.Responses, 1)

			// Act
			_, err = suite.getFullCarrierData(
				ctx,
				driverToken,
				jesseCarrierUuid,
				"20221213",
				c.PaymentNetwork_PAYMENT_NETWORK_CLOSED_LOOP,
				c.BillingType_BILLING_TYPE_FUNDED,
			)

			// Assert
			require.ErrorContains(t, err, codes.Internal.String())

		})
		withSuite(t, "when listing drivers by invalid carrier external id", func(t *testing.T, ctx context.Context, suite testSuite) {
			_, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierExternalId: "foo",
				},
			)
			require.ErrorContains(t, err, codes.NotFound.String())
		})
		withSuite(t, "when listing carrier drivers by invalid carrier uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityListUsers(t, []string{})
			_, err := suite.carrierClient.ListCarrierDrivers(
				ctxWithStaffToken(ctx),
				&carrier.ListCarrierDriversRequest{
					CarrierUuid: "foo",
				},
			)
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when retrieving carrier by invalid carrier uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentityListUsers(t, []string{})

			// Act
			_, err := suite.retrieveCarrier(ctxWithStaffToken(ctx), "invalid_uuid")

			// Assert
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when creating limit with missing category", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			suite.mockIdentitySetUser(t, []string{uuid.NewString()})

			// Act
			limit := &carrier.CarrierLimit{
				AsOfDate:    "20220131",
				CarrierUuid: fiveStarsCarrierUuid,
				Amount:      "20.00",
				Quantity:    "",
			}

			_, err := suite.setLimit(ctxWithStaffToken(ctx), &carrier.SetLimitRequest{
				CarrierUuid: fiveStarsCarrierUuid,
				Limit:       limit,
			})
			// Assert
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when creating limit with negative values", func(t *testing.T, ctx context.Context, suite testSuite) {
			// Arrange
			limit := &carrier.CarrierLimit{
				AsOfDate:        "20220131",
				CarrierUuid:     fiveStarsCarrierUuid,
				Amount:          "-20.00",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_ADDITIVES,
				Quantity:        "",
			}
			_, err := suite.setLimit(ctxWithStaffToken(ctx),
				&carrier.SetLimitRequest{
					CarrierUuid: fiveStarsCarrierUuid,
					Limit:       limit,
				},
			)

			require.ErrorContains(t, err, codes.InvalidArgument.String())

			// Act
			limit = &carrier.CarrierLimit{
				AsOfDate:        "20220131",
				CarrierUuid:     fiveStarsCarrierUuid,
				Amount:          "",
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_ADDITIVES,
				Quantity:        "-20",
			}
			_, err = suite.setLimit(ctxWithStaffToken(ctx),
				&carrier.SetLimitRequest{
					CarrierUuid: fiveStarsCarrierUuid,
					Limit:       limit,
				},
			)

			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when retrieving carrier by external id that does not exist", func(t *testing.T, ctx context.Context, suite testSuite) {
			ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxAuthorization.String(), staffToken)
			suite.mockIdentityListUsers(t, []string{})
			_, err := suite.carrierClient.RetrieveCarrier(ctx,
				&carrier.RetrieveCarrierRequest{
					ExternalId: "foo",
				},
			)
			require.ErrorContains(t, err, codes.NotFound.String())
		})
		withSuite(t, "when retrieving carrier by uuid that does not exist", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentityListUsers(t, []string{})
			_, err := suite.carrierClient.RetrieveCarrier(
				ctxWithStaffToken(ctx),
				&carrier.RetrieveCarrierRequest{
					Uuid: "595a1753-26d4-4412-8dbf-9566b98b5d8e",
				},
			)
			require.ErrorContains(t, err, codes.NotFound.String())
		})
		withSuite(t, "when creating single driver with an invalid uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			// create driver
			_, err := suite.createDriver(t, ctxWithStaffToken(ctx), &carrier.CreateDriverRequest{
				DriverExternalId: "D123",
				CarrierUuid:      "foo",
				FirstName:        "Test",
				LastName:         "Driver",
				PhoneNumber:      "1234567890",
				Email:            "testdriver@email.com",
				StartDate:        "20221115",
				Attributes:       map[string]string{"Foo": "bar"},
			})
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when updating a carrier by invalid uuid", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})

			_, err := suite.updateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.Carrier{
					Uuid: "foo",
					Name: "doesntmatter",
				},
			)

			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when updating a carrier by non-existant externalId", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err := suite.updateCarrier(ctxWithStaffToken(ctx),
				&carrier.Carrier{
					ExternalId: "foo",
					Name:       "doesntmatter",
				},
			)

			require.ErrorContains(t, err, codes.NotFound.String())
		})
		withSuite(t, "when updating a carrier using unset uuid and externalId", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			_, err := suite.updateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.Carrier{
					Uuid:       "",
					ExternalId: "",
					Name:       "doesntmatter",
				},
			)
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when updating a carrier externalId", func(t *testing.T, ctx context.Context, suite testSuite) {
			suite.mockIdentitySetUser(t, []string{uuid.New().String()})
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)

			car := listCarriersResp.Carriers[0]
			_, err = suite.updateCarrier(
				ctxWithStaffToken(ctx),
				&carrier.Carrier{
					Uuid:       car.Uuid,
					ExternalId: car.ExternalId + "updated",
				},
			)
			require.ErrorContains(t, err, "can not update carrier externalId")
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
		withSuite(t, "when creating limit with missing amount and quantity should return error", func(t *testing.T, ctx context.Context, suite testSuite) {
			listCarriersResp, err := suite.listCarriers(ctxWithStaffToken(ctx))
			require.NoError(t, err)
			carrierUuid := listCarriersResp.Carriers[0].Uuid

			// Act
			limit := &carrier.CarrierLimit{
				AsOfDate:        "20220131",
				CarrierUuid:     carrierUuid,
				ProductCategory: carrier.ProductCategory_PRODUCT_CATEGORY_DIESEL,
			}
			limits := []*carrier.CarrierLimit{limit}

			_, err = suite.carrierClient.SetLimits(
				ctxWithStaffToken(ctx),
				&carrier.SetLimitsRequest{
					CarrierUuid: carrierUuid,
					Limits:      limits,
				})
			require.ErrorContains(t, err, limiter.ErrMissingQuantityAndAmount.Error())
			require.ErrorContains(t, err, codes.InvalidArgument.String())
		})
	})
}

const (
	bufSize                = 1024 * 1024
	carrierExternalID      = "Test Carrier External ID"
	carrierContact         = "Primary Contact 1"
	carrierName            = "Jesses Trucking"
	carrierPhone           = "1234567890"
	programExternalID      = "Test Program External ID"
	programName            = "Onramp Aggregregator"
	apiHandle              = "ONA"
	tractorVin             = "test vin"
	unitId                 = "UnitId"
	limitStartDate         = "20220429"
	productCode            = 4
	testDriverUuid         = "595a1753-26d4-4412-8dbf-9566b98b5d8e"
	testDriverIdentityUuid = "cccfd26b-9eed-4b62-98f7-77b6ec6b9c10"
	testProgramUuid        = "4acdfcf2-a68d-4af0-bcfa-97aa0fb0a8fb"

	fiveStarsCarrierUuid = "83b81cff-8aef-4532-bd5b-8a138749bc89"
	oasisCarrierUuid     = "dacd7b7a-47cf-488b-947b-4bdb065f6605"
	jesseCarrierUuid     = "14111301-2b7e-40da-9849-53ca863e8061"

	// Deprecated: prefer validProgramToken instead
	oldProgramToken          = "eyJleHAiOjE3MzA4NzM3MzEsImlhdCI6MTY2NzgwMTczMSwianRpIjoiNjAwNWYyOWItNDkxYS00MjNjLTk4YjQtMmY5ODc3ZGRlM2UzIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLnRlc3Qub25yYW1wY2FyZC5jb20vYXV0aC9yZWFsbXMvQXBpIiwiYXVkIjoiYXBpIiwic3ViIjoiY2I0ZjI0NDktODk0OC00M2FiLWFjNTAtYjQyN2VlY2ZjN2FjIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZWRzIiwic2Vzc2lvbl9zdGF0ZSI6Ijg3ZTYwMzJlLTc0YjktNGIxNC04YjU1LTU2YThiN2FhOTg5ZiIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsicHJvZ3JhbSJdfSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwic2lkIjoiODdlNjAzMmUtNzRiOS00YjE0LThiNTUtNTZhOGI3YWE5ODlmIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInJvbGUiOiJbcHJvZ3JhbV0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJlZHMifQ"
	driverToken              = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkJvYiBEb2UiLCJpYXQiOjE1MTYyMzkwMjIsImlzcyI6Imh0dHA6Ly9sb2NhbGhvc3Q6OTk5MC9hdXRoL3JlYWxtcy9hcHAiLCJyb2xlcyI6WyJkcml2ZXIiXSwiZW50aXR5X3V1aWQiOiI1OTVhMTc1My0yNmQ0LTQ0MTItOGRiZi05NTY2Yjk4YjVkOGUifQ.tfLZ1HiEzZJU5tmr6lkqQkYgbkkKeVpL6_SKCDX8h9M"
	revenueSharePercentage   = "50.00"
	referralPartnerName      = "Basic Block"
	reportTz                 = "EST"
	validProgramToken        = "eyJyb2xlcyI6WyJwcm9ncmFtIl0sImVudGl0eV91dWlkIjoiNGMxZTM2MDYtMjA4ZS00NDQxLTk2N2ItOTk3ZWVhZmQ2MjIxIiwiYWxnIjoiSFMyNTYifQ"
	staffToken               = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwOi8vbG9jYWxob3N0Ojk5OTAvYXV0aC9yZWFsbXMvT25yYW1wIiwic3ViIjoiYzBhNWFjNmYtMmI5Ni00NjZlLTlhODgtNzc4ZGNmNmE4MjgwIiwicm9sZXMiOlsic3RhZmYiXX0.nnxItZl3T0lnBkAJ-43RXRzumXVJIsoRLHTNsTr0j70"
	testReferralPartnerToken = "ewogICAgImlhdCI6IDE1MTYyMzkwMjIsCiAgICAiaXNzIjogImh0dHA6Ly9sb2NhbGhvc3Q6OTk5MC9hdXRoL3JlYWxtcy9BcGkiLAogICAgInN1YiI6ICJjMGE1YWM2Zi0yYjk2LTQ2NmUtOWE4OC03NzhkY2Y2YTgyODAiLAogICAgInJvbGVzIjogWwogICAgICAgICJyZWZlcnJhbF9wYXJ0bmVyIgogICAgXSwKICAgICJlbnRpdHlfdXVpZCI6ICIzYTVjOTg1NC05OGIzLTRiNTktOGJkMS0yYjI0YmE2MDg0MjIiCn0K"
)

var carrierAddress = c.Address{
	Street1:    "123 main st",
	Street2:    "suite a",
	City:       "San Fransisco",
	Region:     "CA",
	PostalCode: "94106",
}

var driver1 = carrier.Driver{
	CarrierName:      carrierName,
	FirstName:        "Test Driver 1 First",
	LastName:         "Test Driver 1 Last",
	Email:            "Driver1@onramp.com",
	PhoneNumber:      "9876543210",
	StartDate:        "20220330",
	EndDate:          "",
	Type:             "Driver Type",
	DriverExternalId: "Driver 1 ext id",
}

type testSuite struct {
	store                *psql.Storage
	carrierClient        carrier.CarrierServiceClient
	identityClient       *mock_identity.MockIdentityServiceClient
	paymentClient        *mock_payment.MockPaymentServiceClient
	paymentGatewayClient *mock_payment_gateway.MockPaymentGatewayServiceClient
}

func newTestSuite(t *testing.T, ctx context.Context) (testSuite, func()) {
	t.Helper()

	var (
		server = grpc.NewServer(
			grpc.UnaryInterceptor(
				interceptors.JWTUnaryServerInterceptor(
					func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
						return false
					},
				),
			),
		)

		listener = bufconn.Listen(bufSize)
	)

	ffYaml := "enableNewTokenScreen:\n" +
		"  rule: carrierUuid in [\"83b81cff-8aef-4532-bd5b-8a138749bc89\"]\n" + // five stars
		"  whentrue: true\n" +
		"  whenfalse: false\n" +
		"  whenunknown: false\n"

	ffs, err := ff.Load([]byte(ffYaml))
	require.NoError(t, err)

	featureFlags := ff.NewFeatureFlags(ffs)

	store, cleanup := newTestStore(ctx, t)
	identityClient := mock_identity.NewMockIdentityServiceClient(gomock.NewController(t))
	paymentClient := mock_payment.NewMockPaymentServiceClient(gomock.NewController(t))
	paymentGatewayClient := mock_payment_gateway.NewMockPaymentGatewayServiceClient(gomock.NewController(t))
	emailProvider := local.NewMockEmailProvider()

	emailClient, _ := email.NewClient(emailProvider)

	// Register the carrier service and start the server.
	carrier.RegisterCarrierServiceServer(
		server,
		NewCarrierServer(
			store,
			identityClient,
			paymentClient,
			paymentGatewayClient,
			emailClient,
			featureFlags,
		),
	)

	utils.MockSlackInit()

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("server shutdown: %s\n", err)
		}
	}()

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}), grpc.WithInsecure(),
	)
	require.NoError(t, err)

	return testSuite{
			carrierClient:        carrier.NewCarrierServiceClient(conn),
			store:                store,
			identityClient:       identityClient,
			paymentClient:        paymentClient,
			paymentGatewayClient: paymentGatewayClient,
		}, func() {
			defer cleanup()
			require.NoError(t, conn.Close())
			server.GracefulStop()
			require.NoError(t, listener.Close())
		}
}

func (s *testSuite) createCarrier(ctx context.Context) (*carrier.CreateCarrierResponse, error) {
	carrierName := strings.ToLower("carrierName" + randAlphaString(3))
	s.mockCreateLedgerAccount(&testing.T{}, carrierName)
	return s.carrierClient.CreateCarrier(ctx,
		&carrier.CreateCarrierRequest{
			ExternalId:         carrierExternalID,
			Address:            &carrierAddress,
			PrimaryContactName: carrierContact,
			Name:               carrierName,
			Phone:              carrierPhone,
		},
	)
}

func (s *testSuite) updateCarrier(ctx context.Context, c *carrier.Carrier) (*carrier.UpdateCarrierResponse, error) {
	return s.carrierClient.UpdateCarrier(ctx,
		&carrier.UpdateCarrierRequest{
			Carrier: c,
		},
	)
}

func (s *testSuite) updateDriver(ctx context.Context, d *carrier.Driver) (*carrier.UpdateDriverResponse, error) {
	return s.carrierClient.UpdateDriver(ctx,
		&carrier.UpdateDriverRequest{
			Driver: d,
		},
	)
}

func (s *testSuite) setLimit(ctx context.Context, req *carrier.SetLimitRequest) (*carrier.SetLimitResponse, error) {
	return s.carrierClient.SetLimit(ctx, req)
}

func (s *testSuite) getFullCarrierData(
	ctx context.Context,
	tkn, cUid, aDate string,
	pn c.PaymentNetwork,
	bt c.BillingType,
) (*carrier.GetFullCarrierDataResponse, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxGrpcMetadataXJwt.String(), tkn)
	return s.carrierClient.GetFullCarrierData(ctx,
		&carrier.GetFullCarrierDataRequest{
			CarrierUuid:    cUid,
			AsOfDate:       aDate,
			PaymentNetwork: pn,
			BillingType:    bt,
		},
	)
}

func (s *testSuite) listDrivers(
	ctx context.Context,
	tkn string,
	dUids []string,
) (*carrier.ListDriversResponse, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, constants.CtxGrpcMetadataXJwt.String(), tkn)
	return s.carrierClient.ListDrivers(ctx, &carrier.ListDriversRequest{DriverUuids: dUids})
}

func (s *testSuite) setProgram(ctx context.Context, req *carrier.SetProgramRequest) (*carrier.SetProgramResponse, error) {
	t := &testing.T{}
	s.mockIdentitySetPermissions(t)
	s.mockIdentitySetUser(t, []string{uuid.NewString()})
	return s.carrierClient.SetProgram(ctxWithStaffToken(ctx), req)
}

func (s *testSuite) createDriver(t *testing.T, ctx context.Context, dr *carrier.CreateDriverRequest) (*carrier.CreateDriverResponse, error) {
	s.mockIdentityCreateUser(t, uuid.NewString())
	s.mockIdentitySetPermissions(t)
	return s.carrierClient.CreateDriver(ctx, dr)
}

func getUserList(uids []string) []*identity.IdentityUser {
	us := []*identity.IdentityUser{}

	for i, uid := range uids {
		us = append(us, &identity.IdentityUser{
			FirstName:   fmt.Sprintf("Test User %v First", i),
			LastName:    fmt.Sprintf("Test User %v Last", i),
			Email:       fmt.Sprintf("User%v@onramp.com", i),
			EntityUuid:  uid,
			PhoneNumber: "9876543210",
			Role:        identity.Role_DRIVER,
			Enabled:     true,
		})
	}

	return us
}

func getUserListWithRole(uids []string) []*identity.IdentityUser {
	us := []*identity.IdentityUser{}

	for i, uid := range uids {
		us = append(us, &identity.IdentityUser{
			FirstName:   fmt.Sprintf("Test User %v First", i),
			LastName:    fmt.Sprintf("Test User %v Last", i),
			Email:       fmt.Sprintf("User%v@onramp.com", i),
			EntityUuid:  uid,
			PhoneNumber: "9876543210",
			Role:        identity.Role_DRIVER,
			Enabled:     true,
		})
	}

	return us
}

func (s *testSuite) listCarriers(ctx context.Context) (*carrier.ListCarriersResponse, error) {
	return s.carrierClient.ListCarriers(ctx, &carrier.ListCarriersRequest{})
}

func (s *testSuite) retrieveCarrier(ctx context.Context, carrierUuid string) (*carrier.RetrieveCarrierResponse, error) {
	return s.carrierClient.RetrieveCarrier(ctx,
		&carrier.RetrieveCarrierRequest{
			Uuid: carrierUuid,
		})
}

func ctxWithStaffToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, constants.CtxGrpcMetadataXJwt.String(), staffToken)
}

func ctxWithProgramToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, constants.CtxGrpcMetadataXJwt.String(), validProgramToken)
}

func (s *testSuite) createCarrierTractorUnit(ctx context.Context, carrieruuid string) (*carrier.CreateCarrierTractorUnitResponse, error) {
	return s.carrierClient.CreateCarrierTractorUnit(
		ctxWithStaffToken(ctx),
		&carrier.CreateCarrierTractorUnitRequest{
			Vin:         tractorVin,
			UnitId:      unitId,
			CarrierUuid: carrieruuid,
			StartDate:   "20200405",
		},
	)
}

func (s *testSuite) getGetCarrierTransactionsReport(
	ctx context.Context, req *carrier.GetCarrierTransactionsReportRequest,
) (*carrier.GetCarrierTransactionsReportResponse, error) {
	return s.carrierClient.GetCarrierTransactionsReport(ctx, req)
}

func (s *testSuite) getProgramTransactionsReport(
	ctx context.Context, pu string,
) (*carrier.GetProgramTransactionsReportResponse, error) {
	return s.carrierClient.GetProgramTransactionsReport(
		ctx,
		&carrier.GetProgramTransactionsReportRequest{
			ProgramUuid: pu,
		},
	)
}

func (s *testSuite) createLimit(ctx context.Context, carrieruuid string, date string, amount string, category carrier.ProductCategory, quantity string) (carrier.CarrierLimit, error) {
	limit := &carrier.CarrierLimit{
		CarrierUuid:     carrieruuid,
		AsOfDate:        date,
		Amount:          amount,
		ProductCategory: category,
		Quantity:        quantity,
	}

	resp, err := s.carrierClient.SetLimits(
		ctxWithStaffToken(ctx),
		&carrier.SetLimitsRequest{
			CarrierUuid: carrieruuid,
			Limits:      []*carrier.CarrierLimit{limit},
		})

	if err != nil {
		return carrier.CarrierLimit{}, err
	}

	return *resp.Limits[0], nil
}

func (s *testSuite) setLimits(ctx context.Context, carrieruuid string, limits []*carrier.CarrierLimit) (*carrier.SetLimitsResponse, error) {
	return s.carrierClient.SetLimits(
		ctx,
		&carrier.SetLimitsRequest{
			CarrierUuid: carrieruuid,
			Limits:      limits,
		})
}

func (s *testSuite) setFees(ctx context.Context, carrieruuid string, fees []*carrier.TransactionFee) (*carrier.SetTransactionFeesResponse, error) {
	return s.carrierClient.SetTransactionFees(
		ctxWithStaffToken(ctx),
		&carrier.SetTransactionFeesRequest{
			CarrierUuid: carrieruuid,
			Fees:        fees,
		})
}

func (s *testSuite) getLimits(ctx context.Context, carrierUuid string, requestDate string) (*carrier.GetLimitsResponse, error) {
	return s.carrierClient.GetLimits(
		ctx,
		&carrier.GetLimitsRequest{
			CarrierUuid: carrierUuid,
			RequestDate: requestDate,
		})
}

func (s *testSuite) createCreditLine(ctx context.Context, carrierUuid string, amount string, date string) (*carrier.CreateCreditLineResponse, error) {
	return s.carrierClient.CreateCreditLine(
		ctxWithStaffToken(ctx),
		&carrier.CreateCreditLineRequest{
			CarrierUuid: carrierUuid,
			Amount:      amount,
			AsOfDate:    date,
		},
	)
}

func (s *testSuite) getCarrierBalance(ctx context.Context, carrierUuid string) (*carrier.GetBalanceResponse, error) {
	return s.carrierClient.GetBalance(
		ctx,
		&carrier.GetBalanceRequest{
			CarrierUuid: carrierUuid,
		})
}
func (s *testSuite) getCreditLine(ctx context.Context, carrierUuid string) (*carrier.GetCreditLineResponse, error) {
	return s.carrierClient.GetCreditLine(
		ctx,
		&carrier.GetCreditLineRequest{
			CarrierUuid: carrierUuid,
		})
}

type testFunc func(t *testing.T, ctx context.Context, suite testSuite)
type testHook func(t *testing.T, name string, fn testFunc)

// withSuite parallelizes the test and initializes a new test suite that can be
// used to inject dependencies in sub-tests.
var withSuite testHook = func(t *testing.T, name string, fn testFunc) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		suite, cleanup := newTestSuite(t, ctx)
		defer cleanup()
		fn(t, ctx, suite)
	})
}

const testDSN = "host=localhost port=5422 user=postgres password=adminPgPassword dbname=%s sslmode=disable"

// newTestStore returns a new test database.
// It should be initialized and torn down within a test hook.
// The returned interface supports parallelization among sub-tests.
// newTestStore returns a cleanup function that should be used to cleanup resources.
func newTestStore(ctx context.Context, t *testing.T) (*psql.Storage, func()) {
	t.Helper()

	// open a connection
	ogDbName := fmt.Sprintf(testDSN, "onramp")
	ogConn, err := sql.Open("postgres", ogDbName)
	require.NoError(t, err)

	// validate the connection
	requireValidConnection(t, ogConn, ogDbName)

	// create a new database name using a suffix with some entropy.
	testDbName := strings.ToLower("test_" + randAlphaString(64))
	_, err = ogConn.Exec("CREATE DATABASE " + testDbName)
	require.NoError(t, err, "failed to create test db")

	// open the newly created db
	testDsn := fmt.Sprintf(testDSN, testDbName)
	testDbConn, err := sql.Open("postgres", testDsn)
	require.NoError(t, err)

	// validate the test db connection
	requireValidConnection(t, testDbConn, testDbName)

	// resolve the migrations dir
	onrampRoot := os.Getenv("ONRAMP_ROOT")
	require.NotEmpty(t, onrampRoot, "failed to read ONRAMP_ROOT")
	migrationsDir := path.Join(onrampRoot, "or-backend/go/carrier/migrations")

	// apply the migrations
	require.NoError(t, goose.Up(testDbConn, migrationsDir))

	Gdb, _ := gorm.Open(gormPostgresDriver.New(gormPostgresDriver.Config{
		Conn: testDbConn,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "carrier.",
			SingularTable: false,
		},
	})

	return &psql.Storage{DB: testDbConn, Gdb: Gdb}, func() {
		// clean up function should drop the test db that was used and close all opened resources.
		defer ogConn.Close()
		// assert that we are able to rollback all migrations
		require.NoError(t, goose.Down(testDbConn, migrationsDir))
		require.NoError(t, testDbConn.Close())
		_, _ = ogConn.Exec("DROP DATABASE " + testDbName)
	}
}

func requireValidConnection(t *testing.T, conn *sql.DB, dbName string) {
	t.Helper()

	var attempts int
	for attempts <= 5 {
		if err := conn.Ping(); err != nil {
			if attempts == 5 {
				require.NoError(t, err, "failed to establish connection with %s db after 5 attempts; did you run make db?", dbName)
			}
			t.Logf("%s not ready, attempt: %d\n", dbName, attempts)
			t.Log("sleeping for 10 seconds then trying again ...")
			time.Sleep(10 * time.Second)
			attempts++
			continue
		}
		break
	}
}

func (suite *testSuite) mockIdentityCreateUsers(t *testing.T, u []string) {
	t.Helper()

	suite.identityClient.EXPECT().CreateUsers(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&identity.CreateUsersResponse{
			Users: getUserList(u),
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockCreateLedgerAccount(t *testing.T, name string) {
	t.Helper()
	suite.paymentClient.EXPECT().CreateLedgerAccount(gomock.Any(), gomock.Any()).
		Return(&payment.CreateLedgerAccountResponse{
			Account: &payment.LedgerAccount{
				Name: name,
			},
		}, nil)
}

func (suite *testSuite) mockIdentitySetUser(t *testing.T, u []string) {
	t.Helper()

	suite.identityClient.EXPECT().SetUser(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&identity.SetUserResponse{
			User: getUserList(u)[0],
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockIdentitySetPermissions(t *testing.T) {
	t.Helper()

	suite.identityClient.EXPECT().SetPermissions(
		gomock.Any(),
		gomock.Any(),
	).Return(&identity.SetPermissionsResponse{}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityHasPermission(t *testing.T, hasPermission bool) {
	t.Helper()

	suite.identityClient.EXPECT().HasPermission(
		gomock.Any(),
		gomock.Any(),
	).Return(&identity.HasPermissionResponse{
		HasPermission: hasPermission,
	}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityDeletePermission(t *testing.T) {
	t.Helper()

	suite.identityClient.EXPECT().DeletePermission(
		gomock.Any(),
		gomock.Any(),
	).Return(&identity.DeletePermissionResponse{}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityDeletePermissions(t *testing.T) {
	t.Helper()

	suite.identityClient.EXPECT().DeletePermissions(
		gomock.Any(),
		gomock.Any(),
	).Return(&identity.DeletePermissionsResponse{}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityCreateUser(t *testing.T, uuid string) {
	t.Helper()

	suite.identityClient.EXPECT().CreateUser(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&identity.CreateUserResponse{
			User: getUserList([]string{uuid})[0],
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockIdentityCreateUserOnce(t *testing.T, uuid string, role identity.Role) {
	t.Helper()

	suite.identityClient.EXPECT().CreateUser(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&identity.CreateUserResponse{
			User: &identity.IdentityUser{
				EntityUuid: uuid,
				Role:       role,
			},
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockIdentityDisableUser(t *testing.T) {
	t.Helper()

	suite.identityClient.EXPECT().DisableUser(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&identity.DisableUserResponse{
			User: getUserList([]string{uuid.NewString()})[0],
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockIdentityGetUser(t *testing.T, uId string) {
	t.Helper()

	suite.identityClient.EXPECT().GetUser(
		gomock.Any(), gomock.Any(),
	).Return(
		&identity.GetUserResponse{
			User: getUserList([]string{uId})[0],
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockPaymentGatewayInitiateReward(t *testing.T, externalId string) {
	t.Helper()

	suite.paymentGatewayClient.EXPECT().InitiateReward(
		gomock.Any(),
		gomock.Any(),
	).Return(
		&paymentgateway.InitiateRewardResponse{
			RewardExternalId: externalId,
		}, nil,
	).AnyTimes()
}

func (suite *testSuite) mockIdentityEnableUser(t *testing.T) {
	t.Helper()

	suite.identityClient.EXPECT().EnableUser(gomock.Any(), gomock.Any()).
		Return(&identity.EnableUserResponse{}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityListUsers(t *testing.T, u []string) {
	t.Helper()

	suite.identityClient.EXPECT().ListUsers(gomock.Any(), gomock.Any()).
		Return(&identity.ListUsersResponse{
			Users: getUserList(u),
		}, nil).AnyTimes()
}

func (suite *testSuite) mockIdentityListUsersOnce(t *testing.T, u []string) {
	t.Helper()

	suite.identityClient.EXPECT().ListUsers(gomock.Any(), gomock.Any()).
		Return(&identity.ListUsersResponse{
			Users: getUserList(u),
		}, nil).Times(1)
}

func (suite *testSuite) mockIdentityListUsersOnceWithRole(t *testing.T, uuid string, role identity.Role, parentUuid string) {
	t.Helper()

	suite.identityClient.EXPECT().ListUsers(gomock.Any(), gomock.Any()).
		Return(&identity.ListUsersResponse{
			Users: []*identity.IdentityUser{
				{
					EntityUuid: uuid,
					Role:       role,
					ParentUuid: parentUuid,
				},
			},
		}, nil).Times(1)
}

func (suite *testSuite) mockIdentitySetPassword(t *testing.T, u []string) {
	t.Helper()

	suite.identityClient.EXPECT().SetPassword(gomock.Any(), gomock.Any()).
		Return(&identity.SetPasswordResponse{
			IdentityUuid: uuid.New().String(),
		}, nil).AnyTimes()
}

func (suite *testSuite) mockListAllReceiptsByCarrierResponse(t *testing.T, d string) {
	t.Helper()

	suite.paymentClient.EXPECT().ListAllReceiptsByCarrier(gomock.Any(), gomock.Any()).
		Return(&payment.ListAllReceiptsByCarrierResponse{
			Receipts: []*payment.FuelReceipt{{
				ReceiptNumber: "4370565737489276",
				Auth:          "1432590",
				Txn: &payment.PaymentTransaction{
					Id: "baadb017-bdbd-4188-80b6-708266cde70a",
					DriverContext: &payment.DriverContext{
						Uuid: d,
					},
					Products: []*payment.ProductSale{{
						Name:             "Regular Diesel #2",
						Amount:           &pdecimal.Decimal{Value: "5.16"},
						Price:            &pdecimal.Decimal{Value: "5.160"},
						DiscountedAmount: &pdecimal.Decimal{Value: "5.11"},
						DiscountedPrice:  &pdecimal.Decimal{Value: "5.110"},
						Quantity:         &pdecimal.Decimal{Value: "1.0"},
						Code:             "19",
						TransactionFee:   &pdecimal.Decimal{Value: "0.00"},
						GrandTotal:       &pdecimal.Decimal{Value: "-12.90"},
					}, {
						Name:             "General Merchandise",
						Amount:           &pdecimal.Decimal{Value: "12.90"},
						Price:            &pdecimal.Decimal{Value: "12.90"},
						DiscountedAmount: &pdecimal.Decimal{Value: "12.90"},
						DiscountedPrice:  &pdecimal.Decimal{Value: "12.90"},
						Quantity:         &pdecimal.Decimal{Value: "1.0"},
						Code:             "400",
						TransactionFee:   &pdecimal.Decimal{Value: "0.0"},
						GrandTotal:       &pdecimal.Decimal{Value: "-12.90"},
					}, {
						Name:             "General Merchandise Refund",
						Amount:           &pdecimal.Decimal{Value: "-12.90"},
						Price:            &pdecimal.Decimal{Value: "-12.90"},
						DiscountedAmount: &pdecimal.Decimal{Value: "-12.90"},
						DiscountedPrice:  &pdecimal.Decimal{Value: "-12.90"},
						Quantity:         &pdecimal.Decimal{Value: "1.0"},
						Code:             "430",
						TransactionFee:   &pdecimal.Decimal{Value: "0.00"},
						GrandTotal:       &pdecimal.Decimal{Value: "-12.90"},
					},
					},
					Location: &payment.SiteContext{
						LocationName:        "San Francisco",
						MerchantName:        "Tree Inc",
						PumpNumber:          5,
						MerchantId:          "Test Merchant",
						LocationCity:        "Redwood City",
						LocationRegion:      "CA",
						StoreNumber:         "12",
						MerchantBillingType: c.BillingType_BILLING_TYPE_FUNDED,
					},
					TransactionData: &payment.TransactionData{
						Prompts: &payment.Prompts{
							Odometer:        3600000,
							TruckNumber:     "123A",
							WorkOrderNumber: "ABC",
						},
						Discount: &payment.Discount{
							Metadata: &c.DiscountMetadata{
								PricingSource: c.PricingSource_PRICING_SOURCE_EXTERNAL,
							},
						},
					},
				},
				CompletionTime: &datetime.DateTime{
					Year:    2021,
					Month:   11,
					Day:     22,
					Hours:   22,
					Minutes: 59,
					Seconds: 15,
				},
			}},
		}, nil).AnyTimes()
}

func (suite *testSuite) mockPaymentGetBalanceV2(t *testing.T, pbbalance *c.Balance) {
	t.Helper()

	suite.paymentClient.EXPECT().GetBalanceV2(gomock.Any(), gomock.Any()).
		Return(&payment.GetBalanceV2Response{
			Balances:      pbbalance,
			PaymentsTotal: "0.00",
		}, nil).AnyTimes()
}

// This is not strong entropy but it's ok in this context.
func randAlphaString(length int) string {
	const charSet = "abcdefghijklmnopqrstuvwxyz"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charSet[seededRand.Intn(len(charSet))]
	}
	return string(b)
}

func checkGrpcStatus(t *testing.T, err error, code codes.Code) {
	if grpcError, ok := status.FromError(err); ok {
		require.Equal(t, code, grpcError.Code())
	} else {
		require.Fail(t, fmt.Sprintf("Not a valid grpc error: %v", err))
	}
}

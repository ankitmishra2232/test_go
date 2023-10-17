package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/0nramp/carrier/pkg/storage/psql"
	pb "github.com/0nramp/protos/carrier"
	i "github.com/0nramp/protos/identity"
	p "github.com/0nramp/protos/payment"
	pg "github.com/0nramp/protos/paymentgateway"

	"github.com/0nramp/utils"
	"github.com/0nramp/utils/email"
	"github.com/0nramp/utils/email/providers/google"
	"github.com/0nramp/utils/email/providers/local"
	"github.com/0nramp/utils/ff"
	"github.com/0nramp/utils/interceptors"
	"github.com/0nramp/utils/trace"
	"github.com/0nramp/utils/xlog"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthcheck "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	grpcPort              = flag.Int("grpc-port", 50050, "gRPC server port")
	httpPort              = flag.Int("http-port", 8050, "The http proxy server port")
	dataSource            = flag.String("datasource", "postgres://", "postgres://user:pass@localhost/db")
	identityService       = flag.String("identity-service", ":50057", "identity service grpc server:port string")
	paymentService        = flag.String("payment-service", ":50055", "identity service grpc server:port string")
	paymentGatewayService = flag.String("paymentgateway-service", ":50090", "payment gateway service grpc server:port string")
	ffConfig              = flag.String("ff-config", "/etc/ff-config.yaml", "feature flag configuration")
)

var (
	customFunc = func(code codes.Code) zapcore.Level {
		if code == codes.OK {
			return zap.InfoLevel
		}
		return zap.ErrorLevel
	}

	customLogProducer = func(ctx context.Context, msg string, level zapcore.Level, code codes.Code, err error, duration zapcore.Field) {
		data := []zapcore.Field{}
		span := trace.SpanFromContext(ctx)
		if span != nil {
			data = append(data, zap.String("spanID", span.SpanContext().SpanID().String()))
			data = append(data, zap.String("traceID", span.SpanContext().TraceID().String()))
		}

		ctxzap.Extract(ctx).With(data...).Check(level, msg).Write(
			zap.Error(err),
			zap.String("grpc.code", code.String()),
			duration,
		)
	}
)

func main() {
	flag.Parse()

	xlog.Init("carrier")
	// Init Slack Client
	utils.SlackInit()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		xlog.Fatalf("failed to listen: %v", err)
	}

	opts := []grpc_zap.Option{
		grpc_zap.WithDecider(func(fullMethodName string, err error) bool {
			// will not log gRPC calls if it was a call to healthcheck and no error was raised
			if err == nil && fullMethodName == "/grpc.health.v1.Health/Check" {
				return false
			}

			// by default everything will be logged
			return true
		}),
		grpc_zap.WithLevels(customFunc),
		grpc_zap.WithMessageProducer(customLogProducer),
	}

	data, err := os.ReadFile(*ffConfig)
	if err != nil {
		xlog.Infof("Invalid feature flag config file (check ff-config flag) %s (%v)", *ffConfig, err)
		data = []byte{}
	}

	ffs, err := ff.Load(data)
	var featureFlags *ff.FeatureFlags

	if err != nil {
		xlog.Infof("Could not read feature flag config from %s (check ff-config flag) (%v)", *ffConfig, err)
		featureFlags = ff.Empty
	} else {
		featureFlags = ff.NewFeatureFlags(ffs)
	}

	var tracerDisabled bool
	if envVal, ok := os.LookupEnv("TRACER_DISABLED"); ok {
		tracerDisabled, _ = strconv.ParseBool(envVal)
	}

	xlog.Infof("binding to trace provider")
	prv, err := trace.NewProvider(trace.ProviderConfig{
		ServiceName: "carrier-service",
		Disabled:    tracerDisabled,
	})
	if err != nil {
		xlog.Infof("failed to create trace provider: %v", err)
	} else {
		xlog.Infof("successfully created trace provider")
	}
	defer prv.Close(context.Background())

	s := grpc.NewServer(
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				interceptors.JWTUnaryServerInterceptor(
					func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
						// extract jwt for all calls except healthcheck
						return fullMethodName == "/grpc.health.v1.Health/Check"
					},
				),
				otelgrpc.UnaryServerInterceptor(),
				grpc_prometheus.UnaryServerInterceptor,
				grpc_ctxtags.UnaryServerInterceptor(
					grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor),
				),
				grpc_zap.UnaryServerInterceptor(xlog.Zap(), opts...),
				grpc_zap.PayloadUnaryServerInterceptor(
					xlog.Zap(),
					func(ctx context.Context, fullMethodName string, servingObject interface{}) bool {
						// log payloads for all calls except healthcheck
						return fullMethodName != "/grpc.health.v1.Health/Check"
					},
				),
			),
		),
	)
	grpc_prometheus.Register(s)

	var conn *grpc.ClientConn

	// identity service
	conn, err = grpc.Dial(
		*identityService,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	if err != nil {
		xlog.Fatalf("failed to connect to identity service: %s", err)
	}
	defer conn.Close()

	idtClient := i.NewIdentityServiceClient(conn)
	xlog.Infof("binding to Identity service on %s", *identityService)

	// payment service
	conn, err = grpc.Dial(
		*paymentService,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	if err != nil {
		xlog.Fatalf("failed to connect to payment service: %s", err)
	}
	defer conn.Close()

	pmtClient := p.NewPaymentServiceClient(conn)
	xlog.Infof("binding to payments service on %s", *paymentService)

	// payment gateway service
	conn, err = grpc.Dial(
		*paymentGatewayService,
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()))
	if err != nil {
		xlog.Fatalf("failed to connect to payment gateway service: %s", err)
	}
	defer conn.Close()

	pmtGatewayClient := pg.NewPaymentGatewayServiceClient(conn)
	xlog.Infof("binding to payment gateway service on %s", *paymentGatewayService)

	db, err := psql.NewStorage(*dataSource, pmtClient, idtClient)
	if err != nil {
		xlog.Fatalf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	emailProvider := local.NewEmailProvider()
	env := os.Getenv("ENVIRONMENT")
	if env != "local" {
		appPassword, ok := os.LookupEnv("MAIL_ADMIN_APP_PASSWORD")
		if !ok {
			xlog.Fatal("MAIL_ADMIN_APP_PASSWORD unset")
		}

		provider, err := google.NewEmailProvider(originAddress, appPassword)
		if err != nil {
			xlog.Fatalf("failed to initialize new gmail provider: %v", err)

		}
		emailProvider = provider
	}
	emailClient, err := email.NewClient(emailProvider)
	if err != nil {
		xlog.Fatalf("failed to initialize email client: %v", err)
	}

	// register carrier service
	pb.RegisterCarrierServiceServer(
		s,
		NewCarrierServer(db, idtClient, pmtClient, pmtGatewayClient, emailClient, featureFlags),
	)

	h := healthcheck.NewServer()
	healthpb.RegisterHealthServer(s, h)
	xlog.Infof("Registering gRPC health check at %v", lis.Addr())

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			xlog.Fatalf("grpc failed to serve: %v", err)
		}
		wg.Done()

	}()

	go func() {
		defer wg.Done()
		grpcServerEndpoint := fmt.Sprintf("localhost:%d", *grpcPort)
		if err := proxyRun(grpcServerEndpoint, *httpPort); err != nil {
			xlog.Fatalf("http failed to serve: %v)", err)
		}
	}()

	wg.Wait()
}

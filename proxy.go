package main

import (
	"context"
	"fmt"
	"net/http"

	pb "github.com/0nramp/protos/carrier"
	"github.com/0nramp/utils/xlog"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func cors(h http.Handler) http.Handler {
	sentryHandler := sentryhttp.New(sentryhttp.Options{})

	return http.HandlerFunc(
		sentryHandler.HandleFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
				w.Header().Set("Access-Control-Allow-Headers", "Access-Control-Allow-Credentials, Access-Control-Allow-Headers, Origin, Accept, ResponseType, Content-Type, Content-Length, Accept-Encoding, Authorization, Access-Control-Allow-Origin, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers")
				if r.Method == "OPTIONS" {
					w.WriteHeader(http.StatusOK)
					return
				}
				h.ServeHTTP(w, r)
			},
		),
	)
}

// need to ensure all headers get mapped to metadata for tracing
func CustomHeaderMatcher(key string) (string, bool) {
	switch key {
	case "Connection":
		return key, false
	default:
		return key, true
	}
}

func proxyRun(grpcServerEndpoint string, httpPort int) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register gRPC server endpoint
	// Note: Make sure the gRPC server is running properly and accessible
	mux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(CustomHeaderMatcher),
		runtime.WithMarshalerOption("application/json", &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				Indent: "  ",
			},
			UnmarshalOptions: protojson.UnmarshalOptions{
				DiscardUnknown: true,
			},
		}),
	)

	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := pb.RegisterCarrierServiceHandlerFromEndpoint(ctx, mux, grpcServerEndpoint, opts)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf(":%d", httpPort)
	xlog.Infof("server listening at %s proxying %s", endpoint, grpcServerEndpoint)
	// Start HTTP server (and proxy calls to gRPC server endpoint)

	// Register metrics route
	metricsHandler := promhttp.Handler()
	err = mux.HandlePath(
		"GET",
		"/metrics",
		func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
			metricsHandler.ServeHTTP(w, r)
		})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:    endpoint,
		Handler: cors(mux),
	}
	return srv.ListenAndServe()

}

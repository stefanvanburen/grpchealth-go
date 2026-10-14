// Copyright 2022-2024 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpchealth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/quick"
	"testing/synctest"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectinprocess"
	healthv1 "connectrpc.com/grpchealth/v2/internal/gen/go/connectext/grpc/health/v1"
)

func TestCode(t *testing.T) {
	t.Parallel()

	knownStatuses := map[Status]struct{}{
		StatusUnknown:        {},
		StatusServing:        {},
		StatusNotServing:     {},
		StatusServiceUnknown: {},
	}
	check := func(s Status) bool {
		got := s.String()
		_, known := knownStatuses[s]
		return known != strings.HasPrefix(got, "status_")
	}
	// always check named statuses
	for status := range knownStatuses {
		if !check(status) {
			t.Fatalf("expected string representation of %q to be customized", status)
		}
	}
	// probabilistically explore other statuses
	if err := quick.Check(check, nil /* config */); err != nil {
		t.Fatal(err)
	}
}

func TestHealth_Check(t *testing.T) {
	const (
		userFQN = "acme.user.v1.UserService"
		unknown = "foobar"
	)
	t.Parallel()
	mux := http.NewServeMux()
	checker := NewStaticChecker(userFQN)
	connectServer := connect.NewServer()
	Register(connectServer, checker)
	connecthttp.Mount(mux, connectServer)
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	transport := connecthttp.NewTransport(
		server.Client(),
		server.URL,
		connecthttp.WithGRPC(),
	)
	client := connect.NewClient(transport)
	checkSpec := connect.Spec{
		StreamType: connect.StreamTypeUnary,
		Procedure:  checkProcedure,
	}

	assertStatus := func(
		t *testing.T,
		service string,
		expect Status,
	) {
		t.Helper()
		var res healthv1.HealthCheckResponse
		err := client.CallUnary(
			t.Context(),
			checkSpec,
			healthv1.HealthCheckRequest_builder{Service: service}.Build(),
			&res,
		)
		if err != nil {
			t.Fatal(err.Error())
		}
		if Status(res.GetStatus()) != expect { //nolint:gosec // Conversion is safe here
			t.Fatalf("got status %v, expected %v", res.GetStatus(), expect)
		}
	}
	assertUnknown := func(
		t *testing.T,
		service string,
	) {
		t.Helper()
		var res healthv1.HealthCheckResponse
		err := client.CallUnary(
			t.Context(),
			checkSpec,
			healthv1.HealthCheckRequest_builder{Service: service}.Build(),
			&res,
		)
		if err == nil {
			t.Fatalf("expected error checking unknown service %q", service)
		}
		var connectErr *connect.Error
		if ok := errors.As(err, &connectErr); !ok {
			t.Fatalf("got %v (%T), expected a *connect.Error", err, err)
		}
		if code := connectErr.Code(); code != connect.CodeNotFound {
			t.Fatalf("check %q: got code %v, expected CodeNotFound", service, code)
		}
	}

	assertStatus(t, "" /* process */, StatusServing)
	checker.SetStatus("", StatusNotServing)
	assertStatus(t, "", StatusNotServing)
	checker.SetStatus("", StatusServing)
	assertStatus(t, "", StatusServing)

	assertStatus(t, userFQN, StatusServing)
	checker.SetStatus(userFQN, StatusNotServing)
	assertStatus(t, userFQN, StatusNotServing)

	assertUnknown(t, unknown)
	checker.SetStatus(unknown, StatusServing)
	assertStatus(t, unknown, StatusServing)
}

func TestHealth_Watch(t *testing.T) {
	const (
		userFQN    = "acme.user.v1.UserService"
		pingFQN    = "acme.ping.v1.PingService"
		unknownFQN = "acme.unknown.v1.UnknownService"
	)
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		checker := NewStaticChecker(userFQN, pingFQN)
		connectServer := connect.NewServer()
		Register(connectServer, checker)
		client := NewClient(connect.NewClient(connectinprocess.New(connectServer)))

		receiveStatus := func(t *testing.T, events <-chan WatchEvent, expect Status) {
			t.Helper()
			event, ok := <-events
			if !ok {
				t.Fatal("channel closed unexpectedly")
			}
			if event.Err != nil {
				t.Fatalf("unexpected error: %v", event.Err)
			}
			if event.Response.Status != expect {
				t.Fatalf("got status %v, expected %v", event.Response.Status, expect)
			}
		}
		assertQuiet := func(t *testing.T, streams ...<-chan WatchEvent) {
			t.Helper()
			synctest.Wait()
			for _, events := range streams {
				select {
				case event := <-events:
					t.Fatalf("unexpected event: %+v", event)
				default:
				}
			}
		}

		// synctest.Test fails if a stream outlives the canceled context.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		user := client.Watch(ctx, &CheckRequest{Service: userFQN})
		ping := client.Watch(ctx, &CheckRequest{Service: pingFQN})
		process := client.Watch(ctx, &CheckRequest{})
		unknown := client.Watch(ctx, &CheckRequest{Service: unknownFQN})

		// Each stream receives its current status immediately. The
		// process-wide stream reports StatusServing without a registered
		// status, and an unknown service reports StatusServiceUnknown.
		receiveStatus(t, user, StatusServing)
		receiveStatus(t, ping, StatusServing)
		receiveStatus(t, process, StatusServing)
		receiveStatus(t, unknown, StatusServiceUnknown)

		// Changing a service sends nothing to other services' streams.
		checker.SetStatus(userFQN, StatusNotServing)
		receiveStatus(t, user, StatusNotServing)
		assertQuiet(t, ping, process, unknown)

		checker.SetStatus("", StatusNotServing)
		receiveStatus(t, process, StatusNotServing)
		assertQuiet(t, user, ping, unknown)

		// An unknown service's stream reports its status once it becomes
		// known.
		checker.SetStatus(unknownFQN, StatusServing)
		receiveStatus(t, unknown, StatusServing)

		// Setting an unchanged status sends nothing.
		checker.SetStatus(userFQN, StatusNotServing)
		assertQuiet(t, user)
	})
}

func TestWatchUnimplemented(t *testing.T) {
	t.Parallel()
	checker := checkerFunc(func(_ context.Context, _ *CheckRequest) (*CheckResponse, error) {
		return &CheckResponse{Status: StatusServing}, nil
	})
	mux := http.NewServeMux()
	connectServer := connect.NewServer()
	Register(connectServer, checker)
	connecthttp.Mount(mux, connectServer)
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	transport := connecthttp.NewTransport(
		server.Client(),
		server.URL,
		connecthttp.WithGRPC(),
	)
	client := connect.NewClient(transport)
	watchSpec := connect.Spec{
		StreamType: connect.StreamTypeServer,
		Procedure:  watchProcedure,
	}

	stream, err := client.CallServerStream(
		t.Context(),
		watchSpec,
		healthv1.HealthCheckRequest_builder{Service: "anything"}.Build(),
	)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer stream.Close()
	var res healthv1.HealthCheckResponse
	err = stream.Receive(&res)
	if err == nil {
		t.Fatalf("got message from Watch")
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("expected error from stream, got EOF")
	}
	var connectErr *connect.Error
	if ok := errors.As(err, &connectErr); !ok {
		t.Fatalf("got %v (%T), expected a *connect.Error", err, err)
	}
	if code := connectErr.Code(); code != connect.CodeUnimplemented {
		t.Fatalf("got code %v, expected CodeUnimplemented", code)
	}
}

// checkerFunc is a Checker that does not implement Watcher.
type checkerFunc func(context.Context, *CheckRequest) (*CheckResponse, error)

func (f checkerFunc) Check(ctx context.Context, req *CheckRequest) (*CheckResponse, error) {
	return f(ctx, req)
}

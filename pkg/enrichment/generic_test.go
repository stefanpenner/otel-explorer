package enrichment

import (
	"strings"
	"testing"
)

func TestGenericEnricher_HTTP(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"http.request.method":       "GET",
		"http.route":                "/api/users",
		"http.response.status_code": "200",
		"server.address":            "api.example.com",
		"server.port":               "8080",
	}
	h := e.Enrich("GET /api/users", attrs, false)

	if h.Category != "http" {
		t.Errorf("expected category 'http', got %q", h.Category)
	}
	if h.Detail == "" {
		t.Error("expected non-empty Detail for HTTP span")
	}
	if h.Icon != "⇄ " {
		t.Errorf("expected HTTP icon, got %q", h.Icon)
	}
}

func TestGenericEnricher_HTTPError(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"http.request.method":       "POST",
		"http.response.status_code": "503",
	}
	h := e.Enrich("POST /api", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected outcome 'failure' for 503, got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected color 'red', got %q", h.Color)
	}
}

func TestGenericEnricher_Database(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"db.system":    "postgresql",
		"db.statement": "SELECT * FROM users WHERE id = $1",
	}
	h := e.Enrich("SELECT users", attrs, false)

	if h.Category != "database" {
		t.Errorf("expected category 'database', got %q", h.Category)
	}
	if h.Detail == "" {
		t.Error("expected non-empty Detail for DB span")
	}
}

func TestGenericEnricher_DatabaseStableSemconv(t *testing.T) {
	e := &GenericEnricher{}

	// Current stable v1.30+ attribute names — must be recognized as a database.
	attrs := map[string]string{
		"db.system.name": "postgresql",
		"db.query.text":  "SELECT * FROM orders WHERE id = $1",
		"db.namespace":   "shop",
	}
	h := e.Enrich("SELECT orders", attrs, false)

	if h.Category != "database" {
		t.Errorf("expected category 'database', got %q", h.Category)
	}
	if h.Detail != "postgresql: SELECT * FROM orders WHERE id = $1" {
		t.Errorf("unexpected Detail: %q", h.Detail)
	}
}

func TestGenericEnricher_DatabaseCollectionOnly(t *testing.T) {
	e := &GenericEnricher{}

	// Operation + collection, new names (e.g. a mongo find).
	attrs := map[string]string{
		"db.system.name":     "mongodb",
		"db.operation.name":  "find",
		"db.collection.name": "users",
	}
	h := e.Enrich("find users", attrs, false)

	if h.Category != "database" {
		t.Errorf("expected category 'database', got %q", h.Category)
	}
	if h.Detail != "mongodb: find users" {
		t.Errorf("expected 'mongodb: find users', got %q", h.Detail)
	}
}

func TestGenericEnricher_RPC(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"rpc.system":  "grpc",
		"rpc.service": "UserService",
		"rpc.method":  "GetUser",
	}
	h := e.Enrich("grpc.UserService/GetUser", attrs, false)

	if h.Category != "rpc" {
		t.Errorf("expected category 'rpc', got %q", h.Category)
	}
	if h.Detail != "grpc UserService/GetUser" {
		t.Errorf("expected 'grpc UserService/GetUser', got %q", h.Detail)
	}
}

func TestGenericEnricher_GRPCStatusError(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"rpc.system":           "grpc",
		"rpc.service":          "Cart",
		"rpc.method":           "Get",
		"rpc.grpc.status_code": "5",
	}
	h := e.Enrich("grpc.Cart/Get", attrs, false)

	if h.Outcome != "failure" {
		t.Errorf("expected failure for non-zero gRPC status, got %q", h.Outcome)
	}
	if h.Color != "red" {
		t.Errorf("expected red, got %q", h.Color)
	}
	if !strings.Contains(h.Detail, "NOT_FOUND") {
		t.Errorf("expected NOT_FOUND in detail, got %q", h.Detail)
	}
}

func TestGenericEnricher_GRPCStatusOK(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"rpc.system":           "grpc",
		"rpc.service":          "Cart",
		"rpc.method":           "Get",
		"rpc.grpc.status_code": "0",
	}
	h := e.Enrich("grpc.Cart/Get", attrs, false)

	if h.Outcome != "success" {
		t.Errorf("expected success for status 0, got %q", h.Outcome)
	}
	if strings.Contains(h.Detail, "[") {
		t.Errorf("expected no status suffix for OK, got %q", h.Detail)
	}
}

func TestGenericEnricher_Messaging(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"messaging.system":           "kafka",
		"messaging.destination.name": "orders",
		"messaging.operation":        "publish",
	}
	h := e.Enrich("publish orders", attrs, false)

	if h.Category != "messaging" {
		t.Errorf("expected category 'messaging', got %q", h.Category)
	}
	if h.Detail == "" {
		t.Error("expected non-empty Detail for messaging span")
	}
}

func TestGenericEnricher_MessagingStableOperation(t *testing.T) {
	e := &GenericEnricher{}

	// Stable messaging.operation.name should be picked up.
	attrs := map[string]string{
		"messaging.system":           "kafka",
		"messaging.destination.name": "orders",
		"messaging.operation.name":   "send",
	}
	h := e.Enrich("send orders", attrs, false)

	if h.Category != "messaging" {
		t.Errorf("expected category 'messaging', got %q", h.Category)
	}
	if h.Detail != "kafka orders (send)" {
		t.Errorf("expected 'kafka orders (send)', got %q", h.Detail)
	}
}

func TestGenericEnricher_GraphQL(t *testing.T) {
	e := &GenericEnricher{}

	// GraphQL spans typically also carry HTTP attributes (POST /graphql);
	// the GraphQL convention must win so the operation is what's shown.
	attrs := map[string]string{
		"http.request.method":    "POST",
		"http.route":             "/graphql",
		"graphql.operation.type": "query",
		"graphql.operation.name": "findBookById",
	}
	h := e.Enrich("query findBookById", attrs, false)

	if h.Category != "graphql" {
		t.Errorf("expected category 'graphql', got %q", h.Category)
	}
	if h.Detail != "query findBookById" {
		t.Errorf("expected Detail 'query findBookById', got %q", h.Detail)
	}
}

func TestGenericEnricher_GraphQLTypeOnly(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"graphql.operation.type": "mutation",
	}
	h := e.Enrich("mutation", attrs, false)

	if h.Category != "graphql" {
		t.Errorf("expected category 'graphql', got %q", h.Category)
	}
	if h.Detail != "mutation" {
		t.Errorf("expected Detail 'mutation', got %q", h.Detail)
	}
}

func TestGenericEnricher_FaaS(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"faas.trigger": "http",
		"faas.name":    "my-function",
	}
	h := e.Enrich("my-function", attrs, false)

	if h.Category != "faas" {
		t.Errorf("expected category 'faas', got %q", h.Category)
	}
}

func TestGenericEnricher_CodeOrigin(t *testing.T) {
	e := &GenericEnricher{}

	// An internal span with no semconv category but code location should show
	// where it originates.
	attrs := map[string]string{
		"code.function.name": "checkout.ProcessOrder",
		"code.file.path":     "/app/internal/checkout/order.go",
		"code.line.number":   "142",
	}
	h := e.Enrich("process-order", attrs, false)

	if h.Category != "operation" {
		t.Errorf("expected category 'operation', got %q", h.Category)
	}
	if h.Detail != "checkout.ProcessOrder (order.go:142)" {
		t.Errorf("unexpected Detail: %q", h.Detail)
	}
}

func TestGenericEnricher_CodeOriginLegacyNames(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"code.function": "handler.Serve",
		"code.filepath": "handler.go",
	}
	h := e.Enrich("serve", attrs, false)

	if h.Detail != "handler.Serve (handler.go)" {
		t.Errorf("unexpected Detail: %q", h.Detail)
	}
}

func TestGenericEnricher_CodeOriginDoesNotOverrideSemconv(t *testing.T) {
	e := &GenericEnricher{}

	// When a more specific convention already set Detail, code.* must not
	// override it.
	attrs := map[string]string{
		"http.request.method": "GET",
		"http.route":          "/x",
		"code.function.name":  "handler.Serve",
	}
	h := e.Enrich("GET /x", attrs, false)

	if h.Category != "http" {
		t.Errorf("expected category 'http', got %q", h.Category)
	}
	if strings.Contains(h.Detail, "handler.Serve") {
		t.Errorf("code origin should not override HTTP detail, got %q", h.Detail)
	}
}

func TestGenericEnricher_PeerService(t *testing.T) {
	e := &GenericEnricher{}

	// An RPC client span toward a logical downstream service.
	attrs := map[string]string{
		"rpc.system":        "grpc",
		"rpc.service":       "Cart",
		"rpc.method":        "Get",
		"service.peer.name": "payments",
	}
	h := e.Enrich("grpc.Cart/Get", attrs, false)

	if h.Detail != "grpc Cart/Get → payments" {
		t.Errorf("expected peer service appended, got %q", h.Detail)
	}
}

func TestGenericEnricher_PeerServiceLegacyAndNoDoubleArrow(t *testing.T) {
	e := &GenericEnricher{}

	// HTTP detail already contains a "→ host"; the legacy peer.service must NOT
	// add a second arrow.
	attrs := map[string]string{
		"http.request.method": "GET",
		"http.route":          "/x",
		"server.address":      "api",
		"server.port":         "443",
		"peer.service":        "api-svc",
	}
	h := e.Enrich("GET /x", attrs, false)

	if strings.Count(h.Detail, "→") != 1 {
		t.Errorf("expected exactly one arrow, got %q", h.Detail)
	}
}

func TestGenericEnricher_ServiceContext(t *testing.T) {
	e := &GenericEnricher{}

	attrs := map[string]string{
		"service.name":           "checkout",
		"deployment.environment": "production",
	}
	h := e.Enrich("some-op", attrs, false)

	if h.ServiceName != "checkout" {
		t.Errorf("expected ServiceName 'checkout', got %q", h.ServiceName)
	}
	if h.Environment != "production" {
		t.Errorf("expected Environment 'production', got %q", h.Environment)
	}
}

func TestGenericEnricher_Marker(t *testing.T) {
	e := &GenericEnricher{}

	h := e.Enrich("event", map[string]string{}, true)

	if !h.IsMarker {
		t.Error("expected IsMarker=true for zero-duration span")
	}
	if h.Category != "marker" {
		t.Errorf("expected category 'marker', got %q", h.Category)
	}
}

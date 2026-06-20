package enrichment

import (
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

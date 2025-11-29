package converter

import (
	"fmt"

	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// "localhost:50051"
func NewConnection(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %v", err)
	}

	return conn, nil
}

func NewClient(conn *grpc.ClientConn) converterpb.ConverterServiceClient {
	client := converterpb.NewConverterServiceClient(conn)

	return client
}

package service

import (
	"context"
	"log/slog"
	"time"

	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"
)

type Converter interface {
	Convert(ctx context.Context, inputPath string, suggestedName string) (string, error)
}

type ConverterService struct {
	converter Converter
	converterpb.UnimplementedConverterServiceServer
}

func NewConverterService(converter Converter) *ConverterService {
	return &ConverterService{converter: converter}
}

func (s *ConverterService) Convert(ctx context.Context, req *converterpb.ConvertRequest) (*converterpb.ConvertResponse, error) {
	convCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pdfName, err := s.converter.Convert(convCtx, req.GetInputPath(), req.GetSuggestedName())
	if err != nil {
		slog.Error("convert failed",
			slog.String("input_path", req.GetInputPath()),
			slog.String("suggested_name", req.GetSuggestedName()),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	slog.Info("convert success",
		slog.String("pdf_name", pdfName),
		slog.String("input_path", req.GetInputPath()),
	)

	return &converterpb.ConvertResponse{
		PdfName: pdfName,
	}, nil
}

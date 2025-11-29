package transport

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/you-humble/dwgtopdf/api/internal/domain"

	"github.com/google/uuid"
)

type Usecase interface {
	Convert(ctx context.Context, file io.Reader, filename, idempotencyKey string, size int64) (string, error)
	GetStatus(ctx context.Context, taskID string) (domain.StatusResponse, error)
	GetResultFile(ctx context.Context, taskID string) (domain.DownloadResult, error)
}

type handler struct {
	maxUploadBytesMb int64
	usecase          Usecase
}

func NewHandler(maxUploadBytesMb int64, uc Usecase) *handler {
	return &handler{
		maxUploadBytesMb: maxUploadBytesMb << 20,
		usecase:          uc,
	}
}

func (h *handler) convert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "")
		return
	}

	requestID := uuid.NewString()
	logger := slog.With(
		slog.String("request_id", requestID),
		slog.String("handler", "convert"),
		slog.String("remote_addr", r.RemoteAddr),
	)

	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, h.maxUploadBytesMb)

	if err := r.ParseMultipartForm(h.maxUploadBytesMb); err != nil {
		logger.Error("ParseMultipartForm", slog.String("error", err.Error()))
		writeError(w, http.StatusBadRequest, "unable to parse multipart form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		logger.Warn("missing file field")
		writeError(w, http.StatusBadRequest, "field `file` is required")
		return
	}
	defer file.Close()

	logger = logger.With(slog.String("file_name", header.Filename))

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey != "" {
		logger = logger.With(slog.String("idempotency_key", idempotencyKey))
	}

	taskID, err := h.usecase.Convert(
		r.Context(),
		file,
		header.Filename,
		idempotencyKey,
		header.Size,
	)
	if err != nil {
		logger.Error("Convert usecase", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "cannot create conversion task")
		return
	}

	writeJSON(w, http.StatusAccepted, domain.ConvertResponse{ID: taskID})
}

func (h *handler) result(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "")
		return
	}

	requestID := uuid.NewString()
	logger := slog.With(
		slog.String("request_id", requestID),
		slog.String("handler", "result"),
		slog.String("remote_addr", r.RemoteAddr),
	)

	taskID := strings.TrimPrefix(r.URL.Path, "/result/")
	if taskID == "" {
		logger.Error("missing ID")
		writeError(w, http.StatusBadRequest, "missing ID")
		return
	}

	resp, err := h.usecase.GetStatus(r.Context(), taskID)
	if err != nil {
		if err == domain.ErrTaskNotFound {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		logger.Error("GetStatus", slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "")
		return
	}

	switch resp.Status {
	case domain.StatusDone:
		writeJSON(w, http.StatusOK, resp)
	case domain.StatusFailed:
		writeJSON(w, http.StatusInternalServerError, resp)
	default:
		writeJSON(w, http.StatusAccepted, resp)
	}
}

func (h *handler) download(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "")
		return
	}

	requestID := uuid.NewString()
	logger := slog.With(
		slog.String("request_id", requestID),
		slog.String("handler", "result"),
		slog.String("remote_addr", r.RemoteAddr),
	)

	taskID := strings.TrimPrefix(r.URL.Path, "/download/")
	if taskID == "" {
		logger.Error("missing ID")
		writeError(w, http.StatusBadRequest, "missing ID")
		return
	}

	result, err := h.usecase.GetResultFile(r.Context(), taskID)
	if err != nil {
		switch err {
		case domain.ErrTaskNotFound:
			writeError(w, http.StatusNotFound, "task not found")
		case domain.ErrTaskFailed:
			writeJSON(w, http.StatusConflict, domain.StatusResponse{
				ID:     taskID,
				Status: domain.StatusFailed,
				Error:  "task failed",
			})
		case domain.ErrTaskNotReady:
			writeJSON(w, http.StatusTooEarly, domain.StatusResponse{
				ID:     taskID,
				Status: domain.StatusProcessing,
				Error:  "result is not ready yet",
			})
		default:
			logger.Error("GetResultFile", slog.String("error", err.Error()))
			writeError(w, http.StatusInternalServerError, "cannot get result file")
		}
		return
	}
	defer result.Content.Close()

	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition",
		`attachment; filename="`+result.FileName+`"`)

	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, result.Content); err != nil {
		logger.Error("download: send file",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	if message == "" {
		message = http.StatusText(status)
	}
	resp := domain.ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	}
	writeJSON(w, status, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writeJSON", slog.String("error", err.Error()))
	}
}

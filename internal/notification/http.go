package notification

import (
	"net/http"

	"rt-ed-streaming/internal/httpserver"
	"rt-ed-streaming/internal/notification/store"
)

// Register exposes GET /notifications?customer_id=... for demos and tests.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /notifications", func(w http.ResponseWriter, r *http.Request) {
		customerID := r.URL.Query().Get("customer_id")
		if customerID == "" {
			httpserver.WriteError(w, http.StatusBadRequest, "customer_id is required")
			return
		}
		list, err := h.List(r.Context(), customerID, 50)
		if err != nil {
			h.log.Error("list notifications", "error", err)
			httpserver.WriteError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		if list == nil {
			list = []store.Notification{} // encode as [], not null
		}
		httpserver.WriteJSON(w, http.StatusOK, list)
	})
}

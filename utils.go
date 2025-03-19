package main

type httpError struct {
    Status int
    Reason error
}

func (r *httpError) Error() string {
    return r.Reason.Error()
}

func (r *httpError) Unwrap() error {
    return r.Reason
}

func newHttpError(status int, reason error) *httpError {
    return &httpError{ Status: status, Reason: reason }
}

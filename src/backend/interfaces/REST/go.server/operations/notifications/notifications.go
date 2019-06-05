/*
 * // Copyleft (ɔ) 2018 The Caliopen contributors.
 * // Use of this source code is governed by a GNU AFFERO GENERAL PUBLIC
 * // license (AGPL) that can be found in the LICENSE file.
 */

package notifications

import (
	"bytes"
	. "github.com/CaliOpen/Caliopen/src/backend/defs/go-objects"
	"github.com/CaliOpen/Caliopen/src/backend/interfaces/REST/go.server/middlewares"
	"github.com/CaliOpen/Caliopen/src/backend/interfaces/REST/go.server/operations"
	"github.com/CaliOpen/Caliopen/src/backend/main/go.main"
	"github.com/gin-gonic/gin"
	swgErr "github.com/go-openapi/errors"
	"github.com/satori/go.uuid"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// GetPendingNotif handles GET /notifications
// two optional params may be in query : `to` and `from` to narrow the notifications list to either :
// - a time range
// - a uuid range
func GetPendingNotif(ctx *gin.Context) {
	userId := ctx.MustGet("user_id").(string)
	to_param := ctx.Query("to")
	from_param := ctx.Query("from")

	var to_kind, from_kind string
	uuidv1Regex := regexp.MustCompile(`[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-1[0-9a-fA-F]{3}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}`)
	rfc3339Regex := regexp.MustCompile(`(?i)^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)([01][0-9]|2[0-3]):([0-5][0-9]))$`)

	// validate from param
	if from_param != "" {
		if uuidv1Regex.MatchString(from_param) {
			from_kind = "id"
		} else if rfc3339Regex.MatchString(from_param) {
			from_kind = "time"
		}
		if from_kind == "" {
			e := swgErr.New(http.StatusUnprocessableEntity, "invalid from param")
			http_middleware.ServeError(ctx.Writer, ctx.Request, e)
			ctx.Abort()
			return
		}
	}

	// validate to param
	if to_param != "" {
		if uuidv1Regex.MatchString(to_param) {
			to_kind = "id"
		} else if rfc3339Regex.MatchString(to_param) {
			to_kind = "time"
		}
		if to_kind == "" {
			e := swgErr.New(http.StatusUnprocessableEntity, "invalid to param")
			http_middleware.ServeError(ctx.Writer, ctx.Request, e)
			ctx.Abort()
			return
		}
	}

	// validate params consistency
	if (from_param != "" && to_param != "") && (from_kind != to_kind) {
		e := swgErr.New(http.StatusUnprocessableEntity, "params are mix of time and uuid")
		http_middleware.ServeError(ctx.Writer, ctx.Request, e)
		ctx.Abort()
		return
	}

	var notifs []Notification
	var err CaliopenError
	// call relevant API
	if from_kind == "time" || to_kind == "time" || (from_param == "" && to_param == "") {
		from, to := time.Time{}, time.Time{}
		if to_param != "" {
			t, err := time.Parse(time.RFC3339, to_param)
			if err != nil {
				e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
				http_middleware.ServeError(ctx.Writer, ctx.Request, e)
				ctx.Abort()
				return
			}
			to = t
		}
		if from_param != "" {
			f, err := time.Parse(time.RFC3339, from_param)
			if err != nil {
				e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
				http_middleware.ServeError(ctx.Writer, ctx.Request, e)
				ctx.Abort()
				return
			}
			from = f
		}

		notifs, err = caliopen.Facilities.Notifiers.NotificationsByTime(userId, from, to)
		if err != nil && err.Cause().Error() != "notifications not found" {
			returnedErr := new(swgErr.CompositeError)
			returnedErr = swgErr.CompositeValidationError(err, err.Cause())
			http_middleware.ServeError(ctx.Writer, ctx.Request, returnedErr)
			ctx.Abort()
		}
	}
	if from_kind == "id" || to_kind == "id" {
		var from, to string
		if to_param != "" {
			t, err := uuid.FromString(to_param)
			if err != nil {
				e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
				http_middleware.ServeError(ctx.Writer, ctx.Request, e)
				ctx.Abort()
				return
			}
			to = t.String()
		}
		if from_param != "" {
			f, err := uuid.FromString(from_param)
			if err != nil {
				e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
				http_middleware.ServeError(ctx.Writer, ctx.Request, e)
				ctx.Abort()
				return
			}
			from = f.String()
		}

		notifs, err = caliopen.Facilities.Notifiers.NotificationsByID(userId, from, to)
		if err != nil && err.Cause().Error() != "notifications not found" {
			returnedErr := new(swgErr.CompositeError)
			returnedErr = swgErr.CompositeValidationError(err, err.Cause())
			http_middleware.ServeError(ctx.Writer, ctx.Request, returnedErr)
			ctx.Abort()
		}
	}

	var respBuf bytes.Buffer
	respBuf.WriteString("{\"total\": " + strconv.Itoa(len(notifs)) + ",")
	respBuf.WriteString(("\"notifications\":["))
	first := true
	for _, notif := range notifs {
		json_notif, err := notif.MarshalFrontEnd()
		if err == nil {
			if first {
				first = false
			} else {
				respBuf.WriteByte(',')
			}
			respBuf.Write(json_notif)
		}
	}
	respBuf.WriteString("]}")
	ctx.Data(http.StatusOK, "application/json; charset=utf-8", respBuf.Bytes())
}

// DeleteNotifications handles DELETE /notifications
func DeleteNotifications(ctx *gin.Context) {

	userId := ctx.MustGet("user_id").(string)

	until := time.Time{}

	until_param := ctx.Query("until")
	if until_param != "" {
		u, err := time.Parse(time.RFC3339, until_param)
		if err != nil {
			e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
			http_middleware.ServeError(ctx.Writer, ctx.Request, e)
			ctx.Abort()
			return
		}
		until = u
	}

	err := caliopen.Facilities.Notifiers.DeleteNotifications(userId, until)
	if err != nil {
		returnedErr := new(swgErr.CompositeError)
		if err.Code() == DbCaliopenErr && err.Cause().Error() == "not found" {
			ctx.Status(http.StatusNoContent)
			return
		} else {
			returnedErr = swgErr.CompositeValidationError(err, err.Cause())
		}
		http_middleware.ServeError(ctx.Writer, ctx.Request, returnedErr)
		ctx.Abort()
		return
	}
	ctx.Status(http.StatusNoContent)
}

// GetNotification handles GET /notifications/:notification_id
func GetNotification(ctx *gin.Context) {
	userID, err := operations.NormalizeUUIDstring(ctx.MustGet("user_id").(string))
	notificationID, err := operations.NormalizeUUIDstring(ctx.Param("notification_id"))

	if err != nil {
		e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
		http_middleware.ServeError(ctx.Writer, ctx.Request, e)
		ctx.Abort()
		return
	}

	notif, e := caliopen.Facilities.Notifiers.RetrieveNotification(userID, notificationID)
	if e != nil {
		returnedErr := new(swgErr.CompositeError)
		if e.Code() == DbCaliopenErr && e.Cause().Error() == "not found" {
			returnedErr = swgErr.CompositeValidationError(swgErr.New(http.StatusNotFound, "db returned not found"), e, e.Cause())
		} else {
			returnedErr = swgErr.CompositeValidationError(e, e.Cause())
		}
		http_middleware.ServeError(ctx.Writer, ctx.Request, returnedErr)
		ctx.Abort()
		return
	} else {
		notif_json, err := notif.MarshalFrontEnd()
		if err != nil {
			e := swgErr.New(http.StatusFailedDependency, err.Error())
			http_middleware.ServeError(ctx.Writer, ctx.Request, e)
			ctx.Abort()
		} else {
			ctx.Data(http.StatusOK, "application/json; charset=utf-8", notif_json)
		}
	}
}

// DeleteNotification handles DELETE /notifications/:notification_id
func DeleteNotification(ctx *gin.Context) {
	userID, err := operations.NormalizeUUIDstring(ctx.MustGet("user_id").(string))
	notificationID, err := operations.NormalizeUUIDstring(ctx.Param("notification_id"))

	if err != nil {
		e := swgErr.New(http.StatusUnprocessableEntity, err.Error())
		http_middleware.ServeError(ctx.Writer, ctx.Request, e)
		ctx.Abort()
		return
	}

	e := caliopen.Facilities.Notifiers.DeleteNotification(userID, notificationID)
	if err != nil {
		returnedErr := new(swgErr.CompositeError)
		if e.Code() == DbCaliopenErr && e.Cause().Error() == "not found" {
			returnedErr = swgErr.CompositeValidationError(swgErr.New(http.StatusNotFound, "db returned not found"), e, e.Cause())
		} else {
			returnedErr = swgErr.CompositeValidationError(e, e.Cause())
		}
		http_middleware.ServeError(ctx.Writer, ctx.Request, returnedErr)
		ctx.Abort()
		return
	}
	ctx.Status(http.StatusNoContent)
}

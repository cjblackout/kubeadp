// Copyright Contributors to the KubeOpenCode project

package server

import (
	"context"
	"fmt"
	"net/http"

	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	authmiddleware "github.com/kubeopencode/kubeopencode/internal/server/middleware"
)

// CheckSubjectAccessReview performs a SubjectAccessReview check for the current user.
// Returns nil if allowed, or an appropriate error (or forbidden error).
func CheckSubjectAccessReview(ctx context.Context, clientset kubernetes.Interface, namespace, resource, verb, subresource string) (bool, error) {
	userInfo := authmiddleware.GetUserInfo(ctx)
	if userInfo == nil {
		// If auth is disabled or anonymous allowed, allow
		return true, nil
	}

	sar := &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User:   userInfo.Username,
			UID:    userInfo.UID,
			Groups: userInfo.Groups,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Namespace:   namespace,
				Verb:        verb,
				Group:       "kubeopencode.io",
				Resource:    resource,
				Subresource: subresource,
			},
		},
	}

	res, err := clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to evaluate SubjectAccessReview: %w", err)
	}

	return res.Status.Allowed, nil
}

// RequireSubjectAccess ensures the user has permission to perform verb on resource in namespace.
func RequireSubjectAccess(w http.ResponseWriter, r *http.Request, clientset kubernetes.Interface, namespace, resource, verb, subresource string) bool {
	allowed, err := CheckSubjectAccessReview(r.Context(), clientset, namespace, resource, verb, subresource)
	if err != nil {
		http.Error(w, fmt.Sprintf("Authorization check failed: %v", err), http.StatusInternalServerError)
		return false
	}
	if !allowed {
		http.Error(w, fmt.Sprintf("Forbidden: %s %s/%s in namespace %s is not permitted", verb, resource, subresource, namespace), http.StatusForbidden)
		return false
	}
	return true
}

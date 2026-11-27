// Copyright Contributors to the KubeOpenCode project

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kubeopenv1alpha1 "github.com/kubeopencode/kubeopencode/api/v1alpha1"
	"github.com/kubeopencode/kubeopencode/internal/controller"
)

func init() {
	rootCmd.AddCommand(newAgentCmd())
}

func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Interact with KubeOpenCode agents",
	}
	cmd.AddCommand(newAgentAttachCmd())
	cmd.AddCommand(newAgentSuspendCmd())
	cmd.AddCommand(newAgentResumeCmd())
	cmd.AddCommand(newAgentShareCmd())
	cmd.AddCommand(newAgentUnshareCmd())
	return cmd
}

func newAgentSuspendCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "suspend <agent-name>",
		Short: "Suspend a server-mode agent",
		Long: `Suspend a server-mode agent by scaling its deployment to 0 replicas.

PVCs and Service are retained, so the agent can be resumed without data loss.
Tasks targeting a suspended agent enter Queued phase until the agent is resumed.

Examples:
  kubeoc agent suspend my-agent -n test`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setSuspendState(cmd.Context(), namespace, args[0], true)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "default", "Agent namespace")
	return cmd
}

func newAgentResumeCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "resume <agent-name>",
		Short: "Resume a suspended server-mode agent",
		Long: `Resume a suspended server-mode agent by scaling its deployment back to 1 replica.

Queued tasks will automatically start running once the agent is ready.

Examples:
  kubeoc agent resume my-agent -n test`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return setSuspendState(cmd.Context(), namespace, args[0], false)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "default", "Agent namespace")
	return cmd
}

func setSuspendState(ctx context.Context, namespace, agentName string, suspend bool) error {
	cfg, err := getKubeConfig()
	if err != nil {
		return fmt.Errorf("cannot connect to cluster: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      agentName,
		Namespace: namespace,
	}, &agent); err != nil {
		return fmt.Errorf("agent %q not found in namespace %q: %w", agentName, namespace, err)
	}

	if agent.Spec.Suspend == suspend {
		if suspend {
			fmt.Printf("Agent %s/%s is already suspended\n", namespace, agentName)
		} else {
			fmt.Printf("Agent %s/%s is already running\n", namespace, agentName)
		}
		return nil
	}

	agent.Spec.Suspend = suspend
	if err := k8sClient.Update(ctx, &agent); err != nil {
		action := "suspend"
		if !suspend {
			action = "resume"
		}
		return fmt.Errorf("failed to %s agent %q: %w", action, agentName, err)
	}

	if suspend {
		fmt.Printf("Agent %s/%s suspended\n", namespace, agentName)
	} else {
		fmt.Printf("Agent %s/%s resumed\n", namespace, agentName)
	}
	return nil
}

func newAgentShareCmd() *cobra.Command {
	var (
		namespace  string
		expiresIn  string
		allowedIPs []string
		show       bool
	)

	cmd := &cobra.Command{
		Use:   "share <agent-name>",
		Short: "Enable share link for an agent",
		Long: `Enable a shareable terminal link for an agent.

When enabled, the controller generates a cryptographic token and the server
exposes a standalone terminal page at /s/{token}. Share this URL with users
who need terminal access without Kubernetes credentials.

Examples:
  kubeoc agent share my-agent -n test
  kubeoc agent share my-agent --expires-in 24h
  kubeoc agent share my-agent --allowed-ips 10.0.0.0/8,192.168.1.0/24
  kubeoc agent share my-agent --show`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentShare(cmd.Context(), namespace, args[0], expiresIn, allowedIPs, show)
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "default", "Agent namespace")
	cmd.Flags().StringVar(&expiresIn, "expires-in", "", "Expiry duration (e.g., 1h, 24h, 168h)")
	cmd.Flags().StringSliceVar(&allowedIPs, "allowed-ips", nil, "Comma-separated CIDR ranges for IP allowlist")
	cmd.Flags().BoolVar(&show, "show", false, "Show existing share link info without modifying")
	return cmd
}

func newAgentUnshareCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "unshare <agent-name>",
		Short: "Disable share link for an agent",
		Long: `Disable the shareable terminal link for an agent.

The share token Secret is deleted and the link becomes invalid immediately.

Examples:
  kubeoc agent unshare my-agent -n test`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentUnshare(cmd.Context(), namespace, args[0])
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "default", "Agent namespace")
	return cmd
}

func runAgentShare(ctx context.Context, namespace, agentName, expiresIn string, allowedIPs []string, showOnly bool) error {
	cfg, err := getKubeConfig()
	if err != nil {
		return fmt.Errorf("cannot connect to cluster: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      agentName,
		Namespace: namespace,
	}, &agent); err != nil {
		return fmt.Errorf("agent %q not found in namespace %q: %w", agentName, namespace, err)
	}

	// Show mode: display current share info and exit
	if showOnly {
		return showShareInfo(ctx, k8sClient, &agent)
	}

	// Build share config
	shareConfig := &kubeopenv1alpha1.ShareConfig{
		Enabled:    true,
		AllowedIPs: allowedIPs,
	}

	if expiresIn != "" {
		d, err := time.ParseDuration(expiresIn)
		if err != nil {
			return fmt.Errorf("invalid --expires-in format (expected Go duration like 24h, 168h): %w", err)
		}
		expiresAt := metav1.NewTime(time.Now().Add(d))
		shareConfig.ExpiresAt = &expiresAt
	}

	agent.Spec.Share = shareConfig
	if err := k8sClient.Update(ctx, &agent); err != nil {
		return fmt.Errorf("failed to enable share for agent %q: %w", agentName, err)
	}

	fmt.Printf("Share link enabled for agent %s/%s\n", namespace, agentName)
	if len(allowedIPs) > 0 {
		fmt.Printf("Allowed IPs: %v\n", allowedIPs)
	}
	if expiresIn != "" {
		fmt.Printf("Expires in: %s\n", expiresIn)
	}

	// Wait briefly for controller to create the Secret, then show the token
	fmt.Println("\nWaiting for share token generation...")
	time.Sleep(3 * time.Second)
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      agentName,
		Namespace: namespace,
	}, &agent); err == nil {
		return showShareInfo(ctx, k8sClient, &agent)
	}

	fmt.Println("Share token is being generated. Run 'kubeoc agent share <name> --show' to see the URL.")
	return nil
}

func showShareInfo(ctx context.Context, k8sClient client.Client, agent *kubeopenv1alpha1.Agent) error {
	if agent.Spec.Share == nil || !agent.Spec.Share.Enabled {
		fmt.Printf("Share link is not enabled for agent %s/%s\n", agent.Namespace, agent.Name)
		return nil
	}

	if agent.Status.Share == nil || agent.Status.Share.SecretName == "" {
		fmt.Println("Share token is being generated. Please try again shortly.")
		return nil
	}

	// Read the token from the Secret
	var secret corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      agent.Status.Share.SecretName,
		Namespace: agent.Namespace,
	}, &secret); err != nil {
		return fmt.Errorf("failed to read share secret: %w", err)
	}

	token := string(secret.Data[controller.ShareTokenKey])

	fmt.Printf("Agent:  %s/%s\n", agent.Namespace, agent.Name)
	fmt.Printf("Active: %v\n", agent.Status.Share.Active)
	fmt.Printf("Token:  %s\n", token)
	fmt.Printf("Path:   /s/%s\n", token)
	if agent.Status.Share.URL != "" {
		fmt.Printf("URL:    %s\n", agent.Status.Share.URL)
	} else {
		fmt.Println("\nNote: Full URL requires server.externalURL to be configured in KubeOpenCodeConfig.")
		fmt.Printf("      Construct manually: https://<your-server>/s/%s\n", token)
	}

	return nil
}

func runAgentUnshare(ctx context.Context, namespace, agentName string) error {
	cfg, err := getKubeConfig()
	if err != nil {
		return fmt.Errorf("cannot connect to cluster: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	var agent kubeopenv1alpha1.Agent
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      agentName,
		Namespace: namespace,
	}, &agent); err != nil {
		return fmt.Errorf("agent %q not found in namespace %q: %w", agentName, namespace, err)
	}

	if agent.Spec.Share == nil || !agent.Spec.Share.Enabled {
		fmt.Printf("Share link is already disabled for agent %s/%s\n", namespace, agentName)
		return nil
	}

	agent.Spec.Share = nil
	if err := k8sClient.Update(ctx, &agent); err != nil {
		return fmt.Errorf("failed to disable share for agent %q: %w", agentName, err)
	}

	fmt.Printf("Share link disabled for agent %s/%s\n", namespace, agentName)
	return nil
}

func newAgentAttachCmd() *cobra.Command {
	var namespace string

	cmd := &cobra.Command{
		Use:   "attach <agent-name>",
		Short: "Open an interactive shell in an agent's server pod",
		Long: `Open an interactive shell in the agent's server pod.

The session is a kubectl-exec (SPDY) into the pod that runs the agent's
kubeadp-agent worker, using your kubeconfig credentials for authentication.
No port-forward or local proxy is needed.

Kubeconfig resolution (in priority order):
  1. KUBEOPENCODE_KUBECONFIG environment variable
  2. KUBECONFIG environment variable
  3. Default ~/.kube/config

Examples:
  kubeoc agent attach my-agent -n test
  kubeoc agent attach server-agent -n production`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentAttach(cmd.Context(), namespace, args[0])
		},
	}

	cmd.Flags().StringVarP(&namespace, "namespace", "n", "default", "Agent namespace")

	return cmd
}

// runAgentAttach opens an interactive shell in the agent's server pod using a
// kube-apiserver exec (SPDY) with the user's kubeconfig credentials.
func runAgentAttach(ctx context.Context, namespace, agentName string) error {
	fmt.Println("Connecting to cluster...")

	cfg, err := getKubeConfig()
	if err != nil {
		return fmt.Errorf("cannot connect to cluster: %w", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Look up the Agent's running server pod and the container to exec into.
	fmt.Printf("Looking up agent %s/%s...\n", namespace, agentName)

	ref, err := controller.ResolveAgentServerPod(ctx, k8sClient, namespace, agentName)
	if err != nil {
		return err
	}

	fmt.Printf("Attaching to %s/%s (container %s)\n\n", ref.Pod, namespace, ref.Container)

	// Cancel the session on SIGINT/SIGTERM so the exec stream and the deferred
	// terminal restore below run cleanly instead of leaving the terminal raw.
	sessionCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	stdinFd := int(os.Stdin.Fd())
	isTTY := term.IsTerminal(stdinFd)

	// Switch to raw mode for an interactive (TTY) session so keystrokes reach
	// the remote shell unbuffered. Restore the terminal on exit.
	var state *term.State
	if isTTY {
		state, err = term.MakeRaw(stdinFd)
		if err != nil {
			return fmt.Errorf("failed to switch terminal to raw mode: %w", err)
		}
		defer func() { _ = term.Restore(stdinFd, state) }()
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	execReq := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(ref.Pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: ref.Container,
			Command:   []string{"/bin/bash"},
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       isTTY,
		}, clientgoscheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(cfg, "POST", execReq.URL())
	if err != nil {
		return fmt.Errorf("failed to create exec connection: %w", err)
	}

	// Forward the terminal size and any subsequent window resize events to the
	// remote terminal.
	sizeQueue := newTerminalSizeQueue(stdinFd)
	defer sizeQueue.Close()

	// Start connection heartbeat to prevent standby auto-suspend
	heartbeatCancel := startConnectionHeartbeat(sessionCtx, k8sClient, namespace, agentName)
	defer heartbeatCancel()

	err = executor.StreamWithContext(sessionCtx, remotecommand.StreamOptions{
		Stdin:             os.Stdin,
		Stdout:            os.Stdout,
		Stderr:            os.Stderr,
		Tty:               isTTY,
		TerminalSizeQueue: sizeQueue,
	})

	if err != nil {
		if sessionCtx.Err() != nil {
			return nil
		}
		return fmt.Errorf("shell session failed: %w", err)
	}

	fmt.Println("\nSession ended.")
	return nil
}

// terminalSizeQueue implements remotecommand.TerminalSizeQueue and forwards
// local terminal size changes to the remote exec session.
type terminalSizeQueue struct {
	ch   chan *remotecommand.TerminalSize
	sig  chan os.Signal
	done chan struct{}
}

func newTerminalSizeQueue(fd int) *terminalSizeQueue {
	q := &terminalSizeQueue{
		ch:   make(chan *remotecommand.TerminalSize, 1),
		sig:  make(chan os.Signal, 1),
		done: make(chan struct{}),
	}

	// Send the current size so the remote terminal starts at the right dimensions.
	if width, height, err := term.GetSize(fd); err == nil {
		q.ch <- &remotecommand.TerminalSize{Width: uint16(width), Height: uint16(height)}
	}

	signal.Notify(q.sig, syscall.SIGWINCH)

	go func() {
		for {
			select {
			case <-q.sig:
				width, height, err := term.GetSize(fd)
				if err != nil {
					return
				}
				select {
				case q.ch <- &remotecommand.TerminalSize{Width: uint16(width), Height: uint16(height)}:
				default:
				}
			case <-q.done:
				return
			}
		}
	}()

	return q
}

// Next returns the next terminal size, or nil once the queue is closed.
func (q *terminalSizeQueue) Next() *remotecommand.TerminalSize {
	select {
	case size, ok := <-q.ch:
		if !ok {
			return nil
		}
		return size
	case <-q.done:
		return nil
	}
}

// Close stops the resize listener. Safe to call multiple times.
func (q *terminalSizeQueue) Close() {
	select {
	case <-q.done:
		return
	default:
	}
	signal.Stop(q.sig)
	close(q.done)
}

// startConnectionHeartbeat starts a background goroutine that periodically updates
// the connection heartbeat annotation on the Agent to prevent standby auto-suspend.
// Returns a cancel function that stops the heartbeat.
func startConnectionHeartbeat(ctx context.Context, k8sClient client.Client, namespace, agentName string) context.CancelFunc {
	heartbeatCtx, cancel := context.WithCancel(ctx)

	// Warn once on first error so users know heartbeat is failing (e.g., missing RBAC)
	var warnOnce sync.Once

	go controller.RunConnectionHeartbeat(heartbeatCtx, k8sClient, namespace, agentName, func(err error) {
		warnOnce.Do(func() {
			fmt.Fprintf(os.Stderr, "Warning: connection heartbeat failed: %v\n", err)
		})
	})

	return cancel
}

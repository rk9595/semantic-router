package evaluationplane

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type faultingOrdinaryRunStatusPersistence struct {
	delegate     runStatusPersistence
	mu           sync.Mutex
	writeFailure bool
	syncFailures int
}

func (p *faultingOrdinaryRunStatusPersistence) Write(path string, run Run) error {
	p.mu.Lock()
	fail := run.Status == StatusRunning && p.writeFailure
	if fail {
		p.writeFailure = false
	}
	p.mu.Unlock()
	if !fail {
		return p.delegate.Write(path, run)
	}
	if err := p.delegate.Write(path, run); err != nil {
		return err
	}
	return errors.New("injected visible ordinary run status commit failure")
}

func (p *faultingOrdinaryRunStatusPersistence) SyncDirectory(path, description string) error {
	p.mu.Lock()
	if p.syncFailures > 0 {
		p.syncFailures--
		p.mu.Unlock()
		return errors.New("injected ordinary run status directory sync failure")
	}
	p.mu.Unlock()
	return p.delegate.SyncDirectory(path, description)
}

type faultingOrdinaryRunEventPersistence struct {
	delegate      runEventPersistence
	mu            sync.Mutex
	appendFailure bool
	syncFailures  int
}

func (p *faultingOrdinaryRunEventPersistence) Append(path string, encoded []byte) error {
	p.mu.Lock()
	fail := p.appendFailure
	if fail {
		p.appendFailure = false
	}
	p.mu.Unlock()
	if !fail {
		return p.delegate.Append(path, encoded)
	}
	file, err := openBundleFile(path, os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(encoded)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return errors.New("injected visible ordinary run event commit failure")
}

func (p *faultingOrdinaryRunEventPersistence) Sync(path, description string) error {
	p.mu.Lock()
	if p.syncFailures > 0 {
		p.syncFailures--
		p.mu.Unlock()
		return errors.New("injected ordinary run event sync failure")
	}
	p.mu.Unlock()
	return p.delegate.Sync(path, description)
}

func TestOrdinaryRunVisibleStatusCommitRetriesBeforeLaunch(t *testing.T) {
	process := &controlledProcess{started: make(chan ProcessSpec, 1), release: make(chan struct{})}
	service, _ := newTestService(t, process, 1)
	run, err := service.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	faults := &faultingOrdinaryRunStatusPersistence{
		delegate: service.store.statusPersistence, writeFailure: true, syncFailures: 1,
	}
	service.store.statusPersistence = faults
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err == nil {
		t.Fatal("first StartRun succeeded across uncertain status commit")
	}
	stored, err := service.GetRunAs(SystemActor(), run.ID)
	if err != nil || stored.Status != StatusRunning || stored.StartedAt == nil {
		t.Fatalf("visible launch intent=%+v err=%v, want Running", stored, err)
	}
	if process.calls.Load() != 0 {
		t.Fatalf("uncertain status commit launched %d workers", process.calls.Load())
	}
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err == nil {
		t.Fatal("retry succeeded while status directory sync remained uncertain")
	}
	if process.calls.Load() != 0 {
		t.Fatalf("failed status retry launched %d workers", process.calls.Load())
	}
	started, err := service.StartRunAs(context.Background(), SystemActor(), run.ID)
	if err != nil || started.Status != StatusRunning {
		t.Fatalf("durable retry StartRun=%+v err=%v", started, err)
	}
	requireProcessStarted(t, process)
	if process.calls.Load() != 1 {
		t.Fatalf("durable retry launched %d workers, want one", process.calls.Load())
	}
	close(process.release)
}

func TestPeerOpenDoesNotTerminalizeOrdinaryRunLaunchIntent(t *testing.T) {
	process := &controlledProcess{started: make(chan ProcessSpec, 1), release: make(chan struct{})}
	service, root := newTestService(t, process, 1)
	run, err := service.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	service.store.statusPersistence = &faultingOrdinaryRunStatusPersistence{
		delegate: service.store.statusPersistence, writeFailure: true,
	}
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err == nil {
		t.Fatal("first StartRun succeeded across uncertain status commit")
	}
	peerProcess := &controlledProcess{}
	peer, err := NewService(Options{
		DataDir: root, PythonPath: "python3", ConfigPath: filepath.Join(root, "config.yaml"),
		RouterAPIURL: "http://router.invalid", EnvoyURL: "http://envoy.invalid",
		CodeRevision: testSourceRevision, MaxConcurrent: 1, Process: peerProcess,
	})
	if err != nil {
		t.Fatalf("peer NewService: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	visible, err := peer.GetRunAs(SystemActor(), run.ID)
	if err != nil || visible.Status != StatusRunning || visible.CompletedAt != nil {
		t.Fatalf("peer opener changed launch intent=%+v err=%v", visible, err)
	}
	if peerProcess.calls.Load() != 0 {
		t.Fatalf("peer opener launched %d workers", peerProcess.calls.Load())
	}
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err != nil {
		t.Fatalf("owner retry StartRun: %v", err)
	}
	requireProcessStarted(t, process)
	if process.calls.Load() != 1 {
		t.Fatalf("owner retry launched %d workers, want one", process.calls.Load())
	}
	close(process.release)
}

func TestOrdinaryRunVisibleEventCommitRetriesWithoutDuplicate(t *testing.T) {
	process := &controlledProcess{started: make(chan ProcessSpec, 1), release: make(chan struct{})}
	service, _ := newTestService(t, process, 1)
	run, err := service.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	faults := &faultingOrdinaryRunEventPersistence{
		delegate: service.store.eventPersistence, appendFailure: true, syncFailures: 1,
	}
	service.store.eventPersistence = faults
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err == nil {
		t.Fatal("first StartRun succeeded across uncertain start event commit")
	}
	if process.calls.Load() != 0 {
		t.Fatalf("uncertain event commit launched %d workers", process.calls.Load())
	}
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err == nil {
		t.Fatal("retry succeeded while start event sync remained uncertain")
	}
	if _, err := service.StartRunAs(context.Background(), SystemActor(), run.ID); err != nil {
		t.Fatalf("durable retry StartRun: %v", err)
	}
	requireProcessStarted(t, process)
	events, err := service.EventsAfterAs(SystemActor(), run.ID, "")
	if err != nil {
		t.Fatalf("EventsAfter: %v", err)
	}
	startEvents := 0
	for _, event := range events {
		if event.ID == "2" && event.Message == "Evaluation worker starting" {
			startEvents++
		}
	}
	if startEvents != 1 || process.calls.Load() != 1 {
		t.Fatalf("start events=%d worker calls=%d, want one each; events=%+v", startEvents, process.calls.Load(), events)
	}
	close(process.release)
}

func TestConcurrentServicesResumeOneOrdinaryRunWorker(t *testing.T) {
	process := &controlledProcess{started: make(chan ProcessSpec, 2), release: make(chan struct{})}
	first, root := newTestService(t, process, 1)
	second, err := NewService(Options{
		DataDir: root, PythonPath: "python3", ConfigPath: filepath.Join(root, "config.yaml"),
		RouterAPIURL: "http://router.invalid", EnvoyURL: "http://envoy.invalid",
		CodeRevision: testSourceRevision, MaxConcurrent: 1, Process: process,
	})
	if err != nil {
		t.Fatalf("NewService peer: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	run, err := first.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	run = stageRunningTestRun(t, first, run)

	results := make(chan error, 2)
	go func() {
		_, startErr := first.StartRunAs(context.Background(), SystemActor(), run.ID)
		results <- startErr
	}()
	go func() {
		_, startErr := second.StartRunAs(context.Background(), SystemActor(), run.ID)
		results <- startErr
	}()
	for index := 0; index < 2; index++ {
		select {
		case startErr := <-results:
			if startErr != nil {
				t.Fatalf("concurrent StartRun: %v", startErr)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent StartRun did not complete")
		}
	}
	requireProcessStarted(t, process)
	if process.calls.Load() != 1 {
		t.Fatalf("cross-service resume launched %d workers, want one", process.calls.Load())
	}
	close(process.release)
}

func TestStartRunHonorsCallerCancellationBeforeCommit(t *testing.T) {
	process := &controlledProcess{started: make(chan ProcessSpec, 1), release: make(chan struct{})}
	service, _ := newTestService(t, process, 1)
	run, err := service.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.StartRunAs(ctx, SystemActor(), run.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("StartRun canceled error=%v, want context.Canceled", err)
	}
	stored, err := service.GetRunAs(SystemActor(), run.ID)
	if err != nil || stored.Status != StatusPending || stored.StartedAt != nil {
		t.Fatalf("canceled start mutated run=%+v err=%v", stored, err)
	}
	if process.calls.Load() != 0 {
		t.Fatalf("canceled start launched %d workers", process.calls.Load())
	}
}

func TestRestartFailsRunningLaunchIntentWithoutReplayingWorker(t *testing.T) {
	process := &controlledProcess{}
	service, root := newTestService(t, process, 1)
	run, err := service.CreateRunAs(context.Background(), SystemActor(), validCreateRequest())
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	run = stageRunningTestRun(t, service, run)
	if _, err := service.store.ensureOrdinaryRunStartEvent(run); err != nil {
		t.Fatalf("persist start event: %v", err)
	}
	if err := service.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	restartedProcess := &controlledProcess{}
	restarted, err := NewService(Options{
		DataDir: root, PythonPath: "python3", ConfigPath: filepath.Join(root, "config.yaml"),
		RouterAPIURL: "http://router.invalid", EnvoyURL: "http://envoy.invalid",
		CodeRevision: testSourceRevision, MaxConcurrent: 1, Process: restartedProcess,
	})
	if err != nil {
		t.Fatalf("restart NewService: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	recovered, err := restarted.GetRunAs(SystemActor(), run.ID)
	if err != nil || recovered.Status != StatusFailed || recovered.CompletedAt == nil {
		t.Fatalf("recovered launch intent=%+v err=%v, want failed", recovered, err)
	}
	if restartedProcess.calls.Load() != 0 {
		t.Fatalf("restart replayed %d ordinary workers", restartedProcess.calls.Load())
	}
}

func requireProcessStarted(t *testing.T, process *controlledProcess) {
	t.Helper()
	select {
	case <-process.started:
	case <-time.After(time.Second):
		t.Fatal("evaluation worker did not start")
	}
}

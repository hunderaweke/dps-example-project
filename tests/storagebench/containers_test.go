package storagebench_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Every store gets the same budget so the comparison is fair on one machine.
const (
	cpuLimit = 2e9 // 2 CPUs
	memLimit = 3 << 30
)

var (
	ybImage    = envOr("SB_YB_IMAGE", "yugabytedb/yugabyte:latest")
	osImage    = envOr("SB_OS_IMAGE", "opensearchproject/opensearch:2.19.3")
	minioImage = envOr("SB_MINIO_IMAGE", "pgsty/minio:latest") // community MinIO fork; upstream images are no longer published
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func limits(mem int64) testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		hc.NanoCPUs = cpuLimit
		hc.Memory = mem
	})
}

type env struct {
	adminDSN, writerDSN string
	admin               *pgxpool.Pool
	s3                  *minio.Client
	s3Endpoint          string
	osURL               string
	yb, os, s3c         *testcontainers.DockerContainer
}

func startEnv(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	e := &env{}

	var wg sync.WaitGroup
	errs := make([]error, 3)
	wg.Go(func() {
		c, err := testcontainers.Run(ctx, ybImage,
			testcontainers.WithCmd("bin/yugabyted", "start", "--background=false"),
			testcontainers.WithExposedPorts("5433/tcp"),
			limits(memLimit),
			testcontainers.WithWaitStrategyAndDeadline(10*time.Minute, wait.ForListeningPort("5433/tcp")))
		e.yb, errs[0] = c, err
	})
	wg.Go(func() {
		c, err := testcontainers.Run(ctx, osImage,
			testcontainers.WithEnv(map[string]string{
				"discovery.type":                    "single-node",
				"DISABLE_SECURITY_PLUGIN":           "true",
				"DISABLE_INSTALL_DEMO_CONFIG":       "true",
				"OPENSEARCH_JAVA_OPTS":              "-Xms1g -Xmx1g",
				"OPENSEARCH_INITIAL_ADMIN_PASSWORD": "Bench-Only-Passw0rd!",
			}),
			testcontainers.WithExposedPorts("9200/tcp"),
			limits(2<<30),
			testcontainers.WithWaitStrategyAndDeadline(10*time.Minute,
				wait.ForHTTP("/_cluster/health").WithPort("9200/tcp").WithStatusCodeMatcher(func(s int) bool { return s == 200 })))
		e.os, errs[1] = c, err
	})
	if ep := os.Getenv("SB_S3_ENDPOINT"); ep == "" {
		wg.Go(func() {
			c, err := testcontainers.Run(ctx, minioImage,
				testcontainers.WithCmd("server", "/data"),
				testcontainers.WithEnv(map[string]string{"MINIO_ROOT_USER": "bench", "MINIO_ROOT_PASSWORD": "bench-secret"}),
				testcontainers.WithExposedPorts("9000/tcp"),
				limits(1<<30),
				testcontainers.WithWaitStrategyAndDeadline(5*time.Minute,
					wait.ForHTTP("/minio/health/live").WithPort("9000/tcp")))
			e.s3c, errs[2] = c, err
		})
	}
	wg.Wait()
	for _, c := range []*testcontainers.DockerContainer{e.yb, e.os, e.s3c} {
		if c != nil {
			testcontainers.CleanupContainer(t, c)
		}
	}
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	ybEP, err := e.yb.PortEndpoint(ctx, "5433/tcp", "")
	must(t, err)
	e.adminDSN = fmt.Sprintf("postgres://yugabyte@%s/yugabyte?sslmode=disable&pool_max_conns=24", ybEP)
	e.writerDSN = fmt.Sprintf("postgres://audit_writer:writer@%s/yugabyte?sslmode=disable&pool_max_conns=32", ybEP)
	e.admin = connectRetry(t, ctx, e.adminDSN)

	osEP, err := e.os.PortEndpoint(ctx, "9200/tcp", "http")
	must(t, err)
	e.osURL = osEP

	access, secret, secure := "bench", "bench-secret", false
	if ep := os.Getenv("SB_S3_ENDPOINT"); ep != "" {
		e.s3Endpoint = ep
		access, secret = os.Getenv("SB_S3_ACCESS_KEY"), os.Getenv("SB_S3_SECRET_KEY")
		secure = os.Getenv("SB_S3_SECURE") == "1"
	} else {
		e.s3Endpoint, err = e.s3c.PortEndpoint(ctx, "9000/tcp", "")
		must(t, err)
	}
	e.s3, err = minio.New(e.s3Endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: secure})
	must(t, err)
	return e
}

// connectRetry waits for YSQL to accept queries; the port opens before the
// tablet servers are ready.
func connectRetry(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	var lastErr error
	for range 120 {
		p, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = p.Ping(ctx); err == nil {
				var one int
				if err = p.QueryRow(ctx, "SELECT 1").Scan(&one); err == nil {
					return p
				}
			}
			p.Close()
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("ysql not ready: %v", lastErr)
	return nil
}

func newBucket(ctx context.Context, c *minio.Client, name string) error {
	return c.MakeBucket(ctx, name, minio.MakeBucketOptions{ObjectLocking: true})
}

// diskBytes returns the bytes under dir inside a container, or -1.
func diskBytes(ctx context.Context, c *testcontainers.DockerContainer, dir string) int64 {
	if c == nil {
		return -1
	}
	code, out, err := c.Exec(ctx, []string{"du", "-sb", dir}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		return -1
	}
	b, _ := io.ReadAll(out)
	f := strings.Fields(string(b))
	for _, s := range f {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
	}
	return -1
}

// ---------------------------------------------------------------- docker stats

type usage struct {
	CPUAvgPct float64 `json:"cpu_avg_pct"`
	CPUMaxPct float64 `json:"cpu_max_pct"`
	MemMaxMiB float64 `json:"mem_max_mib"`
	samples   int
}

type statsSampler struct {
	names map[string]string // container id prefix -> name
	mu    sync.Mutex
	acc   map[string]*usage
	stop  chan struct{}
	done  chan struct{}
}

func sampleStats(e *env) *statsSampler {
	s := &statsSampler{names: map[string]string{}, acc: map[string]*usage{}, stop: make(chan struct{}), done: make(chan struct{})}
	var ids []string
	for name, c := range map[string]*testcontainers.DockerContainer{"yugabytedb": e.yb, "opensearch": e.os, "s3": e.s3c} {
		if c != nil {
			id := c.GetContainerID()[:12]
			s.names[id] = name
			ids = append(ids, id)
		}
	}
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			default:
			}
			args := append([]string{"stats", "--no-stream", "--format", "{{.ID}}|{{.CPUPerc}}|{{.MemUsage}}"}, ids...)
			out, err := exec.Command("docker", args...).Output()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				p := strings.Split(line, "|")
				if len(p) != 3 {
					continue
				}
				name := s.names[p[0][:min(12, len(p[0]))]]
				cpu, _ := strconv.ParseFloat(strings.TrimSuffix(p[1], "%"), 64)
				mem := parseMiB(strings.TrimSpace(strings.Split(p[2], "/")[0]))
				s.mu.Lock()
				u := s.acc[name]
				if u == nil {
					u = &usage{}
					s.acc[name] = u
				}
				u.samples++
				u.CPUAvgPct += (cpu - u.CPUAvgPct) / float64(u.samples)
				u.CPUMaxPct = max(u.CPUMaxPct, cpu)
				u.MemMaxMiB = max(u.MemMaxMiB, mem)
				s.mu.Unlock()
			}
		}
	}()
	return s
}

func (s *statsSampler) finish() map[string]usage {
	close(s.stop)
	<-s.done
	out := map[string]usage{}
	for k, v := range s.acc {
		out[k] = *v
	}
	return out
}

func parseMiB(v string) float64 {
	units := []struct {
		suffix string
		mul    float64
	}{{"GiB", 1024}, {"MiB", 1}, {"KiB", 1.0 / 1024}, {"B", 1.0 / (1024 * 1024)}}
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			n, _ := strconv.ParseFloat(strings.TrimSuffix(v, u.suffix), 64)
			return n * u.mul
		}
	}
	return 0
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

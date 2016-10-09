package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v4"

	"github.com/bcho/qingdao/executor"
	"github.com/bcho/qingdao/job"
	"github.com/bcho/qingdao/queue/priority"
	"golang.org/x/net/context"
)

const (
	JobTypePing = job.JobType("ping")
	JobTypePong = job.JobType("pong")
)

func main() {
	ctx := context.Background()

	addr := os.Getenv("QINGDAO_EXAMPLE_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	queueRedis := redis.NewClient(&redis.Options{Addr: addr})
	queue, err := priority.New(
		priority.WithName("qingdao-example"),
		priority.WithRedis(queueRedis),
		priority.WithPriorityByAvailableAt(),
	)
	if err != nil {
		log.Fatalf("make queue: %+v", err)
	}

	e, err := executor.New(
		executor.WithQueue(queue),
		executor.BeforeStart(func(context.Context, executor.Executor, executor.ExecutorState) error {
			go queue.StartSchedule(ctx)
			return nil
		}),
		executor.AfterStop(func(context.Context, executor.Executor, executor.ExecutorState) error {
			return queue.StopSchedule(ctx)
		}),
	)
	if err != nil {
		log.Fatalf("make executor: %+v", e)
	}

	go e.Start(ctx)
	defer e.Stop(ctx)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for count := 0; ; count++ {
		select {
		case <-ticker.C:
			payload := fmt.Sprintf("ping-%d:%d", count, rand.Intn(5)+1)
			pingJob := job.New()
			pingJob.Type = JobTypePing
			pingJob.Payload = []byte(payload)
			pingJob.AvailableAt = time.Now().Add(time.Duration(rand.Intn(10)) * time.Second)
			e.NewJobChan() <- pingJob
			log.Printf("enqueue new job: %s:%s:%s", pingJob, payload, pingJob.AvailableAt)
		case err := <-e.ErrChan():
			log.Fatalf("execute failed: %+v", err)
		}
	}
}

func executePing(_ context.Context, j *job.Job) (pongJobs []*job.Job, err error) {
	payload := string(j.Payload)

	a := strings.Split(payload, ":")
	str := a[0]
	times, err := strconv.ParseInt(a[1], 10, 32)
	if err != nil {
		return
	}

	for i := 0; i < int(times); i++ {
		pongJob := job.New()
		pongJob.Type = JobTypePong
		pongJob.Payload = []byte(str)
		pongJobs = append(pongJobs, pongJob)
	}

	return pongJobs, nil
}

func executePong(_ context.Context, job *job.Job) ([]*job.Job, error) {
	log.Printf("exeucting pong: %s", string(job.Payload))

	return nil, nil
}

func init() {
	executor.RegisterJobExecutor(JobTypePing, executePing)
	executor.RegisterJobExecutor(JobTypePong, executePong)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const PROCESSING_WORKER_COUNT = 8
const SORTING_WORKER_COUNT = 4
const MAX_PRINT_SUCCESS = 10
const MAX_PRINT_FAILURE = 100
const TOO_OLD = time.Second * 20

type Task struct {
	id             int64
	timeCreatedAt  string
	timeFinishedAt string
	err            error
}

var (
	errMicroOddness = fmt.Errorf("current time in microseconds must be odd")
	errTooOld       = fmt.Errorf("task was in the queue for too long")
)

func createTasks(taskOut chan<- Task, exit <-chan struct{}) {
	var id int64
	for {
		var task Task
		task.id = atomic.AddInt64(&id, 1)
		task.timeCreatedAt = time.Now().Format(time.RFC3339)

		// t := time.Now().Nano() // is too imprecise, causing it to never trigger
		t := time.Now().UnixMicro()
		if t%2 > 0 {
			task.err = errMicroOddness
		}

		select {
		case <-exit:
			close(taskOut)
			return
		case taskOut <- task:
		}
	}
}

func processTask(task Task, taskOut chan<- Task) {
	taskFinishedAt := time.Now()
	task.timeFinishedAt = taskFinishedAt.Format(time.RFC3339Nano)

	taskCreatedAt, _ := time.Parse(time.RFC3339, task.timeCreatedAt)

	tooOld := taskCreatedAt.Before(taskFinishedAt.Add(-TOO_OLD))
	if task.err != nil && tooOld {
		task.err = errTooOld
	}

	taskOut <- task
}

func sortTask(task Task, completedTasks chan<- Task, failedTasks chan<- Task) {
	if task.err == nil {
		completedTasks <- task
	} else {
		failedTasks <- task
	}
}

func pooled(workers int, worker func()) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}
	wg.Wait()
}

func main() {
	log.Print("Starting")
	exit := make(chan struct{})

	newTasks := make(chan Task, 10)
	go createTasks(newTasks, exit)

	processedTasks := make(chan Task, 10)
	go func() {
		pooled(PROCESSING_WORKER_COUNT, func() {
			for {
				task, ok := <-newTasks
				if !ok {
					break
				}
				processTask(task, processedTasks)
			}
		})
		close(processedTasks)
	}()

	doneTasks := make(chan Task)
	failedTasks := make(chan Task)
	go func() {
		pooled(SORTING_WORKER_COUNT, func() {
			for {
				task, ok := <-processedTasks
				if !ok {
					break
				}
				sortTask(task, doneTasks, failedTasks)
			}
		})
		close(doneTasks)
		close(failedTasks)
	}()

	done := make(chan struct{})
	sucesses := make([]Task, 0)
	failures := make([]Task, 0)
	errStats := make(map[error]int64)
	go func() {
		for {
			if doneTasks == nil && failedTasks == nil {
				done <- struct{}{}
				return
			}
			select {
			case doneTask, ok := <-doneTasks:
				if !ok {
					doneTasks = nil
					break
				}
				sucesses = append(sucesses, doneTask)
			case failedTask, ok := <-failedTasks:
				if !ok {
					failedTasks = nil
					break
				}
				failures = append(failures, failedTask)
				errStats[failedTask.err]++
			}
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-sigint

	log.Println("Shutting down")
	exit <- struct{}{}
	<-done

	rate := float64(len(sucesses)) / float64(len(sucesses)+len(failures))
	fmt.Printf("\nCompleted %v tasks (%.1f%% success rate)\n", len(sucesses), rate*100)
	for i, task := range sucesses {
		if i >= MAX_PRINT_SUCCESS {
			fmt.Printf(" ...%v tasks were ommited\n", len(failures)-MAX_PRINT_SUCCESS)
			break
		}
		fmt.Printf(" - #%03v: %v->%v\n", task.id, task.timeCreatedAt, task.timeFinishedAt)
	}

	fmt.Printf("\nFailed tasks (%v):\n", len(failures))
	for i, task := range failures {
		if i >= MAX_PRINT_FAILURE {
			fmt.Printf(" ...%v failed tasks were ommited\n", len(failures)-MAX_PRINT_FAILURE)
			break
		}
		fmt.Printf(" - #%03v: %v @%v->%v\n", task.id, task.err, task.timeCreatedAt, task.timeFinishedAt)
	}

	fmt.Printf("\nFailure breakdown:\n")
	for err, count := range errStats {
		fmt.Printf(" - %5.1f%% - %v failures - %v\n", float64(count)/float64(len(failures))*100, count, err.Error())
	}
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nhooyr.io/websocket"
)

func main() {
	server := flag.String("server", "ws://localhost:8080/api/v1/workers/ws", "server ws url")
	token := flag.String("token", "", "worker token")
	successRate := flag.Float64("success-rate", 1.0, "success rate 0.0-1.0")
	minDelay := flag.Duration("min-delay", 1*time.Second, "min task delay")
	maxDelay := flag.Duration("max-delay", 3*time.Second, "max task delay")
	flag.Parse()

	if *token == "" {
		log.Fatal("--token is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		err := run(ctx, *server, *token, *successRate, *minDelay, *maxDelay)
		if ctx.Err() != nil {
			return
		}
		log.Printf("disconnected: %v, reconnecting in 3s...", err)
		time.Sleep(3 * time.Second)
	}
}

func run(ctx context.Context, serverURL, token string, successRate float64, minDelay, maxDelay time.Duration) error {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+token)

	conn, _, err := websocket.Dial(ctx, serverURL, &websocket.DialOptions{
		HTTPHeader: headers,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")

	log.Printf("connected (token: %s...)", token[:8])

	// heartbeat goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hb := map[string]interface{}{
					"type": "heartbeat",
					"ts":   time.Now().Unix(),
				}
				data, _ := json.Marshal(hb)
				if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var env struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}

		if env.Type != "task" {
			continue
		}

		var taskMsg struct {
			TaskID    string          `json:"task_id"`
			AttemptID string          `json:"attempt_id"`
			TimeoutMs int             `json:"timeout_ms"`
			Payload   json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(data, &taskMsg); err != nil {
			continue
		}

		log.Printf("received task %s (attempt %s)", taskMsg.TaskID, taskMsg.AttemptID)

		// send ACK
		ack := map[string]string{
			"type":       "task_ack",
			"task_id":    taskMsg.TaskID,
			"attempt_id": taskMsg.AttemptID,
		}
		ackData, _ := json.Marshal(ack)
		if err := conn.Write(ctx, websocket.MessageText, ackData); err != nil {
			return fmt.Errorf("write ack: %w", err)
		}
		log.Printf("sent ACK for %s", taskMsg.TaskID)

		// simulate work
		delay := minDelay
		if maxDelay > minDelay {
			delay += time.Duration(rand.Int63n(int64(maxDelay - minDelay)))
		}
		time.Sleep(delay)

		// send result
		var result interface{}
		if rand.Float64() < successRate {
			result = map[string]interface{}{
				"type":        "task_result",
				"task_id":     taskMsg.TaskID,
				"attempt_id":  taskMsg.AttemptID,
				"success":     true,
				"result":      map[string]string{"data": "ok"},
				"duration_ms": int(delay.Milliseconds()),
			}
			log.Printf("task %s succeeded", taskMsg.TaskID)
		} else {
			result = map[string]interface{}{
				"type":          "task_result",
				"task_id":       taskMsg.TaskID,
				"attempt_id":    taskMsg.AttemptID,
				"success":       false,
				"retryable":     true,
				"error_code":    "SIM_FAILURE",
				"error_message": "simulated failure",
				"duration_ms":   int(delay.Milliseconds()),
			}
			log.Printf("task %s failed (retryable)", taskMsg.TaskID)
		}

		resultData, _ := json.Marshal(result)
		if err := conn.Write(ctx, websocket.MessageText, resultData); err != nil {
			return fmt.Errorf("write result: %w", err)
		}
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
	_ = os.Stdout
}

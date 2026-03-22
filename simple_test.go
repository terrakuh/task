package task_test

import (
	"context"
	"fmt"

	"github.com/terrakuh/task"
)

func Example() {
	manager := task.New[int32, int64](context.Background(), 4, 10)

	first, err := manager.Do(func(ctx context.Context, in int32) (out int64, err error) {
		return int64(in) << 2, nil
	}, 4)
	if err != nil {
		panic(err)
	}

	second, err := manager.Do(func(ctx context.Context, in int32) (out int64, err error) {
		data := task.For(ctx).Dependencies[0]
		if data.Full != first {
			panic("this is the same")
		}
		return int64(in) + data.Full.(*task.Handle[int32, int64]).Output, nil
	}, 10, task.WithAfterRun(first))
	if err != nil {
		panic(err)
	}

	<-second.Done
	fmt.Printf("Final result: %d\n", second.Output)

	// Output:
	// Final result: 26
}

package scheduler

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestLua(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	_, err := rdb.Ping(ctx).Result()
	require.NoError(t, err, "bad pong")

	const set = "taskluaset"

	defer func() {
		require.NoError(t, rdb.Del(ctx, set).Err())
	}()

	// this script finds the scores within the specified range
	// and increments those keys by the provided incrby value.
	// this simulates a ranged atomic lock application.
	const atomicRangeLockScript = `
local tasks = redis.call("zrangebyscore", KEYS[1], 0, tonumber(ARGV[1]))

for i, t in pairs(tasks) do
	redis.call("zincrby", KEYS[1], tonumber(ARGV[2]), t)
end

return tasks`

	scriptSHA, err := rdb.ScriptLoad(ctx, atomicRangeLockScript).Result()
	require.NoError(t, err)

	t.Log("atomic range lock script: ", atomicRangeLockScript)
	t.Log("script sha: ", scriptSHA)
	require.NotEmpty(t, scriptSHA)

	// create the set with an entries at time 1, 2, and 3 respectively
	membersAdded, err := rdb.
		ZAdd(ctx, set,
			&redis.Z{
				Score:  1,
				Member: "task1",
			},
			&redis.Z{
				Score:  2,
				Member: "task2",
			},
			&redis.Z{
				Score:  3,
				Member: "task3",
			},
		).
		Result()
	require.NoError(t, err)
	require.Equal(t, int64(3), membersAdded)

	// prints out [{1 task1}, {2 task2}, {3 task3}]
	printSSet(t, ctx, rdb, set)

	/*
		here we are going to find the times in the
		range of [0, 2], so 1 and 2 should match.
		we are also incrementing the key by 10 (seconds).
	*/
	execScript := func(t *testing.T) interface{} {
		t.Helper()
		const (
			maxScore    = "2"
			incrScoreBy = "10"
		)
		res, err := rdb.
			EvalSha(ctx, scriptSHA, []string{set}, maxScore, incrScoreBy).
			Result()
		require.NoError(t, err)
		return res
	}

	// the script should run grab the keys task1 and task2
	// which match the range we specified, task1 & task2
	t.Log("1st scripted results: ", execScript(t))

	// when we print the set now we'll see the values have changed
	// prints out [{3 task1}, {11 task1}, {12 task2}]]
	printSSet(t, ctx, rdb, set)

	/*
		the script should run grab nothing in this case
		since our previous script executed and incremented
		the counts. If we had n number of queries to redis
		using the same script and arguments to the same set
		it would only be possible for 1 query to get those
		results as lua scripts are guaranteed atomic in
		execution.
	*/
	t.Log("2nd scripted results: ", execScript(t))
}

func printSSet(t *testing.T, ctx context.Context, rdb *redis.Client, set string) {
	t.Helper()

	vals, err := rdb.ZRangeByScoreWithScores(ctx, set, &redis.ZRangeBy{
		Min: "-1",
		Max: "10000",
	}).
		Result()
	require.NoError(t, err)
	t.Logf("%s vals and scores: %v", set, vals)
}

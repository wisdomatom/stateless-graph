package embeding

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedEngineTiKV(t *testing.T) {
	tmpDir := fmt.Sprintf("/tmp/dgraph_embed_test_%d", time.Now().UnixNano())
	defer os.RemoveAll(tmpDir)

	conf := DgraphServer{
		EmbedDataDir: tmpDir,
		Embed:        true,
		TiKVAddrs:    []string{"127.0.0.1:2379"},
	}

	ed, err := NewDgraphEmbed(conf)
	require.NoError(t, err)
	defer ed.Close()

	client, err := ed.GetClient()
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Mutate
	mu := &api.Mutation{
		SetJson:   []byte(`{"name":"embedded_test_group"}`),
		CommitNow: true,
	}
	resp, err := client.NewTxn().Mutate(ctx, mu)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Uids)

	var uid string
	for _, u := range resp.Uids {
		uid = u
		break
	}
	t.Logf("Embedded mutation created UID: %s", uid)

	// 2. Query back in-process
	q := fmt.Sprintf(`{ q(func: uid(%s)) { uid name } }`, uid)
	qResp, err := client.NewTxn().Query(ctx, q)
	require.NoError(t, err)
	t.Logf("Embedded query response: %s", string(qResp.Json))
	require.Contains(t, string(qResp.Json), "embedded_test_group")
}
